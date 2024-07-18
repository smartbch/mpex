use crate::exetask::{
    get_change_set_and_check_access_rw, AccInfo, AccessSet, ExeTask, ACC_INFO_LEN, READ_ACC,
    READ_SLOT, WRITE_ACC, WRITE_SLOT,
};
use crate::statecache::{CodeMap, StateCache};
use crate::utils::{addr_to_u256, decode_account_info, is_empty_code_hash};
use anyhow::{anyhow, Error, Result};
use bincode;
use mpads::changeset::ChangeSet;
use mpads::def::{DEFAULT_ENTRY_SIZE, IN_BLOCK_IDX_MASK};
use mpads::entry::EntryBz;
use mpads::tasksmanager::TasksManager;
use mpads::utils::hasher;
use mpads::ADS;
use revm::db::{Database, EmptyDB};
use revm::handler::register::{EvmHandler, HandleRegisterBox};
use revm::handler::{mainnet, register};
use revm::interpreter::gas::ACCESS_LIST_STORAGE_KEY;
use revm::precompile::primitives::{
    AccountInfo, BlockEnv, Bytecode, CfgEnv, Env, FixedBytes, U256,
};
use revm::precompile::Address;
use revm::primitives::{
    Account, Bytes, EVMError, ExecutionResult, HandlerCfg, InvalidTransaction, LatestSpec, Output,
    ResultAndState, SpecId, SuccessReason, TransactTo, TxEnv, KECCAK_EMPTY,
};
use revm::{Evm, Handler};
use std::cell::RefCell;
use std::collections::HashMap;
use std::mem;
use std::rc::Rc;
use std::sync::{Arc, RwLock};

// to support the execution of one transaction
struct TxContext<'a, T: ADS> {
    // cache the original status of the accounts touched by this tx
    // judge whether the account is written
    orig_acc_map: &'a mut HashMap<Address, AccInfo>,
    access_set: &'a AccessSet,
    blk_ctx: &'a BlockContext<T>,
}

// Evm use TxContext as a Database
// It uses blk_ctx as datasource, and uses access_set as constrain
impl<'a, T: ADS> Database for TxContext<'a, T> {
    type Error = anyhow::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>> {
        let key_hash = hasher::hash(&address[..]);
        let in_rdo = self.access_set.rdo_set.contains(&key_hash);
        let in_rnw = self.access_set.rnw_set.contains(&key_hash);
        if !(in_rdo || in_rnw || address == self.blk_ctx.block_env.coinbase) {
            return Err(anyhow!("Account {} is not in access set", address));
        }
        let res = self.blk_ctx.basic(&key_hash, &address, self.orig_acc_map);
        Ok(res)
    }

    fn code_by_hash(&mut self, code_hash: FixedBytes<32>) -> Result<Bytecode> {
        if code_hash == FixedBytes::<32>::ZERO || code_hash == KECCAK_EMPTY {
            return Ok(Bytecode::new());
        }
        let bc = self.blk_ctx.code_by_hash(&code_hash);
        if bc.is_none() {
            panic!("Internal error! Cannot find bytecode");
        }
        Ok(bc.unwrap())
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256> {
        let mut addr_idx = [0u8; 20 + 32];
        addr_idx[..20].copy_from_slice(&address[..]);
        let index_arr: [u8; 32] = index.to_be_bytes();
        addr_idx[20..].copy_from_slice(&index_arr[..]);
        let key_hash = hasher::hash(&addr_idx[..]);
        let in_rdo = self.access_set.rdo_set.contains(&key_hash);
        let in_rnw = self.access_set.rnw_set.contains(&key_hash);
        if !(in_rdo || in_rnw) {
            return Err(anyhow!("Slot {}/{} is not in access set", address, index,));
        }
        Ok(self.blk_ctx.storage(&key_hash, &addr_idx))
    }

    fn block_hash(&mut self, _number: U256) -> Result<FixedBytes<32>> {
        Ok(FixedBytes::<32>::ZERO)
    }
}

// Support all the transactions in a block
pub struct BlockContext<T: ADS> {
    // TasksManage has a large enough vec to contain all the tasks
    // The last task's id in this block: not always equal to task_list.len()-1 because
    // scheduler may mark failed tasks
    pub tasks_manager: Arc<TasksManager<ExeTask>>,
    // StateCache for current block
    pub curr_state: Arc<StateCache>,
    // StateCache for previous block
    prev_state: Arc<StateCache>,
    ads: T,
    block_env: BlockEnv,
    pub results: Vec<RwLock<Option<Vec<Result<ResultAndState>>>>>,
    gas_fee_collect: Arc<RwLock<U256>>, // TODO use AtomicUsize
}

impl<T: ADS> BlockContext<T> {
    pub fn new(ads: T) -> Self {
        Self {
            tasks_manager: Arc::new(TasksManager::default()),
            curr_state: Arc::new(StateCache::new()),
            prev_state: Arc::new(StateCache::new()),
            ads,
            block_env: BlockEnv::default(),
            results: Vec::new(),
            gas_fee_collect: Arc::new(RwLock::new(U256::ZERO)),
        }
    }

    pub fn start_new_block(
        &mut self,
        tasks_manager: Arc<TasksManager<ExeTask>>,
        block_env: BlockEnv,
    ) {
        self.tasks_manager = tasks_manager;
        self.block_env = block_env;
        // move curr_state to prev_state
        mem::swap(&mut self.prev_state, &mut self.curr_state);
        self.curr_state = Arc::new(StateCache::new());
        self.results = (0..self.tasks_manager.tasks_len())
            .map(|_| RwLock::new(Option::None))
            .collect();
    }

    pub fn warmup(&self, idx: usize) -> Vec<Option<Error>> {
        if idx == 0 {
            // warmup coinbase account when the first task
            let mut buf = Vec::with_capacity(8192);
            buf.resize(DEFAULT_ENTRY_SIZE, 0);
            let _ = warmup_acc(
                &self.ads,
                &vec![addr_to_u256(&self.block_env.coinbase)],
                &self.curr_state.bytecode_map,
                &mut buf,
            );
        }

        let mut task_opt = self.tasks_manager.task_for_write(idx);
        let task = task_opt.as_mut().unwrap();
        let tx_nums = task.tx_list.len();
        let mut results = Vec::<Option<Error>>::with_capacity(tx_nums);
        for tx in &task.tx_list {
            results.push(warmup_tx(&tx, &self.ads, &self.curr_state.bytecode_map).err());
        }
        // rewrite access_list for adapting revm
        task.rewrite_txs_access_list();
        results
    }

    fn collect_gas_fee(&self, gas_fee_delta: U256) {
        let mut gas_fee = self.gas_fee_collect.write().unwrap();
        *gas_fee = (*gas_fee).saturating_add(gas_fee_delta.clone());
    }

    pub fn execute(&self, idx: usize, warmup_results: Vec<Option<Error>>) {
        let mut task_opt = self.tasks_manager.task_for_write(idx);
        let task = task_opt.as_mut().unwrap();
        let mut warmup_res = &warmup_results;
        if warmup_res.is_empty() {
            warmup_res = &task.warmup_results;
        }
        let mut change_sets = Vec::with_capacity(task.tx_list.len());
        let mut task_result: Vec<Result<ResultAndState>> = Vec::new();
        for index in 0..task.tx_list.len() {
            let (tx_result, mut change_set) =
                self.handle_transaction(&task, index, &warmup_res[index]);
            task_result.extend(tx_result);
            change_set.sort();
            self.curr_state.apply_change(&change_set);
            change_sets.push(change_set);
        }
        let mut result_opt = self.results[idx].write().unwrap();
        *result_opt = Option::Some(task_result);
        // from now on, change_set is a read-only field in task
        task.set_change_sets(Arc::new(change_sets));
    }

    fn handle_transaction(
        &self,
        task: &ExeTask,
        index: usize,
        warmup_result: &Option<Error>,
    ) -> (Vec<Result<ResultAndState>>, ChangeSet) {
        let tx = &task.tx_list[index];
        let env = Box::new(Env {
            cfg: CfgEnv::default(),
            block: self.block_env.clone(),
            tx: tx.clone(),
        });
        let coinbase_gas_price = get_gas_price(&env);
        let mut tx_result: Vec<Result<ResultAndState>> = Vec::new();
        if let Some(error) = warmup_result {
            tx_result.push(Err(anyhow!("Tx {:?} warmup error: {:?}", index, error)));
            let change_set = self.handle_tx_execute_mpex_err(&tx, coinbase_gas_price);
            return (tx_result, change_set);
        }

        // each tx has its own orig_acc_map
        let mut orig_acc_map: HashMap<Address, [u8; 72]> = HashMap::new();
        let evm_result = {
            // all the txs shares the task's access_set
            let tx_ctx = TxContext {
                orig_acc_map: &mut orig_acc_map,
                access_set: &task.access_set,
                blk_ctx: self,
            };

            let count = task.get_tx_accessed_slots_count(index);
            let handler = create_mpex_handler::<(), TxContext<'_, T>>(count);
            let mut evm = Evm::builder()
                .with_db(tx_ctx)
                .with_env(env)
                .with_handler(handler)
                .build();
            evm.transact()
        };

        match evm_result {
            Ok(res_and_state) => {
                tx_result.push(Ok(res_and_state.clone()));

                let gas_used = res_and_state.result.gas_used();
                let mut change_set = ChangeSet::new();
                // we must check not writing the readonly account and slot.
                match get_change_set_and_check_access_rw(
                    &mut change_set,
                    &res_and_state.state,
                    &mut orig_acc_map,
                    self.curr_state.as_ref(),
                    &task.access_set, //to check out-of-write-set errors
                    true,
                ) {
                    Ok(_) => {
                        // if no error, update world state
                        self.collect_gas_fee(coinbase_gas_price * U256::from(gas_used));
                        return (tx_result, change_set);
                    }
                    Err(err) => {
                        // there has rw error
                        tx_result.push(Err(anyhow!("Commit state change error: {:?}", err)));
                    }
                }
            }
            Err(err) => {
                tx_result.push(Err(anyhow!("EVM transact error: {:?}", err)));
            }
        }

        // at errors, cannot apply state change, but we need deduct all gas from caller.
        let change_set = self.handle_tx_execute_mpex_err(&tx, coinbase_gas_price);
        return (tx_result, change_set);
    }

    // handle_tx_execute_mpex_err will be call at errors because:
    // 1. warmup generates errors
    // 2. revm.transact gets errors from 'Database' or handlers and returns them 
    // 2. get_change_set_and_check_access_rw returns error of writing readonly data
    fn handle_tx_execute_mpex_err(&self, tx: &TxEnv, coinbase_gas_price: U256) -> ChangeSet {
        let mut orig_acc_map: HashMap<Address, [u8; 72]> = HashMap::new();
        // make sure the caller account is in orig_acc_map
        let key_hash = hasher::hash(&tx.caller[..]);
        self.basic(&key_hash, &tx.caller, &mut orig_acc_map);
        let orig_data = orig_acc_map.get(&tx.caller);
        let mut change_set = ChangeSet::new();
        if !orig_data.is_none() {
            let gas_fee = coinbase_gas_price * U256::from(tx.gas_limit);
            let acc = self.deduct_and_collect_caller_gas_fee(&orig_data.unwrap().clone(), gas_fee);
            let mut state = HashMap::new();
            state.insert(tx.caller, acc);
            // it's safe to unwrap the result because the only state change is on tx.caller
            get_change_set_and_check_access_rw(
                &mut change_set,
                &state,
                &orig_acc_map,
                self.curr_state.as_ref(),
                &AccessSet::new(),
                false,
            )
            .unwrap();
        }
        change_set
    }

    fn deduct_and_collect_caller_gas_fee(
        &self,
        orig_data: &[u8; ACC_INFO_LEN],
        gas_fee: U256,
    ) -> Account {
        let mut caller_acc = decode_account_info(orig_data);
        if caller_acc.balance.lt(&gas_fee) {
            self.collect_gas_fee(caller_acc.balance);
            caller_acc.balance = U256::ZERO;
            caller_acc.nonce += 1;
        } else {
            caller_acc.balance = caller_acc.balance.saturating_sub(gas_fee);
            caller_acc.nonce += 1;
            self.collect_gas_fee(gas_fee);
        }
        let mut acc = Account {
            info: caller_acc,
            storage: HashMap::new(),
            status: Default::default(),
        };
        acc.mark_touch();
        acc
    }

    pub fn send_to_ads(&self, task_id: i64) {
        self.ads.add_task(task_id);
    }

    pub fn end_block(&self) {
        let end_block_task_id = self.tasks_manager.get_last_task_id();
        let coinbase = self.block_env.coinbase;
        let key_hash = hasher::hash(&coinbase);
        let mut orig_acc_map = HashMap::<Address, AccInfo>::new();
        let acc_info_opt = self.basic(&key_hash, &coinbase, &mut orig_acc_map);
        let mut gas_fee_collected_guard = self.gas_fee_collect.write().unwrap();
        let gas_fee_collected = *gas_fee_collected_guard;
        let mut task_result: Vec<Result<ResultAndState>> = Vec::new();
        let mut acc_info;
        if acc_info_opt.is_some() {
            acc_info = acc_info_opt.unwrap();
        } else {
            acc_info = AccountInfo::default();
        }
        acc_info.balance = acc_info.balance.saturating_add(gas_fee_collected);
        let mut acc = Account {
            info: acc_info,
            storage: HashMap::new(),
            status: Default::default(),
        };
        acc.mark_touch();
        let mut state = HashMap::new();
        state.insert(coinbase, acc);
        let state_and_result = ResultAndState {
            result: ExecutionResult::Success {
                reason: SuccessReason::Return,
                gas_used: 0,
                gas_refunded: 0,
                logs: vec![],
                output: Output::Call(Bytes::new()),
            },
            state: state.clone(),
        };
        task_result.push(Ok(state_and_result));
        let mut change_set = ChangeSet::new();
        let mut task = ExeTask::new(vec![]);
        get_change_set_and_check_access_rw(
            &mut change_set,
            &state,
            &orig_acc_map,
            self.curr_state.as_ref(),
            &task.access_set,
            false,
        )
        .unwrap();
        change_set.sort();
        self.curr_state.apply_change(&change_set);
        task.set_change_sets(Arc::new(vec![change_set]));
        let idx = (end_block_task_id & IN_BLOCK_IDX_MASK) as usize;
        let mut out_ptr = self.tasks_manager.task_for_write(idx);
        *out_ptr = Some(task);
        let mut result_opt = self.results[idx].write().unwrap();
        *result_opt = Option::Some(task_result);

        self.send_to_ads(end_block_task_id);
        // clear gas fee
        *gas_fee_collected_guard = U256::ZERO;
    }

    fn basic(
        &self,
        key_hash: &[u8; 32],
        address: &Address,
        orig_acc_map: &mut HashMap<Address, AccInfo>,
    ) -> Option<AccountInfo> {
        let mut buf = [0u8; ACC_INFO_LEN];
        let mut cache_hit = self.curr_state.lookup_data(key_hash, &mut buf[..]);
        if !cache_hit {
            cache_hit = self.prev_state.lookup_data(key_hash, &mut buf[..]);
        }
        if !cache_hit {
            let mut ebuf = [0u8; DEFAULT_ENTRY_SIZE];
            let (size, found_it) = self
                .ads
                .read_entry(&key_hash[..], &address[..], &mut ebuf[..]);
            if !found_it {
                return None;
            }
            let entry_bz = EntryBz { bz: &ebuf[..size] };
            buf[..ACC_INFO_LEN].copy_from_slice(entry_bz.value());
        }
        //even if the same address is used to call 'basic' multiple times, the same
        //result will be inserted into orig_acc_map
        orig_acc_map.insert(*address, buf);
        return Some(decode_account_info(&buf));
    }

    fn code_by_hash(&self, code_hash: &FixedBytes<32>) -> Option<Bytecode> {
        let mut opt = self.curr_state.lookup_code(code_hash);
        if opt.is_none() {
            opt = self.prev_state.lookup_code(code_hash);
        }
        if opt.is_some() {
            return opt;
        }
        let mut buf = Vec::with_capacity(8192);
        buf.resize(DEFAULT_ENTRY_SIZE, 0);
        let size = self.ads.read_code(&code_hash[..], &mut buf);
        if size == 0 {
            return None;
        }
        return bincode::deserialize(&buf[..size]).unwrap();
    }

    fn storage(&self, key_hash: &[u8; 32], addr_idx: &[u8; 20 + 32]) -> U256 {
        let mut buf = [0u8; DEFAULT_ENTRY_SIZE];
        let mut cache_hit = self.curr_state.lookup_data(&key_hash, &mut buf[..32]);
        if !cache_hit {
            cache_hit = self.prev_state.lookup_data(&key_hash, &mut buf[..32]);
        }
        if cache_hit {
            return U256::from_be_slice(&buf[..32]);
        }
        let (size, found_it) = self
            .ads
            .read_entry(&key_hash[..], &addr_idx[..], &mut buf[..]);
        if !found_it {
            return U256::ZERO;
        }
        let entry_bz = EntryBz { bz: &buf[..size] };
        U256::from_be_slice(entry_bz.value())
    }
}

fn warmup_tx<T: ADS>(tx: &TxEnv, ads: &T, bytecode_map: &Arc<CodeMap>) -> Result<()> {
    let mut buf = Vec::with_capacity(8192);
    buf.resize(DEFAULT_ENTRY_SIZE, 0);

    // warmup caller and to_address
    let found = warmup_acc(
        ads,
        &vec![addr_to_u256(&tx.caller)],
        &bytecode_map,
        &mut buf,
    )?;
    if found == 0 {
        // caller must exist
        return Err(anyhow!("Cannot find caller account {}", tx.caller));
    }
    if let TransactTo::Call(to_address) = tx.transact_to {
        warmup_acc(
            ads,
            &vec![addr_to_u256(&to_address)],
            &bytecode_map,
            &mut buf,
        )?;
    }

    for (addr, u256list) in &tx.access_list {
        let rd_acc = *addr == READ_ACC;
        let wr_acc = *addr == WRITE_ACC;
        let rd_slot = *addr == READ_SLOT;
        let wr_slot = *addr == WRITE_SLOT;
        if rd_acc || wr_acc {
            warmup_acc(ads, u256list, &bytecode_map, &mut buf)?;
        }
        if rd_slot || wr_slot {
            for u256 in u256list {
                let bytes32: [u8; 32] = u256.to_be_bytes();
                // we are sure buf is large enough to hold slots
                ads.read_entry(&bytes32[..], &[], &mut buf[..]);
            }
        }
    }
    Ok(())
}

fn warmup_acc<T: ADS>(
    ads: &T,
    u256list: &Vec<U256>,
    bytecode_map: &Arc<CodeMap>,
    buf: &mut Vec<u8>,
) -> Result<u64> {
    let mut found = 0;
    for u256 in u256list {
        let bytes32: [u8; 32] = u256.to_be_bytes();
        let key = Address::from_slice(&bytes32[12..]);
        let key_hash = hasher::hash(&key[..]);
        // we are sure buf is large enough to hold account info
        let (size, found_it) = ads.read_entry(&key_hash[..], &key[..], buf);
        if !found_it {
            continue;
        }
        found += 1;
        let code_hash = get_code_hash(&buf[..size])?;
        if is_empty_code_hash(&code_hash) {
            continue; //non-contract account
        }
        if bytecode_map.contains_key(&code_hash) {
            continue; // bytecode already in cache
        }
        let size = ads.read_code(&code_hash[..], buf);
        if size == 0 {
            return Err(anyhow!("Cannot find bytecode for {}", code_hash));
        }
        let bc: Option<Bytecode> = bincode::deserialize(&buf[..size]).unwrap();
        if bc.is_none() {
            return Err(anyhow!(
                "Internal Error! Decoded bytecode is None for {}",
                code_hash
            ));
        }
        bytecode_map.insert(code_hash, bc.unwrap());
    }
    Ok(found)
}

fn get_code_hash(entry_bz_data: &[u8]) -> Result<FixedBytes<32>> {
    let entry_bz = EntryBz { bz: entry_bz_data };
    let key = entry_bz.key();
    if key.len() != 20 {
        panic!("Invalid length for Address");
    }
    let v = entry_bz.value();
    if v.len() != ACC_INFO_LEN {
        panic!("Invalid length for AccInfo");
    }
    Ok(FixedBytes::<32>::from_slice(&v[32 + 8..]))
}

fn create_mpex_handler<'a, EXT, DB: Database>(
    access_solts_count: u64,
) -> Handler<'a, Evm<'a, (), DB>, (), DB> {
    let mut handler = EvmHandler::<(), DB>::new(HandlerCfg::new(SpecId::LATEST));

    handler.post_execution.reward_beneficiary =
        Arc::new(|_c, _g| -> std::result::Result<(), EVMError<DB::Error>> {
            return Ok(());
        });

    handler.validation.initial_tx_gas =
        Arc::new(move |env| -> Result<u64, EVMError<<DB>::Error>> {
            // default is LatestSpec
            let mut initial_gas_spend = mainnet::validate_initial_tx_gas::<LatestSpec, DB>(env)?;
            initial_gas_spend += access_solts_count * ACCESS_LIST_STORAGE_KEY;
            if initial_gas_spend > env.tx.gas_limit {
                return Err(InvalidTransaction::CallGasCostMoreThanGasLimit.into());
            }
            Ok(initial_gas_spend)
        });
    handler
}

fn get_gas_price(env: &Box<Env>) -> U256 {
    let effective_gas_price = env.effective_gas_price();
    // todo: get SPEC or suppose always enable LONDON Fork.
    //let coinbase_gas_price = if SPEC::enabled(LONDON) {
    let coinbase_gas_price = effective_gas_price.saturating_sub(env.block.basefee);
    //} else {
    //   effective_gas_price
    //};
    coinbase_gas_price
}

#[cfg(test)]
mod tests {
    use dashmap::DashMap;
    use mpads::test_helper::EntryBuilder;
    use revm::primitives::{address, hex::FromHex, Bytes, TxEnv};

    use crate::test_helper::MockADS;

    use super::*;

    #[test]
    fn test_get_code_hash() {
        let k = "aaaaaaaaaaaaaaaaaaaa";
        let v = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbccccccccdddddddddddddddddddddddddddddddd";
        let entry_bz = EntryBuilder::kv(k, v).build_and_dump(&[]);
        let code_hash = get_code_hash(&entry_bz[..]).unwrap();
        assert_eq!([0x64u8; 32], code_hash);
    }

    #[test]
    fn test_warmup_acc() {
        let mut ads = MockADS::new();
        let mut u256list: Vec<U256> = Vec::new();

        let addr = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        u256list.push(addr_to_u256(&addr));
        let code = Bytecode::new();
        let code_hash = ads.add_code(&addr, code);

        let bytecode_map: Arc<CodeMap> = Arc::new(DashMap::new());
        let mut buf: Vec<u8> = vec![0u8; 1024];
        warmup_acc(&ads, &u256list, &bytecode_map, &mut buf);
        assert!(bytecode_map.contains_key(&code_hash));
    }

    #[test]
    fn test_warmup() {
        let mut ads = MockADS::new();
        let mut acc_list = Vec::new();

        let addr1 = address!("0000000000000000000000000000000000000001");
        let addr2 = address!("0000000000000000000000000000000000000002");
        let addr3 = address!("0000000000000000000000000000000000000003");
        let addr4 = address!("0000000000000000000000000000000000000004");

        let addr_a = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let addr_b = address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let addr_c = address!("cccccccccccccccccccccccccccccccccccccccc");
        let addr_d = address!("dddddddddddddddddddddddddddddddddddddddd");

        let code_hash1 = ads.add_code(&addr_a, Bytecode::new());
        let code_hash2 = ads.add_code(&addr_b, Bytecode::new_raw(Bytes::from([0x5f, 0x00])));
        let code_hash3 = ads.add_code(&addr_c, Bytecode::new_raw(Bytes::from([0x5f, 0x5f, 0x00])));
        let code_hash4 = ads.add_code(
            &addr_d,
            Bytecode::new_raw(Bytes::from([0x5f, 0x5f, 0x5f, 0x00])),
        );

        acc_list.push((addr1, vec![addr_to_u256(&addr_a)]));
        acc_list.push((addr2, vec![addr_to_u256(&addr_b)]));
        acc_list.push((addr3, vec![addr_to_u256(&addr_c)]));
        acc_list.push((addr4, vec![addr_to_u256(&addr_d)]));
        let mut tx = TxEnv::default();
        tx.access_list = acc_list;

        let bytecode_map: Arc<CodeMap> = Arc::new(DashMap::new());
        warmup_tx(&tx, &ads, &bytecode_map);

        assert!(bytecode_map.contains_key(&code_hash1));
        assert!(bytecode_map.contains_key(&code_hash2));
        assert_eq!(false, bytecode_map.contains_key(&code_hash3));
        assert_eq!(false, bytecode_map.contains_key(&code_hash4));
    }

    #[test]
    fn test_block_ctx_start_new_block() {
        let ads = MockADS::new();
        let mut block_ctx = BlockContext::new(ads);

        let task_list = Vec::with_capacity(100);
        let last_task_id = 123;
        let block_env = BlockEnv::default();
        block_ctx.start_new_block(
            Arc::new(TasksManager::<ExeTask>::new(task_list, last_task_id)),
            block_env,
        );

        // assert_eq!(100, block_ctx.tasks_manager.tasks.capacity());
        assert_eq!(123, block_ctx.tasks_manager.get_last_task_id());
    }

    #[test]
    fn test_block_ctx_warmup() {
        let mut ads = MockADS::new();
        let addr1 = address!("0000000000000000000000000000000000000001");
        let addr_a = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let code_hash1: [u8; 32] = ads.add_code(&addr_a, Bytecode::new());

        let mut tx = TxEnv::default();
        tx.access_list.push((addr1, vec![addr_to_u256(&addr_a)]));
        let tx_list = vec![tx];
        let task = RwLock::new(Option::Some(ExeTask::new(tx_list)));
        let task_list = vec![task];

        let last_task_id = 123;
        let block_env = BlockEnv::default();

        let mut block_ctx = BlockContext::new(ads);
        block_ctx.start_new_block(
            Arc::new(TasksManager::<ExeTask>::new(task_list, last_task_id)),
            block_env,
        );
        block_ctx.warmup(0);
        assert!(block_ctx.curr_state.bytecode_map.contains_key(&code_hash1));
    }

    #[test]
    fn test_tx_ctx_storage() {
        let addr_idx1 = "aaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111".as_bytes();
        let addr_idx2 = "bbbbbbbbbbbbbbbbbbbb22222222222222222222222222222222".as_bytes();
        let a1 = Address::from_slice(&addr_idx1[0..20]);
        let a2 = Address::from_slice(&addr_idx2[0..20]);
        let idx1 = U256::from_le_slice(&addr_idx1[20..]);
        let idx2 = U256::from_le_slice(&addr_idx2[20..]);
        let kh1 = hasher::hash(addr_idx1);
        let kh2 = hasher::hash(addr_idx2);
        let v1 = [0x01; 32];
        let v2 = [0x02; 32];

        let ads = MockADS::new();
        let block_ctx = BlockContext::new(ads);
        block_ctx.curr_state.insert_data(&FixedBytes::new(kh1), &v1);
        block_ctx.prev_state.insert_data(&FixedBytes::new(kh2), &v2);

        let mut acc_set = AccessSet::new();
        acc_set.rdo_set.insert(kh1);
        acc_set.rnw_set.insert(kh2);
        let mut orig_acc_map: HashMap<Address, AccInfo> = HashMap::new();
        let mut tx_ctx = TxContext::<MockADS> {
            orig_acc_map: &mut orig_acc_map,
            access_set: &acc_set,
            blk_ctx: &block_ctx,
        };
        assert_eq!(U256::from_be_bytes(v1), tx_ctx.storage(a1, idx1).unwrap());
        assert_eq!(U256::from_be_bytes(v2), tx_ctx.storage(a2, idx2).unwrap());
    }

    #[test]
    fn test_block_ctx_storage() {
        let k1 = [0x11; 32];
        let v1 = [0x12; 32];
        let k2 = [0x21; 32];
        let v2 = [0x22; 32];
        let v3 = [0x33; 32];
        let entry = EntryBuilder::kv(v3, v3).build_and_dump(&[]);

        let mut ads = MockADS::new();
        let k3 = ads.add_entry(entry);

        let block_ctx = BlockContext::new(ads);
        block_ctx.curr_state.insert_data(&FixedBytes::new(k1), &v1);
        block_ctx.prev_state.insert_data(&FixedBytes::new(k2), &v2);
        assert_eq!(U256::from_be_bytes(v1), block_ctx.storage(&k1, &[0; 52]));
        assert_eq!(U256::from_be_bytes(v2), block_ctx.storage(&k2, &[0; 52]));
        assert_eq!(U256::from_be_bytes(v3), block_ctx.storage(&k3, &[0; 52]));
    }

    #[test]
    fn test_block_ctx_code_by_hash() {
        let mut ads = MockADS::new();

        let a1 = Address::from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let c1 = Bytecode::new();
        let c2 = Bytecode::new_raw(Bytes::from([0x5f, 0x00]));
        let c3 = Bytecode::new_raw(Bytes::from([0x5f, 0x5f, 0x00]));
        let h1 = FixedBytes::from([0x11; 32]);
        let h2 = FixedBytes::from([0x22; 32]);
        let h3 = FixedBytes::from(ads.add_code(&a1, c3.clone()));

        let block_ctx = BlockContext::new(ads);
        block_ctx.curr_state.insert_code(&h1, &c1);
        block_ctx.prev_state.insert_code(&h2, &c2);
        assert_eq!(Option::Some(c1.clone()), block_ctx.code_by_hash(&h1));
        assert_eq!(Option::Some(c2.clone()), block_ctx.code_by_hash(&h2));
        assert_eq!(Option::Some(c3.clone()), block_ctx.code_by_hash(&h3));

        // test TxContext
        let mut orig_acc_map: HashMap<Address, AccInfo> = HashMap::new();
        let mut tx_ctx = TxContext::<MockADS> {
            orig_acc_map: &mut orig_acc_map,
            access_set: &AccessSet::new(),
            blk_ctx: &block_ctx,
        };
        assert_eq!(c1, tx_ctx.code_by_hash(h1).unwrap());
        assert_eq!(c2, tx_ctx.code_by_hash(h2).unwrap());
        assert_eq!(c3, tx_ctx.code_by_hash(h3).unwrap());
    }

    #[test]
    fn test_block_ctx_basic() {
        let a1 = Address::from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let a2 = Address::from_hex("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();
        let a3 = Address::from_hex("cccccccccccccccccccccccccccccccccccccccc").unwrap();
        let kh1 = hasher::hash(&a1[..]);
        let kh2 = hasher::hash(&a2[..]);
        let data = [
            "",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\0\0\0\0\0\0bbcccccccccccccccccccccccccccccccc",
            "dddddddddddddddddddddddddddddddd\0\0\0\0\0\0eeffffffffffffffffffffffffffffffff",
            "gggggggggggggggggggggggggggggggg\0\0\0\0\0\0hhiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii",
        ];

        let mut ads = MockADS::new();
        let entry = EntryBuilder::kv(&[0x33; 32][..], data[3].as_bytes()).build_and_dump(&[]);
        let kh3 = ads.add_entry(entry);

        let block_ctx = BlockContext::new(ads);
        block_ctx.curr_state.insert_data(&kh1, data[1].as_bytes());
        block_ctx.prev_state.insert_data(&kh2, data[2].as_bytes());

        let mut orig_acc_map: HashMap<Address, AccInfo> = HashMap::new();
        let result1 = block_ctx.basic(&kh1, &a1, &mut orig_acc_map);
        let result2 = block_ctx.basic(&kh2, &a2, &mut orig_acc_map);
        let result3 = block_ctx.basic(&kh3, &a3, &mut orig_acc_map);
        assert_eq!(25186, result1.unwrap().nonce);
        assert_eq!(25957, result2.unwrap().nonce);
        assert_eq!(26728, result3.unwrap().nonce);
        assert_eq!(3, orig_acc_map.len());

        // test TxContext
        let mut acc_set = AccessSet::new();
        acc_set.rdo_set.insert(kh1);
        acc_set.rnw_set.insert(kh2);
        let mut orig_acc_map: HashMap<Address, AccInfo> = HashMap::new();
        let mut tx_ctx = TxContext::<MockADS> {
            orig_acc_map: &mut orig_acc_map,
            access_set: &acc_set,
            blk_ctx: &block_ctx,
        };
        assert_eq!(25186, tx_ctx.basic(a1).unwrap().unwrap().nonce);
        assert_eq!(25957, tx_ctx.basic(a2).unwrap().unwrap().nonce);
        assert_eq!(2, orig_acc_map.len());
    }

    // #[test]
    // #[ignore = "wip"]
    // fn test_block_ctx_execute() {
    //     let mut ads = MockADS::new();
    //     let addr1 = address!("0000000000000000000000000000000000000001");
    //     let addr_a = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    //     let _code_hash1: [u8; 32] = ads.add_code(&addr_a, Bytecode::new());

    //     let mut tx = TxEnv::default();
    //     tx.access_list.push((addr1, vec![addr_to_u256(&addr_a)]));
    //     let tx_list = vec![tx];
    //     let task = RwLock::new(Option::Some(ExeTask::new(tx_list)));
    //     let task_list = vec![task];

    //     let last_task_id: Arc<AtomicI64> = Arc::new(AtomicI64::new(123));
    //     let block_env = BlockEnv::default();

    //     let mut block_ctx = BlockContext::new(ads);
    //     block_ctx.start_new_block(Arc::new(task_list), last_task_id, block_env);

    //     block_ctx.execute(0);
    // }
}
