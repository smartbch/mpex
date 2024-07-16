use crate::statecache::StateCache;
use anyhow::{anyhow, Result};
use byteorder::{BigEndian, ByteOrder};
use mpads::bptaskhub::Task;
use mpads::changeset::ChangeSet;
use mpads::def::{CODE_SHARD_ID, OP_CREATE, OP_DELETE, OP_WRITE};
use mpads::utils::hasher;
use revm::precompile::primitives::{Account, Address, TxEnv, U256};
use revm::primitives::{address, StorageSlot, TransactTo};
use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub const READ_ACC: Address = address!("0000000000000000000000000000000000000001");
pub const WRITE_ACC: Address = address!("0000000000000000000000000000000000000002");
pub const READ_SLOT: Address = address!("0000000000000000000000000000000000000003");
pub const WRITE_SLOT: Address = address!("0000000000000000000000000000000000000004");

//  | 32byte code hash
pub const ACC_AND_IDX_LEN: usize = 32 + 8 + 32;

pub type AccAndIdx = [u8; ACC_AND_IDX_LEN];

#[derive(Debug)]
pub struct AccessSet {
    pub rdo_set: HashSet<[u8; 32]>,
    pub rnw_set: HashSet<[u8; 32]>,
    pub rdo_k64_vec: Vec<u64>,
    pub rnw_k64_vec: Vec<u64>,
}

impl AccessSet {
    pub fn new() -> Self {
        Self {
            rdo_set: HashSet::new(),
            rnw_set: HashSet::new(),
            rdo_k64_vec: Vec::new(),
            rnw_k64_vec: Vec::new(),
        }
    }

    pub fn add_tx(&mut self, tx: &TxEnv) {
        self.add_rnw_address(&tx.caller);
        if let TransactTo::Call(to_address) = tx.transact_to {
            if tx.value.is_zero() {
                // to with zero-value -- readonly;
                self.add_rdo_address(&to_address);
            } else {
                // to with non-zero-value -- write
                self.add_rnw_address(&to_address);
            }
        }

        for (addr, u256list) in &tx.access_list {
            let rd_acc = *addr == READ_ACC;
            let wr_acc = *addr == WRITE_ACC;
            let rd_slot = *addr == READ_SLOT;
            let wr_slot = *addr == WRITE_SLOT;
            for u256 in u256list {
                let bytes32: [u8; 32] = u256.to_be_bytes();
                if rd_acc {
                    // push address hash in set.
                    let key_hash = hasher::hash(&bytes32[12..]);
                    self.rdo_set.insert(key_hash);
                    self.rdo_k64_vec.push(BigEndian::read_u64(&key_hash[..8]));
                }
                if wr_acc {
                    // push address hash in set.
                    let key_hash = hasher::hash(&bytes32[12..]);
                    self.rnw_set.insert(key_hash);
                    self.rnw_k64_vec.push(BigEndian::read_u64(&key_hash[..8]));
                }
                if rd_slot {
                    self.rdo_set.insert(bytes32);
                    self.rdo_k64_vec.push(BigEndian::read_u64(&bytes32[..8]));
                }
                if wr_slot {
                    self.rnw_set.insert(bytes32);
                    self.rnw_k64_vec.push(BigEndian::read_u64(&bytes32[..8]));
                }
            }
        }
    }

    pub fn add_rnw_address(&mut self, address: &Address) {
        let hash = hasher::hash(address);
        self.rnw_set.insert(hash);
        self.rnw_k64_vec.push(BigEndian::read_u64(&hash[..8]));
    }

    fn add_rdo_address(&mut self, address: &Address) {
        let hash = hasher::hash(address);
        self.rdo_set.insert(hash);
        self.rdo_k64_vec.push(BigEndian::read_u64(&hash[..8]));
    }
}

#[derive(Debug)]
pub struct ExeTask {
    pub tx_list: Vec<TxEnv>,
    pub access_set: AccessSet,
    pub change_sets: Option<Arc<Vec<ChangeSet>>>,
    bundle_start: AtomicUsize,
    min_all_done_index: AtomicUsize,
}

impl Task for ExeTask {
    fn get_change_sets(&self) -> Arc<Vec<ChangeSet>> {
        let change_set = self.change_sets.as_ref().unwrap();
        change_set.clone()
    }
}

impl ExeTask {
    pub fn new(tx_list: Vec<TxEnv>) -> Self {
        let mut access_set = AccessSet::new();
        for tx in &tx_list {
            access_set.add_tx(&tx);
        }
        Self {
            tx_list,
            access_set,
            change_sets: None,
            bundle_start: AtomicUsize::new(usize::MAX),
            min_all_done_index: AtomicUsize::new(usize::MAX),
        }
    }
    pub fn set_change_sets(&mut self, change_sets: Arc<Vec<ChangeSet>>) {
        self.change_sets = Some(change_sets);
    }
    pub fn rdo_list_size(&self) -> usize {
        self.access_set.rdo_k64_vec.len()
    }
    pub fn rnw_list_size(&self) -> usize {
        self.access_set.rnw_k64_vec.len()
    }
    pub fn get_rdo_k64(&self, idx: usize) -> u64 {
        self.access_set.rdo_k64_vec[idx]
    }
    pub fn get_rnw_k64(&self, idx: usize) -> u64 {
        self.access_set.rnw_k64_vec[idx]
    }
    pub fn set_bundle_start(&self, bundle_start: usize) {
        self.bundle_start.store(bundle_start, Ordering::SeqCst);
    }
    pub fn get_bundle_start(&self) -> usize {
        self.bundle_start.load(Ordering::SeqCst)
    }
    pub fn set_min_all_done_index(&self, min_all_done_index: usize) {
        self.min_all_done_index
            .store(min_all_done_index, Ordering::SeqCst);
    }
    pub fn get_min_all_done_index(&self) -> usize {
        self.min_all_done_index.load(Ordering::SeqCst)
    }

    pub fn rewrite_txs_access_list(&mut self) {
        for tx in self.tx_list.iter_mut() {
            let mut access_list = vec![];
            for (addr, u256list) in &tx.access_list {
                let rd_acc = *addr == READ_ACC;
                let wr_acc = *addr == WRITE_ACC;
                for u256 in u256list {
                    let bytes32: [u8; 32] = u256.to_be_bytes();
                    if rd_acc || wr_acc {
                        let address = Address::from_slice(&bytes32[12..]);
                        access_list.push((address, vec![]));
                    }
                }
            }
            tx.access_list = access_list;
        }
    }
}

pub fn get_change_set_and_check_access_rw(
    change_set: &mut ChangeSet,
    state: &HashMap<Address, Account>,
    orig_acc_map: &HashMap<Address, AccAndIdx>,
    state_cache: &StateCache,
    access_set: &AccessSet, //cannot write read-only members
    check_access: bool,     // end_block logic not need check_access
) -> Result<()> {
    let mut buf32 = [0u8; 32];
    let mut acc_buf = [0u8; ACC_AND_IDX_LEN];
    let mut addr_idx = [0u8; 20 + 32];

    for (address, account) in state {
        if !account.is_touched() {
            continue; // no change, so ignore
        }
        if account.is_empty() && account.is_loaded_as_not_existing() {
            continue; // no need to write empty or new created account  TODO  account op = OP_DELETE;
        }
        let mut save_acc = false;
        if account.is_created() && !account.is_selfdestructed() {
            let bytecode = account.info.code.as_ref().unwrap();
            let v = bincode::serialize(bytecode).unwrap();
            buf32[..].copy_from_slice(&account.info.code_hash[..]);
            change_set.add_op(
                OP_WRITE,
                CODE_SHARD_ID as u8,
                &buf32,                      //used as key_hash during sort
                &account.info.code_hash[..], //the key
                &v[..],                      //the value
                None,
            );
            // bytecode is updated even when commit_state_change returns Error
            // it's safe because bytecode shard is not part of consensus
            state_cache.insert_code(&account.info.code_hash, bytecode);
            save_acc = true;
        }
        let balance_b32: [u8; 32] = account.info.balance.to_be_bytes();
        acc_buf[0..32].copy_from_slice(&balance_b32);
        BigEndian::write_u64(&mut acc_buf[32..40], account.info.nonce);
        acc_buf[40..].copy_from_slice(&account.info.code_hash[..]);
        let orig_data = orig_acc_map.get(address);
        if orig_data.is_none() {
            save_acc = true; //new account
        } else {
            let orig_data = orig_data.unwrap();
            if *orig_data != acc_buf {
                save_acc = true; //changed account
            }
        }
        if save_acc {
            buf32 = hasher::hash(address);
            if check_access && access_set.rnw_set.get(&buf32).is_none() {
                return Err(anyhow!("Account {} is not in write set", address));
            }
            change_set.add_op(
                OP_WRITE,
                buf32[0] >> 4,
                &buf32,       //used during sort as key_hash
                &address[..], //the key
                &acc_buf[..], //the value
                None,
            );
        }
        addr_idx[..20].copy_from_slice(&address[..]);
        for (idx, slot) in &account.storage {
            if slot.previous_or_original_value == slot.present_value {
                continue; // not changed, so ignore this slot
            }
            let mut op = OP_WRITE;
            if slot.previous_or_original_value == U256::ZERO {
                op = OP_CREATE;
            } else if slot.present_value == U256::ZERO {
                op = OP_DELETE;
            }
            buf32 = idx.to_be_bytes();
            addr_idx[20..].copy_from_slice(&buf32[..]);
            buf32 = hasher::hash(addr_idx);
            if check_access && access_set.rnw_set.get(&buf32).is_none() {
                return Err(anyhow!("Slot {}/{} is not in write set", address, idx));
            }
            let v: [u8; 32] = slot.present_value.to_be_bytes();
            change_set.add_op(
                op,
                buf32[0] >> 4,
                &buf32,        //used during sort as key_hash
                &addr_idx[..], //the key
                &v[..],        //the value
                None,
            );
        }
    }
    return Ok(());
}

#[cfg(test)]
pub mod test_exe_task {
    use super::*;
    use revm::precompile::primitives::{Address, TxEnv, U256};
    use revm::primitives::alloy_primitives::U160;
    use revm::primitives::{AccountInfo, StorageSlot};

    #[test]
    fn test_access_set() {
        let mut set = AccessSet::new();
        let mut access_list = vec![];
        let mut tx = TxEnv::default();
        tx.caller = Address::from(U160::from(10));
        tx.transact_to = TransactTo::Call(Address::from(U160::from(20)));
        tx.value = U256::from(100);
        build_access_list(&mut access_list);
        tx.access_list = access_list;
        set.add_tx(&tx);
        assert_eq!(set.rdo_set.len(), 2);
        let byte32: [u8; 32] = U256::from(1).to_be_bytes();
        let read_address_hash = hasher::hash(&byte32[12..]);
        assert!(set.rdo_set.contains(&read_address_hash));
        let bytes32: [u8; 32] = U256::from(3).to_be_bytes();
        assert!(set.rdo_set.contains(&bytes32));

        assert_eq!(set.rdo_k64_vec.len(), 2);
        let u64 = BigEndian::read_u64(&read_address_hash[..8]);
        assert_eq!(set.rdo_k64_vec.get(0).unwrap(), &u64);
        assert_eq!(set.rdo_k64_vec.get(1).unwrap(), &0);

        assert_eq!(set.rnw_set.len(), 4);
        assert_eq!(set.rnw_k64_vec.len(), 4);
    }

    #[test]
    fn test_exe_task() {
        let mut tx = TxEnv::default();
        let mut access_list = vec![];
        build_access_list(&mut access_list);
        tx.access_list = access_list;
        let task = ExeTask::new(vec![tx]);
        assert_eq!(task.rdo_list_size(), 3);
        assert_eq!(task.rnw_list_size(), 3);
        assert_eq!(task.access_set.rdo_set.len(), 3);
        assert_eq!(task.access_set.rnw_set.len(), 3);
        assert_eq!(task.get_rdo_k64(1), 16861211397351628197);
        assert_eq!(task.get_rnw_k64(1), 3570104568112108476);
        task.set_bundle_start(1);
        assert_eq!(task.get_bundle_start(), 1);
        task.set_min_all_done_index(1);
        assert_eq!(task.get_min_all_done_index(), 1);
    }

    #[test]
    fn test_commit_state_change() {
        let mut change_set = ChangeSet::new();
        let mut state = HashMap::<Address, Account>::new();
        let from_address = Address::from(U160::from(10));
        let mut from_acc = Account::from(AccountInfo::default());
        from_acc.mark_touch();
        from_acc.mark_created();
        // prepare write slot
        let slot_w_idx = U256::from(101);
        let slot_w = StorageSlot::new_changed(U256::from(1), U256::from(2));
        from_acc.storage.insert(slot_w_idx.clone(), slot_w);
        // prepare create slot
        let slot_c_idx = U256::from(102);
        let slot_c = StorageSlot::new_changed(U256::from(0), U256::from(3));
        from_acc.storage.insert(slot_c_idx, slot_c);
        // prepare delete slot
        let slot_d_idx = U256::from(103);
        let slot_d = StorageSlot::new_changed(U256::from(4), U256::from(0));
        from_acc.storage.insert(slot_d_idx, slot_d);
        state.insert(from_address.clone(), from_acc.clone());

        let orig_acc_map = HashMap::<Address, AccAndIdx>::new();
        let state_cache = StateCache::new();
        let mut access_set = AccessSet::new();
        let buf32 = hasher::hash(from_address);
        access_set.rnw_set.insert(buf32);
        let mut addr_idx = [0u8; 32 + 20];
        addr_idx[..20].copy_from_slice(&from_address[..]);
        let mut buf32 = slot_w_idx.to_be_bytes();
        addr_idx[20..].copy_from_slice(&buf32[..]);
        buf32 = hasher::hash(addr_idx);
        access_set.rnw_set.insert(buf32);

        let mut buf32 = slot_c_idx.to_be_bytes();
        addr_idx[20..].copy_from_slice(&buf32[..]);
        buf32 = hasher::hash(addr_idx);
        access_set.rnw_set.insert(buf32);

        let mut buf32 = slot_d_idx.to_be_bytes();
        addr_idx[20..].copy_from_slice(&buf32[..]);
        buf32 = hasher::hash(addr_idx);
        access_set.rnw_set.insert(buf32);

        let res = get_change_set_and_check_access_rw(
            &mut change_set,
            &state,
            &orig_acc_map,
            &state_cache,
            &access_set,
            true,
        );
        assert!(res.is_ok());
        assert_eq!(change_set.op_list.len(), 5);
        let op_set: HashSet<u8> = change_set.op_list.iter().map(|p| p.op_type).collect();
        assert!(op_set.contains(&OP_WRITE));
        assert!(op_set.contains(&OP_CREATE));
        assert!(op_set.contains(&OP_DELETE));
        assert_eq!(from_acc.info.code_hash[..], change_set.data[..32]);
    }

    pub fn build_access_list(access_list: &mut Vec<(Address, Vec<U256>)>) {
        access_list.push((READ_ACC, vec![U256::from(1)]));
        access_list.push((WRITE_ACC, vec![U256::from(2)]));
        access_list.push((READ_SLOT, vec![U256::from(3)]));
        access_list.push((WRITE_SLOT, vec![U256::from(4)]));
    }
}
