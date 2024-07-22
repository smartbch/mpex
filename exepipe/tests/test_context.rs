use std::{
    f32::consts::E,
    ops::Deref,
    sync::{Arc, RwLock},
};

use exepipe::{
    context::BlockContext,
    exetask::{ExeTask, ACC_INFO_LEN},
    statecache::StateCache,
    test_helper::{addr_idx_hash, calc_code_hash, MockADS},
    utils::decode_account_info,
};
use mpads::{tasksmanager::TasksManager, utils::hasher};
use revm::{
    interpreter::gas::ZERO,
    primitives::{
        address, hex::FromHex, keccak256, ruint::Uint, AccountInfo, Address, BlockEnv, Bytecode,
        Bytes, Env, TransactTo, TxEnv, U256,
    },
};

#[cfg(test)]
mod tests {
    use std::{
        f32::consts::E,
        ops::Deref,
        sync::{Arc, RwLock},
    };

    use exepipe::{
        context::BlockContext,
        exetask::{ExeTask, ACC_INFO_LEN, READ_SLOT, WRITE_SLOT},
        test_helper::{addr_idx_hash, calc_code_hash, MockADS},
        utils::decode_account_info,
    };
    use mpads::{tasksmanager::TasksManager, utils::hasher};
    use revm::{
        interpreter::gas::ZERO,
        primitives::{
            address, hex::FromHex, keccak256, ruint::Uint, AccountInfo, Address, BlockEnv,
            Bytecode, Bytes, Env, TransactTo, TxEnv, U256,
        },
    };

    use crate::{check_account_info, create_contract_call, create_transfer};

    #[test]
    fn test_transfer() {
        let from = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let to_address = address!("00000000000000000000000000000000000000a3");
        let coinbase = address!("000000000000000000000000000000000000cbcb");
        let init_balance = 1_000_000_000_000_000_000u128 + 2 * (2 * 21000 + 1);

        let block_ctx = create_transfer(from, to_address, init_balance, coinbase, true);

        block_ctx.warmup(0);
        block_ctx.execute(0);
        block_ctx.end_block();

        // println!("block_ctx.results = {:?}", block_ctx.results);

        let result0 = block_ctx.results[0].read().unwrap();
        for r in result0.deref().as_ref().unwrap() {
            assert!(r.as_ref().unwrap().result.is_success());
        }
        check_account_info(&block_ctx, &from, 1_000_000_000_000_000_000u128, 2);
        check_account_info(&block_ctx, &to_address, 2u128, 0);
        check_account_info(&block_ctx, &coinbase, 84000u128, 0);
    }

    #[test]
    fn test_transfer_to_coinbase() {
        let from = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let to_address = Address::ZERO;
        let coinbase = to_address.clone();
        let init_balance = 1_000_000_000_000_000_000u128 + 2 * (2 * 21000 + 1);

        let block_ctx = create_transfer(from, to_address, init_balance, coinbase, true);

        block_ctx.warmup(0);
        block_ctx.execute(0);
        block_ctx.end_block();

        // println!("block_ctx.results = {:?}", block_ctx.results);

        let result0 = block_ctx.results[0].read().unwrap();
        for r in result0.deref().as_ref().unwrap() {
            assert!(r.as_ref().unwrap().result.is_success());
        }
        check_account_info(&block_ctx, &from, 1_000_000_000_000_000_000u128, 2);
        check_account_info(&block_ctx, &coinbase, 84002u128, 0);
    }

    #[test]
    fn test_error_when_lock_of_fund_for_max_fee() {
        let from = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let to_address = address!("00000000000000000000000000000000000000a3");
        let coinbase = address!("000000000000000000000000000000000000cbcb");

        let init_balance = 1_000_000_000_000_000_000u128;

        let block_ctx = create_transfer(from, to_address, init_balance, coinbase, true);

        block_ctx.warmup(0);
        block_ctx.execute(0);
        block_ctx.end_block();

        // println!("block_ctx.results = {:?}", block_ctx.results);
        let r = block_ctx.results[0].read().unwrap();
        let s = r.deref().as_ref().unwrap()[0].as_ref().unwrap_err();
        assert!(format!("{:?}", s).contains("EVM transact error: Transaction(LackOfFundForMaxFee"));

        check_account_info(&block_ctx, &from, 0, 2);
        check_account_info(&block_ctx, &to_address, 0, 0);
        check_account_info(&block_ctx, &coinbase, init_balance, 0);
    }

    #[test]
    fn test_error_when_warmup() {
        let from = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let to_address = address!("00000000000000000000000000000000000000a3");
        let coinbase = address!("000000000000000000000000000000000000cbcb");

        let init_balance = 1_000_000_000_000_000_000u128;

        let block_ctx = create_transfer(from, to_address, init_balance, coinbase, false);

        block_ctx.warmup(0);
        block_ctx.execute(0);
        block_ctx.end_block();

        // println!("block_ctx.results = {:?}", block_ctx.results);
        let r = block_ctx.results[0].read().unwrap();
        let s = r.deref().as_ref().unwrap()[0].as_ref().unwrap_err();
        assert!(format!("{:?}", s).contains("Tx 0 warmup error: Cannot find caller account"));

        check_account_info(&block_ctx, &from, 0, 0);
        check_account_info(&block_ctx, &to_address, 0, 0);
        check_account_info(&block_ctx, &coinbase, 0, 0);
    }

    #[test]
    fn test_error_when_create_empty_account() {
        let from = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let to_address = address!("00000000000000000000000000000000000000a3");

        let init_balance = 1_000_000_000_000_000_000u128 + 2 * (2 * 21000 + 1);

        let block_ctx = create_transfer(from, to_address, init_balance, Address::ZERO, true);
        {
            let mut opt = block_ctx.tasks_manager.task_for_write(0);
            let task = opt.as_mut().unwrap();
            task.tx_list[0].value = U256::ZERO;
            task.tx_list[1].value = U256::ZERO;
        }

        block_ctx.warmup(0);
        block_ctx.execute(0);
        block_ctx.end_block();

        // println!("block_ctx.results = {:?}", block_ctx.results);
        check_account_info(&block_ctx, &from, 1000000000000000002, 2);
        check_account_info(&block_ctx, &to_address, 0, 0);
        check_account_info(&block_ctx, &Address::ZERO, 84000, 0);
    }

    #[test]
    fn test_counter() {
        let eoa_addr = address!("0000000000000000000000000000000000000e0a");
        let ca_addr = address!("0000000000000000000000000000000000000ccc");

        let block_ctx = create_contract_call(
            eoa_addr,
            ca_addr,
            U256::ZERO,
            vec![(
                WRITE_SLOT,
                vec![addr_idx_hash(
                    &address!("0000000000000000000000000000000000000ccc"),
                    &U256::ZERO,
                )],
            )],
            Address::ZERO,
            true,
        );
        block_ctx.warmup(0);
        block_ctx.execute(0);
        block_ctx.end_block();

        // println!("block_ctx.results = {:?}", block_ctx.results);
        let result0 = block_ctx.results[0].read().unwrap();
        let v = result0.deref().as_ref().unwrap();
        let rs = v[0].as_ref().unwrap();
        let contract_acc = rs.state.get(&ca_addr).unwrap();
        let counter_slot = contract_acc.storage.get(&U256::ZERO).unwrap();
        assert!(rs.result.is_success());
        assert_eq!(U256::ZERO, counter_slot.previous_or_original_value);
        assert_eq!(U256::from(3), counter_slot.present_value);

        check_account_info(&block_ctx, &eoa_addr, 999999999999908266, 1);
        check_account_info(&block_ctx, &ca_addr, 0, 0);
        check_account_info(&block_ctx, &Address::ZERO, 91734, 0);
    }

    #[test]
    fn test_error_when_cannot_find_bytecode() {
        let eoa_addr = address!("0000000000000000000000000000000000000e0a");
        let ca_addr = address!("0000000000000000000000000000000000000ccc");

        let block_ctx = create_contract_call(
            eoa_addr,
            ca_addr,
            U256::ZERO,
            vec![(
                WRITE_SLOT,
                vec![addr_idx_hash(
                    &address!("0000000000000000000000000000000000000ccc"),
                    &U256::ZERO,
                )],
            )],
            Address::ZERO,
            false,
        );
        block_ctx.warmup(0);
        block_ctx.execute(0);
        block_ctx.end_block();

        let r = block_ctx.results[0].read().unwrap();
        let s = r.deref().as_ref().unwrap()[0].as_ref().unwrap_err();
        assert!(format!("{:?}", s).contains("Tx 0 warmup error: Cannot find bytecode for 0x7807a83790a6c471b7da667f716ae2d628a3012619829c7d31b5c3257bf39ecc"));

        // println!("block_ctx.results = {:?}", block_ctx.results);
        check_account_info(&block_ctx, &eoa_addr, 999999998000000000u128, 1);
        check_account_info(&block_ctx, &Address::ZERO, 2_000_000_000u128, 0);
    }

    // read from a slot that is not in the access list
    #[test]
    fn test_error_when_read_not_in_accesslist() {
        let eoa_addr = address!("0000000000000000000000000000000000000e0a");
        let ca_addr = address!("0000000000000000000000000000000000000ccc");
        let coinbase = address!("0000000000000000000000000000000000000bbb");

        let block_ctx = create_contract_call(eoa_addr, ca_addr, U256::ZERO, vec![], coinbase, true);
        block_ctx.warmup(0);
        block_ctx.execute(0);
        block_ctx.end_block();

        // println!("block_ctx.results = {:?}", block_ctx.results);
        let r = block_ctx.results[0].read().unwrap();
        let s = r.deref().as_ref().unwrap()[0].as_ref().unwrap_err();
        assert!(format!("{:?}", s).contains("EVM transact error: Database(Slot 0x0000000000000000000000000000000000000CcC/0 is not in access set"));
        check_account_info(&block_ctx, &eoa_addr, 999999998000000000u128, 1);
        check_account_info(&block_ctx, &coinbase, 2_000_000_000u128, 0);
    }

    // write to a slot that is only read in the access list
    #[test]
    fn test_error_when_write_only_read_in_accesslist() {
        let eoa_addr = address!("0000000000000000000000000000000000000e0a");
        let ca_addr = address!("0000000000000000000000000000000000000ccc");
        let coinbase = address!("0000000000000000000000000000000000000bbb");

        let block_ctx = create_contract_call(
            eoa_addr,
            ca_addr,
            U256::ZERO,
            vec![(
                READ_SLOT,
                vec![addr_idx_hash(
                    &address!("0000000000000000000000000000000000000ccc"),
                    &U256::ZERO,
                )],
            )],
            coinbase,
            true,
        );
        block_ctx.warmup(0);
        block_ctx.execute(0);
        block_ctx.end_block();
        // println!("block_ctx.results = {:?}", block_ctx.results);

        let r = block_ctx.results[0].read().unwrap();
        let s = r.deref().as_ref().unwrap()[0].as_ref().unwrap_err();
        assert!(format!("{:?}", s).contains("Commit state change error: Slot 0x0000000000000000000000000000000000000CcC/0 is not in write set"));

        check_account_info(&block_ctx, &eoa_addr, 999999998000000000u128, 1);
        check_account_info(&block_ctx, &coinbase, 2_000_000_000u128, 0);
    }
}

fn check_account_info(
    block_ctx: &BlockContext<MockADS>,
    eoa_addr: &Address,
    balance: u128,
    nonce: u64,
) {
    let mut buf = [0u8; ACC_INFO_LEN];
    block_ctx
        .curr_state
        .lookup_data(&hasher::hash(eoa_addr.to_vec().as_slice()), &mut buf);
    let acc = decode_account_info(&buf);
    assert_eq!(acc.balance, U256::from(balance));
    assert_eq!(acc.nonce, nonce);
}

fn create_contract_call(
    eoa_addr: Address,
    ca_addr: Address,
    value: Uint<256, 4>,
    access_list: Vec<(Address, Vec<U256>)>,
    coinbase: Address,
    has_bytecode: bool,
) -> BlockContext<MockADS> {
    let bc_hex = "0x608060405234801561001057600080fd5b50600436106100415760003560e01c806361bc221a146100465780636299a6ef14610064578063ab470f0514610080575b600080fd5b61004e61009e565b60405161005b91906100e0565b60405180910390f35b61007e6004803603810190610079919061012c565b6100a4565b005b6100886100bf565b604051610095919061019a565b60405180910390f35b60005481565b806000808282546100b591906101e4565b9250508190555050565b600033905090565b6000819050919050565b6100da816100c7565b82525050565b60006020820190506100f560008301846100d1565b92915050565b600080fd5b610109816100c7565b811461011457600080fd5b50565b60008135905061012681610100565b92915050565b600060208284031215610142576101416100fb565b5b600061015084828501610117565b91505092915050565b600073ffffffffffffffffffffffffffffffffffffffff82169050919050565b600061018482610159565b9050919050565b61019481610179565b82525050565b60006020820190506101af600083018461018b565b92915050565b7f4e487b7100000000000000000000000000000000000000000000000000000000600052601160045260246000fd5b60006101ef826100c7565b91506101fa836100c7565b925082820190508281121560008312168382126000841215161715610222576102216101b5565b5b9291505056fea26469706673582212205d20d6227111b55922ff75f33c5a4680a5d9db8c9f9bbd6c0336f287e7174aea64736f6c63430008140033";
    let calldata = "0x6299a6ef0000000000000000000000000000000000000000000000000000000000000003";
    let bc_bytes = Bytes::from_hex(bc_hex).unwrap();
    let bytecode = Bytecode::new_raw(bc_bytes);
    let bc_hash = calc_code_hash(&bytecode);

    let eoa_info = AccountInfo {
        nonce: 0,
        balance: U256::from(1_000_000_000_000_000_000u128),
        code_hash: keccak256(Bytes::new()),
        code: None,
    };

    let mut ads = MockADS::new();
    let _ = ads.add_account(&eoa_addr, &eoa_info);
    let mut contract_info = AccountInfo {
        nonce: 0,
        balance: U256::ZERO,
        code_hash: bc_hash,
        code: Option::None,
    };
    if has_bytecode {
        contract_info.code = Option::Some(bytecode);
    }
    let _ = ads.add_account(&ca_addr, &contract_info);

    let mut tx = TxEnv::default();
    tx.caller = eoa_addr.clone();
    tx.transact_to = TransactTo::Call(ca_addr);
    tx.data = Bytes::from_hex(calldata).unwrap();
    tx.value = value;
    tx.access_list = access_list;
    tx.gas_price = U256::from(2);
    tx.gas_limit = 1_000_000_000u64;

    let tx_list: Vec<TxEnv> = vec![tx];

    let task = ExeTask::new(tx_list);

    let tasks = vec![RwLock::new(Some(task)), RwLock::new(None)];

    let mut block_ctx = BlockContext::new(ads);
    let tasksmanager = TasksManager::new(tasks, 1);
    let mut env = BlockEnv::default();
    env.coinbase = coinbase;
    block_ctx.start_new_block(Arc::new(tasksmanager), env);
    block_ctx
}

fn create_transfer(
    from: Address,
    to_address: Address,
    _init_balance: u128,
    coinbase: Address,
    add_from_to_ads: bool,
) -> BlockContext<MockADS> {
    let mut ads = MockADS::new();

    if add_from_to_ads {
        let init_balance = U256::from(_init_balance);
        let from_account_info = AccountInfo {
            nonce: 0,
            balance: init_balance.clone(),
            code_hash: keccak256(Bytes::new()),
            code: None,
        };
        ads.add_account(&from, &from_account_info);
    }

    let mut tx: TxEnv = TxEnv::default();
    if add_from_to_ads {
        // only for test
        tx.gas_price = U256::from(2);
        tx.gas_limit = 500_000_000_000_000_000u64;
        tx.value = U256::from(1);
    }
    tx.caller = from.clone();
    tx.transact_to = TransactTo::Call(to_address);
    tx.data = Bytes::new();
    tx.access_list = vec![];
    let tx_list: Vec<TxEnv> = vec![tx.clone(), tx];
    let task = ExeTask::new(tx_list);
    let tasks = vec![RwLock::new(Some(task)), RwLock::new(None)];

    let mut block_ctx = BlockContext::new(ads);
    let tasksmanager = TasksManager::new(tasks, 1);
    let mut env = BlockEnv::default();
    env.coinbase = coinbase;
    block_ctx.start_new_block(Arc::new(tasksmanager), env);
    block_ctx
}
