#[cfg(test)]
mod tests {
    use core::task;
    use std::{
        borrow::BorrowMut,
        collections::HashMap,
        hash::{Hash, Hasher},
        str::FromStr,
        sync::{Arc, RwLock},
        time::Instant,
    };

    use exepipe::{
        bench::{
            address::{create_caller_address, create_to_address},
            erc20::{create_deploy_and_transfer_tx, create_transfer_token_tx},
            tasks::{create_collide_tasks, create_no_collide_tasks},
        },
        context::BlockContext,
        coordinator::Coordinator,
        exetask::{get_change_set_and_check_access_rw, AccessSet, ExeTask, READ_SLOT, WRITE_SLOT},
        scheduler::{EarlyExeInfo, Scheduler},
        statecache::StateCache,
        test_helper::{
            addr_idx_hash, calc_code_hash, encode_account_info, generate_ads_wrap, MockADS,
        },
    };
    use mpads::{
        changeset::ChangeSet,
        entry::EntryBz,
        tasksmanager::TasksManager,
        test_helper::{EntryBuilder, TempDir},
        utils::hasher,
    };
    use revm::{
        db::{states::state, CacheDB, EmptyDB, State},
        precompile::bn128::add,
        primitives::{
            address, alloy_primitives::U160, bitvec::vec, keccak256, ruint::Uint, Account,
            AccountInfo, Address, BlockEnv, Bytecode, Bytes, Storage, StorageSlot, TransactTo,
            TxEnv, KECCAK_EMPTY, U256,
        },
        Database, Evm,
    };

    #[test]
    #[serial_test::serial]
    fn test_serial_excute() {
        let dir = "./tmp_ads";
        let _tmp_dir = TempDir::new(dir);
        let (shared_ads_wrap, tpool, sender, receivers, s, r) = generate_ads_wrap(dir);
        let mut blk_ctx = BlockContext::new(shared_ads_wrap);
        let (ca_addr, slotmap) = get_slotmap_and_set_state(blk_ctx.curr_state.as_ref());

        // transfer  tokens
        let tasks_in = create_collide_tasks(&slotmap, &ca_addr, 20, 500);
        let task_list = (0..tasks_in.len() + 1)
            .map(|_| RwLock::new(Option::None))
            .collect();
        let task_manager = Arc::new(TasksManager::new(task_list, i64::MAX));
        blk_ctx.start_new_block(task_manager, BlockEnv::default());
        let blk_ctx = Arc::new(blk_ctx);
        let mut scheduler = Scheduler::new(tpool.clone(), sender, blk_ctx.clone(), s.clone());
        scheduler.start_new_block(1, blk_ctx.clone());
        let mut coordinator = Box::new(Coordinator::new(
            receivers,
            tpool.clone(),
            blk_ctx.clone(),
            s,
            r,
        ));

        let start = Instant::now();
        let coord_thread = std::thread::spawn(move || {
            coordinator.run();
            coordinator
        });
        scheduler.add_tasks(tasks_in);
        scheduler.flush_all_bundle_tasks();
        let mut coordinator = coord_thread.join().unwrap();
        coordinator.end_block();
        let duration = start.elapsed();
        println!("Time elapsed: {:?}", duration);

        let r = &blk_ctx.results[blk_ctx.results.len() - 2].read().unwrap();
        let r1 = r.as_ref().unwrap();
        let r2 = r1.get(r1.len() - 1).unwrap();
        let r3 = &r2.as_ref().unwrap().result;
        assert!(r3.is_success());
    }

    #[test]
    #[serial_test::serial]
    fn test_parallel_excute() {
        let dir = "./tmp_ads";
        let _tmp_dir = TempDir::new(dir);
        let (shared_ads_wrap, tpool, sender, receivers, s, r) = generate_ads_wrap(dir);
        let mut blk_ctx = BlockContext::new(shared_ads_wrap);
        let (ca_addr, slotmap) = get_slotmap_and_set_state(blk_ctx.curr_state.as_ref());

        // transfer  tokens
        let tasks_in = create_no_collide_tasks(&slotmap, &ca_addr, 20, 500);
        let task_list = (0..tasks_in.len() + 1)
            .map(|_| RwLock::new(Option::None))
            .collect();
        let task_manager = Arc::new(TasksManager::new(task_list, i64::MAX));
        blk_ctx.start_new_block(task_manager, BlockEnv::default());
        let blk_ctx = Arc::new(blk_ctx);
        let mut scheduler = Scheduler::new(tpool.clone(), sender, blk_ctx.clone(), s.clone());
        scheduler.start_new_block(1, blk_ctx.clone());
        let mut coordinator = Box::new(Coordinator::new(
            receivers,
            tpool.clone(),
            blk_ctx.clone(),
            s,
            r,
        ));

        let start = Instant::now();
        let coord_thread = std::thread::spawn(move || {
            coordinator.run();
            coordinator
        });
        scheduler.add_tasks(tasks_in);
        scheduler.flush_all_bundle_tasks();
        let mut coordinator = coord_thread.join().unwrap();
        coordinator.end_block();
        let duration = start.elapsed();
        println!("Time elapsed: {:?}", duration);

        let r = &blk_ctx.results[blk_ctx.results.len() - 2].read().unwrap();
        let r1 = r.as_ref().unwrap();
        let r2 = r1.get(r1.len() - 1).unwrap();
        let r3 = &r2.as_ref().unwrap().result;
        assert!(r3.is_success());
    }

    #[test]
    #[serial_test::serial]
    fn test_erc20_excute() {
        let dir = "./tmp_ads";
        let _tmp_dir = TempDir::new(dir);
        let (shared_ads_wrap, tpool, sender, receivers, s, r) = generate_ads_wrap(dir);
        let mut blk_ctx = BlockContext::new(shared_ads_wrap);
        let (ca_addr, slotmap) = get_slotmap_and_set_state(blk_ctx.curr_state.as_ref());

        // transfer 1 token
        let addrs = slotmap.keys().cloned().collect::<Vec<_>>();
        let caller = &addrs[0];
        let mut tasks_in = vec![];
        let dest = &addrs[1];
        let tx = create_transfer_token_tx(&caller, &ca_addr, &dest, &U256::from(1), &slotmap);
        let tx_list = vec![tx];
        let task = ExeTask::new(tx_list);
        tasks_in.push(task);

        let task_list = (0..tasks_in.len() + 1)
            .map(|_| RwLock::new(Option::None))
            .collect();
        let task_manager = Arc::new(TasksManager::new(task_list, i64::MAX));
        blk_ctx.start_new_block(task_manager, BlockEnv::default());
        let blk_ctx = Arc::new(blk_ctx);
        let mut scheduler = Scheduler::new(tpool.clone(), sender, blk_ctx.clone(), s.clone());
        scheduler.start_new_block(1, blk_ctx.clone());
        let mut coordinator = Box::new(Coordinator::new(
            receivers,
            tpool.clone(),
            blk_ctx.clone(),
            s,
            r,
        ));
        let coord_thread = std::thread::spawn(move || {
            coordinator.run();
            coordinator
        });
        scheduler.add_tasks(tasks_in);
        let start = Instant::now();
        scheduler.flush_all_bundle_tasks();
        let mut coordinator = coord_thread.join().unwrap();
        coordinator.end_block();

        println!("----results: {:?}", blk_ctx.results);
    }

    fn get_slotmap_and_set_state(blk_state: &StateCache) -> (Address, HashMap<Address, U256>) {
        // transfer index + per_addr_base_tokens tokens to each address
        let per_addr_base_tokens = 10000000;
        let deploy_tx = create_deploy_and_transfer_tx();
        let cache_db = CacheDB::new(EmptyDB::default());
        let mut evm = Evm::builder()
            .with_ref_db(cache_db.clone())
            .modify_tx_env(|tx| {
                tx.caller = deploy_tx.caller;
                tx.transact_to = deploy_tx.transact_to.clone();
                tx.data = deploy_tx.data
            })
            .build();
        let mut result = evm.transact().unwrap();
        let ca_addr = result.result.logs()[0].address.clone();

        let mut slot_map = HashMap::new();
        let ca_state = result.state.get(&ca_addr).unwrap();
        for s in ca_state.storage.iter() {
            if s.0.gt(&U256::from(10)) {
                let balance: usize = s.1.present_value.try_into().unwrap();
                let address = create_caller_address(balance - per_addr_base_tokens);
                let slot_index = s.0.clone();
                slot_map.insert(address, slot_index);
            }
        }

        for addr in &slot_map.keys().cloned().collect::<Vec<_>>() {
            let s = result.state.get_mut(addr).unwrap();
            s.info.balance = U256::from(1_000_000_000_000_000_000u128);
        }

        let mut change_set = ChangeSet::new();
        let _ = get_change_set_and_check_access_rw(
            &mut change_set,
            &result.state,
            &HashMap::new(),
            blk_state,
            &AccessSet::new(),
            false,
        );
        change_set.sort();
        blk_state.apply_change(&change_set);

        (ca_addr, slot_map)
    }
}
