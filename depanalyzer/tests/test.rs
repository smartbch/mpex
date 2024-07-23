use byteorder::{BigEndian, ByteOrder};
use depanalyzer::main::{Block, Tx, WETH_ADDR};
use exepipe::coordinator::Coordinator;
use exepipe::exetask::AccessSet;
use exepipe::scheduler::{PBElement, ParaBloom, MAX_TASKS_LEN_IN_BUNDLE, SET_MAX_SIZE};
use mpads::utils::hasher;
use serde::Deserialize;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::io::{self, BufRead};
use std::path::Path;
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
};

#[cfg(test)]
mod tests {
    use core::task;
    use std::{
        collections::{HashMap, HashSet},
        str::FromStr,
        sync::{Arc, Mutex, RwLock},
        thread,
    };

    use depanalyzer::main::{run_scheduler, run_serial_issuer};
    use exepipe::{
        bench::tasks,
        coordinator::Coordinator,
        exetask::{ExeTask, READ_ACC, READ_SLOT, WRITE_SLOT},
        scheduler::{BlockContext, EarlyExeInfo, Scheduler, EARLY_EXE_WINDOW_SIZE},
        test_helper::generate_ads_wrap,
    };
    use mpads::{tasksmanager::TasksManager, test_helper::TempDir, utils::hasher};

    use crate::read_txs_in_from_files;
    use revm::{
        db::states::bundle_account,
        primitives::{Address, BlockEnv, TxEnv, U256},
    };

    #[test]
    fn test_depanalyzer() {
        let (total_tx, total_bundle) = run_scheduler();
        assert_eq!(total_tx, 5839);
        assert_eq!(total_bundle, 242);

        let (total_tx, total_bundle) = run_serial_issuer();
        assert_eq!(total_tx, 5839);
        assert_eq!(total_bundle, 599);
    }

    #[test]
    fn test_depanalyzer_1() {
        let txs_in_from_files = read_txs_in_from_files(true);
        let tasks_in: Vec<ExeTask> = txs_in_from_files
            .iter()
            .map(|t| {
                let mut tx_access_list: Vec<(Address, Vec<U256>)> = vec![];

                let mut list: Vec<U256> = vec![];
                for addr in t.rdo_addr_list.iter() {
                    list.push(U256::from_str(addr).unwrap());
                }
                tx_access_list.push((READ_ACC, list));

                let mut list: Vec<U256> = vec![];
                for addr in t.rnw_addr_list.iter() {
                    list.push(U256::from_str(addr).unwrap());
                }
                tx_access_list.push((READ_ACC, list));

                let mut list: Vec<U256> = vec![];
                for rdo_slot in t.rdo_slot_list.iter() {
                    let hash = hasher::hash("".to_owned() + &rdo_slot.addr + &rdo_slot.index);
                    list.push(U256::from_be_bytes(hash));
                }
                tx_access_list.push((READ_SLOT, list));

                let mut list: Vec<U256> = vec![];
                for rdo_slot in t.rnw_slot_list.iter() {
                    let hash = hasher::hash("".to_owned() + &rdo_slot.addr + &rdo_slot.index);
                    list.push(U256::from_be_bytes(hash));
                }
                tx_access_list.push((WRITE_SLOT, list));

                let mut tx = TxEnv::default();
                tx.access_list = tx_access_list;
                exepipe::exetask::ExeTask::new_for_test(vec![tx])
            })
            .collect();

        let dir = "./tmp_ads";
        let _tmp_dir = TempDir::new(dir);
        let (shared_ads_wrap, tpool, sender, receiver, s, r) = generate_ads_wrap(dir);
        let mut blk_ctx = BlockContext::new(shared_ads_wrap);
        blk_ctx.start_new_block(
            Arc::new(TasksManager::new(
                (0..=tasks_in.len()).map(|_| RwLock::new(None)).collect(),
                i64::MAX,
            )),
            BlockEnv::default(),
        );

        let blk_ctx = Arc::new(blk_ctx);
        let mut scheduler = Scheduler::new(tpool, sender, blk_ctx.clone(), s.clone());
        scheduler.start_new_block(1, blk_ctx);

        let recived_count = Arc::new(Mutex::new(0));
        let executed_set = Arc::new(Mutex::new(HashSet::<i32>::new()));
        let all_done_index = Arc::new(Mutex::new(-1));
        let early_exe_map = Arc::new(Mutex::new(HashMap::<i32, i32>::new()));

        let _count = recived_count.clone();
        let _all_done_index = all_done_index.clone();
        let _early_exe_map = early_exe_map.clone();
        let _executed_set = executed_set.clone();
        let _s = s.clone();
        thread::spawn(move || {
            while let Ok(received) = receiver.recv() {
                // println!("Received: {:?}", received);

                let mut count = _count.lock().unwrap();
                *count += 1;
                _early_exe_map
                    .lock()
                    .unwrap()
                    .insert(received.my_idx, received.min_all_done_index);
                _s.send(received.my_idx).unwrap();
            }
        });

        thread::spawn(move || {
            while let Ok(received) = r.recv() {
                {
                    let mut executed_set = _executed_set.lock().unwrap();
                    executed_set.insert(received);
                }
            }
        });

        let tasks_in_in = tasks_in.len();
        let _executed_set = executed_set.clone();
        let _early_exe_map = early_exe_map.clone();
        thread::spawn(move || loop {
            let mut new_all_done = _all_done_index.lock().unwrap();
            loop {
                let mut executed_set = _executed_set.lock().unwrap();
                let existed = executed_set.remove(&(*new_all_done + 1));
                if existed {
                    *new_all_done += 1;
                    if *new_all_done == tasks_in_in as i32 - 1 {
                        println!("All done");
                        break;
                    }
                } else {
                    break;
                }

                let start = *new_all_done + 1;
                for idx in start..(*new_all_done + EARLY_EXE_WINDOW_SIZE as i32) {
                    if let Some(min_all_done) = _early_exe_map.lock().unwrap().get(&idx) {
                        if *min_all_done < start {
                            s.send(idx).unwrap();
                            let mut early_exe_map = _early_exe_map.lock().unwrap();
                            early_exe_map.remove(&idx);
                        }
                    }
                }
            }
        });

        println!("Total txs: {}", tasks_in.len());
        scheduler.add_tasks(tasks_in);
        scheduler.flush_all_bundle_tasks();

        thread::sleep(std::time::Duration::from_secs(2));

        println!("Received count: {}", recived_count.lock().unwrap());
    }
}

fn read_txs_in_from_files(ignore_eth: bool) -> Vec<Tx> {
    let mut tasks_in = vec![];

    for id in (20338810..20338850).step_by(10) {
        let mut file = OpenOptions::new()
            .read(true)
            .open(format!(
                "blocks/blocks_{}.json",
                id
            ))
            .expect("Could not read file");
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();

        let blocks: Vec<Block> = serde_json::from_str(&contents).unwrap();

        for blk in blocks {
            let coinbase = blk.coinbase.to_lowercase();
            for mut tx in blk.tx_list {
                tx.rnw_addr_list.retain(|addr| {
                    if ignore_eth {
                        addr.to_lowercase() != WETH_ADDR && addr.to_lowercase() != coinbase
                    } else {
                        true
                    }
                });
                tasks_in.push(tx);
            }
        }
    }
    tasks_in
}
