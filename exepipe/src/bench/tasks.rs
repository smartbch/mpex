use std::collections::HashMap;

use revm::primitives::{Address, U256};

use crate::{exetask::ExeTask, scheduler::MIN_TASKS_LEN_IN_BUNDLE};

use super::{address::create_caller_address, erc20::create_transfer_token_tx};

fn get_addr_and_slot_index(
    slotmap: &HashMap<Address, U256>,
    addr_index: usize, // start from 1
) -> (Address, U256) {
    let caller = create_caller_address(addr_index);
    (caller, slotmap.get(&caller).unwrap().clone())
}

// fn create_task(caller: &Address, ca_addr: &Address, task_len: u64) -> Vec<ExeTask> {
//     let mut tasks = Vec::with_capacity(task_len as usize);
//     for _ in 0..task_len {
//         let tx = create_transfer_token_tx(&caller, &ca_addr, &Address::ZERO, &U256::from(1));
//         let tx_list = vec![tx];
//         let task = ExeTask::new(tx_list);
//         tasks.push(task);
//     }
//     tasks
// }

pub fn create_non_colliding_tasks(
    slotmap: &HashMap<Address, U256>,
    ca_addr: &Address,
    txs_len_in_task: usize, // 20
    tasks_len: usize,       // 500
) -> Vec<ExeTask> {
    let max_addrs_len: usize = slotmap.len();
    if txs_len_in_task * tasks_len > max_addrs_len / 2 {
        panic!("task_txs_len * tasks_len > max_addrs_len / 2 ");
    }
    let mut caller_start_index = 1 as usize;
    let mut tasks = Vec::with_capacity(tasks_len);
    for _ in 0..tasks_len {
        let mut txs = Vec::with_capacity(txs_len_in_task);
        let caller = create_caller_address(caller_start_index);
        for _ in 0..txs_len_in_task {
            let tx = create_transfer_token_tx(
                &caller,
                ca_addr,
                &create_caller_address(caller_start_index),
                &U256::from(1),
                &slotmap,
            );
            caller_start_index += 1;
            txs.push(tx);
        }
        let task = ExeTask::new(txs);
        tasks.push(task);
    }
    tasks
}

pub fn create_colliding_tasks(
    slotmap: &HashMap<Address, U256>,
    ca_addr: &Address,
    txs_len_in_task: usize, // 20
    tasks_len: usize,       // 500
) -> Vec<ExeTask> {
    let max_addrs_len: usize = slotmap.len();
    if txs_len_in_task * tasks_len > max_addrs_len / 2 {
        panic!("task_txs_len * tasks_len > max_addrs_len / 2 ");
    }
    let mut caller_start_index = 1 as usize;
    let mut tasks = Vec::with_capacity(tasks_len);
    for _ in 0..tasks_len {
        let mut txs = Vec::with_capacity(txs_len_in_task);
        let caller = create_caller_address(caller_start_index % (MIN_TASKS_LEN_IN_BUNDLE + 1) + 1);
        for _ in 0..txs_len_in_task {
            let tx = create_transfer_token_tx(
                &caller,
                ca_addr,
                &create_caller_address(caller_start_index),
                &U256::from(1),
                &slotmap,
            );
            caller_start_index += 1;
            txs.push(tx);
        }
        let task = ExeTask::new(txs);
        tasks.push(task);
    }
    tasks
}
