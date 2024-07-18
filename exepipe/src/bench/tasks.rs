use revm::primitives::{Address, U256};

use crate::exetask::ExeTask;

use super::erc20::create_transfer_token_tx;

// pub fn create_task(caller: &Address, txs_count: usize) -> ExeTask {
//     let mut txs = Vec::with_capacity(txs_count);
//     txs.push(create_transfer_token_tx(
//         caller,
//         &Address::ZERO,
//         U256::from(100),
//     ));
//     ExeTask::new(txs)
// }
