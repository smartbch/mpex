#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use exepipe::{
        bench::{
            address::{create_caller_address, create_to_address},
            erc20::{create_deploy_erc20_tx, create_transfer_token_tx},
        },
        context::BlockContext,
        exetask::ExeTask,
        test_helper::MockADS,
    };
    use mpads::tasksmanager::TasksManager;
    use revm::primitives::{
        address, keccak256, AccountInfo, BlockEnv, Bytes, TransactTo, TxEnv, KECCAK_EMPTY, U256,
    };

    #[test]
    fn test_erc20() {
        let caller = create_caller_address(1);
        let mut ads = MockADS::new();
        let from_account_info = AccountInfo {
            nonce: 0,
            balance: U256::from(1_000_000_000_000_000_000u128).clone(),
            code_hash: KECCAK_EMPTY,
            code: None,
        };
        ads.add_account(&caller, &from_account_info);
        let mut block_ctx = BlockContext::new(ads);

        // deploy erc20 contract and mint max uint256 tokens
        let created_ca_addr = address!("522B3294E6d06aA25Ad0f1B8891242E335D3B459");
        {
            let tx: TxEnv = create_deploy_erc20_tx(&caller, &created_ca_addr);
            let tx_list: Vec<TxEnv> = vec![tx];
            let task = ExeTask::new(tx_list);
            let tasks = vec![RwLock::new(Some(task)), RwLock::new(None)];
            let tasksmanager = TasksManager::new(tasks, 1);
            let env = BlockEnv::default();
            block_ctx.start_new_block(Arc::new(tasksmanager), env);
            let res = block_ctx.warmup(0);
            block_ctx.execute(0, res);
            block_ctx.end_block();

            // println!("results: {:?}", block_ctx.results);
        }

        // transfer 1 token
        {
            let dest = create_to_address(1);
            let tx = create_transfer_token_tx(&caller, &created_ca_addr, &dest, &U256::from(1));
            let tx_list = vec![tx];
            let task = ExeTask::new(tx_list);
            let tasks = vec![RwLock::new(Some(task)), RwLock::new(None)];
            let tasksmanager = TasksManager::new(tasks, 1);
            let env = BlockEnv::default();
            block_ctx.start_new_block(Arc::new(tasksmanager), env);
            let res = block_ctx.warmup(0);
            block_ctx.execute(0, res);
            block_ctx.end_block();

            // println!("----results: {:?}", block_ctx.results);
        }
    }
}
