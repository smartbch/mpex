extern crate core;

pub mod context;
pub mod coordinator;
pub mod exetask;
pub mod scheduler;
pub mod statecache;
pub mod test_helper;
pub mod utils;

use crate::context::BlockContext;
use crate::coordinator::Coordinator;
use crate::exetask::ExeTask;
use crate::scheduler::Scheduler;
use mpads::tasksmanager::TasksManager;
use mpads::{ADS, AdsWrap};
use revm::precompile::primitives::BlockEnv;
use std::sync::mpsc;
use std::sync::{Arc, RwLock};
use std::thread;
use threadpool::ThreadPool;

pub struct ExePipe {
    scheduler: Scheduler,
    coordinator: Option<Coordinator>,
    ads: AdsWrap<ExeTask>,
}

impl ExePipe {
    pub fn new(ads: AdsWrap<ExeTask>) -> Self {
        let blk_ctx = Arc::new(BlockContext::new(ads.get_shared()));
        let tpool = Arc::new(ThreadPool::new(128));
        let (schd_sender, schd_receiver) = mpsc::sync_channel(1024);
        let (exec_sender, exec_recevier) = mpsc::sync_channel(8192);
        let scheduler = Scheduler::new(
            tpool.clone(),
            schd_sender,
            blk_ctx.clone(),
            exec_sender.clone(),
        );
        let coordinator = Some(Coordinator::new(
            schd_receiver,
            tpool,
            blk_ctx,
            exec_sender,
            exec_recevier,
        ));
        Self {
            scheduler,
            coordinator,
            ads,
        }
    }

    pub fn run_block(&mut self, tasks_in: Vec<ExeTask>, block_env: BlockEnv, height: i64) {
        // allocate entries that will be filled by the scheduler
        let task_list = (0..tasks_in.len() + 1).map(|_| RwLock::new(None)).collect();

        // adswrap and blk_ctx will share the same last_task_id
        let task_manager = Arc::new(TasksManager::new(task_list, i64::MAX));

        self.ads.start_block(height, task_manager.clone());

        // create BlockContext for every new block
        let mut blk_ctx = BlockContext::new(self.ads.get_shared());
        blk_ctx.start_new_block(task_manager, block_env);

        let blk_ctx = Arc::new(blk_ctx);
        self.scheduler.start_new_block(height, blk_ctx.clone());

        // start coordinator to work in another thread
        let mut coordinator = self.coordinator.take().unwrap();
        coordinator.start_new_block(height, blk_ctx);
        let coord_thread = thread::spawn(move || {
            coordinator.run();
            coordinator
        });

        self.scheduler.add_tasks(tasks_in);
        self.scheduler.flush_all_bundle_tasks();

        //wait coordinator to finish
        self.coordinator = Some(coord_thread.join().unwrap());
        // add endblock logic here as of all txs has done here when coordinator.run() exit.
        self.end_block();
    }

    fn end_block(&mut self) {
        // coordinator must exist here.
        self.coordinator.as_mut().unwrap().end_block();
    }
}

#[cfg(test)]
mod test_exe_pipe {
    use crate::exetask::test_exe_task::build_access_list;
    use crate::exetask::ExeTask;
    use crate::ExePipe;
    use mpads::{AdsCore, AdsWrap};
    use revm::primitives::{BlockEnv, TxEnv, U256};
    use std::fs::{create_dir_all, remove_dir_all};

    #[test]
    fn test_run_block() {
        let dir = "test_exe_pipe";
        let buffer_size = 8192;
        let block_size = 8192;
        create_dir_all(dir).unwrap();
        AdsCore::init_dir(dir, block_size);
        let ads_exe_task = AdsWrap::<ExeTask>::new(dir, buffer_size, block_size);
        let mut pipe = ExePipe::new(ads_exe_task);

        let mut tx = TxEnv::default();
        let mut access_list = vec![];
        build_access_list(&mut access_list);
        tx.access_list = access_list;
        let task = ExeTask::new(vec![tx]);
        let task_in = vec![task];
        let mut block_env = BlockEnv::default();
        block_env.number = U256::from(1);

        pipe.run_block(task_in, block_env.clone(), 1);
        remove_dir_all(dir).unwrap();
    }
}
