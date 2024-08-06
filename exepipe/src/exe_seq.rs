// use std::sync::{Arc, mpsc, RwLock};
// use std::thread;
// use revm::primitives::BlockEnv;
// use threadpool::ThreadPool;
// use mpads::AdsWrap;
// use mpads::tasksmanager::TasksManager;
// use crate::context::BlockContext;
// use crate::coordinator::Coordinator;
// use crate::exetask::ExeTask;
// use crate::scheduler::Scheduler;
// use seqads::SeqAdsWrap;
// pub struct SeqExePipe {
//     scheduler: Scheduler,
//     coordinator: Option<Coordinator>,
//     ads: SeqAdsWrap<ExeTask>,
// }
//
// impl SeqExePipe {
//     pub fn new(ads: SeqAdsWrap<ExeTask>) -> Self {
//         let blk_ctx = Arc::new(BlockContext::new(ads));
//         let tpool = Arc::new(ThreadPool::new(128));
//         let (schd_sender, schd_receiver) = mpsc::sync_channel(1024);
//         let (exec_sender, exec_recevier) = mpsc::sync_channel(8192);
//         let scheduler = Scheduler::new(
//             tpool.clone(),
//             blk_ctx.clone(),
//             schd_sender,
//             exec_sender.clone(),
//         );
//         let coordinator = Some(Coordinator::new(
//             tpool,
//             blk_ctx,
//             schd_receiver,
//             exec_sender,
//             exec_recevier,
//         ));
//         Self {
//             scheduler,
//             coordinator,
//             ads,
//         }
//     }
//
//     pub fn run_block(&mut self, tasks_in: Vec<ExeTask>, block_env: BlockEnv, height: i64) {
//         // allocate entries that will be filled by the scheduler
//         // +1 for endblock task
//         let task_list = (0..tasks_in.len() + 1).map(|_| RwLock::new(None)).collect();
//
//         // adswrap and blk_ctx will share the same last_task_id
//         let task_manager = Arc::new(TasksManager::new(task_list, i64::MAX));
//
//         self.ads.start_block(height, task_manager.clone());
//
//         // create BlockContext for every new block
//         let mut blk_ctx = BlockContext::new(self.ads.get_shared());
//         blk_ctx.start_new_block(task_manager, block_env);
//
//         let blk_ctx = Arc::new(blk_ctx);
//         self.scheduler.start_new_block(height, blk_ctx.clone());
//
//         // start coordinator to work in another thread
//         let mut coordinator = self.coordinator.take().unwrap();
//         coordinator.start_new_block(height, blk_ctx);
//         let coord_thread = thread::spawn(move || {
//             coordinator.run();
//             coordinator
//         });
//
//         self.scheduler.add_tasks(tasks_in);
//         self.scheduler.flush_all_bundle_tasks();
//
//         //wait coordinator to finish
//         self.coordinator = Some(coord_thread.join().unwrap());
//         // add endblock logic here as of all txs has done here when coordinator.run() exit.
//
//         // coordinator must exist here.
//         self.coordinator.as_mut().unwrap().end_block();
//     }
// }