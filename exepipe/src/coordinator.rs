use crate::context;
use crate::scheduler::{EarlyExeInfo, EARLY_EXE_WINDOW_SIZE};
use mpads::def::{IN_BLOCK_IDX_BITS, IN_BLOCK_IDX_MASK};
use mpads::SharedAdsWrap;
use std::collections::{HashMap, HashSet};
use std::sync::mpsc;
use std::sync::Arc;
use threadpool::ThreadPool;

type BlockContext = context::BlockContext<SharedAdsWrap>;

pub struct Coordinator {
    // scheduled-not-issued tasks:
    early_exe_map: HashMap<i32, i32>,
    // executed-not-sent2ads tasks:
    executed_set: HashSet<i32>,
    // all the tasks with id <= all_done_index were executed
    all_done_index: i32,
    height: i64,
    scheduled_chan: mpsc::Receiver<EarlyExeInfo>,
    executed_sender: mpsc::SyncSender<i32>,
    executed_receiver: mpsc::Receiver<i32>,
    tpool: Arc<ThreadPool>,
    blk_ctx: Arc<BlockContext>,
}

impl Coordinator {
    pub fn new(
        scheduled_chan: mpsc::Receiver<EarlyExeInfo>,
        tpool: Arc<ThreadPool>,
        blk_ctx: Arc<BlockContext>,
        executed_sender: mpsc::SyncSender<i32>,
        executed_receiver: mpsc::Receiver<i32>,
    ) -> Coordinator {
        Coordinator {
            early_exe_map: HashMap::new(),
            executed_set: HashSet::new(),
            all_done_index: -1,
            height: 0,
            scheduled_chan,
            executed_sender,
            executed_receiver,
            tpool,
            blk_ctx,
        }
    }

    pub fn start_new_block(&mut self, height: i64, blk_ctx: Arc<BlockContext>) {
        self.all_done_index = -1;
        self.height = height;
        self.blk_ctx = blk_ctx;
    }

    fn read_from_scheduled_chan(&mut self) {
        loop {
            if let Ok(i) = self.scheduled_chan.try_recv() {
                self.early_exe_map.insert(i.my_idx, i.min_all_done_index);
            } else {
                break;
            }
        }
    }

    fn read_from_executed_chan(&mut self) {
        loop {
            if let Ok(done_idx) = self.executed_receiver.try_recv() {
                self.executed_set.insert(done_idx);
            } else {
                break;
            }
        }
    }

    // after all_done_index is increased, try to issue new task for execution
    fn try_issue(&mut self, start: i32, end: i32) {
        for idx in start..end {
            if let Some(min_all_done) = self.early_exe_map.get(&idx) {
                if *min_all_done < start {
                    let blk_ctx = self.blk_ctx.clone();
                    let executed_sender = self.executed_sender.clone();

                    self.tpool.execute(move || {
                        blk_ctx.execute(idx as usize);
                        executed_sender.send(idx).unwrap();
                    });
                    self.early_exe_map.remove(&idx);
                }
            }
        }
    }

    pub fn run(&mut self) {
        let task_len = self.blk_ctx.tasks_manager.tasks_len();
        if task_len == 1 {
            // No task_in and only has endblock task
            return;
        }

        //polling for the tasks in block
        loop {
            self.read_from_scheduled_chan(); //non-blocking
            self.read_from_executed_chan(); //non-blocking
            let mut new_all_done = self.all_done_index;
            loop {
                let existed = self.executed_set.remove(&(new_all_done + 1));
                if existed {
                    new_all_done += 1;
                    let task_id = (self.height << IN_BLOCK_IDX_BITS) | (new_all_done as i64);
                    self.blk_ctx.send_to_ads(task_id);
                    let last_task_id = self.blk_ctx.tasks_manager.get_last_task_id();
                    // the last task is endblock task.
                    if new_all_done == ((last_task_id & IN_BLOCK_IDX_MASK) - 1) as i32 {
                        self.all_done_index = new_all_done;
                        return; // the last task is done, exit 'run' function
                    }
                } else {
                    break;
                }
            }
            self.try_issue(
                new_all_done + 1,
                new_all_done + EARLY_EXE_WINDOW_SIZE as i32,
            );
            self.all_done_index = new_all_done;
        }
    }

    pub fn end_block(&mut self) {
        self.blk_ctx.end_block();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use mpads::{tasksmanager::TasksManager, test_helper::TempDir};
    use revm::primitives::{BlockEnv, TxEnv};

    use crate::{
        context::BlockContext, coordinator::Coordinator, exetask::ExeTask, scheduler::EarlyExeInfo,
        test_helper::generate_ads_wrap,
    };

    #[test]
    #[serial_test::serial]
    fn test_new_coordinator() {
        let dir = "./tmp_ads";
        let _tmp_dir = TempDir::new(dir);
        let (shared_ads_wrap, tpool, _sender, receiver, s, r) = generate_ads_wrap(dir);
        let blk_ctx = Arc::new(BlockContext::new(shared_ads_wrap));
        let coordinator = Coordinator::new(receiver, tpool.clone(), blk_ctx.clone(), s, r);

        assert_eq!(coordinator.all_done_index, -1);
        assert_eq!(coordinator.height, 0);
        assert_eq!(coordinator.early_exe_map.len(), 0);
        assert_eq!(coordinator.executed_set.len(), 0);
    }

    #[test]
    #[serial_test::serial]
    fn test_start_new_block() {
        let dir = "./tmp_ads";
        let _tmp_dir = TempDir::new(dir);
        let (shared_ads_wrap, tpool, _sender, receiver, s, r) = generate_ads_wrap(dir);
        let blk_ctx = Arc::new(BlockContext::new(shared_ads_wrap));
        let mut coordinator = Coordinator::new(receiver, tpool.clone(), blk_ctx.clone(), s, r);

        coordinator.start_new_block(1, blk_ctx.clone());

        assert_eq!(coordinator.all_done_index, -1);
        assert_eq!(coordinator.height, 1);
        assert_eq!(Arc::ptr_eq(&coordinator.blk_ctx, &blk_ctx), true);
    }

    #[test]
    #[serial_test::serial]
    fn test_read_from_scheduled_chan() {
        let dir = "./tmp_ads";
        let _tmp_dir = TempDir::new(dir);
        let (shared_ads_wrap, tpool, sender, receiver, s, r) = generate_ads_wrap(dir);
        let blk_ctx = Arc::new(BlockContext::new(shared_ads_wrap));
        let mut coordinator = Coordinator::new(receiver, tpool.clone(), blk_ctx.clone(), s, r);

        let early_exe_info = EarlyExeInfo {
            my_idx: 1,
            min_all_done_index: 0,
        };
        sender.send(early_exe_info).unwrap();

        coordinator.read_from_scheduled_chan();

        assert_eq!(coordinator.early_exe_map.len(), 1);
        assert_eq!(*coordinator.early_exe_map.get(&1).unwrap(), 0);
    }

    #[test]
    #[serial_test::serial]
    fn test_read_from_executed_chan() {
        let dir = "./tmp_ads";
        let _tmp_dir = TempDir::new(dir);
        let (shared_ads_wrap, tpool, _sender, receiver, s, r) = generate_ads_wrap(dir);
        let blk_ctx = Arc::new(BlockContext::new(shared_ads_wrap));
        let mut coordinator = Coordinator::new(receiver, tpool.clone(), blk_ctx.clone(), s, r);

        coordinator.executed_sender.send(1).unwrap();

        coordinator.read_from_executed_chan();

        assert_eq!(coordinator.executed_set.len(), 1);
        assert_eq!(coordinator.executed_set.contains(&1), true);
    }

    #[test]
    #[serial_test::serial]
    fn test_try_issue() {
        let dir = "./tmp_ads";
        let _tmp_dir = TempDir::new(dir);
        let (shared_ads_wrap, tpool, _sender, receiver, s, r) = generate_ads_wrap(dir);
        let mut blk_ctx = BlockContext::new(shared_ads_wrap);

        let mut tasks = vec![];
        for _ in 0..3 {
            let mut tx = TxEnv::default();
            tx.access_list = vec![];
            let tx_list: Vec<TxEnv> = vec![tx];
            let task = RwLock::new(Some(ExeTask::new(tx_list)));
            {
                let mut task_opt = task.write().unwrap();
                let _task = task_opt.as_mut().unwrap();
                _task.warmup_results = vec![Option::None];
            }
            tasks.push(task);
        }
        blk_ctx.start_new_block(
            Arc::new(TasksManager::new(tasks, i64::MAX)),
            BlockEnv::default(),
        );
        let mut coordinator = Coordinator::new(receiver, tpool.clone(), Arc::new(blk_ctx), s, r);

        coordinator.early_exe_map.insert(2, 1);

        coordinator.try_issue(1, 3);
        assert_eq!(coordinator.early_exe_map.len(), 1);

        coordinator.try_issue(2, 3);
        std::thread::sleep(std::time::Duration::from_secs(1));

        assert_eq!(coordinator.early_exe_map.len(), 0);
        coordinator.read_from_executed_chan();
        // assert_eq!(coordinator.executed_set.len(), 1);
        assert_eq!(coordinator.executed_set.contains(&2), true);
    }

    #[test]
    #[serial_test::serial]
    fn test_run() {
        let dir = "./tmp_ads";
        let _tmp_dir = TempDir::new(dir);
        let (shared_ads_wrap, tpool, sender, receivers, s, r) = generate_ads_wrap(dir);
        let mut blk_ctx = BlockContext::new(shared_ads_wrap);

        let tasks_len = 2;
        let mut tasks = Vec::with_capacity(tasks_len);
        for _ in 0..tasks_len {
            let mut tx = TxEnv::default();
            tx.access_list = vec![];
            let tx_list: Vec<TxEnv> = vec![tx];
            let task = RwLock::new(Some(ExeTask::new(tx_list)));
            {
                let mut task_opt = task.write().unwrap();
                let _task = task_opt.as_mut().unwrap();
                _task.warmup_results = vec![Option::None];
            }
            tasks.push(task);
        }
        tasks.push(RwLock::new(None)); // endblock task

        blk_ctx.start_new_block(
            Arc::new(TasksManager::new(tasks, tasks_len as i64)),
            BlockEnv::default(),
        );
        let mut coordinator = Box::new(Coordinator::new(
            receivers,
            tpool.clone(),
            Arc::new(blk_ctx),
            s,
            r,
        ));
        let coordinator_p = &mut *coordinator as *mut Coordinator;

        let handle = std::thread::spawn(move || {
            coordinator.run();
        });

        for i in 0..2 {
            let early_exe_info = EarlyExeInfo {
                my_idx: i,
                min_all_done_index: i - 1,
            };
            sender.send(early_exe_info).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        handle.join().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1000));
        unsafe {
            let _coordinator = &mut (*coordinator_p);
            assert_eq!(_coordinator.all_done_index, 1);
        }
    }

    #[test]
    #[serial_test::serial]
    fn test_run_when_zero_task_in() {
        let dir = "./tmp_ads";
        let _tmp_dir = TempDir::new(dir);
        let (shared_ads_wrap, tpool, sender, receivers, s, r) = generate_ads_wrap(dir);
        let mut blk_ctx = BlockContext::new(shared_ads_wrap);

        blk_ctx.start_new_block(
            Arc::new(TasksManager::new(vec![RwLock::new(None)], i64::MAX)),
            BlockEnv::default(),
        );
        let mut coordinator = Box::new(Coordinator::new(
            receivers,
            tpool.clone(),
            Arc::new(blk_ctx),
            s,
            r,
        ));
        let handle = std::thread::spawn(move || {
            coordinator.run();
        });

        handle.join().unwrap();
    }
}
