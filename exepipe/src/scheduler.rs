use crate::context;
use crate::exetask::{AccessSet, ExeTask};
use mpads::def::IN_BLOCK_IDX_BITS;
use mpads::SharedAdsWrap;
use num_traits::Num;
use std::collections::VecDeque;
use std::sync::mpsc;
use std::sync::Arc;
use threadpool::ThreadPool;

pub const FUNC_COUNT: usize = 5; //hash function count for bloomfilter
pub const BLOOM_BITS_SHIFT: u64 = 11; // 11*5 = 55 < 64
pub const BLOOM_BITS: u64 = 1 << BLOOM_BITS_SHIFT; //bit count in one bloomfilter
pub const BLOOM_BITS_MASK: u64 = BLOOM_BITS - 1;
pub const SET_MAX_SIZE: usize = BLOOM_BITS as usize / 8;
pub const MAX_TASKS_LEN_IN_BUNDLE: usize = 64;
pub const MIN_TASKS_LEN_IN_BUNDLE: usize = 8;
pub const BUNDLE_COUNT: usize = 64;
pub const EARLY_EXE_WINDOW_SIZE: usize = 128;

pub type BlockContext = context::BlockContext<SharedAdsWrap>;

pub trait PBElement:
    Num
    + std::marker::Copy
    + std::ops::BitAnd<Output = Self>
    + std::ops::BitOr<Output = Self>
    + std::ops::Not<Output = Self>
    + std::ops::Shl<usize, Output = Self>
{
    const BITS: u32;
}

impl PBElement for u16 {
    const BITS: u32 = u16::BITS;
}
impl PBElement for u32 {
    const BITS: u32 = u32::BITS;
}
impl PBElement for u64 {
    const BITS: u32 = u64::BITS;
}
impl PBElement for u128 {
    const BITS: u32 = u128::BITS;
}

// 64 BloomFilters in parallel
pub struct ParaBloom<T: PBElement> {
    rdo_arr: [T; BLOOM_BITS as usize],
    rnw_arr: [T; BLOOM_BITS as usize],
    rdo_set_size: Vec<usize>,
    rnw_set_size: Vec<usize>,
}

type ParaBloom64 = ParaBloom<u64>;

impl<T: PBElement> ParaBloom<T> {
    pub fn new() -> ParaBloom<T> {
        let mut res = ParaBloom::<T> {
            rdo_arr: [T::zero(); BLOOM_BITS as usize],
            rnw_arr: [T::zero(); BLOOM_BITS as usize],
            rdo_set_size: Vec::with_capacity(T::BITS as usize),
            rnw_set_size: Vec::with_capacity(T::BITS as usize),
        };
        for _ in 0..T::BITS {
            res.rdo_set_size.push(0);
            res.rnw_set_size.push(0);
        }
        res
    }

    fn get_rdo_mask(&self, mut k64: u64) -> T {
        let mut rdo_mask = !T::zero(); // all-ones
        for _ in 0..FUNC_COUNT {
            let idx = (k64 & BLOOM_BITS_MASK) as usize;
            rdo_mask = rdo_mask & self.rdo_arr[idx]; //bitwise-and
            k64 >>= BLOOM_BITS_SHIFT;
        }
        rdo_mask
    }

    fn get_rnw_mask(&self, mut k64: u64) -> T {
        let mut rnw_mask = !T::zero(); // all-ones
        for _ in 0..FUNC_COUNT {
            let idx = (k64 & BLOOM_BITS_MASK) as usize;
            rnw_mask = rnw_mask & self.rnw_arr[idx]; //bitwise-and
            k64 >>= BLOOM_BITS_SHIFT;
        }
        rnw_mask
    }

    fn add_rdo_k64(&mut self, id: usize, mut k64: u64) {
        let target_bit: T = T::one() << id;
        for _ in 0..FUNC_COUNT {
            let idx = (k64 & BLOOM_BITS_MASK) as usize;
            self.rdo_arr[idx] = self.rdo_arr[idx] | target_bit;
            k64 >>= BLOOM_BITS_SHIFT;
        }
        self.rdo_set_size[id] += 1;
    }

    fn add_rnw_k64(&mut self, id: usize, mut k64: u64) {
        let target_bit: T = T::one() << id;
        for _ in 0..FUNC_COUNT {
            let idx = (k64 & BLOOM_BITS_MASK) as usize;
            self.rnw_arr[idx] = self.rnw_arr[idx] | target_bit;
            k64 >>= BLOOM_BITS_SHIFT;
        }
        self.rnw_set_size[id] += 1;
    }

    pub fn get_rdo_set_size(&self, id: usize) -> usize {
        return self.rdo_set_size[id];
    }

    pub fn get_rnw_set_size(&self, id: usize) -> usize {
        return self.rnw_set_size[id];
    }

    pub fn clear(&mut self, id: usize) {
        let keep_mask = !(T::one() << id); //clear 'id', keep other bits
        for idx in 0..(BLOOM_BITS as usize) {
            self.rdo_arr[idx] = self.rdo_arr[idx] & keep_mask;
            self.rnw_arr[idx] = self.rnw_arr[idx] & keep_mask;
        }
        self.rdo_set_size[id] = 0;
        self.rnw_set_size[id] = 0;
    }

    pub fn clear_all(&mut self) {
        for idx in 0..(BLOOM_BITS as usize) {
            self.rdo_arr[idx] = T::zero();
            self.rnw_arr[idx] = T::zero();
        }
        for id in 0..(T::BITS as usize) {
            self.rdo_set_size[id] = 0;
            self.rnw_set_size[id] = 0;
        }
    }

    pub fn get_dep_mask(&self, access_set: &AccessSet) -> T {
        let mut mask = T::zero();
        // other.rdo vs self.rnw
        for &k64 in access_set.rdo_k64_vec.iter() {
            mask = mask | self.get_rnw_mask(k64);
        }
        // others.rnw vs self.rdo+self.rnw
        for &k64 in access_set.rnw_k64_vec.iter() {
            mask = mask | self.get_rdo_mask(k64);
            mask = mask | self.get_rnw_mask(k64);
        }
        mask
    }

    pub fn add(&mut self, id: usize, access_set: &AccessSet) {
        for &k64 in access_set.rdo_k64_vec.iter() {
            self.add_rdo_k64(id, k64);
        }
        for &k64 in access_set.rnw_k64_vec.iter() {
            self.add_rnw_k64(id, k64);
        }
    }
}

#[derive(Debug)]
pub struct EarlyExeInfo {
    pub my_idx: i32,
    pub min_all_done_index: i32, // task with my_idx does not collide with all task before min_all_done_index.
}

pub struct Scheduler {
    // following three fields need to be updated every block
    height: i64,
    blk_ctx: Arc<BlockContext>,

    pb: ParaBloom64,
    bundles: Vec<VecDeque<ExeTask>>,
    fail_vec: Vec<ExeTask>,
    out_idx: usize, // when flush bundle, pop the task in self.bundles to self.blk_ctx.tasks_manager.tasks, and make self.out_idx++ to specific newest task index in self.blk_ctx.tasks_manager.tasks
    tpool: Arc<ThreadPool>, // thread pool for task execute and task prepare.
    scheduled_chan: mpsc::SyncSender<EarlyExeInfo>, // send eei (not executed task) to coordinator.run() for task execute.
    executed_sender: mpsc::SyncSender<i32>, // send already executed task in scheduled period to coordinator.run()
}

impl Scheduler {
    pub fn new(
        tpool: Arc<ThreadPool>,
        scheduled_chan: mpsc::SyncSender<EarlyExeInfo>,
        blk_ctx: Arc<BlockContext>,
        executed_sender: mpsc::SyncSender<i32>,
    ) -> Scheduler {
        let mut bundles = Vec::with_capacity(BUNDLE_COUNT);
        for _ in 0..BUNDLE_COUNT {
            bundles.push(VecDeque::with_capacity(MAX_TASKS_LEN_IN_BUNDLE));
        }
        Scheduler {
            height: 0,
            blk_ctx,
            pb: ParaBloom64::new(),
            bundles,
            fail_vec: Vec::new(),
            out_idx: 0,
            tpool,
            scheduled_chan,
            executed_sender,
        }
    }

    pub fn start_new_block(&mut self, height: i64, blk_ctx: Arc<BlockContext>) {
        self.height = height;
        self.blk_ctx = blk_ctx;
        for i in 0..BUNDLE_COUNT {
            self.bundles[i].clear();
        }
        self.pb.clear_all();
        self.fail_vec.clear();
        self.out_idx = 0;
    }

    fn first_can_flush_bundle(&self) -> usize {
        for i in 0..BUNDLE_COUNT {
            if self.bundles[i].len() > MIN_TASKS_LEN_IN_BUNDLE {
                return i;
            }
        }
        BUNDLE_COUNT // cannot find a large enough bundle
    }

    pub fn add_tasks(&mut self, tasks: Vec<ExeTask>) {
        let mut task_len = tasks.len();
        for task in tasks {
            task_len -= 1;
            self.add_task(task, task_len == 0);
        }
    }

    fn add_task(&mut self, task: ExeTask, is_last_task: bool) {
        let mask = self.pb.get_dep_mask(&task.access_set);
        let mut bundle_id = mask.trailing_ones() as usize;
        // if we cannot find a bundle to insert task because
        // it collides with all the bundles
        if bundle_id == BUNDLE_COUNT {
            bundle_id = self.first_can_flush_bundle();
            if bundle_id < BUNDLE_COUNT {
                // if we find a large enough bundle, flush it
                self.pb.clear(bundle_id);
                self.flush_bundle(bundle_id);
            } else {
                // no large enough bundle to flush, so task fails
                self.fail_vec.push(task);
                return;
            }
        }

        // now the task can be inserted into a bundle
        self.pb.add(bundle_id, &task.access_set);

        let target = self.bundles.get_mut(bundle_id).unwrap();
        target.push_back(task);

        // ensuring bundle still has tasks when execute flush_all_bundle_tasks
        if is_last_task {
            return;
        }

        // flush the bundle if it's large enough
        if self.pb.get_rdo_set_size(bundle_id) > SET_MAX_SIZE
            || self.pb.get_rnw_set_size(bundle_id) > SET_MAX_SIZE
            || target.len() >= MAX_TASKS_LEN_IN_BUNDLE
        {
            self.pb.clear(bundle_id);
            self.flush_bundle(bundle_id);
        }
    }

    pub fn flush_all_bundle_tasks(&mut self) {
        let last_idx = (self.blk_ctx.tasks_manager.tasks_len() - self.fail_vec.len() - 1) as i64;
        let last_task_id = (self.height << IN_BLOCK_IDX_BITS) | last_idx;
        //update last_task_id such that blk_ctx and task_hub can read it
        self.blk_ctx.tasks_manager.set_last_task_id(last_task_id);
        for id in 0..BUNDLE_COUNT {
            self.flush_bundle(id);
        }
    }

    fn flush_bundle(&mut self, bundle_id: usize) {
        let target = self.bundles.get_mut(bundle_id).unwrap();
        let task_out_start = self.out_idx;
        while target.len() != 0 {
            {
                // move task from the target bundle to tasks_manager
                let task = target.pop_front().unwrap();
                task.set_task_out_start(task_out_start);
                // append task to out_vec
                self.blk_ctx.tasks_manager.set_task(self.out_idx, task);
            }
            let ctx = self.blk_ctx.clone();
            let task_idx = self.out_idx;
            if task_out_start == 0 {
                // if it's in the first bundle, blk_ctx run it immediately
                let executed_sender = self.executed_sender.clone();
                self.tpool.execute(move || {
                    ctx.warmup(task_idx);
                    ctx.execute(task_idx);
                    executed_sender.send(task_idx as i32).unwrap();
                });
            } else {
                let scheduled_chan = self.scheduled_chan.clone();
                // after prepare_task, the task will be issued by Coordinator
                self.tpool.execute(move || {
                    prepare_task_and_send_eei(task_idx, task_out_start, scheduled_chan, ctx);
                });
            }
            self.out_idx += 1;
        }
    }
}

fn prepare_task_and_send_eei(
    my_idx: usize,
    task_out_start: usize,
    scheduled_chan: mpsc::SyncSender<EarlyExeInfo>,
    blk_ctx: Arc<BlockContext>,
) {
    let mut stop_detect = 0;
    if task_out_start > EARLY_EXE_WINDOW_SIZE {
        stop_detect = task_out_start - EARLY_EXE_WINDOW_SIZE;
    }
    blk_ctx.warmup(my_idx);
    let mut min_all_done_index = {
        if stop_detect == 0 {
            0
        } else {
            stop_detect - 1
        }
    };

    let mut task_opt = blk_ctx.tasks_manager.task_for_write(my_idx);
    let task = task_opt.as_mut().unwrap();
    // find the smallest early_idx
    for early_idx in (stop_detect..task_out_start).rev() {
        let other_opt = blk_ctx.tasks_manager.task_for_read(early_idx);
        let other = other_opt.as_ref().unwrap();
        // stop loop when we find a colliding peer
        if ExeTask::has_collision(&task, other) {
            min_all_done_index = early_idx;
            break;
        }
    }
    task.set_min_all_done_index(min_all_done_index);
    let eei = EarlyExeInfo {
        my_idx: my_idx as i32,
        min_all_done_index: min_all_done_index as i32,
    };
    scheduled_chan.send(eei).unwrap();
}

#[cfg(test)]
mod tests {
    use serial_test::serial;
    use std::{
        sync::{Mutex, RwLock},
        thread,
    };

    use mpads::{tasksmanager::TasksManager, test_helper::TempDir};
    use revm::primitives::{alloy_primitives::U160, Address, BlockEnv, TransactTo, TxEnv, U256};

    use crate::{
        exetask::{READ_ACC, WRITE_ACC},
        test_helper::generate_ads_wrap,
    };

    use super::*;

    #[test]
    fn test_parabloom_new() {
        let pb = ParaBloom64::new();

        assert_eq!(pb.rdo_arr.len(), BLOOM_BITS as usize);
        assert_eq!(pb.rnw_arr.len(), BLOOM_BITS as usize);
        assert_eq!(pb.rdo_set_size.len(), BUNDLE_COUNT);
        assert_eq!(pb.rnw_set_size.len(), BUNDLE_COUNT);
        assert!(pb.rdo_arr.iter().all(|&x| x == 0));
        assert!(pb.rnw_arr.iter().all(|&x| x == 0));
        assert!(pb.rdo_set_size.iter().all(|&x| x == 0));
        assert!(pb.rnw_set_size.iter().all(|&x| x == 0));
    }

    #[test]
    fn test_parabloom_add_and_get_rdo_mask() {
        let mut pb = ParaBloom64::new();
        let id: usize = 20;
        let key: u64 = 0xFF00_FF00_FF00_FF00;

        pb.add_rdo_k64(id, key);
        let rdo_mask = pb.get_rdo_mask(key);

        assert_eq!(rdo_mask, 1u64 << id);
        assert_eq!(pb.get_rdo_set_size(id), 1);
        assert_eq!(pb.get_rnw_set_size(id), 0);
    }

    #[test]
    fn test_parabloom_add_and_get_rnw_mask() {
        let mut pb = ParaBloom64::new();
        let id: usize = 20;
        let key: u64 = 0xFF00_FF00_FF00_FF00;

        pb.add_rnw_k64(id, key);
        let rnw_mask = pb.get_rnw_mask(key);

        assert_eq!(rnw_mask, 1u64 << id);
        assert_eq!(pb.get_rdo_set_size(id), 0);
        assert_eq!(pb.get_rnw_set_size(id), 1);
    }

    #[test]
    fn test_parabloom_clear() {
        let mut pb = ParaBloom64::new();
        let key: u64 = 12345;
        let id: usize = 1;

        pb.add_rdo_k64(id, key);
        pb.add_rnw_k64(id, key);

        pb.clear(id);

        assert!(pb.rdo_arr.iter().all(|&x| x == 0));
        assert!(pb.rnw_arr.iter().all(|&x| x == 0));
        assert_eq!(pb.get_rdo_set_size(id), 0);
        assert_eq!(pb.get_rnw_set_size(id), 0);
    }

    #[test]
    #[serial]
    fn test_parabloom_clear_all() {
        let mut pb = ParaBloom64::new();
        let key: u64 = 12345;
        let id: usize = 1;

        pb.add_rdo_k64(id, key);
        pb.add_rnw_k64(id, key);

        pb.clear_all();

        assert!(pb.rdo_arr.iter().all(|&x| x == 0));
        assert!(pb.rnw_arr.iter().all(|&x| x == 0));
        assert!(pb.rdo_set_size.iter().all(|&x| x == 0));
        assert!(pb.rnw_set_size.iter().all(|&x| x == 0));
    }

    #[test]
    #[serial]
    fn test_scheduler_new() {
        let dir = "./tmp_ads";
        let _tmp_dir = TempDir::new(dir);
        let (shared_ads_wrap, tpool, sender, _, s, _) = generate_ads_wrap(dir);
        let blk_ctx = Arc::new(BlockContext::new(shared_ads_wrap));
        let scheduler = Scheduler::new(tpool, sender, blk_ctx.clone(), s);

        assert_eq!(scheduler.height, 0);
        assert_eq!(scheduler.out_idx, 0);
        assert_eq!(scheduler.bundles.len(), BUNDLE_COUNT);
        assert_eq!(scheduler.blk_ctx.tasks_manager.get_last_task_id(), -1);
        assert!(scheduler.fail_vec.is_empty());
    }

    #[test]
    #[serial]
    fn test_start_new_block() {
        let dir = "./tmp_ads";
        let _tmp_dir = TempDir::new(dir);
        let (shared_ads_wrap, tpool, sender, _, s, _) = generate_ads_wrap(dir);
        let blk_ctx = Arc::new(BlockContext::new(shared_ads_wrap));
        let mut scheduler = Scheduler::new(tpool, sender, blk_ctx.clone(), s);

        scheduler.start_new_block(1, blk_ctx.clone());

        assert_eq!(scheduler.height, 1);
        assert!(scheduler.fail_vec.is_empty());
        assert!(scheduler.bundles.iter().all(|v| v.is_empty()));
        assert_eq!(scheduler.out_idx, 0);
    }

    #[test]
    #[serial]
    fn test_add_task_not_collide() {
        let dir = "./tmp_ads";
        let _tmp_dir = TempDir::new(dir);
        let (shared_ads_wrap, tpool, sender, _, s, _) = generate_ads_wrap(dir);
        let blk_ctx = Arc::new(BlockContext::new(shared_ads_wrap));
        let mut scheduler = Scheduler::new(tpool, sender, blk_ctx.clone(), s);

        scheduler.start_new_block(1, blk_ctx.clone());

        {
            let mut tx = TxEnv::default();
            tx.caller = Address::from_slice(&U160::from(0).as_le_slice());
            tx.transact_to = TransactTo::create();
            let tx_list: Vec<TxEnv> = vec![tx];
            let task = ExeTask::new(tx_list);
            scheduler.add_task(task, false);
        }
        assert_eq!(scheduler.bundles[0].len(), 1);

        {
            let mut tx = TxEnv::default();
            tx.caller = Address::from_slice(&U160::from(1).as_le_slice());
            tx.transact_to = TransactTo::create();
            let tx_list: Vec<TxEnv> = vec![tx];
            let task = ExeTask::new(tx_list);
            scheduler.add_task(task, false);
        }

        assert_eq!(scheduler.bundles[0].len(), 2);
    }

    #[test]
    #[serial]
    fn test_add_task_collide() {
        let dir = "./tmp_ads";
        let _tmp_dir = TempDir::new(dir);
        let (shared_ads_wrap, tpool, sender, _, s, _) = generate_ads_wrap(dir);
        let blk_ctx = Arc::new(BlockContext::new(shared_ads_wrap));
        let mut scheduler = Scheduler::new(tpool, sender, blk_ctx.clone(), s);
        scheduler.start_new_block(1, blk_ctx.clone());

        {
            let mut tx = TxEnv::default();
            tx.caller = Address::from_slice(&U160::from(0).as_le_slice());
            tx.transact_to = TransactTo::create();
            let tx_list: Vec<TxEnv> = vec![tx];
            let task = ExeTask::new(tx_list);
            scheduler.add_task(task, false);
        }
        assert_eq!(scheduler.bundles[0].len(), 1);
        assert_eq!(scheduler.bundles[1].len(), 0);

        {
            let mut tx = TxEnv::default();
            tx.caller = Address::from_slice(&U160::from(0).as_le_slice());
            tx.transact_to = TransactTo::create();
            let tx_list: Vec<TxEnv> = vec![tx];
            let task = ExeTask::new(tx_list);
            scheduler.add_task(task, false);
        }
        assert_eq!(scheduler.bundles[0].len(), 1);
        assert_eq!(scheduler.bundles[1].len(), 1);
    }

    #[test]
    #[serial]
    fn test_add_task_collide_with_all_bundls() {
        let dir = "./tmp_ads";
        let _tmp_dir = TempDir::new(dir);
        let (shared_ads_wrap, tpool, sender, receiver, s, r) = generate_ads_wrap(dir);

        let mut blk_ctx = BlockContext::new(shared_ads_wrap);
        let tasks = (0..100).map(|_| RwLock::new(None)).collect::<Vec<_>>();
        blk_ctx.start_new_block(
            Arc::new(TasksManager::new(tasks, i64::MAX)),
            BlockEnv::default(),
        );

        let blk_ctx = Arc::new(blk_ctx);
        let mut scheduler = Scheduler::new(tpool, sender, blk_ctx.clone(), s);
        scheduler.start_new_block(1, blk_ctx);

        for _ in 0..BUNDLE_COUNT {
            let mut tx = TxEnv::default();
            tx.access_list = vec![(WRITE_ACC, vec![U256::ZERO])];
            let tx_list: Vec<TxEnv> = vec![tx];
            let task = ExeTask::new_for_test(tx_list);
            scheduler.add_task(task, false);
        }

        assert_eq!(scheduler.bundles[63].len(), 1);

        let mut tx = TxEnv::default();
        tx.access_list = vec![(WRITE_ACC, vec![U256::ZERO])];
        let tx_list: Vec<TxEnv> = vec![tx];
        let task = ExeTask::new_for_test(tx_list);
        scheduler.add_task(task, false);
        // no large enough bundle to flush, so task fails
        assert_eq!(scheduler.fail_vec.len(), 1);

        for i in 1..=MIN_TASKS_LEN_IN_BUNDLE {
            let mut tx = TxEnv::default();
            tx.access_list = vec![(WRITE_ACC, vec![U256::from(i)])];
            let tx_list: Vec<TxEnv> = vec![tx];
            let task = ExeTask::new_for_test(tx_list);
            scheduler.add_task(task, false);
        }
        assert_eq!(scheduler.pb.get_rdo_set_size(0), 0);
        assert_eq!(
            scheduler.pb.get_rnw_set_size(0),
            MIN_TASKS_LEN_IN_BUNDLE + 1
        );
        assert_eq!(scheduler.out_idx, 0);

        let mut tx = TxEnv::default();
        tx.access_list = vec![(WRITE_ACC, vec![U256::ZERO])];
        let tx_list: Vec<TxEnv> = vec![tx];
        let task = ExeTask::new_for_test(tx_list);
        // flush a large enough bundle
        scheduler.add_task(task, false);
        thread::sleep(std::time::Duration::from_secs(1));
        // flush_bundle:: if it's in the first bundle, blk_ctx run it immediately
        assert_eq!(scheduler.pb.get_rdo_set_size(0), 0);
        assert_eq!(scheduler.pb.get_rnw_set_size(0), 1);
        assert_eq!(scheduler.out_idx, MIN_TASKS_LEN_IN_BUNDLE + 1);

        // target.len() >= MAX_TASKS_LEN_IN_BUNDLE
        //flush the bundle if it's large enough
        // flush_bundle: after prepare_task, the task will be issued by Coordinator
        for i in 1..MAX_TASKS_LEN_IN_BUNDLE {
            let mut tx = TxEnv::default();
            tx.access_list = vec![(READ_ACC, vec![U256::from(i)])];
            let tx_list: Vec<TxEnv> = vec![tx];
            let task = ExeTask::new_for_test(tx_list);
            scheduler.add_task(task, false);
        }
        thread::sleep(std::time::Duration::from_secs(1));

        let recived_count = Arc::new(Mutex::new(0));
        let _count = recived_count.clone();
        thread::spawn(move || {
            while let Ok(received) = receiver.recv() {
                assert!(received.my_idx >= 9);
                if received.my_idx <= 17 {
                    assert_eq!(
                        received.my_idx - received.min_all_done_index,
                        MIN_TASKS_LEN_IN_BUNDLE as i32 + 1
                    );
                } else {
                    assert_eq!(received.min_all_done_index, 0);
                }
                let mut count = _count.lock().unwrap();
                *count += 1;
            }
        });

        thread::sleep(std::time::Duration::from_secs(1));

        let task = scheduler.blk_ctx.tasks_manager.task_for_read(17);

        let t = task.as_ref().unwrap();
        assert_eq!(t.get_min_all_done_index(), 8);
        assert_eq!(t.get_task_out_start(), 9);

        assert_eq!(*recived_count.lock().unwrap(), 64);
        assert_eq!(scheduler.pb.get_rdo_set_size(0), 0);
        assert_eq!(scheduler.pb.get_rnw_set_size(0), 0);
        assert_eq!(scheduler.out_idx, 73);
        assert_eq!(scheduler.bundles[0].len(), 0);
    }

    #[test]
    #[serial]
    fn test_flush_all_bundle_tasks() {
        let dir = "./tmp_ads";
        let _tmp_dir = TempDir::new(dir);
        let (shared_ads_wrap, tpool, sender, _receiver, s, r) = generate_ads_wrap(dir);

        let mut blk_ctx = BlockContext::new(shared_ads_wrap);
        let tasks = (0..100).map(|_| RwLock::new(None)).collect::<Vec<_>>();
        blk_ctx.start_new_block(
            Arc::new(TasksManager::new(tasks, i64::MAX)),
            BlockEnv::default(),
        );

        let blk_ctx = Arc::new(blk_ctx);
        let mut scheduler = Scheduler::new(tpool, sender, blk_ctx.clone(), s);
        scheduler.start_new_block(1, blk_ctx);

        let mut tx = TxEnv::default();
        tx.caller = Address::from_slice(&U160::from(0).as_le_slice());
        tx.transact_to = TransactTo::create();
        let tx_list: Vec<TxEnv> = vec![tx];
        let task = ExeTask::new(tx_list);
        scheduler.add_task(task, false);

        scheduler.flush_all_bundle_tasks();

        let executed_task_id = r.recv().unwrap();
        assert_eq!(executed_task_id, 0);

        assert_eq!(
            scheduler.blk_ctx.tasks_manager.get_last_task_id(),
            (1 << IN_BLOCK_IDX_BITS) + 100 - 1
        );
        assert_eq!(scheduler.out_idx, 1);
    }

    #[test]
    fn test_add_tasks() {
        let dir = "./tmp_ads";
        let _tmp_dir = TempDir::new(dir);
        let (shared_ads_wrap, tpool, sender, _receiver, s, r) = generate_ads_wrap(dir);

        let mut blk_ctx = BlockContext::new(shared_ads_wrap);
        let tasks = (0..100).map(|_| RwLock::new(None)).collect::<Vec<_>>();
        blk_ctx.start_new_block(
            Arc::new(TasksManager::new(tasks, i64::MAX)),
            BlockEnv::default(),
        );

        let blk_ctx = Arc::new(blk_ctx);
        let mut scheduler = Scheduler::new(tpool, sender, blk_ctx.clone(), s);
        scheduler.start_new_block(1, blk_ctx);

        let task_in: Vec<ExeTask> = (0..MAX_TASKS_LEN_IN_BUNDLE)
            .map(|_| {
                let tx = TxEnv::default();
                let tx_list: Vec<TxEnv> = vec![tx];
                ExeTask::new_for_test(tx_list)
            })
            .collect();
        scheduler.add_tasks(task_in);

        // ensuring bundle still has tasks when execute flush_all_bundle_tasks
        let bundle = &scheduler.bundles[0];
        assert_eq!(bundle.len(), MAX_TASKS_LEN_IN_BUNDLE);

        scheduler.flush_all_bundle_tasks();

        let bundle = &scheduler.bundles[0];
        assert_eq!(bundle.len(), 0);
    }
}
