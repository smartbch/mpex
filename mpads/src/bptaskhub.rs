use crate::changeset::ChangeSet;
use crate::def::{IN_BLOCK_IDX_BITS, IN_BLOCK_IDX_MASK};
use crate::entrycache::EntryCache;
use crate::tasksmanager::TasksManager;
use atomptr::{AtomPtr, Ref};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

pub trait Task: Send + Sync {
    fn get_change_sets(&self) -> Arc<Vec<ChangeSet>>;
}

pub trait TaskHub: Send + Sync {
    fn check_begin_end(&self, task_id: i64) -> (Option<Arc<EntryCache>>, bool);
    fn get_change_sets(&self, task_id: i64) -> Arc<Vec<ChangeSet>>;
}

pub struct BlockPairTaskHub<T: Task> {
    tasks_in_blk0: AtomPtr<Arc<TasksManager<T>>>,
    tasks_in_blk1: AtomPtr<Arc<TasksManager<T>>>,
    height0: AtomicI64,
    height1: AtomicI64,
    cache0: AtomPtr<Arc<EntryCache>>,
    cache1: AtomPtr<Arc<EntryCache>>,
}

impl<T: Task> BlockPairTaskHub<T> {
    pub fn new() -> Self {
        Self {
            tasks_in_blk0: AtomPtr::new(Arc::new(TasksManager::<T>::default())),
            tasks_in_blk1: AtomPtr::new(Arc::new(TasksManager::<T>::default())),
            height0: AtomicI64::new(-1),
            height1: AtomicI64::new(-1),
            cache0: AtomPtr::new(Arc::new(EntryCache::new_uint())),
            cache1: AtomPtr::new(Arc::new(EntryCache::new_uint())),
        }
    }

    pub fn free_slot_count(&self) -> usize {
        let mut count = 0;
        if self.height0.load(Ordering::SeqCst) < 0 {
            count += 1;
        }
        if self.height1.load(Ordering::SeqCst) < 0 {
            count += 1;
        }
        count
    }

    pub fn end_block(&self, height: i64) {
        let height0 = self.height0.load(Ordering::SeqCst);
        if height0 == height {
            self.height0.store(-1, Ordering::SeqCst);
            return;
        }
        let height1 = self.height1.load(Ordering::SeqCst);
        if height1 == height {
            self.height1.store(-1, Ordering::SeqCst);
            return;
        }
        panic!("no data found for height");
    }

    pub fn start_block(
        &self,
        height: i64,
        tasks_in_blk: Arc<TasksManager<T>>,
        cache: Arc<EntryCache>,
    ) {
        let height0 = self.height0.load(Ordering::SeqCst);
        if height0 < 0 {
            let old = self.tasks_in_blk0.swap(tasks_in_blk);
            drop(old);
            let old = self.cache0.swap(cache);
            drop(old);
            self.height0.store(height, Ordering::SeqCst);
            return;
        }
        let height1 = self.height1.load(Ordering::SeqCst);
        if height1 < 0 {
            let old = self.tasks_in_blk1.swap(tasks_in_blk);
            drop(old);
            let old = self.cache1.swap(cache);
            drop(old);
            self.height1.store(height, Ordering::SeqCst);
            return;
        }
        panic!("no data found for height");
    }
}

impl<T: Task> TaskHub for BlockPairTaskHub<T> {
    // updater in ads check this to known if a block is end.
    fn check_begin_end(&self, task_id: i64) -> (Option<Arc<EntryCache>>, bool) {
        let height0 = self.height0.load(Ordering::SeqCst);
        if height0 == (task_id >> IN_BLOCK_IDX_BITS) {
            let last_task_in_blk0 = self.tasks_in_blk0.get_ref().as_ref().get_last_task_id();
            if (task_id & IN_BLOCK_IDX_MASK) != 0 {
                return (None, last_task_in_blk0 == task_id); // not first task in block
            }
            let arc0: Ref<Arc<EntryCache>> = self.cache0.get_ref();
            let cache0: Arc<EntryCache> = Arc::clone(&arc0);
            return (Some(cache0.clone()), last_task_in_blk0 == task_id);
        }
        let height1 = self.height1.load(Ordering::SeqCst);
        if height1 == (task_id >> IN_BLOCK_IDX_BITS) {
            let last_task_in_blk1 = self.tasks_in_blk1.get_ref().as_ref().get_last_task_id();
            if (task_id & IN_BLOCK_IDX_MASK) != 0 {
                return (None, last_task_in_blk1 == task_id); // not first task in block
            }
            let arc1: Ref<Arc<EntryCache>> = self.cache1.get_ref();
            let cache1: Arc<EntryCache> = Arc::clone(&arc1);
            return (Some(cache1.clone()), last_task_in_blk1 == task_id);
        }
        panic!("no data found for height");
    }

    fn get_change_sets(&self, task_id: i64) -> Arc<Vec<ChangeSet>> {
        let height0 = self.height0.load(Ordering::SeqCst);
        if height0 == (task_id >> IN_BLOCK_IDX_BITS) {
            let idx = (task_id & IN_BLOCK_IDX_MASK) as usize;
            return self
                .tasks_in_blk0
                .get_ref()
                .as_ref()
                .get_tasks_change_sets(idx);
        }
        let height1 = self.height1.load(Ordering::SeqCst);
        if height1 == (task_id >> IN_BLOCK_IDX_BITS) {
            let idx = (task_id & IN_BLOCK_IDX_MASK) as usize;
            return self
                .tasks_in_blk1
                .get_ref()
                .as_ref()
                .get_tasks_change_sets(idx);
        }
        panic!("no data found for height");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changeset::ChangeSet;
    use crate::entrycache::EntryCache;
    use crate::test_helper::SimpleTask;
    use std::sync::{Arc, RwLock};

    #[test]
    fn test_initialize() {
        let hub: BlockPairTaskHub<SimpleTask> = BlockPairTaskHub::new();
        assert_eq!(hub.free_slot_count(), 2);
    }

    #[test]
    fn test_start_end_block() {
        let hub: BlockPairTaskHub<SimpleTask> = BlockPairTaskHub::new();
        let tasks_in_blk = Arc::new(TasksManager::default());
        let cache = Arc::new(EntryCache::new_uint());

        hub.start_block(1, tasks_in_blk.clone(), cache.clone());
        assert_eq!(hub.free_slot_count(), 1);
        assert_eq!(hub.height0.load(Ordering::SeqCst), 1);
        assert_eq!(hub.height1.load(Ordering::SeqCst), -1);
        assert_eq!(hub.tasks_in_blk0.get_ref().as_ref().get_last_task_id(), -1);
        assert_eq!(hub.tasks_in_blk1.get_ref().as_ref().get_last_task_id(), -1);

        hub.start_block(2, tasks_in_blk.clone(), cache.clone());

        hub.end_block(1);
        assert_eq!(hub.free_slot_count(), 1);

        assert!(std::panic::catch_unwind(move || {
            hub.end_block(1);
        })
        .is_err());
    }

    #[test]
    fn test_check_begin_end() {
        let hub: BlockPairTaskHub<SimpleTask> = BlockPairTaskHub::new();
        let changeset = ChangeSet::new();
        let tasks_in_blk = vec![RwLock::new(Some(SimpleTask::new(vec![changeset])))];
        let last_task_id_in_blk = 1 << IN_BLOCK_IDX_BITS;
        let tasks_manager = Arc::new(TasksManager::new(tasks_in_blk, last_task_id_in_blk));
        let cache = Arc::new(EntryCache::new_uint());

        hub.start_block(1, tasks_manager, cache.clone());

        let (cache_opt, is_end) = hub.check_begin_end((1 << IN_BLOCK_IDX_BITS) + 1);
        assert!(cache_opt.is_none());
        assert!(!is_end);

        let (cache_opt, is_end) = hub.check_begin_end(1 << IN_BLOCK_IDX_BITS);
        assert!(cache_opt.is_some());
        assert!(is_end);

        hub.end_block(1);

        assert!(std::panic::catch_unwind(move || {
            hub.check_begin_end(0);
        })
        .is_err());
    }

    #[test]
    fn test_get_change_sets() {
        let hub: BlockPairTaskHub<SimpleTask> = BlockPairTaskHub::new();
        let changeset = ChangeSet::new();
        let tasks_in_blk = vec![RwLock::new(Some(SimpleTask::new(vec![changeset])))];
        let tasks_manager = Arc::new(TasksManager::new(tasks_in_blk, 0));
        let cache = Arc::new(EntryCache::new_uint());

        hub.start_block(1, tasks_manager, cache.clone());

        let change_sets = hub.get_change_sets(1 << IN_BLOCK_IDX_BITS);
        assert_eq!(change_sets.len(), 1);

        hub.end_block(1);

        assert!(std::panic::catch_unwind(move || {
            hub.get_change_sets(0);
        })
        .is_err());
    }
}
