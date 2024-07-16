use byteorder::{BigEndian, ByteOrder};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::SyncSender;
use std::sync::Arc;
use threadpool::ThreadPool;

use crate::bptaskhub::TaskHub;
use crate::bytescache::new_cache_pos;
use crate::def;
use crate::entry::EntryBz;
use crate::entrybuffer::EntryBuffer;
use crate::entrycache::EntryCache;
use crate::entryfile::EntryFile;
use crate::indexer::BTreeIndexer;

pub struct Prefetcher {
    task_hub: Arc<dyn TaskHub>,
    shard_id: usize,
    update_buffer: Arc<EntryBuffer>,
    entry_file: Arc<EntryFile>,
    cache: Arc<EntryCache>,
    indexer: Arc<BTreeIndexer>,
    done_chan: SyncSender<(i64, i64)>,
    tpool: Arc<ThreadPool>,
}

fn fetch_entry_to_cache(
    update_buffer: Arc<EntryBuffer>,
    entry_file: Arc<EntryFile>,
    cache: Arc<EntryCache>,
    shard_id: usize,
    file_pos: i64,
) {
    let entry_pos = new_cache_pos();
    // try to insert a locked entry_pos
    let cache_hit = !cache.allocate_if_missing(shard_id, file_pos, entry_pos);
    if cache_hit {
        return; // no need to fetch
    }
    // in 'get_entry_bz_at', 'curr_buf' is None because we cannot read it
    let (in_disk, have_accessed) = update_buffer.get_entry_bz(file_pos, |entry_bz| {
        cache.insert(shard_id, file_pos, &entry_bz);
    });
    if in_disk && !have_accessed {
        let mut small = [0u8; def::DEFAULT_ENTRY_SIZE];
        let size = entry_file.read_entry(file_pos, &mut small[..]);
        let e;
        let mut buf: Vec<u8>;
        if size <= small.len() {
            e = EntryBz { bz: &small[..size] };
        } else {
            buf = Vec::with_capacity(size);
            entry_file.read_entry(file_pos, &mut buf[..]);
            e = EntryBz { bz: &buf[..size] };
        }
        cache.insert(shard_id, file_pos, &e);
    }
}

impl Prefetcher {
    pub fn new(
        task_hub: Arc<dyn TaskHub>,
        shard_id: usize,
        update_buffer: Arc<EntryBuffer>,
        entry_file: Arc<EntryFile>,
        cache: Arc<EntryCache>,
        indexer: Arc<BTreeIndexer>,
        done_chan: SyncSender<(i64, i64)>,
        tpool: Arc<ThreadPool>,
    ) -> Self {
        Self {
            task_hub,
            shard_id,
            update_buffer,
            entry_file,
            cache,
            indexer,
            done_chan,
            tpool,
        }
    }

    pub fn run_task(&mut self, task_id: i64) {
        let (cache_for_new_block, end_block) = self.task_hub.check_begin_end(task_id);
        if cache_for_new_block.is_some() {
            self.cache = cache_for_new_block.unwrap();
        }
        let mut next_task_id = task_id + 1;
        if end_block {
            //next_task_id is the first task of the next block
            next_task_id = ((task_id >> def::IN_BLOCK_IDX_BITS) + 1) << def::IN_BLOCK_IDX_BITS;
        }
        let indexer = self.indexer.clone();
        let shard_id = self.shard_id;
        let task_hub = self.task_hub.clone();
        let mut thread_count = 0usize;
        // first we need to know the count of threads that will be spawned
        for change_set in &*task_hub.get_change_sets(task_id) {
            change_set.run_in_shard(shard_id, |op, key_hash: &[u8; 32], _k, _v, _r| {
                let k64 = BigEndian::read_u64(&key_hash[0..8]);
                indexer.for_each(op, k64, |_k, _offset| -> bool {
                    thread_count += 1;
                    false // do not exit loop
                });
            });
        }
        // if there are no OP_*, prefetcher needs to do nothing.
        if thread_count == 0 {
            self.done_chan.send((task_id, next_task_id)).unwrap();
            return;
        }
        // spawn a thread for each OP_*
        let thread_count = Arc::new(AtomicUsize::new(thread_count));
        for change_set in &*task_hub.get_change_sets(task_id) {
            change_set.run_in_shard(shard_id, |op, key_hash: &[u8; 32], _k, _v, _r| {
                let k64 = BigEndian::read_u64(&key_hash[0..8]);
                indexer.for_each(op, k64, |_k, offset| -> bool {
                    let update_buffer = self.update_buffer.clone();
                    let entry_file = self.entry_file.clone();
                    let cache = self.cache.clone();
                    let thread_count = thread_count.clone();
                    let done_chan = self.done_chan.clone();
                    self.tpool.execute(move || {
                        fetch_entry_to_cache(update_buffer, entry_file, cache, shard_id, offset);
                        // the last finished thread send task_id to done_chan
                        if thread_count.fetch_sub(1, Ordering::SeqCst) == 1 {
                            done_chan.send((task_id, next_task_id)).unwrap();
                        }
                    });
                    false // do not exit loop
                });
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::sync_channel;
    use std::sync::{Arc, RwLock};

    use crate::bptaskhub::BlockPairTaskHub;
    use crate::changeset::ChangeSet;
    use crate::entryfile::EntryFileWriter;
    use crate::tasksmanager::TasksManager;
    use crate::test_helper::SimpleTask;
    use crate::{
        entry::{Entry, EntryBz},
        entrybuffer::{self},
        entrycache::EntryCache,
        entryfile::EntryFile,
        indexer,
        test_helper::TempDir,
    };

    use super::*;

    #[test]
    fn test_fetch_entry_to_cache_cache_hit() {
        let dir = TempDir::new("./prefetcher_ut_cache_hit");

        let (entry_buffer_writer, _reader) = entrybuffer::new(0, 1024);
        let entry_buffer = entry_buffer_writer.entry_buffer;
        let entry_file = Arc::new(EntryFile::new(8 * 1024, 128 * 1024, dir.to_string()));
        let entry_cache = Arc::new(EntryCache::new());

        entry_cache.insert(7, 123, &EntryBz { bz: &[1, 2, 3] });

        fetch_entry_to_cache(entry_buffer, entry_file, entry_cache, 7, 123);
    }

    #[test]
    fn test_fetch_entry_to_cache_in_buffer() {
        let dir = TempDir::new("./prefetcher_ut_in_buffer");

        let entry_file = Arc::new(EntryFile::new(8 * 1024, 128 * 1024, dir.to_string()));
        let (mut entry_buffer_writer, _reader) = entrybuffer::new(0, 1024);
        let entry_cache = Arc::new(EntryCache::new());

        let entry = Entry {
            key: "key".as_bytes(),
            value: "value".as_bytes(),
            next_key_hash: &[0xab; 32],
            version: 12345,
            last_version: 11111,
            serial_number: 99999,
        };
        let deactived_sn_list = [];

        for _i in 0..1000 {
            entry_buffer_writer.append(&entry, &deactived_sn_list);
        }
        // entry_buffer_writer.end_block(0, 0, 0);

        fetch_entry_to_cache(
            entry_buffer_writer.entry_buffer,
            entry_file,
            entry_cache.clone(),
            7,
            0,
        );

        let mut filled_size = 0;
        entry_cache.lookup(7, 0, |entry_bz| {
            filled_size = entry_bz.bz.len();
        });
        assert_eq!(72, filled_size);
    }

    #[test]
    fn test_fetch_entry_to_cache_from_file() {
        let dir = TempDir::new("./prefetcher_ut_file");

        let entry_file = Arc::new(EntryFile::new(8 * 1024, 128 * 1024, dir.to_string()));
        let mut entry_file_w = EntryFileWriter::new(entry_file.clone(), 8 * 1024);
        let (entry_buffer_writer, _reader) = entrybuffer::new(1, 1024);
        let entry_buffer = entry_buffer_writer.entry_buffer;
        let entry_cache = Arc::new(EntryCache::new());

        let entry = Entry {
            key: "key".as_bytes(),
            value: "value".as_bytes(),
            next_key_hash: &[0xab; 32],
            version: 12345,
            last_version: 11111,
            serial_number: 99999,
        };
        let deactived_sn_list = [];

        let mut entry_bz_buf: [u8; 1000] = [0; 1000];
        let entry_bz_size = entry.dump(&mut entry_bz_buf[..], &deactived_sn_list);
        let entry_bz = EntryBz {
            bz: &entry_bz_buf[..entry_bz_size],
        };

        entry_file_w.append(&entry_bz);
        entry_file_w.flush();

        fetch_entry_to_cache(entry_buffer, entry_file, entry_cache.clone(), 7, 0);

        let mut filled_size = 0;
        entry_cache.lookup(7, 0, |entry_bz| {
            filled_size = entry_bz.bz.len();
        });
        assert_eq!(72, filled_size);
    }

    #[test]
    fn test_run_task() {
        let dir = TempDir::new("./prefetcher_ut_task");
        let entry_file = Arc::new(EntryFile::new(8 * 1024, 128 * 1024, dir.to_string()));
        let (mut entry_buffer_writer, _reader) = entrybuffer::new(0, 1024);
        let entry_cache = Arc::new(EntryCache::new());
        let indexer_arc = Arc::new(indexer::BTreeIndexer::new(1024));
        let task_hub = Arc::new(BlockPairTaskHub::<SimpleTask>::new());
        let (sender, done_receiver) = sync_channel(1);
        let pool = ThreadPool::new(1);

        let mut cs1 = ChangeSet::new();
        cs1.add_op(
            def::OP_DELETE,
            1,
            &[0x77u8; 32],
            "key".as_bytes(),
            "val".as_bytes(),
            Option::None,
        );
        cs1.sort();
        let task0 = SimpleTask::new(vec![ChangeSet::new()]);
        let task1 = SimpleTask::new(vec![cs1]);
        let tasks = vec![
            RwLock::new(Option::Some(task0)),
            RwLock::new(Option::Some(task1)),
        ];

        let entry = Entry {
            key: "key".as_bytes(),
            value: "val".as_bytes(),
            next_key_hash: &[0xab; 32],
            version: 12345,
            last_version: 11111,
            serial_number: 99999,
        };
        let deactived_sn_list = [];
        for _i in 0..1000 {
            let pos = entry_buffer_writer.append(&entry, &deactived_sn_list);
            indexer_arc.add_kv(0x7777_7777_7777_7777, pos);
        }
        // entry_buffer_writer.end_block(0, 0, 0);

        task_hub.start_block(
            0x123,
            Arc::new(TasksManager::new(tasks, 0)),
            entry_cache.clone(),
        );

        let mut prefetcher = Prefetcher {
            shard_id: 1,
            update_buffer: entry_buffer_writer.entry_buffer,
            task_hub: task_hub.clone(),
            entry_file: entry_file.clone(),
            cache: entry_cache.clone(),
            indexer: indexer_arc.clone(),
            done_chan: sender,
            tpool: Arc::new(pool),
        };

        prefetcher.run_task(0x123000001);
        done_receiver.recv().unwrap();

        let mut filled_size = 0;
        entry_cache.lookup(1, 0, |entry_bz| {
            filled_size = entry_bz.bz.len();
        });
        assert_eq!(72, filled_size);
        // TODO: check more
    }
}
