extern crate core;

pub mod bptaskhub;
pub mod bytescache;
pub mod changeset;
pub mod check;
pub mod compactor;
pub mod def;
pub mod entry;
pub mod entrybuffer;
pub mod entrycache;
pub mod entryfile;
pub mod flusher;
pub mod hpfile;
pub mod indexer;
pub mod metadb;
pub mod multiproof;
pub mod prefetcher;
pub mod proof;
pub mod recover;
pub mod refdb;
pub mod rocksdb;
pub mod stackmachine;
pub mod tasksmanager;
pub mod tree;
pub mod twig;
pub mod twigfile;
pub mod updater;
pub mod utils;

// for test
pub mod test_helper;
pub mod tree_helper;

use crate::bptaskhub::{BlockPairTaskHub, Task, TaskHub};
use crate::compactor::{CompactJob, Compactor};
use crate::def::{
    CODE_PATH, COMPACT_RING_SIZE, COMPACT_THRES, DEFAULT_ENTRY_SIZE, IN_BLOCK_IDX_BITS,
    PREFETCHER_THREAD_COUNT, SENTRY_COUNT, SHARD_COUNT, TASK_CHAN_SIZE, TWIG_SHIFT,
    UTILIZATION_DIV, UTILIZATION_RATIO, COMPACT_TRIGGER,
};
use crate::entry::{sentry_entry, EntryBz};
use crate::entrycache::EntryCache;
use crate::entryfile::{EntryFile, EntryFileWithPreReader};
use crate::flusher::{Flusher, FlusherShard, FlusherShardForCode};
use crate::indexer::{BTreeIndexer, CodeIndexer};
use crate::metadb::MetaDB;
use crate::prefetcher::Prefetcher;
use crate::recover::{bytes_to_edge_nodes, recover_tree};
use crate::rocksdb::RocksDB;
use crate::tasksmanager::TasksManager;
use crate::tree::Tree;
use crate::updater::{CodeUpdater, Updater};
use crate::utils::ringchannel;
use byteorder::{BigEndian, ByteOrder};
use std::fs;
use std::path::Path;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, RwLock};
use std::thread;
use threadpool::ThreadPool;

pub struct AdsCore {
    task_hub: Arc<dyn TaskHub>,
    task_senders: Vec<SyncSender<i64>>,
    indexer: Arc<BTreeIndexer>,
    entry_files: Vec<Arc<EntryFile>>,
    code_file: Arc<EntryFile>,
    code_idxr: Arc<CodeIndexer>,
    meta: Arc<RwLock<MetaDB>>,
    wrbuf_size: usize,
}

impl AdsCore {
    pub fn new(
        task_hub: Arc<dyn TaskHub>,
        dir: &str,
        wrbuf_size: usize,
        file_segment_size: usize,
    ) -> (Self, Receiver<i64>, Flusher) {
        let data_dir = dir.to_owned() + "/data";
        let code_dir = format!("{}/{}{}", data_dir, CODE_PATH, "bc");
        let _ = fs::create_dir_all(&code_dir);
        let code_file = Arc::new(EntryFile::new(
            wrbuf_size,
            file_segment_size as i64,
            code_dir,
        ));
        let code_idxr = CodeIndexer::new();
        Self::index_code(&code_file, &code_idxr);
        let code_shard = Some(Box::new(FlusherShardForCode::new(
            code_file.clone(),
            wrbuf_size,
        )));

        let indexer = Arc::new(BTreeIndexer::new(1 << 16));
        let (eb_sender, eb_receiver) = sync_channel(2);
        let meta_dir = dir.to_owned() + "/metadb";
        let meta = MetaDB::new_with_dir(&meta_dir);
        let curr_height = meta.get_curr_height();
        let meta = Arc::new(RwLock::new(meta));

        // recover trees in parallel
        let mut recover_handles = Vec::with_capacity(SHARD_COUNT);
        for shard_id in 0..SHARD_COUNT {
            let meta = meta.clone();
            let data_dir = data_dir.clone();
            let indexer = indexer.clone();

            let handle = thread::spawn(move || {
                let (tree, oldest_active_twig_id, oldest_active_sn) = Self::_recover_tree(
                    meta,
                    data_dir,
                    wrbuf_size,
                    file_segment_size,
                    curr_height,
                    shard_id,
                );

                Self::index_tree(&tree, oldest_active_twig_id, &indexer);

                (tree, oldest_active_sn)
            });
            recover_handles.push(handle);
        }

        let mut entry_files = Vec::with_capacity(SHARD_COUNT);
        let mut shards: Vec<Box<FlusherShard>> = Vec::with_capacity(SHARD_COUNT);

        for shard_id in 0..SHARD_COUNT {
            let handle = recover_handles.remove(0);
            let (tree, oldest_active_sn) = handle.join().unwrap();
            entry_files.push(tree.entry_file_wr.entry_file.clone());
            shards.push(Box::new(FlusherShard::new(
                tree,
                oldest_active_sn,
                shard_id,
            )));
        }

        let max_kept_height = 10; //1000;
        let flusher = Flusher::new(
            shards,
            code_shard,
            meta.clone(),
            curr_height,
            max_kept_height,
            eb_sender,
        );

        let ads_core = Self {
            task_hub,
            task_senders: Vec::with_capacity(SHARD_COUNT + 1),
            indexer,
            entry_files,
            code_idxr: Arc::new(code_idxr),
            code_file,
            meta: meta.clone(),
            wrbuf_size,
        };
        (ads_core, eb_receiver, flusher)
    }

    pub fn _recover_tree(
        meta: Arc<RwLock<MetaDB>>,
        data_dir: String,
        buffer_size: usize,
        file_segment_size: usize,
        curr_height: i64,
        shard_id: usize,
    ) -> (Tree, u64, u64) {
        let meta = meta.read().unwrap();
        let oldest_active_sn = meta.get_oldest_active_sn(shard_id);
        let oldest_active_twig_id = oldest_active_sn >> TWIG_SHIFT;
        let youngest_twig_id = meta.get_youngest_twig_id(shard_id);
        let edge_nodes = bytes_to_edge_nodes(&meta.get_edge_nodes(shard_id));
        let last_pruned_twig_id = meta.get_last_pruned_twig(shard_id);
        let root = meta.get_root_hash(shard_id);
        let entryfile_size = meta.get_entry_file_size(shard_id);
        let twigfile_size = meta.get_twig_file_size(shard_id);
        let (tree, recovered_root) = recover_tree(
            shard_id,
            buffer_size,
            file_segment_size,
            data_dir,
            format!("{}", shard_id),
            &edge_nodes,
            last_pruned_twig_id,
            oldest_active_twig_id,
            youngest_twig_id,
            &vec![entryfile_size, twigfile_size],
        );

        if root != recovered_root && curr_height != 0 {
            println!(
                "root mismatch, shard_id: {}, root: {:?}, recovered_root: {:?}",
                shard_id, root, recovered_root
            );
            panic!("recover error");
        }

        (tree, oldest_active_twig_id, oldest_active_sn)
    }

    pub fn index_code(code_file: &Arc<EntryFile>, code_indexer: &CodeIndexer) {
        let mut code_file_rd = EntryFileWithPreReader::new(code_file);
        code_file_rd.scan_entries_lite(0, |_k64, code_hash, pos, _sn| {
            code_indexer.add_kv(code_hash, pos);
        });
    }

    pub fn index_tree(tree: &Tree, oldest_active_twig_id: u64, indexer: &BTreeIndexer) {
        tree.scan_entries_lite(oldest_active_twig_id, |k64, _nkh, pos, sn| {
            if tree.get_active_bit(sn) {
                indexer.add_kv(k64, pos);
            }
        });
    }

    pub fn init_dir(dir: &str, file_segment_size: usize) {
        if Path::new(dir).exists() {
            fs::remove_dir_all(dir).unwrap();
        }
        fs::create_dir(dir).unwrap();
        let kvdb = RocksDB::new("metadb", &(dir.to_owned() + "/metadb"));
        let mut meta = MetaDB::new(kvdb);
        for shard_id in 0..SHARD_COUNT {
            let mut tree = Tree::new(
                shard_id,
                8192,
                file_segment_size as i64,
                dir.to_owned() + "/data",
                format!("{}", shard_id),
            );
            let mut bz = [0u8; DEFAULT_ENTRY_SIZE];
            for sn in 0..SENTRY_COUNT {
                let e = sentry_entry(shard_id, sn as u64, &mut bz[..]);
                tree.append_entry(&e);
            }
            tree.flush_files(0, 0);
            let (entry_file_size, twig_file_size) = tree.get_file_sizes();
            meta.set_entry_file_size(shard_id, entry_file_size);
            meta.set_twig_file_size(shard_id, twig_file_size);
            meta.set_next_serial_num(shard_id, SENTRY_COUNT as u64);
        }
        meta.commit()
    }

    fn check_entry(key_hash: &[u8], key: &[u8], entry_bz: &EntryBz) -> bool {
        if key.len() == 0 {
            entry_bz.key_hash() == key_hash
        } else {
            entry_bz.key() == key
        }
    }

    pub fn read_entry(
        &self,
        key_hash: &[u8],
        key: &[u8],
        cache: Option<&EntryCache>,
        buf: &mut [u8],
    ) -> (usize, bool) {
        let k64 = BigEndian::read_u64(&key_hash[0..8]);
        let shard_id = (key_hash[0] as usize) >> 4;
        let mut size = 0;
        let mut found_it = false;
        self.indexer.for_each_value(k64, |file_pos| -> bool {
            let mut buf_too_small = false;
            if cache.is_some() {
                cache.unwrap().lookup(shard_id, file_pos, |entry_bz| {
                    found_it = Self::check_entry(key_hash, key, &entry_bz);
                    if found_it {
                        size = entry_bz.len();
                        if buf.len() < size {
                            buf_too_small = true;
                        } else {
                            buf[..size].copy_from_slice(entry_bz.bz);
                        }
                    }
                });
            }
            if found_it || buf_too_small {
                return true; //stop loop if key matches or buf is too small
            }
            size = self.entry_files[shard_id].read_entry(file_pos, buf);
            if buf.len() < size {
                return true; //stop loop if buf is too small
            }
            let entry_bz = EntryBz { bz: &buf[..size] };
            found_it = Self::check_entry(key_hash, key, &entry_bz);
            if found_it && cache.is_some() {
                cache.unwrap().insert(shard_id, file_pos, &entry_bz);
            }
            found_it // stop loop if key matches
        });
        (size, found_it)
    }

    pub fn read_code(&self, code_hash: &[u8], buf: &mut Vec<u8>) -> usize {
        if buf.len() < DEFAULT_ENTRY_SIZE {
            panic!("buf.len() less than DEFAULT_ENTRY_SIZE");
        }
        let mut size = 0;
        self.code_idxr
            .for_each_value(code_hash, |file_pos| -> bool {
                size = self.code_file.read_entry(file_pos, &mut buf[..]);
                if buf.len() < size {
                    buf.resize(size, 0);
                    self.code_file.read_entry(file_pos, &mut buf[..]);
                }
                let entry_bz = EntryBz { bz: &buf[..size] };
                let match_code_hash = entry_bz.next_key_hash() == code_hash;
                if !match_code_hash {
                    size = 0;
                }
                match_code_hash // stop loop if code_hash matches
            });
        size
    }

    pub fn add_task(&self, task_id: i64) {
        for sender in &self.task_senders {
            sender.send(task_id).unwrap();
        }
    }

    pub fn start_threads(&mut self, mut flusher: Flusher) {
        let meta = self.meta.read().unwrap();
        let curr_height = meta.get_curr_height() + 1;

        let codefile_size = meta.get_code_file_size();
        let (cb_wr, cb_rd) = entrybuffer::new(codefile_size, self.wrbuf_size);
        flusher.set_code_buf_reader(cb_rd);
        let (task_sender, task_reciever) = sync_channel(TASK_CHAN_SIZE);
        self.task_senders.push(task_sender);
        let mut code_updater = CodeUpdater {
            task_hub: self.task_hub.clone(),
            update_buffer: cb_wr,
            indexer: self.code_idxr.clone(),
        };
        thread::spawn(move || loop {
            let task_id = task_reciever.recv().unwrap();
            code_updater.run_task(task_id);
        });

        let job = CompactJob {
            old_pos: 0,
            entry_bz: Vec::with_capacity(DEFAULT_ENTRY_SIZE),
        };
        let tpool = Arc::new(ThreadPool::new(PREFETCHER_THREAD_COUNT));
        for shard_id in 0..SHARD_COUNT {
            let (task_sender, task_reciever) = sync_channel(TASK_CHAN_SIZE);
            let (mid_sender, mid_reciever) = sync_channel(TASK_CHAN_SIZE);
            let entryfile_size = meta.get_entry_file_size(shard_id);
            let (u_eb_wr, u_eb_rd) = entrybuffer::new(entryfile_size, self.wrbuf_size);
            self.task_senders.push(task_sender);
            let entry_file = flusher.get_entry_file(shard_id);
            let mut prefetcher = Prefetcher::new(
                self.task_hub.clone(),
                shard_id,
                u_eb_wr.entry_buffer.clone(),
                entry_file.clone(),
                Arc::new(EntryCache::new_uninit()),
                self.indexer.clone(),
                mid_sender.clone(),
                tpool.clone(),
            );
            thread::spawn(move || loop {
                let task_id = task_reciever.recv().unwrap();
                prefetcher.run_task(task_id);
            });
            let (cmpt_producer, cmpt_consumer) = ringchannel::new(COMPACT_RING_SIZE, &job);
            let mut compactor = Compactor::new(
                shard_id,
                COMPACT_TRIGGER,
                entry_file.clone(),
                self.indexer.clone(),
                cmpt_producer,
            );
            let compact_start = meta.get_oldest_active_file_pos(shard_id);
            thread::spawn(move || {
                compactor.fill_compact_chan(compact_start);
            });
            let sn_start = meta.get_oldest_active_sn(shard_id);
            let sn_end = meta.get_next_serial_num(shard_id);
            let mut updater = Updater::new(
                shard_id,
                self.task_hub.clone(),
                u_eb_wr,
                entry_file.clone(),
                self.indexer.clone(),
                -1, // curr_version, will be overwritten
                sn_start,
                sn_end,
                cmpt_consumer,
                compact_start,
                UTILIZATION_DIV,
                UTILIZATION_RATIO,
                COMPACT_THRES,
                curr_height << IN_BLOCK_IDX_BITS,
            );
            thread::spawn(move || loop {
                let (task_id, next_task_id) = mid_reciever.recv().unwrap();
                updater.run_task_with_ooo_id(task_id, next_task_id);
            });
            flusher.set_entry_buf_reader(shard_id, u_eb_rd)
        }
        drop(meta);
        thread::spawn(move || {
            flusher.flush(SHARD_COUNT);
        });
    }
}

pub struct AdsWrap<T: Task> {
    task_hub: Arc<BlockPairTaskHub<T>>,
    ads: Arc<AdsCore>,
    cache: Arc<EntryCache>,
    end_block_chan: Receiver<i64>, // when ads finish the prev block disk job, there will receive something.
}

pub struct SharedAdsWrap {
    ads: Arc<AdsCore>,
    cache: Arc<EntryCache>,
}

impl<T: Task + 'static> AdsWrap<T> {
    pub fn new(dir: &str, wrbuf_size: usize, file_segment_size: usize) -> Self {
        let task_hub = Arc::new(BlockPairTaskHub::<T>::new());
        let (mut ads, end_block_chan, flusher) =
            AdsCore::new(task_hub.clone(), dir, wrbuf_size, file_segment_size);
        ads.start_threads(flusher);

        Self {
            task_hub,
            ads: Arc::new(ads),
            cache: Arc::new(EntryCache::new_uninit()),
            end_block_chan,
        }
    }

    pub fn flush(&mut self) {
        while self.task_hub.free_slot_count() < 2 {
            let height = self.end_block_chan.recv().unwrap();
            self.task_hub.end_block(height);
        }
    }

    pub fn start_block(&mut self, height: i64, tasks_manager: Arc<TasksManager<T>>) {
        self.cache = Arc::new(EntryCache::new());

        if self.task_hub.free_slot_count() == 0 {
            // adscore and task_hub are busy, wait for them to finish an old block
            let height = self.end_block_chan.recv().unwrap();
            self.task_hub.end_block(height);
        }

        self.task_hub
            .start_block(height, tasks_manager, self.cache.clone());
    }

    pub fn get_shared(&self) -> SharedAdsWrap {
        SharedAdsWrap {
            ads: self.ads.clone(),
            cache: self.cache.clone(),
        }
    }
}

pub trait ADS {
    fn read_entry(&self, key_hash: &[u8], key: &[u8], buf: &mut [u8]) -> (usize, bool);
    fn read_code(&self, code_hash: &[u8], buf: &mut Vec<u8>) -> usize;
    fn add_task(&self, task_id: i64);
}

impl ADS for SharedAdsWrap {
    fn read_entry(&self, key_hash: &[u8], key: &[u8], buf: &mut [u8]) -> (usize, bool) {
        self.ads
            .read_entry(key_hash, key, Some(self.cache.as_ref()), buf)
    }

    fn read_code(&self, code_hash: &[u8], buf: &mut Vec<u8>) -> usize {
        self.ads.read_code(code_hash, buf)
    }

    fn add_task(&self, task_id: i64) {
        self.ads.add_task(task_id);
    }
}

impl SharedAdsWrap {
    pub fn new(ads: Arc<AdsCore>, cache: Arc<EntryCache>) -> Self {
        SharedAdsWrap { ads, cache }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        bptaskhub::BlockPairTaskHub,
        test_helper::{SimpleTask, TempDir},
    };

    use super::*;

    #[test]
    fn test_init_dir() {
        let dir = "test_init_dir";
        let tmp_dir = TempDir::new(dir);
        AdsCore::init_dir(dir, 64 * 1024);

        assert_eq!(
            tmp_dir.list().join(","),
            ["test_init_dir/data", "test_init_dir/metadb",].join(",")
        );
        assert_eq!(
            TempDir::list_dir("test_init_dir/data").join(","),
            [
                "test_init_dir/data/entries0",
                "test_init_dir/data/entries1",
                "test_init_dir/data/entries10",
                "test_init_dir/data/entries11",
                "test_init_dir/data/entries12",
                "test_init_dir/data/entries13",
                "test_init_dir/data/entries14",
                "test_init_dir/data/entries15",
                "test_init_dir/data/entries2",
                "test_init_dir/data/entries3",
                "test_init_dir/data/entries4",
                "test_init_dir/data/entries5",
                "test_init_dir/data/entries6",
                "test_init_dir/data/entries7",
                "test_init_dir/data/entries8",
                "test_init_dir/data/entries9",
                "test_init_dir/data/twig0",
                "test_init_dir/data/twig1",
                "test_init_dir/data/twig10",
                "test_init_dir/data/twig11",
                "test_init_dir/data/twig12",
                "test_init_dir/data/twig13",
                "test_init_dir/data/twig14",
                "test_init_dir/data/twig15",
                "test_init_dir/data/twig2",
                "test_init_dir/data/twig3",
                "test_init_dir/data/twig4",
                "test_init_dir/data/twig5",
                "test_init_dir/data/twig6",
                "test_init_dir/data/twig7",
                "test_init_dir/data/twig8",
                "test_init_dir/data/twig9",
            ]
            .join(",")
        );

        let kvdb = RocksDB::new("metadb", &(dir.to_owned() + "/metadb"));
        let mut meta = MetaDB::new(kvdb);
        meta.reload_from_kvdb();
        for i in 0..16 {
            assert_eq!(4096 * 96, meta.get_entry_file_size(i));
            assert_eq!(147416, meta.get_twig_file_size(i));
            assert_eq!(4096, meta.get_next_serial_num(i));
        }
    }

    #[test]
    fn test_adscore() {
        let dir = "test_adscore";
        let _tmp_dir = TempDir::new(dir);
        let task_hub = Arc::new(BlockPairTaskHub::<SimpleTask>::new());
        AdsCore::init_dir(dir, 64 * 1024);
        let _ads_core = AdsCore::new(task_hub, dir, 8 * 1024, 64 * 1024);
        // assert_eq!("?", tmp_dir.list().join("\n"));
    }
}
