mod entry_loader;
mod entry_updater;
mod entry_flusher;

use std::cell::{RefCell};
use std::fs;
use std::rc::Rc;
use std::sync::{Arc, RwLock};
use byteorder::{BigEndian, ByteOrder};
use mpads::{ADS, AdsCore};
use mpads::bptaskhub::{Task};
use mpads::changeset::ChangeSet;
use mpads::def::{CODE_PATH, DEFAULT_ENTRY_SIZE, SHARD_COUNT};
use mpads::entry::{EntryBz};
use mpads::entrycache::EntryCache;
use mpads::entryfile::{EntryFile};
use mpads::indexer::{BTreeIndexer, CodeIndexer};
use mpads::metadb::{MetaDB};
use mpads::tasksmanager::TasksManager;
use crate::entry_flusher::{CodeFlusherShard, EntryFlusher, FlusherShard};
use crate::entry_loader::EntryLoader;
use crate::entry_updater::{CodeUpdater, EntryUpdater};

pub struct SeqAdsWrap<T: Task> {
    tasks_manager: Arc<TasksManager<T>>,
    ads: Arc<SeqAds>,
    cache: Arc<EntryCache>,
}

impl<T: Task + 'static> SeqAdsWrap<T> {
    pub fn new(dir: &str, wrbuf_size: usize, file_segment_size: usize) -> Self {
        let ads= SeqAds::new(dir, wrbuf_size, file_segment_size);
        Self {
            tasks_manager:Arc::new(TasksManager::default()),
            ads: Arc::new(ads),
            cache: Arc::new(EntryCache::new_uninit()),
        }
    }

    pub fn start_block(&mut self, height: i64, tasks_manager: Arc<TasksManager<T>>) {
        self.cache = Arc::new(EntryCache::new());
        self.tasks_manager = tasks_manager;
    }
}

pub struct SeqAds {
    write_buf_size: usize,
    entry_files: Vec<Arc<EntryFile>>,
    indexer: Rc<BTreeIndexer>,
    code_file: Arc<EntryFile>,
    code_indexer: Rc<CodeIndexer>,
    meta_db: Arc<RwLock<MetaDB>>,
    entry_cache: EntryCache,
    entry_loaders: Vec<RefCell<EntryLoader>>,
    entry_updaters: Vec<Rc<RefCell<EntryUpdater>>>,
    code_updater: Rc<RefCell<CodeUpdater>>,
    entry_flusher: RefCell<EntryFlusher>,
}

impl SeqAds {
    pub fn new(
        dir: &str,
        write_buf_size: usize,
        file_segment_size: usize,
    ) -> Self {
        let data_dir = dir.to_owned() + "/data";
        let code_dir = format!("{}/{}{}", data_dir, CODE_PATH, "bc");
        let _ = fs::create_dir_all(&code_dir);
        let code_file = Arc::new(EntryFile::new(
            write_buf_size,
            file_segment_size as i64,
            code_dir,
        ));
        let code_indexer = Rc::new(CodeIndexer::new());
        AdsCore::index_code(&code_file, &code_indexer);
        let code_updater = Rc::new(RefCell::new(CodeUpdater::new(code_indexer.clone())));

        let code_shard = Some(Box::new(CodeFlusherShard::new(
            code_file.clone(),
            write_buf_size,
            code_updater.clone(),
        )));

        let indexer = Rc::new(BTreeIndexer::new(1 << 16));

        let meta_dir = dir.to_owned() + "/metadb";
        let meta = MetaDB::new_with_dir(&meta_dir);
        let curr_height = meta.get_curr_height();

        let mut entry_files = Vec::with_capacity(SHARD_COUNT);
        let mut shards: Vec<Box<FlusherShard>> = Vec::with_capacity(SHARD_COUNT);
        let mut entry_updaters = Vec::<Rc<RefCell<EntryUpdater>>>::with_capacity(SHARD_COUNT);
        let meta = Arc::new(RwLock::new(meta));
        for shard_id in 0..SHARD_COUNT {
            let (tree, oldest_active_twig_id, oldest_active_sn) = AdsCore::_recover_tree(
                meta.clone(),
                data_dir.clone(),
                write_buf_size,
                file_segment_size,
                curr_height,
                shard_id,
            );

            AdsCore::index_tree(&tree, oldest_active_twig_id, &indexer);

            let entry_file = tree.entry_file_wr.entry_file.clone();
            entry_files.push(entry_file.clone());
            let sn_end =  meta.clone().read().unwrap().get_next_serial_num(shard_id);
            let compact_start =  meta.clone().read().unwrap().get_oldest_active_file_pos(shard_id);
            let oldest_active_sn =  meta.clone().read().unwrap().get_oldest_active_sn(shard_id);
            let updater = Rc::new(RefCell::new(
                EntryUpdater::new(shard_id, entry_file.clone(), indexer.clone(), -1, sn_end, compact_start, 10000)));
            shards.push(Box::new(FlusherShard::new(
                tree,
                oldest_active_sn,
                shard_id,
                updater.clone(),
            )));
            entry_updaters.push(updater);
        }

        let flusher = RefCell::new(EntryFlusher::new(
            shards,
            code_shard,
            meta.clone()
        ));
        let entry_cache = Arc::new(EntryCache::new());
        let mut entry_loaders = Vec::<RefCell<EntryLoader>>::with_capacity(SHARD_COUNT);
        for i in 0..SHARD_COUNT {
            entry_loaders[i] = RefCell::new(EntryLoader::new(i, entry_files[i].clone(), entry_cache.clone(), indexer.clone()));
        }
        let seq_ads = Self {
            write_buf_size,
            indexer,
            entry_files,
            code_file,
            code_indexer,
            meta_db: meta.clone(),
            entry_cache: EntryCache::new(),
            entry_loaders,
            entry_updaters,
            code_updater,
            entry_flusher: flusher,
        };
        seq_ads
    }

    pub fn commit_tx(&self, task_id: i64, change_sets: &Vec<ChangeSet>) {
        for loader in &self.entry_loaders {
            loader.borrow_mut().run_task(change_sets);
        }
        for updater in & self.entry_updaters {
            updater.borrow_mut().run_task(change_sets);
        }
        self.code_updater.borrow_mut().run_task(task_id, change_sets);
        self.entry_flusher.borrow_mut().flush_tx(SHARD_COUNT + 1);
    }

    pub fn commit_block(&mut self, height: i64) {
        self.entry_flusher.borrow_mut().flush_block(height);
    }
}

// impl<T: Task + 'static> ADS for SeqAdsWrap<T> {
//     fn read_entry(&self, key_hash: &[u8], key: &[u8], buf: &mut [u8]) -> (usize, bool) {
//         let k64 = BigEndian::read_u64(&key_hash[0..8]);
//         let shard_id = (key_hash[0] as usize) >> 4;
//         let mut size = 0;
//         let mut found_it = false;
//         self.ads.indexer.for_each_value(k64, |file_pos| -> bool {
//             let mut buf_too_small = false;
//             self.ads.entry_cache.lookup(shard_id, file_pos, |entry_bz| {
//                 found_it = AdsCore::check_entry(key_hash, key, &entry_bz);
//                 if found_it {
//                     size = entry_bz.len();
//                     if buf.len() < size {
//                         buf_too_small = true;
//                     } else {
//                         buf[..size].copy_from_slice(entry_bz.bz);
//                     }
//                 }
//             });
//             if found_it || buf_too_small {
//                 return true; //stop loop if key matches or buf is too small
//             }
//             size = self.ads.entry_files[shard_id].read_entry(file_pos, buf);
//             if buf.len() < size {
//                 return true; //stop loop if buf is too small
//             }
//             let entry_bz = EntryBz { bz: &buf[..size] };
//             found_it = AdsCore::check_entry(key_hash, key, &entry_bz);
//             if found_it {
//                 self.ads.entry_cache.insert(shard_id, file_pos, &entry_bz);
//             }
//             found_it // stop loop if key matches
//         });
//         (size, found_it)
//     }
//
//     fn read_code(&self, code_hash: &[u8], buf: &mut Vec<u8>) -> usize {
//         if buf.len() < DEFAULT_ENTRY_SIZE {
//             panic!("buf.len() less than DEFAULT_ENTRY_SIZE");
//         }
//         let mut size = 0;
//         self.ads.code_indexer
//             .for_each_value(code_hash, |file_pos| -> bool {
//                 size = self.ads.code_file.read_entry(file_pos, &mut buf[..]);
//                 if buf.len() < size {
//                     buf.resize(size, 0);
//                     self.ads.code_file.read_entry(file_pos, &mut buf[..]);
//                 }
//                 let entry_bz = EntryBz { bz: &buf[..size] };
//                 let match_code_hash = entry_bz.next_key_hash() == code_hash;
//                 if !match_code_hash {
//                     size = 0;
//                 }
//                 match_code_hash // stop loop if code_hash matches
//             });
//         size
//     }
//
//     fn add_task(&self, task_id: i64) {
//         let change_sets = self.tasks_manager.get_tasks_change_sets(task_id as usize);
//         self.ads.commit_tx(task_id, &change_sets);
//     }
// }