mod entry_loader;
mod entry_updater;
mod entry_flusher;

use std::cell::RefCell;
use std::fs;
use std::rc::Rc;
use std::sync::{Arc, RwLock};
use mpads::AdsCore;
use mpads::changeset::ChangeSet;
use mpads::def::{CODE_PATH, SHARD_COUNT};
use mpads::entry::Entry;
use mpads::entrycache::EntryCache;
use mpads::entryfile::{EntryFile};
use mpads::indexer::{BTreeIndexer, CodeIndexer};
use mpads::metadb::{MetaDB};
use crate::entry_flusher::{CodeFlusherShard, EntryFlusher, FlusherShard};
use crate::entry_loader::EntryLoader;
use crate::entry_updater::EntryUpdater;

pub struct SeqAds {
    write_buf_size: usize,
    entry_files: Vec<Arc<EntryFile>>,
    indexer: Arc<BTreeIndexer>,
    code_file: Arc<EntryFile>,
    code_indexer: CodeIndexer,
    meta_db: Arc<RwLock<MetaDB>>,
    entry_cache: EntryCache,
    entry_loaders: Vec<EntryLoader>,
    entry_updaters: Vec<Rc<RefCell<EntryUpdater>>>,
    entry_flusher: EntryFlusher,
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
        let code_indexer = CodeIndexer::new();
        AdsCore::index_code(&code_file, &code_indexer);

        let code_shard = Some(Box::new(CodeFlusherShard::new(
            code_file.clone(),
            write_buf_size,
        )));

        let indexer = Arc::new(BTreeIndexer::new(1 << 16));

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
            let sn_end = meta.clone().read().unwrap().get_next_serial_num(shard_id);
            let updater = Rc::new(RefCell::new(EntryUpdater::new(shard_id, entry_file.clone(), indexer.clone(), -1, sn_end)));
            shards.push(Box::new(FlusherShard::new(
                tree,
                oldest_active_sn,
                shard_id,
                updater.clone(),
            )));
            entry_updaters.push(updater);
        }

        let flusher = EntryFlusher::new(
            shards,
            code_shard,
            meta.clone()
        );
        let entry_cache = Arc::new(EntryCache::new());
        let mut entry_loaders = Vec::<EntryLoader>::with_capacity(SHARD_COUNT);
        for i in 0..SHARD_COUNT {
            entry_loaders[i] = EntryLoader::new(i, entry_files[i].clone(), entry_cache.clone(), indexer.clone());
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
            entry_flusher: flusher,
        };
        seq_ads
    }

    pub fn commit_tx(&mut self, change_sets: Vec<ChangeSet>) {
        for loader in &mut self.entry_loaders {
            loader.run_task(&change_sets);
        }
        for updater in &mut self.entry_updaters {
            updater.borrow_mut().run_task(&change_sets);
        }
        self.entry_flusher.flush_tx(SHARD_COUNT + 1);
    }

    pub fn commit_block(&mut self, height: i64) {
        self.entry_flusher.flush_block(height);
    }
}