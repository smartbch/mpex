use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use mpads::def::{LEAF_COUNT_IN_TWIG};
use mpads::entryfile::{EntryFile, EntryFileWriter};
use mpads::metadb::MetaDB;
use mpads::tree::{Tree};
use crate::entry_updater::{CodeUpdater, EntryUpdater};

pub struct CodeFlusherShard {
    updater: Arc<Mutex<CodeUpdater>>,
    code_file_wr: EntryFileWriter,
}
impl CodeFlusherShard {
    pub fn new(code_file: Arc<EntryFile>, buffer_size: usize, updater: Arc<Mutex<CodeUpdater>>) -> Self {
        Self {
            updater,
            code_file_wr: EntryFileWriter::new(code_file, buffer_size),
        }
    }

    pub fn flush(&mut self) {
        for entry_bz in self.updater.lock().unwrap().update_buffer.get_all_entry_bz() {
            self.code_file_wr.append(&entry_bz);
        }
        self.code_file_wr.flush();
    }
}

pub struct FlusherShard {
    updater: Arc<Mutex<EntryUpdater>>,
    pub tree: Tree,
    last_compact_done_sn: u64,
    shard_id: usize,
}

impl FlusherShard {
    pub fn new(tree: Tree, oldest_active_sn: u64, shard_id: usize, updater: Arc<Mutex<EntryUpdater>>) -> Self {
        Self {
            updater,
            tree,
            last_compact_done_sn: oldest_active_sn,
            shard_id,
        }
    }

    pub fn flush(
        &mut self,
    ) {
        for entry_bz in self.updater.lock().unwrap().get_all_entry_bz() {
            self.tree.append_entry(&entry_bz);
            for i in 0..entry_bz.dsn_count() {
                let dsn = entry_bz.get_deactived_sn(i);
                self.tree.deactive_entry(dsn);
            }
        }
        let del_start = self.last_compact_done_sn / (LEAF_COUNT_IN_TWIG as u64);
        let del_end = del_start; // not compact in serial mode
        let tmp_list = self.tree.flush_files(del_start, del_end);
        let youngest_twig_id = self.tree.youngest_twig_id;
        let last_compact_done_sn = self.last_compact_done_sn;
        let n_list =  self.tree.upper_tree.evict_twigs(tmp_list, last_compact_done_sn, last_compact_done_sn);
        self.tree.upper_tree.sync_upper_nodes(n_list, youngest_twig_id);
    }
}

pub struct EntryFlusher {
    pub shards: Vec<Box<FlusherShard>>,
    code_shard: Option<Box<CodeFlusherShard>>,
    meta: Arc<RwLock<MetaDB>>,
}

impl EntryFlusher {
    pub fn new(
        shards: Vec<Box<FlusherShard>>,
        code_shard: Option<Box<CodeFlusherShard>>,
        meta: Arc<RwLock<MetaDB>>,
    ) -> Self {
        Self {
            shards,
            code_shard,
            meta,
        }
    }

    pub fn flush_block(&mut self, curr_height: i64) {
        //todo:
    }

    pub fn flush_tx(&mut self, shard_count: usize) {
        let mut code_shard = self.code_shard.take().unwrap();
        code_shard.flush();
        self.code_shard = Some(code_shard);
        for i in (0..shard_count).rev() {
            self.shards[i].flush();
        }
    }
}