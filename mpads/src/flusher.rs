use crate::def::{LEAF_COUNT_IN_TWIG, PRUNE_EVERY_NBLOCKS, MIN_PRUNE_COUNT, TWIG_SHIFT};
use crate::entrybuffer::EntryBufferReader;
use crate::entryfile::{EntryFile, EntryFileWriter};
use crate::metadb::MetaDB;
use crate::tree::{Tree, UpperTree};
use std::mem;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Barrier, RwLock};
use std::thread;

type RocksMetaDB = MetaDB;

pub struct BarrierSet {
    pub flush_bar: Barrier,
    pub metadb_bar: Barrier,
}

impl BarrierSet {
    pub fn new(n: usize) -> Self {
        Self {
            // "+ 1" for code shard
            flush_bar: Barrier::new(n + 1),
            metadb_bar: Barrier::new(n + 1),
        }
    }
}

pub struct Flusher {
    shards: Vec<Box<FlusherShard>>,
    code_shard: Option<Box<FlusherShardForCode>>,
    meta: Arc<RwLock<RocksMetaDB>>,
    curr_height: i64,
    max_kept_height: i64,
    end_block_chan: SyncSender<i64>,
}

impl Flusher {
    pub fn new(
        shards: Vec<Box<FlusherShard>>,
        code_shard: Option<Box<FlusherShardForCode>>,
        meta: Arc<RwLock<RocksMetaDB>>,
        curr_height: i64,
        max_kept_height: i64,
        end_block_chan: SyncSender<i64>,
    ) -> Self {
        Self {
            shards,
            code_shard,
            meta,
            curr_height,
            max_kept_height,
            end_block_chan,
        }
    }

    pub fn flush(&mut self, shard_count: usize) {
        loop {
            self.curr_height += 1;
            let prune_to_height = self.curr_height - self.max_kept_height;
            let bar_set = Arc::new(BarrierSet::new(shard_count));
            thread::scope(|s| {
                let mut handlers = Vec::with_capacity(shard_count);
                let mut code_shard = self.code_shard.take().unwrap();
                let meta_for_code = self.meta.clone();
                let bar_set_for_code = bar_set.clone();
                let curr_height = self.curr_height;
                let code_handler = s.spawn(move || {
                    code_shard.flush(curr_height, meta_for_code, bar_set_for_code);
                    code_shard
                });
                for _ in (0..shard_count).rev() {
                    let bar_set = bar_set.clone();
                    let mut shard = self.shards.pop().unwrap();
                    let curr_height = self.curr_height;
                    let meta = self.meta.clone();
                    let end_block_chan = self.end_block_chan.clone();
                    handlers.push(s.spawn(move || {
                        shard.flush(prune_to_height, curr_height, meta, bar_set, end_block_chan);
                        shard
                    }));
                }
                self.code_shard = Some(code_handler.join().unwrap());
                for _ in 0..shard_count {
                    let handler = handlers.pop().unwrap();
                    self.shards.push(handler.join().unwrap());
                }
            });
        }
    }

    pub fn get_entry_file(&self, shard_id: usize) -> Arc<EntryFile> {
        self.shards[shard_id].tree.entry_file_wr.entry_file.clone()
    }

    pub fn set_entry_buf_reader(&mut self, shard_id: usize, ebr: EntryBufferReader) {
        self.shards[shard_id].buf_read = Some(ebr);
    }

    pub fn set_code_buf_reader(&mut self, ebr: EntryBufferReader) {
        self.code_shard.as_mut().unwrap().buf_read = Some(ebr);
    }
}

pub struct FlusherShardForCode {
    buf_read: Option<EntryBufferReader>,
    code_file_wr: EntryFileWriter,
}

impl FlusherShardForCode {
    pub fn new(code_file: Arc<EntryFile>, buffer_size: usize) -> Self {
        Self {
            buf_read: None,
            code_file_wr: EntryFileWriter::new(code_file, buffer_size),
        }
    }

    pub fn flush(&mut self, height: i64, meta: Arc<RwLock<RocksMetaDB>>, bar_set: Arc<BarrierSet>) {
        let buf_read = self.buf_read.as_mut().unwrap();
        loop {
            let mut file_pos: i64 = 0;
            let (is_end_of_block, expected_file_pos) = buf_read.read_next_entry(|entry_bz| {
                file_pos = self.code_file_wr.append(&entry_bz);
            });
            if !is_end_of_block && file_pos != expected_file_pos {
                panic!("File_pos mismatch!");
            }
            if is_end_of_block {
                let (_, _, _) = buf_read.read_extra_info(); //ignore
                break;
            }
        }

        self.code_file_wr.flush();
        let code_file_size = self.code_file_wr.entry_file.size();
        bar_set.flush_bar.wait();
        thread::spawn(move || {
            let mut meta = meta.write().unwrap();
            meta.set_code_file_size(code_file_size);
            drop(meta);
            bar_set.metadb_bar.wait();
        });
    }
}

pub struct FlusherShard {
    buf_read: Option<EntryBufferReader>,
    tree: Tree,
    last_compact_done_sn: u64,
    shard_id: usize,
    upper_tree_sender: SyncSender<UpperTree>,
    upper_tree_receiver: Receiver<UpperTree>,
}

impl FlusherShard {
    pub fn new(tree: Tree, oldest_active_sn: u64, shard_id: usize) -> Self {
        let (ut_sender, ut_receiver) = sync_channel(2);
        Self {
            buf_read: None,
            tree,
            last_compact_done_sn: oldest_active_sn,
            shard_id,
            upper_tree_sender: ut_sender,
            upper_tree_receiver: ut_receiver,
        }
    }

    pub fn flush(
        &mut self,
        prune_to_height: i64,
        curr_height: i64,
        meta: Arc<RwLock<RocksMetaDB>>,
        bar_set: Arc<BarrierSet>,
        end_block_chan: SyncSender<i64>,
    ) {
        let buf_read = self.buf_read.as_mut().unwrap();
        loop {
            let mut file_pos: i64 = 0;
            let (is_end_of_block, expected_file_pos) = buf_read.read_next_entry(|entry_bz| {
                file_pos = self.tree.append_entry(&entry_bz);
                for i in 0..entry_bz.dsn_count() {
                    let dsn = entry_bz.get_deactived_sn(i);
                    self.tree.deactive_entry(dsn);
                }
            });
            if !is_end_of_block && file_pos != expected_file_pos {
                panic!("File_pos mismatch!");
            }
            if is_end_of_block {
                break;
            }
        }
        let (compact_done_pos, compact_done_sn, sn_end) = buf_read.read_extra_info();

        if self.tree.upper_tree.is_empty() {
            let mut upper_tree = self.upper_tree_receiver.recv().unwrap();
            mem::swap(&mut self.tree.upper_tree, &mut upper_tree);
        }
        let mut start_twig_id: u64 = 0;
        let mut end_twig_id: u64 = 0;
        if prune_to_height > 0 && prune_to_height % PRUNE_EVERY_NBLOCKS == 0 {
            let meta = meta.read().unwrap();
            start_twig_id = meta.get_last_pruned_twig(self.shard_id);
            end_twig_id = meta.get_first_twig_at_height(self.shard_id, prune_to_height);
            if end_twig_id == u64::MAX {
                panic!("FirstTwigAtHeight Not Found");
            }
            let last_evicted_twig_id = compact_done_sn / (LEAF_COUNT_IN_TWIG as u64) - 1;
            if end_twig_id > last_evicted_twig_id {
                end_twig_id = last_evicted_twig_id;
            }
            if start_twig_id <= end_twig_id && end_twig_id < start_twig_id + MIN_PRUNE_COUNT {
                end_twig_id = start_twig_id;
            } else {
                self.tree.prune_twigs(start_twig_id, end_twig_id);
            }
        }
        let del_start = self.last_compact_done_sn / (LEAF_COUNT_IN_TWIG as u64);
        let del_end = compact_done_sn / (LEAF_COUNT_IN_TWIG as u64);
        let tmp_list = self.tree.flush_files(del_start, del_end);
        let (entry_file_size, twig_file_size) = self.tree.get_file_sizes();
        let last_compact_done_sn = self.last_compact_done_sn;
        self.last_compact_done_sn = compact_done_sn;
        bar_set.flush_bar.wait();

        let youngest_twig_id = self.tree.youngest_twig_id;
        let shard_id = self.shard_id;
        let mut upper_tree = UpperTree::empty();
        mem::swap(&mut self.tree.upper_tree, &mut upper_tree);
        let upper_tree_sender = self.upper_tree_sender.clone();
        thread::spawn(move || {
            let n_list = upper_tree.evict_twigs(
                tmp_list,
                last_compact_done_sn >> TWIG_SHIFT,
                compact_done_sn >> TWIG_SHIFT,
            );
            let (_new_n_list, root_hash) = upper_tree.sync_upper_nodes(n_list, youngest_twig_id);
            let mut edge_nodes_bytes = Vec::<u8>::with_capacity(0);
            if prune_to_height > 0 &&
                prune_to_height % PRUNE_EVERY_NBLOCKS == 0 &&
                start_twig_id < end_twig_id {
                edge_nodes_bytes =
                    upper_tree.prune_nodes(start_twig_id, end_twig_id, youngest_twig_id);
            }
            //shard#0 must wait other shards to finish
            if shard_id == 0 {
                bar_set.metadb_bar.wait();
            }

            let mut meta = meta.write().unwrap();
            if edge_nodes_bytes.len() != 0 {
                meta.set_edge_nodes(shard_id, &edge_nodes_bytes[..]);
                meta.set_last_pruned_twig(shard_id, end_twig_id);
            }
            meta.set_root_hash(shard_id, root_hash);
            meta.set_oldest_active_sn(shard_id, compact_done_sn);
            meta.set_oldest_active_file_pos(shard_id, compact_done_pos);
            meta.set_next_serial_num(shard_id, sn_end);
            if (curr_height + 1) % PRUNE_EVERY_NBLOCKS == 0 {
                meta.set_first_twig_at_height(
                    shard_id,
                    curr_height + 1,
                    compact_done_sn / (LEAF_COUNT_IN_TWIG as u64),
                )
            }
            meta.set_entry_file_size(shard_id, entry_file_size);
            meta.set_twig_file_size(shard_id, twig_file_size);

            if shard_id == 0 {
                meta.set_curr_height(curr_height);
                meta.commit();
                drop(meta);
                end_block_chan.send(curr_height).unwrap();
            } else {
                drop(meta);
                bar_set.metadb_bar.wait();
            }
            upper_tree_sender.send(upper_tree).unwrap();
        });
    }
}

#[cfg(test)]
mod flusher_tests {
    use crate::check::check_hash_consistency;
    use crate::def::{
        CODE_PATH, DEFAULT_ENTRY_SIZE, DEFAULT_FILE_SIZE, ENTRIES_PATH, SENTRY_COUNT,
        SMALL_BUFFER_SIZE, TWIG_PATH, TWIG_SHIFT,
    };
    use crate::entry::{entry_to_bytes, sentry_entry, Entry};
    use crate::entrybuffer;
    use crate::entryfile::{EntryFile, EntryFileWithPreReader, EntryFileWriter};
    use crate::flusher::{Flusher, FlusherShard, FlusherShardForCode};
    use crate::indexer::CodeIndexer;
    use crate::metadb::MetaDB;
    use crate::proof::check_proof;
    use crate::recover::{bytes_to_edge_nodes, recover_tree};
    use crate::test_helper::TempDir;
    use crate::tree::Tree;
    use std::sync::mpsc::sync_channel;
    use std::sync::{Arc, RwLock};
    use std::thread::sleep;
    use std::{fs, mem, thread, time};

    #[test]
    fn test_flusher() {
        let _dir = TempDir::new("./test_flusher");
        let data_dir = "./test_flusher/data";
        let code_dir = format!("{}/{}{}", data_dir, CODE_PATH, "bc");
        let _ = fs::create_dir_all(&code_dir);
        let dir_entry = format!("{}/{}{}", data_dir, ENTRIES_PATH, ".test");
        let _ = fs::create_dir_all(dir_entry);
        let dir_twig = format!("{}/{}{}", data_dir, TWIG_PATH, ".test");
        let _ = fs::create_dir_all(dir_twig);

        let code_file = Arc::new(EntryFile::new(1024, 2048, code_dir));
        let code_indexer = CodeIndexer::new();
        let mut code_file_rd = EntryFileWithPreReader::new(&code_file);
        code_file_rd.scan_entries_lite(0, |_k64, code_hash, pos, _sn| {
            code_indexer.add_kv(code_hash, pos);
        });

        let (eb_sender, eb_receiver) = sync_channel(2);
        let meta = Arc::new(RwLock::new(MetaDB::new_with_dir(
            &("./test_flusher/metadb"),
        )));

        let (mut c_eb_wr, c_eb_rd) = entrybuffer::new(0, SMALL_BUFFER_SIZE as usize);
        // prepare code entry
        let new_code_entry = Entry {
            key: &[0u8],
            value: &[1u8],
            next_key_hash: &[2; 32],
            version: 0,
            last_version: -1,
            serial_number: 0,
        };
        let new_pos = c_eb_wr.append(&new_code_entry, &[]);
        let code_size = new_code_entry.get_serialized_len(0);
        code_indexer.add_kv(&[2; 32], new_pos);
        c_eb_wr.end_block(0, 0, 0);

        let mut flusher = Flusher {
            shards: Vec::with_capacity(1),
            code_shard: Some(Box::new(FlusherShardForCode {
                buf_read: Some(c_eb_rd),
                code_file_wr: EntryFileWriter::new(code_file.clone(), 1024),
            })),
            meta,
            curr_height: 0,
            max_kept_height: 1000,
            end_block_chan: eb_sender,
        };
        let meta = flusher.meta.clone();
        let data_dir = "./test_flusher/data";
        // prepare tree
        let shard_id = 0;
        let (mut entry_file_size, mut twig_file_size) = (0, 0);
        let mut tree = Tree::new(
            0,
            SMALL_BUFFER_SIZE as usize,
            DEFAULT_FILE_SIZE as i64,
            data_dir.to_string(),
            ".test".to_string(),
        );
        {
            let mut meta = meta.write().unwrap();
            let mut bz = [0u8; DEFAULT_ENTRY_SIZE];
            for sn in 0..SENTRY_COUNT {
                let e = sentry_entry(shard_id, sn as u64, &mut bz[..]);
                tree.append_entry(&e);
            }
            let n_list = tree.flush_files(0, 0);
            let n_list = tree.upper_tree.evict_twigs(n_list, 0, 0);
            tree.upper_tree
                .sync_upper_nodes(n_list, tree.youngest_twig_id);
            check_hash_consistency(&tree);
            (entry_file_size, twig_file_size) = tree.get_file_sizes();
            meta.set_entry_file_size(shard_id, entry_file_size);
            meta.set_twig_file_size(shard_id, twig_file_size);
            meta.set_next_serial_num(shard_id, SENTRY_COUNT as u64);
            meta.commit();
        }

        let entry_file = tree.entry_file_wr.entry_file.clone();
        let (ut_sender, ut_receiver) = sync_channel(2);
        let fs = FlusherShard::new(tree, 0, shard_id);

        flusher.shards.push(Box::new(fs));
        let tree_p = &mut flusher.shards[0].tree as *mut Tree;
        let (mut u_eb_wr, u_eb_rd) = entrybuffer::new(entry_file_size, SMALL_BUFFER_SIZE as usize);
        flusher.shards[shard_id].buf_read = Some(u_eb_rd);
        // prepare entry
        let e0 = Entry {
            key: "Key0Key0Key0Key0Key0Key0Key0Key0Key0".as_bytes(),
            value: "Value0Value0Value0Value0Value0Value0".as_bytes(),
            next_key_hash: [1; 32].as_slice(),
            version: 1,
            last_version: 0,
            serial_number: SENTRY_COUNT as u64,
        };
        let mut buf = [0; 1024];
        let bz0 = entry_to_bytes(&e0, &[], &mut buf);
        let pos0 = u_eb_wr.append(&e0, &[]);
        u_eb_wr.end_block(0, 0, SENTRY_COUNT as u64);
        let handler = thread::spawn(move || {
            flusher.flush(1);
        });
        sleep(time::Duration::from_secs(3));
        let mut buf = [0; 1024];
        let size0 = entry_file.read_entry(pos0, &mut buf);
        assert_eq!(buf[..size0], *bz0.bz);
        let mut upper_tree = ut_receiver.recv().unwrap();

        let meta = meta.read().unwrap();
        let oldest_active_sn = meta.get_oldest_active_sn(shard_id);
        let oldest_active_twig_id = oldest_active_sn >> TWIG_SHIFT;
        let youngest_twig_id = meta.get_youngest_twig_id(shard_id);
        let edge_nodes = bytes_to_edge_nodes(&meta.get_edge_nodes(shard_id));
        let last_pruned_twig_id = meta.get_last_pruned_twig(shard_id);
        let root = meta.get_root_hash(shard_id);
        let entryfile_size = meta.get_entry_file_size(shard_id);
        let twigfile_size = meta.get_twig_file_size(shard_id);
        let code_size_after = meta.get_code_file_size();
        assert_eq!(code_size_after as usize, code_size);
        let (mut tree, recovered_root) = recover_tree(
            0,
            SMALL_BUFFER_SIZE as usize,
            DEFAULT_FILE_SIZE as usize,
            data_dir.to_string(),
            ".test".to_string(),
            &edge_nodes,
            last_pruned_twig_id,
            oldest_active_twig_id,
            youngest_twig_id,
            &vec![entryfile_size, twigfile_size],
        );
        assert_eq!(recovered_root, root);
        check_hash_consistency(&tree);
        let mut proof_path = tree.get_proof(SENTRY_COUNT as u64);
        check_proof(&mut proof_path).unwrap();
        unsafe {
            let mut tree = &mut (*tree_p);
            mem::swap(&mut tree.upper_tree, &mut upper_tree);
            check_hash_consistency(&tree);
            let mut proof_path = tree.get_proof(SENTRY_COUNT as u64);
            assert_eq!(check_proof(&mut proof_path).is_ok(), true);
        }
    }
}
