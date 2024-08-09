use byteorder::{BigEndian, ByteOrder};
use mpads::changeset::ChangeSet;
use mpads::def::{CODE_SHARD_ID, DEFAULT_ENTRY_SIZE, OP_CREATE, OP_DELETE, OP_WRITE};
use mpads::entry::{Entry, EntryBz};
use mpads::entrycache::EntryCache;
use mpads::entryfile::EntryFile;
use mpads::indexer::{BTreeIndexer, CodeIndexer};
use mpads::refdb::OpRecord;
use mpads::utils::hasher;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

pub struct UpdateBuffer {
    pub entry_map: HashMap<i64, Vec<u8>>,
    pub start: i64,
    pub end: i64,
}

impl UpdateBuffer {
    pub fn new() -> Self {
        return Self {
            entry_map: Default::default(),
            start: -1,
            end: -1,
        };
    }
    pub fn get_entry_bz_at<F>(&mut self, file_pos: i64, mut access: F) -> bool
    where
        F: FnMut(EntryBz),
    {
        if file_pos < self.start {
            return true; // in disk
        }
        if file_pos > self.end {
            panic!("file_pose exceed self.end")
        }
        let entry_bz = EntryBz {
            bz: self.entry_map.get(&file_pos).unwrap(),
        };
        access(entry_bz);
        return false;
    }

    pub fn append(&mut self, entry: &Entry, deactived_serial_num_list: &[u64]) -> i64 {
        let size = entry.get_serialized_len(deactived_serial_num_list.len());
        let mut e = vec![0;size];
        entry.dump(&mut e, deactived_serial_num_list);
        if self.end == -1 {
            self.end = self.start;
        } else {
            self.end = self.end + size as i64;
        }
        self.entry_map.insert(self.end, e);
        self.end
    }

    pub fn get_all_entry_bz(&self) -> Vec<EntryBz> {
        let mut res = vec![];
        for value in self.entry_map.values() {
            let entry_bz = EntryBz { bz: value };
            res.push(entry_bz);
        }
        return res;
    }
}

pub struct CodeUpdater {
    pub update_buffer: UpdateBuffer,
    pub indexer: Arc<CodeIndexer>,
}

impl CodeUpdater {
    pub fn new(indexer: Arc<CodeIndexer>) -> Self {
        return Self {
            update_buffer: UpdateBuffer::new(),
            indexer,
        };
    }
    pub fn run_task(&mut self, task_id: i64, change_sets: &Vec<ChangeSet>) {
        for change_set in change_sets {
            change_set.run_in_shard(CODE_SHARD_ID, |op, _kh, k, v, _r| match op {
                OP_CREATE => self.create_kv(task_id, k, v),
                _ => {
                    panic!("CodeUpdater: unsupported operation");
                }
            });
        }
    }

    fn create_kv(&mut self, task_id: i64, code_hash: &[u8], value: &[u8]) {
        let new_entry = Entry {
            key: &[0u8],
            value: value,
            next_key_hash: code_hash,
            version: task_id,
            last_version: -1,
            serial_number: 0,
        };
        let new_pos = self.update_buffer.append(&new_entry, &[]);
        self.indexer.add_kv(code_hash, new_pos);
    }
}

pub struct EntryUpdater {
    shard_id: usize,
    indexer: Arc<BTreeIndexer>,

    cache: Arc<EntryCache>,
    entry_file: Arc<EntryFile>,
    read_entry_buf: Vec<u8>,
    curr_version: i64,

    update_buffer: UpdateBuffer,
    pub sn_end: u64,

    compact_start: i64,
    compact_thres: usize,
}

impl EntryUpdater {
    pub fn new(
        shard_id: usize,
        cache: Arc<EntryCache>,
        entry_file: Arc<EntryFile>,
        indexer: Arc<BTreeIndexer>,
        curr_version: i64,
        sn_end: u64,
        compact_start: i64,
        compact_thres: usize,
    ) -> Self {
        Self {
            shard_id,
            cache,
            entry_file,
            indexer,
            read_entry_buf: Vec::with_capacity(DEFAULT_ENTRY_SIZE),
            curr_version,
            sn_end,
            compact_start,
            update_buffer: UpdateBuffer::new(),
            compact_thres,
        }
    }
    pub fn run_task(&mut self, change_sets: &Vec<ChangeSet>) {
        for change_set in change_sets {
            change_set.run_in_shard(self.shard_id, |op, key_hash, k, v, r| match op {
                OP_WRITE => self.write_kv(&key_hash, k, v, r),
                OP_CREATE => self.create_kv(&key_hash, k, v, r),
                OP_DELETE => self.delete_kv(&key_hash, k, r),
                _ => {}
            });
        }
    }

    fn write_kv(
        &mut self,
        key_hash: &[u8; 32],
        key: &[u8],
        value: &[u8],
        r: Option<&Box<OpRecord>>,
    ) {
        let k64 = BigEndian::read_u64(&key_hash[0..8]);
        let mut old_pos = -1;
        let indexer = self.indexer.clone();
        indexer.for_each_value(k64, |file_pos| -> bool {
            self.read_entry(self.shard_id, file_pos);
            let old_entry = EntryBz {
                bz: &self.read_entry_buf[..],
            };
            if old_entry.key() == key {
                old_pos = file_pos;
            }
            old_pos >= 0
        });
        if old_pos < 0 {
            panic!("Write to non-exist key");
        }
        let old_entry = EntryBz {
            bz: &self.read_entry_buf[..],
        };
        let new_entry = Entry {
            key,
            value,
            next_key_hash: old_entry.next_key_hash(),
            version: self.curr_version,
            last_version: old_entry.version(),
            serial_number: self.sn_end,
        };
        let dsn_list: [u64; 1] = [old_entry.serial_number()];
        let new_pos = self.update_buffer.append(&new_entry, &dsn_list[..]);
        self.indexer.change_kv(k64, old_pos, new_pos);
        self.try_compact();
    }

    fn delete_kv(&mut self, key_hash: &[u8; 32], key: &[u8], r: Option<&Box<OpRecord>>) {
        let k64 = BigEndian::read_u64(&key_hash[0..8]);
        let mut del_entry_pos = -1;
        let mut del_entry_sn = 0;
        let mut old_next_key_hash = [0u8; 32];
        let indexer = self.indexer.clone();
        indexer.for_each_value(k64, |file_pos| -> bool {
            self.read_entry(self.shard_id, file_pos);
            let entry_bz = EntryBz {
                bz: &self.read_entry_buf[..],
            };
            if entry_bz.key() == key {
                del_entry_pos = file_pos;
                del_entry_sn = entry_bz.serial_number();
                old_next_key_hash.copy_from_slice(entry_bz.next_key_hash());
            }
            del_entry_pos >= 0 // break if del_entry_pos was assigned
        });
        if del_entry_pos < 0 {
            panic!("Delete non-exist key");
        }
        self.indexer.erase_kv(k64, del_entry_pos);

        let mut prev_k64 = 0;
        let mut old_pos = -1;
        indexer.for_each_adjacent_value(k64, |k64_adj, file_pos| -> bool {
            if file_pos == del_entry_pos {
                return false; // skip myself entry, continue loop
            }
            self.read_entry(self.shard_id, file_pos);
            let prev_entry = EntryBz {
                bz: &self.read_entry_buf[..],
            };
            if prev_entry.next_key_hash() == key_hash {
                prev_k64 = k64_adj;
                old_pos = file_pos;
            }
            old_pos >= 0 // exit loop if old_pos was assigned
        });
        if old_pos < 0 {
            panic!("Cannot find prevEntry");
        }
        let prev_entry = EntryBz {
            bz: &self.read_entry_buf[..],
        };
        let prev_changed = Entry {
            key: prev_entry.key(),
            value: prev_entry.value(),
            next_key_hash: &old_next_key_hash[..],
            version: self.curr_version,
            last_version: prev_entry.version(),
            serial_number: self.sn_end,
        };
        let deactived_sn_list: [u64; 2] = [del_entry_sn, prev_entry.serial_number()];
        let new_pos = self
            .update_buffer
            .append(&prev_changed, &deactived_sn_list[..]);

        self.sn_end += 1;
        self.indexer.change_kv(prev_k64, old_pos, new_pos);
    }

    fn create_kv(
        &mut self,
        key_hash: &[u8; 32],
        key: &[u8],
        value: &[u8],
        r: Option<&Box<OpRecord>>,
    ) {
        let k64 = BigEndian::read_u64(&key_hash[0..8]);
        let mut old_pos = -1;
        let mut prev_k64 = 0;
        let indexer = self.indexer.clone();
        indexer.for_each_adjacent_value(k64, |k64_adj, file_pos| -> bool {
            self.read_entry(self.shard_id, file_pos);
            let prev_entry = EntryBz {
                bz: &self.read_entry_buf[..],
            };
            if prev_entry.key_hash() < *key_hash && &key_hash[..] < prev_entry.next_key_hash() {
                prev_k64 = k64_adj;
                old_pos = file_pos;
            }
            old_pos >= 0
        });
        if old_pos < 0 {
            panic!(
                "Write to non-exist key shard_id={} key={:?}",
                self.shard_id, key
            );
        }
        let prev_entry = EntryBz {
            bz: &self.read_entry_buf[..],
        };
        let new_entry = Entry {
            key,
            value,
            next_key_hash: prev_entry.next_key_hash(),
            version: self.curr_version,
            last_version: -1,
            serial_number: self.sn_end,
        };
        let create_pos = self.update_buffer.append(&new_entry, &[]);
        let hash = hasher::hash(key);
        let prev_changed = Entry {
            key: prev_entry.key(),
            value: prev_entry.value(),
            next_key_hash: &hash[..],
            version: self.curr_version,
            last_version: prev_entry.version(),
            serial_number: self.sn_end + 1,
        };
        let deactivated_sn_list: [u64; 1] = [prev_entry.serial_number()];
        let new_pos = self
            .update_buffer
            .append(&prev_changed, &deactivated_sn_list[..]);
        self.indexer.add_kv(k64, create_pos);
        self.indexer.change_kv(prev_k64, old_pos, new_pos);
        self.try_compact();
        self.try_compact();
    }

    fn try_compact(&mut self) {
        if self.indexer.len(self.shard_id) < self.compact_thres {
            return;
        }
        let mut bz: Vec<u8> = vec![0; DEFAULT_ENTRY_SIZE];
        let size = self.entry_file.read_entry(self.compact_start, &mut bz[..]);
        if bz.len() < size {
            bz.resize(size, 0);
            self.entry_file.read_entry(self.compact_start, &mut bz[..]);
        }
        self.compact_start += bz.len() as i64;
        let old_entry = EntryBz { bz: &bz[..size] };
        let new_entry = Entry {
            key: old_entry.key(),
            value: old_entry.value(),
            next_key_hash: old_entry.next_key_hash(),
            version: old_entry.version(),
            last_version: old_entry.last_version(),
            serial_number: self.sn_end,
        };
        self.sn_end += 1;
        self.update_buffer
            .append(&new_entry, &vec![old_entry.serial_number()]);
    }

    pub fn get_all_entry_bz(&self) -> Vec<EntryBz> {
        return self.update_buffer.get_all_entry_bz();
    }

    pub fn read_entry(&mut self, shard_id: usize, file_pos: i64) {
        let cache_hit = self.cache.lookup(shard_id, file_pos, |entry_bz| {
            self.read_entry_buf.resize(0, 0);
            self.read_entry_buf.extend_from_slice(entry_bz.bz);
        });
        if cache_hit {
            return;
        }
        let in_disk = self.update_buffer.get_entry_bz_at(file_pos, |entry_bz| {
            self.read_entry_buf.resize(0, 0);
            self.read_entry_buf.extend_from_slice(entry_bz.bz);
            self.cache.insert(shard_id, file_pos, &entry_bz);
        });
        if !in_disk {
            return;
        }
        self.read_entry_buf.resize(DEFAULT_ENTRY_SIZE, 0);
        let ef = &self.entry_file;
        let size = ef.read_entry(file_pos, &mut self.read_entry_buf[..]);
        self.read_entry_buf.resize(size, 0);
        if self.read_entry_buf.len() < size {
            ef.read_entry(file_pos, &mut self.read_entry_buf[..]);
        }
    }
}
