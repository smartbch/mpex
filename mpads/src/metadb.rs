use crate::def::{SHARD_COUNT, TWIG_SHIFT};
use crate::rocksdb::{RocksBatch, RocksDB};
use crate::utils::codec::*;

const BYTE_CURR_HEIGHT: u8 = 0x10;
const BYTE_TWIG_FILE_SIZE: u8 = 0x11;
const BYTE_ENTRY_FILE_SIZE: u8 = 0x12;
const BYTE_FIRST_TWIG_AT_HEIGHT: u8 = 0x13;
const BYTE_LAST_PRUNED_TWIG: u8 = 0x14;
const BYTE_EDGE_NODES: u8 = 0x15;
const BYTE_NEXT_SERIAL_NUM: u8 = 0x16;
const BYTE_OLDEST_ACTIVE_SN: u8 = 0x17;
const BYTE_OLDEST_ACTIVE_FILE_POS: u8 = 0x18; // TODO
const BYTE_ROOT_HASH: u8 = 0x19;
const BYTE_CODE_FILE_SIZE: u8 = 0x20;

pub struct MetaDB {
    kvdb: RocksDB,
    curr_height: i64,
    last_pruned_twig: [u64; SHARD_COUNT],
    next_serial_num: [u64; SHARD_COUNT],
    oldest_active_sn: [u64; SHARD_COUNT],
    oldest_active_file_pos: [i64; SHARD_COUNT],
    root_hash: [[u8; 32]; SHARD_COUNT],
    //TODO:
    edge_nodes: [Vec<u8>; SHARD_COUNT],
    twig_file_sizes: [i64; SHARD_COUNT],
    entry_file_sizes: [i64; SHARD_COUNT],
    code_file_size: i64,
    first_twig_at_height: [(u64, i64); SHARD_COUNT],
}

impl MetaDB {
    pub fn new_with_dir(dir: &str) -> Self {
        let mut res = Self::new(RocksDB::new("metadb", dir));
        res.reload_from_kvdb();
        res
    }

    pub fn new(kvdb: RocksDB) -> Self {
        Self {
            kvdb: kvdb,
            curr_height: 0,
            last_pruned_twig: [0; SHARD_COUNT],
            next_serial_num: [0; SHARD_COUNT],
            oldest_active_sn: [0; SHARD_COUNT],
            oldest_active_file_pos: [0; SHARD_COUNT],
            root_hash: [[0; 32]; SHARD_COUNT],
            edge_nodes: Default::default(),
            twig_file_sizes: [0; SHARD_COUNT],
            entry_file_sizes: [0; SHARD_COUNT],
            code_file_size: 0,
            first_twig_at_height: [(0, 0); SHARD_COUNT],
        }
    }

    pub fn close(&self) {
        self.kvdb.close()
    }

    pub fn reload_from_kvdb(&mut self) {
        self.curr_height = 0;
        for i in 0..SHARD_COUNT {
            self.last_pruned_twig[i] = 0;
            self.next_serial_num[i] = 0;
            self.oldest_active_sn[i] = 0;
            self.oldest_active_file_pos[i] = 0;
        }

        let bz = self.kvdb.get(&vec![BYTE_CURR_HEIGHT]);
        if bz.is_some() {
            self.curr_height = decode_le_i64(&bz.unwrap());
        }

        for i in 0..SHARD_COUNT {
            match self.kvdb.get(&vec![BYTE_LAST_PRUNED_TWIG, i as u8]) {
                Some(bz) => self.last_pruned_twig[i] = decode_le_u64(&bz),
                None => (),
            }

            match self.kvdb.get(&vec![BYTE_NEXT_SERIAL_NUM, i as u8]) {
                Some(bz) => self.next_serial_num[i] = decode_le_u64(&bz),
                None => (),
            }

            match self.kvdb.get(&vec![BYTE_OLDEST_ACTIVE_SN, i as u8]) {
                Some(bz) => self.oldest_active_sn[i] = decode_le_u64(&bz),
                None => (),
            }

            match self.kvdb.get(&vec![BYTE_OLDEST_ACTIVE_FILE_POS, i as u8]) {
                Some(bz) => self.oldest_active_file_pos[i] = decode_le_u64(&bz) as i64,
                None => (),
            }

            //TODO: read following data
            //edge_nodes: [Vec<u8>; SHARD_COUNT],

            //twig_file_sizes: [Vec<u8>; SHARD_COUNT],
            //entry_file_sizes: [Vec<u8>; SHARD_COUNT],
            //first_twig_at_height: [(u64, i64); SHARD_COUNT],

            // TODO
            match self.kvdb.get(&vec![BYTE_ROOT_HASH, i as u8]) {
                Some(bz) => {
                    self.root_hash[i].copy_from_slice(&bz);
                }
                None => (),
            }
        }
    }

    pub fn commit(&mut self) {
        let mut batch = RocksBatch::new();

        batch.set(
            &vec![BYTE_CURR_HEIGHT],
            &encode_le_u64(self.curr_height as u64),
        );

        for i in 0..SHARD_COUNT {
            batch.set(
                &vec![BYTE_LAST_PRUNED_TWIG, i as u8],
                &encode_le_u64(self.last_pruned_twig[i]),
            );

            batch.set(
                &vec![BYTE_NEXT_SERIAL_NUM, i as u8],
                &encode_le_u64(self.next_serial_num[i]),
            );

            batch.set(
                &vec![BYTE_OLDEST_ACTIVE_SN, i as u8],
                &encode_le_u64(self.oldest_active_sn[i]),
            );

            batch.set(
                &vec![BYTE_OLDEST_ACTIVE_FILE_POS, i as u8],
                &encode_le_u64(self.oldest_active_file_pos[i] as u64),
            );

            let v = self.root_hash[i];
            batch.set(&vec![BYTE_ROOT_HASH, i as u8], &v[..]);
            //TODO: write following data
            //edge_nodes: [Vec<u8>; SHARD_COUNT],
            //twig_file_sizes: [Vec<u8>; SHARD_COUNT],
            batch.set(
                &vec![BYTE_TWIG_FILE_SIZE, i as u8],
                &encode_le_u64(self.twig_file_sizes[i as usize] as u64),
            );
            batch.set(
                &vec![BYTE_EDGE_NODES, i as u8],
                &self.edge_nodes[i as usize],
            );

            //entry_file_sizes: [Vec<u8>; SHARD_COUNT],
            batch.set(
                &vec![BYTE_ENTRY_FILE_SIZE, i as u8],
                &encode_le_u64(self.entry_file_sizes[i as usize] as u64),
            );
            //first_twig_at_height: [(u64, i64); SHARD_COUNT]
            let (twig_id, height) = self.first_twig_at_height[i];
            if height == self.curr_height {
                //the height must equal curr_height
                let mut key = vec![BYTE_FIRST_TWIG_AT_HEIGHT, i as u8];
                key.append(&mut encode_le_i64(height));
                let val = encode_le_u64(twig_id);
                batch.set(key, val);
            }
        }
        batch.set(
            &vec![BYTE_CODE_FILE_SIZE],
            &encode_le_u64(self.code_file_size as u64),
        );

        self.kvdb.batch_write_sync(batch);
    }

    pub fn set_curr_height(&mut self, h: i64) {
        self.curr_height = h;
    }

    pub fn get_curr_height(&self) -> i64 {
        self.curr_height
    }

    pub fn set_twig_file_size(&mut self, shard_id: usize, size: i64) {
        self.twig_file_sizes[shard_id] = size;
    }

    pub fn get_twig_file_size(&self, shard_id: usize) -> i64 {
        match self.kvdb.get(&vec![BYTE_TWIG_FILE_SIZE, shard_id as u8]) {
            Some(v) => decode_le_i64(&v),
            None => 0,
        }
    }

    pub fn set_entry_file_size(&mut self, shard_id: usize, size: i64) {
        self.entry_file_sizes[shard_id] = size;
    }

    pub fn get_entry_file_size(&self, shard_id: usize) -> i64 {
        match self.kvdb.get(&vec![BYTE_ENTRY_FILE_SIZE, shard_id as u8]) {
            Some(v) => decode_le_i64(&v),
            None => 0,
        }
    }

    pub fn set_code_file_size(&mut self, size: i64) {
        self.code_file_size = size;
    }

    pub fn get_code_file_size(&self) -> i64 {
        match self.kvdb.get(&vec![BYTE_CODE_FILE_SIZE]) {
            Some(v) => decode_le_i64(&v),
            None => 0,
        }
    }

    pub fn set_first_twig_at_height(&mut self, shard_id: usize, height: i64, twig_id: u64) {
        self.first_twig_at_height[shard_id] = (twig_id, height);
    }

    pub fn get_first_twig_at_height(&self, shard_id: usize, height: i64) -> u64 {
        let mut key = vec![BYTE_FIRST_TWIG_AT_HEIGHT, shard_id as u8];
        key.append(&mut encode_le_i64(height));
        match self.kvdb.get(&key) {
            Some(v) => decode_le_u64(&v),
            None => u64::MAX,
        }
    }

    pub fn set_last_pruned_twig(&mut self, shard_id: usize, twig_id: u64) {
        self.last_pruned_twig[shard_id] = twig_id;
    }

    pub fn get_last_pruned_twig(&self, shard_id: usize) -> u64 {
        self.last_pruned_twig[shard_id]
    }

    pub fn get_edge_nodes(&self, shard_id: usize) -> Vec<u8> {
        self.kvdb
            .get(&vec![BYTE_EDGE_NODES, shard_id as u8])
            .unwrap()
    }

    pub fn set_edge_nodes(&mut self, shard_id: usize, bz: &[u8]) {
        self.edge_nodes[shard_id] = bz.to_vec();
    }

    pub fn get_next_serial_num(&self, shard_id: usize) -> u64 {
        self.next_serial_num[shard_id]
    }

    pub fn get_youngest_twig_id(&self, shard_id: usize) -> u64 {
        self.next_serial_num[shard_id] >> TWIG_SHIFT
    }

    pub fn set_next_serial_num(&mut self, shard_id: usize, sn: u64) {
        // called when new entry is appended
        self.next_serial_num[shard_id] = sn
    }

    pub fn get_root_hash(&self, shard_id: usize) -> [u8; 32] {
        self.root_hash[shard_id]
    }

    pub fn set_root_hash(&mut self, shard_id: usize, h: [u8; 32]) {
        self.root_hash[shard_id] = h
    }

    pub fn get_oldest_active_sn(&self, shard_id: usize) -> u64 {
        self.oldest_active_sn[shard_id]
    }

    pub fn set_oldest_active_sn(&mut self, shard_id: usize, id: u64) {
        self.oldest_active_sn[shard_id] = id
    }

    pub fn get_oldest_active_file_pos(&self, shard_id: usize) -> i64 {
        self.oldest_active_file_pos[shard_id]
    }

    pub fn set_oldest_active_file_pos(&mut self, shard_id: usize, pos: i64) {
        self.oldest_active_file_pos[shard_id] = pos
    }

    pub fn init(&mut self) {
        self.curr_height = 0;
        for i in 0..SHARD_COUNT {
            self.last_pruned_twig[i] = 0;
            self.next_serial_num[i] = 0;
            self.oldest_active_sn[i] = 0;
            self.oldest_active_file_pos[i] = 0;
            self.set_twig_file_size(i, 0);
            self.set_entry_file_size(i, 0);
        }
        self.commit();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rocksdb::RocksDB;
    use crate::test_helper::TempDir;
    use serial_test::serial;

    fn create_metadb() -> (MetaDB, TempDir) {
        let dir = TempDir::new("./testdb.db");
        let kvdb = RocksDB::new("testdb", ".");
        let mdb = MetaDB::new(kvdb);
        (mdb, dir)
    }

    #[test]
    #[serial]
    fn test_metadb_init() {
        let (mut mdb, _dir) = create_metadb();
        mdb.init();
        mdb.reload_from_kvdb();

        assert_eq!(0, mdb.get_curr_height());
        assert_eq!(0, mdb.get_code_file_size());
        for i in 0..SHARD_COUNT {
            assert_eq!(0, mdb.get_last_pruned_twig(i));
            assert_eq!(0, mdb.get_next_serial_num(i));
            assert_eq!(0, mdb.get_oldest_active_sn(i));
            assert_eq!(0, mdb.get_oldest_active_file_pos(i));
            assert_eq!(0, mdb.get_twig_file_size(i));
            assert_eq!(0, mdb.get_entry_file_size(i));
            assert_eq!([0u8; 32], mdb.get_root_hash(i));
            assert_eq!(vec![0u8; 0], mdb.get_edge_nodes(i));
        }
    }

    #[test]
    #[serial]
    fn test_metadb2() {
        let (mut mdb, _dir) = create_metadb();

        for i in 0..SHARD_COUNT {
            mdb.set_curr_height(12345);
            mdb.set_code_file_size(23456);
            mdb.set_last_pruned_twig(i, 1000 + i as u64);
            mdb.set_next_serial_num(i, 2000 + i as u64);
            mdb.set_oldest_active_sn(i, 3000 + i as u64);
            mdb.set_oldest_active_file_pos(i, 4000 + i as i64);
            mdb.set_twig_file_size(i, 5000 + i as i64);
            mdb.set_entry_file_size(i, 6000 + i as i64);
            mdb.set_root_hash(i, [i as u8; 32]);
            mdb.set_edge_nodes(i, &[i as u8; 8]);
            mdb.set_first_twig_at_height(i, 100 + i as i64, 200 + i as u64);
        }
        mdb.commit();
        mdb.reload_from_kvdb();

        assert_eq!(12345, mdb.get_curr_height());
        assert_eq!(23456, mdb.get_code_file_size());
        for i in 0..SHARD_COUNT {
            assert_eq!(1000 + i as u64, mdb.get_last_pruned_twig(i));
            assert_eq!(2000 + i as u64, mdb.get_next_serial_num(i));
            assert_eq!(3000 + i as u64, mdb.get_oldest_active_sn(i));
            assert_eq!(4000 + i as i64, mdb.get_oldest_active_file_pos(i));
            assert_eq!(5000 + i as i64, mdb.get_twig_file_size(i));
            assert_eq!(6000 + i as i64, mdb.get_entry_file_size(i));
            assert_eq!([i as u8; 32], mdb.get_root_hash(i));
            assert_eq!(vec![i as u8; 8], mdb.get_edge_nodes(i));
            // assert_eq!(200+i as u64, mdb.get_first_twig_at_height(i, 100+i as i64));
        }
    }
}
