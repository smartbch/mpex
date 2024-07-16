use crate::def::{
    DEFAULT_ENTRY_SIZE, ENTRY_FIXED_LENGTH, IN_BLOCK_IDX_BITS, OP_CREATE, OP_DELETE, OP_WRITE,
    SENTRY_COUNT, SHARD_COUNT,
};
use crate::entry::{sentry_entry, Entry, EntryBz};
use crate::utils::hasher::hash;
use byteorder::{ByteOrder, LittleEndian};
use rocksdb::{Direction, IteratorMode, WriteBatch, WriteBatchWithTransaction, WriteOptions, DB};
use std::path::Path;
use std::sync::mpsc::SyncSender;

const KH2E: u8 = 1; // key_hash to entry
const SN2KH: u8 = 2; // serial_number to key_hash
const META: u8 = 3; //meta info (update per block)
const H2C: u8 = 4; // hash to code

#[derive(Debug, PartialEq, Clone)]
pub struct OpRecord {
    pub op_type: u8,
    pub num_active: usize,
    pub oldest_active_sn: u64,
    pub shard_id: usize,
    pub next_sn: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub rd_list: Vec<Vec<u8>>,
    pub wr_list: Vec<Vec<u8>>,
    pub dig_list: Vec<Vec<u8>>, //old entries in compaction
    pub put_list: Vec<Vec<u8>>, //new entries in compaction
}

impl OpRecord {
    pub fn new(op_type: u8) -> OpRecord {
        OpRecord {
            op_type,
            num_active: 0,
            oldest_active_sn: 0,
            shard_id: 0,
            next_sn: 0,
            key: Vec::with_capacity(0),
            value: Vec::with_capacity(0),
            rd_list: Vec::with_capacity(2),
            wr_list: Vec::with_capacity(2),
            dig_list: Vec::with_capacity(2),
            put_list: Vec::with_capacity(2),
        }
    }
}

pub struct RefDB {
    curr_version: i64,
    oldest_active_sn_arr: [u64; SHARD_COUNT],
    // the youngest active sn is: next_sn_arr[shard_id] - 1
    next_sn_arr: [u64; SHARD_COUNT],
    // number of active entries
    num_active_arr: [usize; SHARD_COUNT],
    db: DB,
    pub utilization_div: i64,
    pub utilization_ratio: i64,
    pub compact_thres: i64,
}

impl RefDB {
    pub fn new(name: &str, dir: &str) -> Self {
        let path = Path::new(dir).join(name.to_owned() + ".db");
        let mut res = RefDB {
            curr_version: 1 << IN_BLOCK_IDX_BITS,
            oldest_active_sn_arr: [0; SHARD_COUNT],
            next_sn_arr: [SENTRY_COUNT as u64; SHARD_COUNT],
            num_active_arr: [SENTRY_COUNT; SHARD_COUNT],
            db: DB::open_default(path).unwrap(),
            utilization_div: 256,
            utilization_ratio: 230,
            compact_thres: 200,
        };
        if let Some(meta_bz) = res.raw_get(&vec![META]) {
            res.load_meta_info(meta_bz);
        } else {
            res.init_db();
        }
        res
    }

    fn init_db(&self) {
        // initialize an empty DB with sentry entries
        let mut batch = WriteBatch::default();
        let mut bz = [0u8; DEFAULT_ENTRY_SIZE];
        for shard_id in 0..SHARD_COUNT {
            for sn in 0..SENTRY_COUNT {
                let e = sentry_entry(shard_id, sn as u64, &mut bz[..]);
                add_entry(&mut batch, &e.key_hash(), &e);
            }
        }
        self.write_batch_sync(batch);
    }

    fn write_batch_sync(&self, batch: WriteBatch) {
        let mut write_options = WriteOptions::default();
        write_options.set_sync(true);
        self.db.write_opt(batch, &write_options).unwrap();
    }

    fn load_meta_info(&mut self, meta_bz: Vec<u8>) {
        // if we can find meta info, this DB is not empty
        if meta_bz.len() != 8 + 8 * 3 * SHARD_COUNT {
            panic!("invalid meta info");
        }
        let mut start = 0;
        self.curr_version = LittleEndian::read_i64(&meta_bz[start..start + 8]) + 1;
        start += 8;
        for i in 0..SHARD_COUNT {
            self.oldest_active_sn_arr[i] = LittleEndian::read_u64(&meta_bz[start..start + 8]);
            start += 8;
            self.next_sn_arr[i] = LittleEndian::read_u64(&meta_bz[start..start + 8]);
            start += 8;
            self.num_active_arr[i] = LittleEndian::read_u64(&meta_bz[start..start + 8]) as usize;
            start += 8;
        }
    }

    // get code from a H2C record
    pub fn get_code(&self, prefix: &[u8]) -> Option<(i64, Vec<u8>, Vec<u8>)> {
        let mut k = [0u8; 33];
        k[0] = H2C;
        k[1..prefix.len()].copy_from_slice(prefix);
        let iter = self
            .db
            .iterator(IteratorMode::From(&k[..], Direction::Forward));
        for item in iter {
            let (key, value) = item.unwrap();
            if key[0] == H2C {
                let version = LittleEndian::read_i64(&value[..8]);
                let bytecode = value[8..].to_vec();
                return Some((version, key.to_vec(), bytecode));
            } else {
                break;
            }
        }
        None
    }

    // create a new bytecode
    pub fn set_code(&mut self, code_hash: &[u8; 32], bytecode: &[u8]) {
        let mut k = [0u8; 33];
        k[0] = H2C;
        k[1..].copy_from_slice(&code_hash[..]);
        let mut batch = WriteBatch::default();
        let mut v = vec![0u8; 8];
        LittleEndian::write_i64(&mut v[..8], self.curr_version);
        v.extend_from_slice(bytecode);
        batch.put(&k[..], v);
        self.write_batch_sync(batch);
    }

    // get an entry from a KH2E record
    pub fn get_entry(&self, key_hash: &[u8; 32]) -> Option<Vec<u8>> {
        let mut k = [0u8; 33];
        k[0] = KH2E;
        k[1..].copy_from_slice(&key_hash[..]);
        self.raw_get(&k)
    }

    // remove an entry that may or may not be existent
    pub fn remove_entry<K: AsRef<[u8]>>(&mut self, key: K) -> Option<OpRecord> {
        let key_hash = hash(key.as_ref());
        if let Some(old) = self.get_entry(&key_hash) {
            let mut op_rec = OpRecord::new(OP_DELETE);
            op_rec.key.extend_from_slice(key.as_ref());
            self.remove(&key_hash, old, &mut op_rec);
            return Some(op_rec);
        }
        None // do nothing when removing a non-existent entry
    }

    // create a new entry or update an existent entry
    pub fn set_entry<K: AsRef<[u8]>>(&mut self, key: K, value: &[u8]) -> OpRecord {
        let key_hash = hash(key.as_ref());
        let mut op_rec;
        if let Some(old) = self.get_entry(&key_hash) {
            op_rec = OpRecord::new(OP_WRITE);
            self.update(&key_hash, value, old, &mut op_rec);
        } else {
            op_rec = OpRecord::new(OP_CREATE);
            self.create(key.as_ref(), &key_hash, value, &mut op_rec);
        }
        op_rec
    }

    pub fn read_all_entries(&self, out: SyncSender<Vec<u8>>) {
        let end_hash = [0xFFu8; 32];
        let iter = self
            .db
            .iterator(IteratorMode::From(&vec![KH2E], Direction::Forward));
        for item in iter {
            let (key, value) = item.unwrap();
            if key.len() < 32 || key[3..].iter().all(|&x| x == 0) {
                continue;
            }
            let curr_entry = EntryBz { bz: &value[..] };
            let mut curr_hash = curr_entry.key_hash();
            assert_eq!(curr_hash, &key[1..], "incorrect key_hash");
            self.check_entry(&curr_entry);
            assert_eq!(curr_hash, curr_entry.key_hash(), "incorrect key_hash");
            curr_hash[..].copy_from_slice(curr_entry.next_key_hash());
            out.send(value.to_vec()).unwrap();
            if curr_hash == end_hash {
                break;
            }
        }
    }

    // flush the meta info to DB when ending a block
    pub fn end_block(&mut self) {
        let meta_info = self.encode_meta_info();
        let mut batch = WriteBatch::default();
        batch.put(&vec![META], &meta_info);
        self.write_batch_sync(batch);

        // high 40 bits is block height, low 24 bits is tx index
        self.curr_version = ((self.curr_version >> IN_BLOCK_IDX_BITS) + 1) << IN_BLOCK_IDX_BITS;
    }

    fn encode_meta_info(&self) -> Vec<u8> {
        let mut bz = [0u8; 8 + 8 * 3 * SHARD_COUNT];
        let mut start = 0;
        LittleEndian::write_i64(&mut bz[start..start + 8], self.curr_version);
        start += 8;
        for i in 0..SHARD_COUNT {
            LittleEndian::write_u64(&mut bz[start..start + 8], self.oldest_active_sn_arr[i]);
            start += 8;
            LittleEndian::write_u64(&mut bz[start..start + 8], self.next_sn_arr[i]);
            start += 8;
            LittleEndian::write_u64(&mut bz[start..start + 8], self.num_active_arr[i] as u64);
            start += 8;
        }

        Vec::from(bz)
    }

    pub fn end_task(&mut self) {
        self.curr_version += 1;
    }

    pub fn get_num_active(&self, shard_id: usize) -> usize {
        return self.num_active_arr[shard_id];
    }

    pub fn total_num_active(&self) -> usize {
        let mut count = 0;
        for shard_id in 0..SHARD_COUNT {
            count += self.num_active_arr[shard_id];
        }
        count
    }

    // =============== the private methods ===========

    fn raw_get(&self, key: &[u8]) -> Option<Vec<u8>> {
        if key.len() == 0 {
            panic!("Empty Key")
        }
        self.db.get(key).unwrap()
    }

    fn check_entry(&self, entry_bz: &EntryBz) {
        let shard_id = byte0_to_shard_id(entry_bz.key_hash()[0]);
        let kh = self
            .get_key_hash(entry_bz.serial_number(), shard_id)
            .unwrap();
        assert_eq!(entry_bz.key_hash(), kh, "incorrect sn->key_hash");
        for i in 0..entry_bz.dsn_count() {
            let option = self.get_key_hash(entry_bz.get_deactived_sn(i), shard_id);
            assert_eq!(option.is_none(), true, "sn is not deactived");
        }
    }

    // get key_hash from a SN2KH record
    fn get_key_hash(&self, sn: u64, shard_id: usize) -> Option<[u8; 32]> {
        let mut k = [0u8; 10];
        k[0] = SN2KH;
        k[1] = shard_id as u8;
        LittleEndian::write_u64(&mut k[2..], sn);
        match self.raw_get(&k) {
            None => None,
            Some(v) => {
                let mut arr = [0u8; 32];
                arr[..].copy_from_slice(&v[..]);
                Some(arr)
            }
        }
    }

    // get the previous entry from the previous KH2E record
    fn get_prev_entry(&self, key_hash: &[u8; 32]) -> ([u8; 32], Vec<u8>) {
        let mut k = [0u8; 33];
        k[0] = KH2E;
        k[1..].copy_from_slice(&key_hash[..]);
        let iter = self
            .db
            .iterator(IteratorMode::From(&k[..], Direction::Reverse));
        let mut prev_key_hash = [0u8; 32];
        for item in iter {
            let (key, value) = item.unwrap();
            prev_key_hash.copy_from_slice(&key[1..]);
            if prev_key_hash == *key_hash {
                continue; //ignore myself
            }
            return (prev_key_hash, value.to_vec());
        }
        panic!("prev_entry not found");
    }

    // remove the entry 'old'
    fn remove(&mut self, key_hash: &[u8; 32], old: Vec<u8>, op_rec: &mut OpRecord) {
        let old_entry = EntryBz { bz: &old[..] };
        let shard_id = byte0_to_shard_id(key_hash[0]);
        op_rec.num_active = self.num_active_arr[shard_id];
        op_rec.oldest_active_sn = self.oldest_active_sn_arr[shard_id];
        let (prev_key_hash, prev_e_bz) = self.get_prev_entry(key_hash);
        let prev_entry = EntryBz { bz: &prev_e_bz[..] };
        let prev_changed = Entry {
            key: prev_entry.key(),
            value: prev_entry.value(),
            // the old next_key_hash is old_entry.key_hash()
            next_key_hash: old_entry.next_key_hash(),
            version: self.curr_version,
            last_version: prev_entry.version(),
            serial_number: self.next_sn_arr[shard_id],
        };
        self.next_sn_arr[shard_id] += 1;
        let dsn_list = &mut vec![old_entry.serial_number(), prev_entry.serial_number()];
        //if dsn_list[0] > dsn_list[1] {
        //    (dsn_list[0], dsn_list[1]) = (dsn_list[1], dsn_list[0]); //sort it
        //}
        let prev_changed_bz = entry_to_bytes(&prev_changed, dsn_list);
        let e = EntryBz {
            bz: &prev_changed_bz[..],
        };
        let mut batch = WriteBatch::default();
        self.add_entry(&mut batch, &prev_key_hash, shard_id, &e);

        op_rec.shard_id = shard_id;
        op_rec.next_sn = self.next_sn_arr[shard_id];
        op_rec.rd_list.push(prev_e_bz);
        op_rec.rd_list.push(old);
        op_rec.wr_list.push(prev_changed_bz);

        remove_kh2e_record(&mut batch, key_hash);

        self.write_batch_sync(batch);
        //println!("BB s{} remove next_sn={} num_active={}", op_rec.shard_id, op_rec.next_sn, self.num_active_arr[shard_id]);
        self.num_active_arr[shard_id] -= 1;
    }

    // update an existent entry 'old': change its value to 'value'
    fn update(&mut self, key_hash: &[u8; 32], value: &[u8], old: Vec<u8>, op_rec: &mut OpRecord) {
        let old_entry = EntryBz { bz: &old[..] };
        let shard_id = byte0_to_shard_id(key_hash[0]);
        op_rec.num_active = self.num_active_arr[shard_id];
        op_rec.oldest_active_sn = self.oldest_active_sn_arr[shard_id];
        let new_entry = Entry {
            key: old_entry.key(),
            value: value,
            next_key_hash: old_entry.next_key_hash(),
            version: self.curr_version,
            last_version: old_entry.version(),
            serial_number: self.next_sn_arr[shard_id],
        };
        self.next_sn_arr[shard_id] += 1;
        let new_e_bz = entry_to_bytes(&new_entry, &vec![old_entry.serial_number()]);
        let new_e = EntryBz { bz: &new_e_bz[..] };
        let mut batch = WriteBatch::default();
        self.add_entry(&mut batch, key_hash, shard_id, &new_e);

        op_rec.shard_id = shard_id;
        op_rec.next_sn = self.next_sn_arr[shard_id];
        op_rec.key.extend_from_slice(new_entry.key);
        op_rec.value.extend_from_slice(new_entry.value);
        op_rec.rd_list.push(old);
        op_rec.wr_list.push(new_e_bz);

        self.write_batch_sync(batch);
        let mut batch = WriteBatch::default();

        self.try_compact(&mut batch, shard_id, op_rec);
        self.write_batch_sync(batch);
    }

    // create a new entry with 'key' and 'value'
    fn create(&mut self, key: &[u8], key_hash: &[u8; 32], value: &[u8], op_rec: &mut OpRecord) {
        let shard_id = byte0_to_shard_id(key_hash[0]);
        op_rec.num_active = self.num_active_arr[shard_id];
        op_rec.oldest_active_sn = self.oldest_active_sn_arr[shard_id];
        let (prev_key_hash, prev_e_bz) = self.get_prev_entry(key_hash);
        let prev_entry = EntryBz { bz: &prev_e_bz[..] };
        let new_entry = Entry {
            key: key,
            value: value,
            next_key_hash: prev_entry.next_key_hash(),
            version: self.curr_version,
            last_version: -1,
            serial_number: self.next_sn_arr[shard_id],
        };
        let mut batch = WriteBatch::default();
        let new_entry_bz = entry_to_bytes(&new_entry, &[]);
        let new_entry = EntryBz {
            bz: &new_entry_bz[..],
        };
        self.add_entry(&mut batch, key_hash, shard_id, &new_entry);

        let prev_changed = Entry {
            key: prev_entry.key(),
            value: prev_entry.value(),
            next_key_hash: &key_hash[..],
            version: self.curr_version,
            last_version: prev_entry.version(),
            serial_number: self.next_sn_arr[shard_id] + 1,
        };
        self.next_sn_arr[shard_id] += 2;
        let prev_changed_bz = entry_to_bytes(&prev_changed, &[prev_entry.serial_number()]);
        let prev_changed = EntryBz {
            bz: &prev_changed_bz[..],
        };
        self.add_entry(&mut batch, &prev_key_hash, shard_id, &prev_changed);

        self.write_batch_sync(batch);
        let mut batch = WriteBatch::default();

        op_rec.shard_id = shard_id;
        op_rec.next_sn = self.next_sn_arr[shard_id];
        op_rec.key.extend_from_slice(key);
        op_rec.value.extend_from_slice(value);
        op_rec.rd_list.push(prev_e_bz);
        op_rec.wr_list.push(prev_changed_bz);
        op_rec.wr_list.push(new_entry_bz);

        //println!("BB s{} create next_sn={} num_active={}", op_rec.shard_id, op_rec.next_sn, self.num_active_arr[shard_id]);
        self.num_active_arr[shard_id] += 1;
        self.try_compact(&mut batch, shard_id, op_rec);
        self.try_compact(&mut batch, shard_id, op_rec); // do it twice
        self.write_batch_sync(batch);
    }

    // if the utilization ratio is too slow, deactivate and recreate an old active entry
    fn try_compact(
        &mut self,
        batch: &mut WriteBatchWithTransaction<false>,
        shard_id: usize,
        op_rec: &mut OpRecord,
    ) {
        let active_count = self.num_active_arr[shard_id];
        let total_count = self.next_sn_arr[shard_id] - self.oldest_active_sn_arr[shard_id];

        //if total_count % 2000 == 0 {
        //    println!("AA Cmpt tot={} act={} {}-{} thres={}", total_count, active_count,
        //        total_count as i64 / self.utilization_div,
        //        active_count as i64 / self.utilization_ratio, self.compact_thres);
        //}
        if (total_count as i64 / self.utilization_div
            < active_count as i64 / self.utilization_ratio)
            || (total_count as i64) < self.compact_thres
        {
            return; // no need to compact
        }
        //if total_count % 2000 == 0 {
        //    println!("AA Cmpt NOW!");
        //}
        let key_hash = self.get_oldest_active(shard_id);
        let old = self.get_entry(&key_hash).unwrap();
        let old_entry = EntryBz { bz: &old[..] };
        if shard_id == 1 {
            let r = Entry::from_bz(&old_entry);
            //println!("BB s1 Cmpt old_E {:?}", r);
        }
        let new_entry = Entry {
            key: old_entry.key(),
            value: old_entry.value(),
            next_key_hash: old_entry.next_key_hash(),
            version: old_entry.version(),
            last_version: old_entry.last_version(),
            serial_number: self.next_sn_arr[shard_id],
        };
        self.next_sn_arr[shard_id] += 1;
        self.oldest_active_sn_arr[shard_id] = old_entry.serial_number() + 1;
        let e_bz = entry_to_bytes(&new_entry, &vec![old_entry.serial_number()]);
        let e = EntryBz { bz: &e_bz[..] };
        self.add_entry(batch, &key_hash, shard_id, &e);

        op_rec.dig_list.push(old);
        op_rec.put_list.push(e_bz);
    }

    fn add_entry(
        &mut self,
        batch: &mut WriteBatchWithTransaction<false>,
        key_hash: &[u8; 32],
        shard_id: usize,
        e: &EntryBz,
    ) {
        add_entry(batch, key_hash, e);

        //let mut last_sn: u64 = 0;
        //for i in 0..e.dsn_count() {
        //    let sn = e.get_deactived_sn(i);
        //    if i > 0 && last_sn >= sn {
        //        panic!("dsn_list is not sorted!");
        //    }
        //    last_sn = sn;
        //}
    }

    fn get_oldest_active(&mut self, shard_id: usize) -> [u8; 32] {
        let mut sn = self.oldest_active_sn_arr[shard_id];
        loop {
            if let Some(kh) = self.get_key_hash(sn, shard_id) {
                return kh;
            }
            sn += 1;
        }
    }
}

pub fn byte0_to_shard_id(byte0: u8) -> usize {
    (byte0 as usize) * SHARD_COUNT / 256
}

fn add_entry(batch: &mut WriteBatchWithTransaction<false>, key_hash: &[u8; 32], e: &EntryBz) {
    let shard_id = byte0_to_shard_id(key_hash[0]);
    let mut k = [0u8; 10];
    k[0] = SN2KH;
    k[1] = shard_id as u8;
    // delete the deactived SN2KH record
    for i in 0..e.dsn_count() {
        LittleEndian::write_u64(&mut k[2..], e.get_deactived_sn(i));
        let mut _k = [0u8; 10];
        _k.copy_from_slice(&k[..]);
        batch.delete(_k);
    }
    // add a new SN2KH record for the new entry
    LittleEndian::write_u64(&mut k[2..], e.serial_number());
    batch.put(&k[..], &key_hash[..]);

    let mut k = [0u8; 33];
    k[0] = KH2E;
    k[1..].copy_from_slice(&key_hash[..]);
    // add a new KH2E record for the new entry
    batch.put(k, e.bz);
}

fn remove_kh2e_record(batch: &mut WriteBatchWithTransaction<false>, key_hash: &[u8; 32]) {
    let mut k = [0u8; 33];
    k[0] = KH2E;
    k[1..].copy_from_slice(&key_hash[..]);
    batch.delete(&k[..]);
}

fn entry_to_bytes(e: &Entry, deactived_sn_list: &[u64]) -> Vec<u8> {
    let total_len = ((ENTRY_FIXED_LENGTH + e.key.len() + e.value.len() + 7) / 8) * 8
        + deactived_sn_list.len() * 8;
    let mut v = Vec::with_capacity(total_len);
    v.resize(total_len, 0);
    e.dump(&mut v[..], deactived_sn_list);
    v
}

pub fn entry_equal(bz: &[u8], e: &Entry, deactived_sn_list: &[u64]) -> bool {
    &entry_to_bytes(e, deactived_sn_list)[..] == bz
}

#[cfg(test)]
mod tests {

    use serial_test::serial;

    use crate::test_helper::TempDir;

    use super::*;
    use std::sync::mpsc::sync_channel;

    fn setup_db() -> (RefDB, TempDir) {
        let dir = TempDir::new("./tmp");
        let db = RefDB::new("testdb", "./tmp");
        (db, dir)
    }

    #[test]
    #[serial]
    fn test_new_db() {
        let (db, _dir) = setup_db();
        assert_eq!(db.curr_version, 16777216);
        for i in 0..SHARD_COUNT {
            assert_eq!(db.oldest_active_sn_arr[i], 0);
            assert_eq!(db.next_sn_arr[i], SENTRY_COUNT as u64);
            assert_eq!(db.num_active_arr[i], SENTRY_COUNT);
        }
    }

    #[test]
    #[serial]
    fn test_meta_db() {
        let (mut db, _dir) = setup_db();

        db.curr_version = 100;
        for i in 0..SHARD_COUNT {
            db.oldest_active_sn_arr[i] = 200 + i as u64;
            db.next_sn_arr[i] = 300 + i as u64;
            db.num_active_arr[i] = 400 + i;
        }
        db.end_block();

        let meta_info = db.raw_get(&vec![META]).unwrap();
        db.load_meta_info(meta_info);
        assert_eq!(101, db.curr_version);
        for i in 0..SHARD_COUNT {
            assert_eq!(200 + i as u64, db.oldest_active_sn_arr[i]);
            assert_eq!(300 + i as u64, db.next_sn_arr[i]);
            assert_eq!(400 + i, db.num_active_arr[i]);
        }
    }

    #[test]
    #[serial]
    fn test_set_entry() {
        let (mut db, _dir) = setup_db();
        let key: &[u8; 7] = b"testkey";

        let value = b"testvalue";
        let op_record = db.set_entry(key, value.as_ref());
        assert_eq!(op_record.op_type, OP_CREATE);
        let entry = db.get_entry(&hash(key)).unwrap();
        let entry_bz = EntryBz { bz: &entry[..] };
        assert_eq!(entry_bz.value(), value);
        assert_eq!(db.oldest_active_sn_arr[9], 0);
        assert_eq!(db.next_sn_arr[9], SENTRY_COUNT as u64 + 2);
        assert_eq!(db.num_active_arr[9], SENTRY_COUNT + 1);

        // update
        let value = b"testvalue1";
        let op_record = db.set_entry(key, value.as_ref());
        assert_eq!(op_record.op_type, OP_WRITE);
        let entry = db.get_entry(&hash(key)).unwrap();
        let entry_bz = EntryBz { bz: &entry[..] };
        assert_eq!(entry_bz.value(), value);
        assert_eq!(db.oldest_active_sn_arr[9], 0);
        assert_eq!(db.next_sn_arr[9], SENTRY_COUNT as u64 + 2 + 1);
        assert_eq!(db.num_active_arr[9], SENTRY_COUNT + 1);
    }

    #[test]
    #[serial]
    fn test_remove_entry() {
        let (mut db, _dir) = setup_db();
        let key = b"testkey";
        let value = b"testvalue";
        db.set_entry(key, value.as_ref());

        let op_record = db.remove_entry(key).unwrap();
        assert_eq!(op_record.op_type, OP_DELETE);
        assert_eq!(db.oldest_active_sn_arr[9], 0);
        assert_eq!(db.next_sn_arr[9], SENTRY_COUNT as u64 + 2 + 1);
        assert_eq!(db.num_active_arr[9], SENTRY_COUNT);

        let entry = db.get_entry(&hash(key));
        assert!(entry.is_none());
    }

    #[test]
    #[serial]
    fn test_read_all_entries() {
        let (mut db, _dir) = setup_db();
        let key = b"testkey";
        let value = b"testvalue";
        db.set_entry(key, value.as_ref());

        let key = b"testkey1";
        db.set_entry(key, value.as_ref());
        db.set_entry(key, value.as_ref());

        let (sender, receiver) = sync_channel(1000);
        db.read_all_entries(sender);
        let entries: Vec<Vec<u8>> = receiver.iter().collect();
        assert_eq!(entries.len(), 2);

        let key = b"testkey11111";
        db.set_entry(key, value.as_ref());

        let (sender, receiver) = sync_channel(1000);
        db.read_all_entries(sender);
        let entries: Vec<Vec<u8>> = receiver.iter().collect();
        assert_eq!(entries.len(), 3);
    }

    #[test]
    #[serial]
    fn test_end_block() {
        let mut _dir = TempDir::new("./tmp");
        {
            let mut db = RefDB::new("testdb", "./tmp");
            assert_eq!(db.curr_version, 16777216);
            db.end_block()
        }
        let db = RefDB::new("testdb", "./tmp");
        assert_eq!(db.curr_version, 16777217);
    }

    #[test]
    #[serial]
    fn test_compact() {
        let (mut db, _dir) = setup_db();
        let value = b"testvalue";

        for i in 0..3740 as usize {
            db.set_entry(i.to_ne_bytes(), value.as_ref());
        }

        assert_eq!(db.oldest_active_sn_arr[10], 2);
    }
}
