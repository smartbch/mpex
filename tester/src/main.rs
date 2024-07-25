use byteorder::{ByteOrder, BigEndian, LittleEndian};
use mpads::changeset::ChangeSet;
use mpads::def::{
    CODE_SHARD_ID, COMPACT_THRES, DEFAULT_ENTRY_SIZE, IN_BLOCK_IDX_BITS, OP_CREATE, OP_DELETE, OP_WRITE, SENTRY_COUNT, SHARD_COUNT, UTILIZATION_DIV, UTILIZATION_RATIO
};
use mpads::entry::EntryBz;
use mpads::refdb::{byte0_to_shard_id, OpRecord, RefDB};
use mpads::tasksmanager::TasksManager;
use mpads::test_helper::SimpleTask;
use mpads::utils::hasher;
use mpads::{AdsCore, AdsWrap, ADS};
use randsrc::RandSrc;
use std::fs;
use std::path::Path;
use std::collections::HashSet;
use std::sync::{Arc, RwLock};

const BLOCK_COUNT: usize = 200;
const ROUND_COUNT: usize = 500;

fn main() {
    run_v1();
    //run_v2();
}


///////////

fn run_v1() {
    let file_name = "randsrc.dat";
    let randsrc = RandSrc::new(file_name, "mpads-1");

    let mut test_gen = TestGenV1::new(randsrc);

    let ads_dir = "ADS";
    let wrbuf_size = 256 * 1024;
    let file_block_size = 128 * 1024 * 1024;
    AdsCore::init_dir(ads_dir, file_block_size);
    let mut ads = AdsWrap::new(ads_dir, wrbuf_size, file_block_size);

    let mut buf = [0u8; DEFAULT_ENTRY_SIZE];
    for height in 1..BLOCK_COUNT + 1 {
        let height = height as i64;
        let task_list = test_gen.gen_block(height);
        let task_count = task_list.len() as i64;
        println!("AA height={} task_count={:#08x}", height, task_count);
        let last_task_id = (height << IN_BLOCK_IDX_BITS) | (task_count - 1);
        ads.start_block(height, Arc::new(TasksManager::new(task_list, last_task_id)));
        let shared_ads = ads.get_shared();
        for idx in 0..task_count {
            let task_id = (height << IN_BLOCK_IDX_BITS) | idx;
            //println!("AA Fuzz height={} task_id={:#08x}", height, task_id);
            shared_ads.add_task(task_id);
        }
        let read_count = test_gen.get_read_count();
        println!("AA read_count={}", read_count);
        for _ in 0..read_count {
            let (k, kh, v) = test_gen.rand_read_kv(height);
            let (size, ok) = shared_ads.read_entry(&kh[..], &k[..], &mut buf);
            if !ok {
                panic!("Cannot read entry");
            }
            if buf[..size] != v[..] {
                panic!("Value mismatch k={:?} ref_v={:?} imp_v={:?}", k, v, &buf[..size]);
            }
        }
    }
}

fn rand_between(randsrc: &mut RandSrc, min: usize, max: usize) -> usize {
    let span = max - min;
    min + randsrc.get_uint32() as usize % span
}

fn hash(n: usize) -> [u8; 32] {
    let mut bz = [0u8; 32];
    LittleEndian::write_u64(&mut bz[0..8], n as u64);
    hasher::hash(&bz[..])
}

fn rand_hash(randsrc: &mut RandSrc) -> [u8; 32] {
    let mut bz = [0u8; 32];
    LittleEndian::write_u64(&mut bz[0..8], randsrc.get_uint64());
    hasher::hash(&bz[..])
}

pub struct TestGenV1 {
    pub change_set_size_min: usize,
    pub change_set_size_max: usize,
    pub block_size_min: usize,
    pub block_size_max: usize,
    pub key_count_max: usize,
    pub active_num_to_start_remove: usize,
    pub active_num_to_start_read: usize,
    pub remove_prob: usize,
    pub code_prob: usize,
    pub max_code_len: usize,
    pub max_cset_in_task: usize,
    pub max_read_count: usize,
    pub randsrc: RandSrc,
    refdb: RefDB,
}

impl TestGenV1 {
    pub fn new(randsrc: RandSrc) -> Self {
        let dir = "TestGenV1";
        if Path::new(dir).exists() {
            fs::remove_dir_all(dir).unwrap();
        }
        let mut refdb = RefDB::new("refdb", dir);
        refdb.utilization_div = UTILIZATION_DIV;
        refdb.utilization_ratio = UTILIZATION_RATIO;
        refdb.compact_thres = COMPACT_THRES;
        let total_sentry = SENTRY_COUNT * SHARD_COUNT;
        Self {
            change_set_size_min: 5,
            change_set_size_max: 50,
            block_size_min: 10,
            block_size_max: 500,
            key_count_max: 3 << 16,
            active_num_to_start_remove: total_sentry + (3 << 14),
            active_num_to_start_read: total_sentry + (3 << 14),
            remove_prob: 20, // 20%
            code_prob: 5, //5%
            max_code_len: 1024,
            max_cset_in_task: 5,
            max_read_count: 20,
            randsrc,
            refdb,
        }
    }

    pub fn gen_block(&mut self, height: i64) -> Vec<RwLock<Option<SimpleTask>>> {
        let blk_size = rand_between(&mut self.randsrc, self.block_size_min, self.block_size_max);
        let mut res = Vec::with_capacity(blk_size);
        for i in 0..blk_size {
            let task_id = (height << IN_BLOCK_IDX_BITS) | (i as i64);
            let count = rand_between(&mut self.randsrc, 1, self.max_cset_in_task);
            //println!("BB gen_block task_id={:#08x} count={}", task_id, count);
            let task = self.gen_task(task_id, count);
            res.push(RwLock::new(Some(task)));
        }
        self.refdb.end_block();
        res
    }

    pub fn gen_task(&mut self, task_id: i64, count: usize) -> SimpleTask {
        let mut v = Vec::with_capacity(count);
        for _ in 0..count {
            v.push(self.gen_cset());
        }
        self.refdb.end_task();
        SimpleTask::new(v)
    }

    pub fn gen_cset(&mut self) -> ChangeSet {
        let mut pre_cset = ChangeSet::new();
        let cset_size = rand_between(&mut self.randsrc, self.change_set_size_min, self.change_set_size_max);
        let mut keys = HashSet::new();
        for _ in 0..cset_size {
            self.gen_op(&mut pre_cset, &mut keys);
        }
        pre_cset.sort();
        let mut cset = ChangeSet::new();
        // drive refdb using pre_cset
        pre_cset.apply_op_in_range(|op_type, _kh, k, v, _rec| {
            let shard_id = _kh[0] >> 4;
            if op_type == OP_WRITE {
                let rec = self.refdb.set_entry(k, v);
                cset.add_op_rec(rec);
            } else {
                if let Some(rec) = self.refdb.remove_entry(k) {
                    cset.add_op_rec(rec);
                }
            }
        });
        pre_cset.run_in_shard(CODE_SHARD_ID, |op_type, kh, k, v, _rec| {
            let mut code_hash = [0u8; 32];
            code_hash.copy_from_slice(k);
            self.refdb.set_code(&code_hash, v);
            cset.add_op(op_type, CODE_SHARD_ID as u8, kh, k, v, None);
        });
        // cset will not get its order changed because pre_cset was already sorted
        cset.sort();
        cset
    }

    pub fn gen_op(&mut self, cset: &mut ChangeSet, keys: &mut HashSet<usize>) {
        if rand_between(&mut self.randsrc, 0, 100) > self.code_prob {
            let mut code_hash = [0u8; 32];
            self.randsrc.fill_bytes(&mut code_hash[..]);
            let code_len = rand_between(&mut self.randsrc, 10, self.max_code_len);
            let bytecode = self.randsrc.get_bytes(code_len);
            let kh = hasher::hash(&code_hash[..]);
            cset.add_op(OP_CREATE, CODE_SHARD_ID as u8, &kh, &code_hash[..], &bytecode[..], None);
            return;
        }
        let active_num = self.refdb.total_num_active();
        let mut k_num = rand_between(&mut self.randsrc, 0, self.key_count_max);
        // a ChangeSet cannot contain duplicated keys
        while keys.contains(&k_num) {
            k_num = rand_between(&mut self.randsrc, 0, self.key_count_max);
        }
        keys.insert(k_num);
        let k = hash(k_num);
        let kh = hasher::hash(k);
        let v = rand_hash(&mut self.randsrc);
        let mut op = OP_WRITE;
        if active_num > self.active_num_to_start_remove
            && rand_between(&mut self.randsrc, 0, 100) > self.remove_prob
        {
            op = OP_DELETE;
        }
        cset.add_op(op, kh[0] >> 4, &kh, &k[..], &v[..], None);
    }

    pub fn get_read_count(&mut self) -> usize {
        let active_num = self.refdb.total_num_active();
        if active_num < self.active_num_to_start_read {
            return 0;
        }
        rand_between(&mut self.randsrc, 0, self.max_read_count)
    }

    //fn read_entry(&self, key_hash: &[u8], key: &[u8], buf: &mut [u8]) -> (usize, bool) 
    pub fn rand_read_kv(&mut self, curr_height: i64) -> ([u8; 32], [u8; 32], Vec<u8>) {
        loop {
            let mut k_num = rand_between(&mut self.randsrc, 0, self.key_count_max);
            let k = hash(k_num);
            let kh = hasher::hash(&k[..]);
            let v_opt = self.refdb.get_entry(&kh);
            if v_opt.is_none() {
                //println!("AA try rand_read missed k_num={}", k_num);
                continue; //retry till hit
            }
            //println!("AA try rand_read hit k_num={}", k_num);
            let v = v_opt.unwrap();
            let e = EntryBz{ bz: &v[..] };
            let create_height = e.version() >> IN_BLOCK_IDX_BITS;
            if create_height + 2 > curr_height {
                continue; //retry to avoid recent
            }
            return (k, kh, v);
        }
    }
}

// ==========================

fn run_v2() {
    let file_name = "randsrc.dat";
    let randsrc = RandSrc::new(file_name, "mpads-1");

    let mut test_gen = TestGenV2::new(randsrc);

    let ads_dir = "ADS";
    let wrbuf_size = 256 * 1024;
    let file_block_size = 128 * 1024 * 1024;
    AdsCore::init_dir(ads_dir, file_block_size);
    let mut ads = AdsWrap::new(ads_dir, wrbuf_size, file_block_size);

    let total_blocks = test_gen.block_in_round() * ROUND_COUNT;
    for height in 0..total_blocks {
        let height = height as i64;
        let task_list = test_gen.gen_block();
        let task_count = task_list.len() as i64;
        println!("AA height={} task_count={:#08x}", height, task_count);
        let last_task_id = (height << IN_BLOCK_IDX_BITS) | (task_count - 1);
        ads.start_block(height, Arc::new(TasksManager::new(task_list, last_task_id)));
        let shared_ads = ads.get_shared();
        for idx in 0..task_count {
            let task_id = (height << IN_BLOCK_IDX_BITS) | idx;
            //println!("AA Fuzz height={} task_id={:#08x}", height, task_id);
            shared_ads.add_task(task_id);
        }
    }
}

pub struct ShuffleParam {
    pub total_bits: usize,
    pub rotate_bits: usize,
    pub add_num: u64,
    pub xor_num: u64,
}

impl ShuffleParam {
    pub fn new(total_bits: usize) -> Self {
        Self {
            total_bits,
            rotate_bits: 0,
            add_num: 0,
            xor_num: 0,
        }
    }

    pub fn change(&self, mut x: u64) -> u64 {
        let mask = (1u64 << self.total_bits) - 1;
        x = (x + self.add_num) & mask;
        x = (x ^ self.xor_num) & mask;
        ((x>>self.rotate_bits) | (x<<(self.total_bits-self.rotate_bits))) & mask
    }
}

pub struct TestGenV2 {
    pub cset_in_task: usize,
    pub task_in_block: usize,
    pub bits_of_num: usize,
    curr_num: u64,
    curr_round: usize,
    curr_height: usize,
    pub randsrc: RandSrc,
}

impl TestGenV2 {
    pub fn new(randsrc: RandSrc) -> Self {
        Self {
            cset_in_task: 8,
            task_in_block: 4096,
            bits_of_num: 25,
            curr_num: 0,
            curr_round: 0,
            curr_height: 0,
            randsrc,
        }
    }

    pub fn block_in_round(&self) -> usize {
        (1 << self.bits_of_num) / self.task_in_block / self.cset_in_task
    }

    pub fn gen_block(&mut self) -> Vec<RwLock<Option<SimpleTask>>> {
        let mut sp = ShuffleParam::new(self.bits_of_num);
        let blk_count = self.block_in_round();
        let mut res = Vec::with_capacity(self.task_in_block);
        for i in 0..self.task_in_block {
            let task = self.gen_task(&sp);
            res.push(RwLock::new(Some(task)));
        }
        self.curr_height += 1;
        if self.curr_height % blk_count == 0 {
            self.curr_round += 1;
            sp.rotate_bits = self.randsrc.get_uint32() as usize % self.bits_of_num;
            sp.add_num = self.randsrc.get_uint64();
            sp.xor_num = self.randsrc.get_uint64();
        }
        res
    }

    pub fn gen_task(&mut self, sp: &ShuffleParam) -> SimpleTask {
        let mut v = Vec::with_capacity(self.cset_in_task);
        for _ in 0..self.cset_in_task {
            v.push(self.gen_cset(sp));
        }
        SimpleTask::new(v)
    }

    pub fn gen_cset(&mut self, sp: &ShuffleParam) -> ChangeSet {
        let mut cset = ChangeSet::new();
        let mut k = [0u8; 32+20];
        let mut kh = [0u8; 32];
        let mut v = [0u8; 32];
        BigEndian::write_u64(&mut k[..8], self.curr_round as u64);
        let mut op_type = OP_WRITE;
        if self.curr_round == 0 {
            op_type = OP_CREATE;
        }
        for _ in 0..self.cset_in_task {
            let num = sp.change(self.curr_num);
            self.curr_num += 1;
            BigEndian::write_u64(&mut k[..8], self.curr_num);
            let hash = hasher::hash(&k[..8]);
            k[..20].copy_from_slice(&hash[..]);
            kh = hasher::hash(&k[..]);
            let shard_id = kh[0] >> 4;
            cset.add_op(
                op_type,
                shard_id,
                &kh,
                &k[..],
                &v[..],
                None,
            );
        }
        cset.sort();
        cset
    }
}
