use byteorder::{ByteOrder, LittleEndian};
use mpads::changeset::ChangeSet;
use mpads::def::{
    COMPACT_THRES, CODE_SHARD_ID, IN_BLOCK_IDX_BITS, OP_DELETE, OP_WRITE, SHARD_COUNT,
    UTILIZATION_DIV, UTILIZATION_RATIO,
};
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

fn main() {
    let file_name = "randsrc.dat";
    let randsrc = RandSrc::new(file_name, "mpads-1");

    let mut test_gen = TestGenV1::new(randsrc);

    let ads_dir = "ADS";
    let wrbuf_size = 256 * 1024;
    let file_block_size = 128 * 1024 * 1024;
    AdsCore::init_dir(ads_dir, file_block_size);
    let mut ads = AdsWrap::new(ads_dir, wrbuf_size, file_block_size);
    let shared_ads = ads.get_shared();

    for height in 1..BLOCK_COUNT + 1 {
        let height = height as i64;
        let task_list = test_gen.gen_block(height);
        let task_count = task_list.len() as i64;
        println!("AA height={} task_count={:#08x}", height, task_count);
        let last_task_id = (height << IN_BLOCK_IDX_BITS) | (task_count - 1);
        ads.start_block(height, Arc::new(TasksManager::new(task_list, last_task_id)));
        for idx in 0..task_count {
            let task_id = (height << IN_BLOCK_IDX_BITS) | idx;
            println!("AA Fuzz height={} task_id={:#08x}", height, task_id);
            shared_ads.add_task(task_id);
        }
    }
}

///////////

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
        Self {
            change_set_size_min: 5,
            change_set_size_max: 50,
            block_size_min: 10,
            block_size_max: 500,
            key_count_max: 1 << 16,
            active_num_to_start_remove: 1 << 15,
            active_num_to_start_read: 1 << 15,
            remove_prob: 20, // 20%
            code_prob: 5, //5%
            max_code_len: 1024,
            max_cset_in_task: 5,
            max_read_count: 200,
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
        pre_cset.apply_op_in_range(|op_type, _kh, k, v, _rec| {
            let shard_id = _kh[0] >> 4;
            if op_type == OP_WRITE {
                let rec = self.refdb.set_entry(k, v);
                cset.add_op_rec(rec);
            } else {
                let rec = self.refdb.remove_entry(k);
                if rec.is_some() {
                    let rec = rec.unwrap();
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
        cset.sort(); // this will not change its order
        cset
    }

    pub fn get_read_count(&mut self) -> usize {
        let active_num = self.refdb.total_num_active();
        if active_num < self.active_num_to_start_read {
            return 0;
        }
        rand_between(&mut self.randsrc, 0, self.max_read_count)
    }

    pub fn gen_op(&mut self, cset: &mut ChangeSet, keys: &mut HashSet<usize>) {
        if rand_between(&mut self.randsrc, 0, 100) > self.code_prob {
            let mut code_hash = [0u8; 32];
            self.randsrc.fill_bytes(&mut code_hash[..]);
            let code_len = rand_between(&mut self.randsrc, 10, self.max_code_len);
            let bytecode = self.randsrc.get_bytes(code_len);
            let kh = hasher::hash(&code_hash[..]);
            cset.add_op(OP_WRITE, CODE_SHARD_ID as u8, &kh, &code_hash[..], &bytecode[..], None);
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
}
