use byteorder::{ByteOrder, BigEndian, LittleEndian};
use mpads::changeset::ChangeSet;
use mpads::def::{
    CODE_SHARD_ID, COMPACT_THRES, DEFAULT_ENTRY_SIZE, IN_BLOCK_IDX_BITS, OP_CREATE, OP_DELETE, OP_WRITE, SENTRY_COUNT, SHARD_COUNT, UTILIZATION_DIV, UTILIZATION_RATIO
};
use mpads::entry::EntryBz;
use mpads::refdb::{byte0_to_shard_id, OpRecord, RefDB};
use mpads::tasksmanager::TasksManager;
use mpads::test_helper::{SimpleTask, TempDir};
use mpads::utils::hasher;
use mpads::{AdsCore, AdsWrap, ADS};
use randsrc::RandSrc;
use std::{fs, thread};
use std::path::Path;
use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::thread::sleep;
use std::time::{Duration, Instant};

const ROUND_COUNT: usize = 2;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut total_bits: usize = 20;
    if args.len() == 2 {
        let bits: i32 = match args[1].parse() {
            Ok(n) => n,
            Err(_) => 0,
        };
        if bits != 0 {
            total_bits = bits as usize;
        }
    }
    println!("total_bits: {}", total_bits);

    let file_name = "randsrc.dat";
    let randsrc = RandSrc::new(file_name, "mpads-1");

    let mut test_gen = TestGenV2::new(randsrc, total_bits);

    let ads_dir = "ADS";
    let wrbuf_size = 8 * 1024 * 1024; //8MB
    let file_block_size = 2 * 1024 * 1024 * 1024; //2GB
    AdsCore::init_dir(ads_dir, file_block_size);
    let mut ads = AdsWrap::new(ads_dir, wrbuf_size, file_block_size);

    let blk_in_round = test_gen.block_in_round();
    let total_blocks = blk_in_round * ROUND_COUNT;
    println!("AA block_in_round={} total_blocks={}", blk_in_round, total_blocks);
    let mut now = Instant::now();
    let mut start = Instant::now();
    for height in 1..(1+total_blocks as i64){
        println!("END height={}", height);
        let task_list = test_gen.gen_block();
        let task_count = task_list.len() as i64;
        if height as usize == blk_in_round {
            start = Instant::now();
        }
        if height as usize > blk_in_round && height % 10 == 5 {
            let elapsed = now.elapsed();
            println!("Elapse height={} task_count={:#08x} elapsed={:.2?}", height, task_count, elapsed);
            now = Instant::now();
        }
        let last_task_id = (height << IN_BLOCK_IDX_BITS) | (task_count - 1);
        ads.start_block(height, Arc::new(TasksManager::new(task_list, last_task_id)));
        let shared_ads = ads.get_shared();
        for idx in 0..task_count {
            let task_id = (height << IN_BLOCK_IDX_BITS) | idx;
            //println!("AA bench height={} task_id={:#08x}", height, task_id);
            shared_ads.add_task(task_id);
        }
    }
    let elapsed = start.elapsed();
    println!("Since beginning of round#1 elapsed={:.2?}", elapsed);

    // check
    
    //thread::sleep(Duration::from_secs(30));

    //let mut buf = [0u8; DEFAULT_ENTRY_SIZE];
    //let mut v = [0u8; 32];
    //BigEndian::write_u64(&mut v[..8], (ROUND_COUNT - 1) as u64);
    //for num in 0..(1<<test_gen.sp.total_bits) {
    //    let mut k = [0u8; 32+20];
    //    BigEndian::write_u64(&mut k[..8], num);
    //    let hash = hasher::hash(&k[..8]);
    //    k[20..20+32].copy_from_slice(&hash[..]);
    //    let kh = hasher::hash(&k[..]);
    //    let shared_ads = ads.get_shared();
    //    let (size, ok) = shared_ads.read_entry(&kh[..], &k[..], &mut buf);
    //    if !ok {
    //        panic!("Cannot read entry num={} k={:?}", num, k);
    //    }
    //    let entry_bz = EntryBz{ bz: &buf[..size] };
    //    if entry_bz.value() != v {
    //        panic!("Value mismatch k={:?} ref_v={:?} imp_v={:?}", k, v, entry_bz.value());
    //    }
    //}
}

#[derive(Debug)]
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
        x = x.reverse_bits() >> (64 - self.total_bits);
        x = x + self.add_num;
        x = (!x) & mask;
        x = (x>>self.rotate_bits) | (x<<(self.total_bits-self.rotate_bits));
        x = x ^ self.xor_num;

        let mut buf = [0u8; 8];
        BigEndian::write_u64(&mut buf[..8], x);
        let hash = hasher::hash(&buf[..]);
        x = BigEndian::read_u64(&hash[..8]);

        x & mask
    }
}

pub struct TestGenV2 {
    pub op_in_cset: usize,
    pub cset_in_task: usize,
    pub task_in_block: usize,
    cur_num: u64,
    cur_round: usize,
    block_count: usize,
    pub randsrc: RandSrc,
    sp: ShuffleParam
}

impl TestGenV2 {
    pub fn new(randsrc: RandSrc, total_bits: usize) -> Self {
        Self {
            op_in_cset: 8,
            cset_in_task: 2,
            task_in_block: 8 * 1024,
            cur_num: 0,
            cur_round: 0,
            block_count: 0,
            randsrc,
            sp: ShuffleParam::new(total_bits),
        }
    }

    pub fn block_in_round(&self) -> usize {
        let result = (1 << self.sp.total_bits) / self.task_in_block / self.cset_in_task / self.op_in_cset;
        if result == 0 {
            panic!("total_bits not enough");
        }
        result
    }

    pub fn gen_block(&mut self) -> Vec<RwLock<Option<SimpleTask>>> {
        let blk_in_round = self.block_in_round();
        if self.block_count != 0 && self.block_count % blk_in_round == 0  {
            self.cur_round += 1;
            self.cur_num = 0;
            self.sp.rotate_bits = self.randsrc.get_uint32() as usize % self.sp.total_bits;
            self.sp.add_num = self.randsrc.get_uint64();
            self.sp.xor_num = self.randsrc.get_uint64();
        }
        //println!("AA gen_block cur_round={} block_count={} sp={:?}", self.cur_round, self.block_count, self.sp);
        let mut res = Vec::with_capacity(self.task_in_block);
        for i in 0..self.task_in_block {
            let task = self.gen_task();
            res.push(RwLock::new(Some(task)));
        }
        self.block_count += 1;
        res
    }

    pub fn gen_task(&mut self) -> SimpleTask {
        let mut v = Vec::with_capacity(self.cset_in_task);
        for _ in 0..self.cset_in_task {
            v.push(self.gen_cset());
        }
        SimpleTask::new(v)
    }

    pub fn gen_cset(&mut self) -> ChangeSet {
        let mut cset = ChangeSet::new();
        let mut k = [0u8; 32+20];
        let mut v = [0u8; 32];
        let mut op_type = OP_WRITE;
        if self.cur_round == 0 {
            op_type = OP_CREATE;
        }
        for _ in 0..self.op_in_cset {
            let mut num = self.cur_num;
            if self.cur_round > 0 {
                num = self.sp.change(self.cur_num);
            }
            BigEndian::write_u64(&mut k[..8], num);
            let hash = hasher::hash(&k[..8]);
            k[20..20+32].copy_from_slice(&hash[..]);
            let kh = hasher::hash(&k[..]);
            let k64 = BigEndian::read_u64(&kh[0..8]);
            //println!("AA blkcnt={:#04x} r={:#04x} op={} cur_num={:#08x} num={:#08x} k64={:#016x} k={:?} kh={:?}", self.block_count, self.cur_round, op_type, self.cur_num, num, k64, k, kh);
            self.cur_num += 1;

            BigEndian::write_u64(&mut v[..32], self.cur_round as u64);
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
