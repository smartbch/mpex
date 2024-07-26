use byteorder::{BigEndian, ByteOrder};
use exepipe::exetask::AccessSet;
use exepipe::scheduler::{PBElement, ParaBloom, MAX_TASKS_LEN_IN_BUNDLE, SET_MAX_SIZE};
use mpads::utils::hasher;
use serde::Deserialize;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::io::{self, BufRead};
use std::path::Path;
use std::mem;
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
};

#[cfg(feature = "aggregate_tx")]
const MAX_TX_IN_TASK: usize = 4;

#[cfg(not(feature = "aggregate_tx"))]
const MAX_TX_IN_TASK: usize = 1;

const START_HEIGHT: usize = 20338950;
const END_HEIGHT: usize = 20343510;

pub const WETH_ADDR: &str = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2";

fn main() {
    if cfg!(feature = "reorder") {
        run_scheduler();
    } else {
        run_serial_issuer();
    }
    //run_aggregate_tx();
}


#[derive(Deserialize, Debug, Clone)]
pub struct Slot {
    pub addr: String,
    pub index: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Tx {
    pub txid: String,
    pub rdo_addr_list: Vec<String>,
    pub rnw_addr_list: Vec<String>,
    pub rdo_slot_list: Vec<Slot>,
    pub rnw_slot_list: Vec<Slot>,
    pub from: String,
}

impl Tx {
    fn fill_access_set(&self, coinbase: &str, access_set: &mut AccessSet) {
        for rdo_addr in self.rdo_addr_list.iter() {
            let hash = hasher::hash(rdo_addr);
            if !access_set.rdo_set.contains(&hash) {
                access_set.rdo_k64_vec.push(BigEndian::read_u64(&hash[..8]));
                access_set.rdo_set.insert(hash);
            }
        }
        for rnw_addr in self.rnw_addr_list.iter() {
            if rnw_addr == coinbase {
                continue;
            }
            if cfg!(feature = "ignore_weth") {
                if rnw_addr == WETH_ADDR {
                    continue;
                }
            }
            let hash = hasher::hash(rnw_addr);
            if !access_set.rnw_set.contains(&hash) {
                access_set.rnw_k64_vec.push(BigEndian::read_u64(&hash[..8]));
                access_set.rnw_set.insert(hash);
            }
        }
        let mut buf = [0u8; 52];
        for rdo_slot in self.rdo_slot_list.iter() {
            let hash = hasher::hash("".to_owned() + &rdo_slot.addr + &rdo_slot.index);
            if !access_set.rdo_set.contains(&hash) {
                access_set.rdo_k64_vec.push(BigEndian::read_u64(&hash[..8]));
                access_set.rdo_set.insert(hash);
            }
        }
        for rnw_slot in self.rnw_slot_list.iter() {
            let hash = hasher::hash("".to_owned() + &rnw_slot.addr + &rnw_slot.index);
            if !access_set.rnw_set.contains(&hash) {
                access_set.rnw_k64_vec.push(BigEndian::read_u64(&hash[..8]));
                access_set.rnw_set.insert(hash);
            }
        }
    }

    fn to_access_set(&self, coinbase: &str) -> AccessSet {
        let mut access_set = AccessSet::new();
        self.fill_access_set(coinbase, &mut access_set);
        access_set
    }
}

fn aggregate_tx(tx_list: &Vec<Tx>) -> Vec<Vec<&Tx>> {
    let mut sender_list: Vec<&String> = Vec::with_capacity(tx_list.len());
    let mut sender_map: HashMap<&String, Vec<&Tx>> = HashMap::with_capacity(tx_list.len());
    for tx in tx_list.iter() {
        if sender_map.contains_key(&tx.from) {
            let mut v = sender_map.get_mut(&tx.from).unwrap();
            v.push(tx);
        } else {
            sender_list.push(&tx.from);
            sender_map.insert(&tx.from, vec![tx]);
        }
    }
    let mut result: Vec<Vec<&Tx>> = Vec::with_capacity(sender_list.len());
    for &sender in sender_list.iter() {
        let v = sender_map.remove(sender).unwrap();
        //println!("AA sender={} task_size= {}", sender, v.len());
        result.push(v);
    }
    result
}

pub fn run_aggregate_tx() {
    let mut total_tx = 0;
    let mut total_task = 0;
    for id in (START_HEIGHT..END_HEIGHT).step_by(10) {
        let mut file = OpenOptions::new()
            .read(true)
            .open(format!(
                "blocks/blocks_{}.json",
                id
            ))
            .expect("Could not read file");
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();

        let blocks: Vec<Block> = serde_json::from_str(&contents).unwrap();

        for blk in blocks.iter() {
            total_tx += blk.tx_list.len();
            let task_list = aggregate_tx(&blk.tx_list);
            total_task += task_list.len();
        }
    }
    println!("MergeResult total_tx={} total_task={}", total_tx, total_task);
}

#[derive(Deserialize, Debug)]
pub struct Block {
    pub height: i64,
    pub tx_list: Vec<Tx>,
    pub coinbase: String,
}

#[derive(Deserialize, Debug)]
struct User {
    fingerprint: String,
    location: String,
}

#[cfg(feature = "U128")]
type UINT = u128;
#[cfg(feature = "U64")]
type UINT = u64;
#[cfg(feature = "U32")]
type UINT = u32;

struct AccessSetList {
    rdo_set_list: Vec<HashSet<[u8; 32]>>,
    rnw_set_list: Vec<HashSet<[u8; 32]>>,
    num_sets: usize,
}

impl AccessSetList {
    pub fn new(num_sets: usize) -> Self {
        let mut res = Self {
            rdo_set_list: Vec::with_capacity(num_sets),
            rnw_set_list: Vec::with_capacity(num_sets),
            num_sets,
        };
        for _ in 0..num_sets {
            res.rdo_set_list.push(HashSet::new());
            res.rnw_set_list.push(HashSet::new());
        }
        res
    }

    pub fn get_rdo_set_size(&self, id: usize) -> usize {
        return self.rdo_set_list[id].len();
    }

    pub fn get_rnw_set_size(&self, id: usize) -> usize {
        return self.rnw_set_list[id].len();
    }

    //pub rdo_set: HashSet<[u8; 32]>,
    //pub rnw_set: HashSet<[u8; 32]>,

    pub fn get_dep_mask(&self, access_set: &AccessSet) -> UINT {
        let mut result = 0 as UINT;
        let mut mask = 1 as UINT;
        for idx in 0..self.rnw_set_list.len() {
            for hash in access_set.rdo_set.iter() {
                if self.rnw_set_list[idx].contains(hash) {
                    result = result | mask;
                }
            }
            for hash in access_set.rnw_set.iter() {
                if self.rdo_set_list[idx].contains(hash) {
                    result = result | mask;
                }
                if self.rnw_set_list[idx].contains(hash) {
                    result = result | mask;
                }
            }
            mask = mask << 1usize;
        }
        result
    }

    pub fn clear(&mut self, idx: usize) {
        self.rdo_set_list[idx].clear();
        self.rnw_set_list[idx].clear();
    }

    pub fn clear_all(&mut self) {
        for idx in 0..self.rnw_set_list.len() {
            self.rdo_set_list[idx].clear();
            self.rnw_set_list[idx].clear();
        }
    }

    pub fn add(&mut self, idx: usize, access_set: &AccessSet) {
        for &hash in access_set.rdo_set.iter() {
            self.rdo_set_list[idx].insert(hash);
        }
        for &hash in access_set.rnw_set.iter() {
            self.rnw_set_list[idx].insert(hash);
        }
    }
}

fn merge_access_set(a: &mut AccessSet, b: &AccessSet, only_rnw: bool) {
    for &hash in b.rdo_set.iter() {
        if only_rnw {
            a.rnw_set.insert(hash);
        } else {
            a.rdo_set.insert(hash);
        }
    }
    for &hash in b.rnw_set.iter() {
        a.rnw_set.insert(hash);
    }
}

fn access_set_collide(a: &AccessSet, b: &AccessSet) -> bool {
    for hash in b.rdo_set.iter() {
        if a.rnw_set.contains(hash) {
            return true;
        }
    }
    for hash in b.rnw_set.iter() {
        if a.rdo_set.contains(hash) {
            return true;
        }
        if a.rnw_set.contains(hash) {
            return true;
        }
    }
    false
}

#[cfg(feature = "bloomfilter")]
struct Scheduler {
    pb: ParaBloom<UINT>,
    bundles: Vec<VecDeque<(String, AccessSet)>>,
    num_sets: usize,
}

#[cfg(feature = "bloomfilter")]
impl Scheduler {
    pub fn new() -> Scheduler {
        let mut bundles = Vec::with_capacity(UINT::BITS);
        for _ in 0..UINT::BITS {
            bundles.push(VecDeque::with_capacity(MAX_TASKS_LEN_IN_BUNDLE));
        }
        Scheduler {
            pb: ParaBloom::new(),
            bundles,
            num_sets: UINT::BITS,
        }
    }
}

#[cfg(not(feature = "bloomfilter"))]
struct Scheduler {
    pb: AccessSetList,
    bundles: Vec<VecDeque<(String, AccessSet)>>,
    num_sets: usize,
}

#[cfg(not(feature = "bloomfilter"))]
impl Scheduler {
    pub fn new() -> Scheduler {
        let mut num_sets = UINT::BITS as usize;
        if cfg!(feature = "only_one_bundle") {
            num_sets = 1;
        }
        let mut bundles = Vec::with_capacity(num_sets);
        for _ in 0..num_sets {
            bundles.push(VecDeque::with_capacity(MAX_TASKS_LEN_IN_BUNDLE));
        }
        Scheduler {
            pb: AccessSetList::new(num_sets),
            bundles,
            num_sets,
        }
    }
}

impl Scheduler {
    pub fn largest_bundle(&self) -> usize {
        let mut largest_size = 0;
        let mut largest_id = 0;
        for i in 0..self.num_sets {
            if self.bundles[i].len() > largest_size {
                largest_size = self.bundles[i].len();
                largest_id = i;
            }
        }
        largest_id
    }

    fn flush_all(&mut self, total_bundle: &mut usize) {
        println!("AA flush_all");
        let mut bundle_set = AccessSet::new();
        let mut bundle_size = 0;
        for bundle_id in 0..self.num_sets {
            let target = self.bundles.get_mut(bundle_id).unwrap();
            while target.len() != 0 {
                let (_txid, access_set) = target.pop_front().unwrap();
                if access_set_collide(&bundle_set, &access_set) {
                    println!("BundleSize {}", bundle_size);
                    bundle_set.rdo_set.clear();
                    bundle_set.rnw_set.clear();
                    bundle_size = 0;
                    *total_bundle += 1;
                } else {
                    merge_access_set(&mut bundle_set, &access_set, false);
                    bundle_size += 1;
                }
            }
        }
    }

    fn flush_bundle(&mut self, bundle_id: usize) {
        let target = self.bundles.get_mut(bundle_id).unwrap();
        println!("AA BeginBundle size={}", target.len());
        while target.len() != 0 {
            let (txid, _access_set) = target.pop_front().unwrap();
            println!("{:?}", txid);
        }
        println!("EndBundle");
    }

    fn add_access_set(&mut self, txid: String, access_set: AccessSet, total_bundle: &mut usize) {
        let mask = self.pb.get_dep_mask(&access_set);
        //println!("depmask={:#016x}", mask);
        let mut bundle_id = mask.trailing_ones() as usize;
        // if we cannot find a bundle to insert task because
        // it collides with all the bundles
        if bundle_id == self.num_sets {
            bundle_id = self.largest_bundle();
            self.pb.clear(bundle_id);
            self.flush_bundle(bundle_id);
            *total_bundle += 1;
        }

        // now the task can be inserted into a bundle
        self.pb.add(bundle_id, &access_set);

        let target = self.bundles.get_mut(bundle_id).unwrap();
        target.push_back((txid, access_set));

        // flush the bundle if it's large enough
        if self.pb.get_rdo_set_size(bundle_id) > SET_MAX_SIZE
            || self.pb.get_rnw_set_size(bundle_id) > SET_MAX_SIZE
            || target.len() >= MAX_TASKS_LEN_IN_BUNDLE
        {
            self.pb.clear(bundle_id);
            self.flush_bundle(bundle_id);
            *total_bundle += 1;
        }
    }
}

struct SerialIssuer {
    bundle_set: AccessSet,
    bundle_size: usize,
    only_rnw: bool,
}

impl SerialIssuer {
    fn new(only_rnw: bool) -> Self {
        Self {
            bundle_set: AccessSet::new(),
            bundle_size: 0,
            only_rnw,
        }
    }

    fn add_access_set(&mut self, access_set: AccessSet, total_bundle: &mut usize) {
        if access_set_collide(&self.bundle_set, &access_set) {
            println!("BundleSize {}", self.bundle_size);
            self.bundle_set.rdo_set.clear();
            self.bundle_set.rnw_set.clear();
            self.bundle_size = 0;
            *total_bundle += 1;
        }
        merge_access_set(&mut self.bundle_set, &access_set, self.only_rnw);
        self.bundle_size += 1;
    }
}

fn count_addr(blocks: &Vec<Block>) {
    let mut addr2count = HashMap::<&String, usize>::new();
    for blk in blocks.iter() {
        for tx in blk.tx_list.iter() {
            for addr in tx.rnw_addr_list.iter() {
                if let Some(count) = addr2count.get(addr) {
                    addr2count.insert(addr, count + 1);
                } else {
                    addr2count.insert(addr, 1);
                }
            }
        }
    }
    println!("size={}", addr2count.len());
    for (addr, count) in addr2count {
        println!("addr2count {} {}", addr, count);
    }
}

pub fn run_serial_issuer() -> (usize, usize) {
    let mut uni_set = false;
    if cfg!(feature = "uni_set") {
        uni_set = true;
    }
    let mut serial_issuer = SerialIssuer::new(uni_set);
    let mut total_tx = 0;
    let mut total_bundle = 0;
    for id in (START_HEIGHT..END_HEIGHT).step_by(10) {
        let mut file = OpenOptions::new()
            .read(true)
            .open(format!(
                "blocks/blocks_{}.json",
                id
            ))
            .expect("Could not read file");
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();

        let blocks: Vec<Block> = serde_json::from_str(&contents).unwrap();

        for blk in blocks.iter() {
            let coinbase = blk.coinbase.to_lowercase();
            for tx in blk.tx_list.iter() {
                total_tx += 1;
                let access_set = tx.to_access_set(&coinbase);
                serial_issuer.add_access_set(access_set, &mut total_bundle);
            }
        }
    }
    if serial_issuer.bundle_size != 0 {
        total_bundle += 1;
    }
    println!("total_tx={} total_bundle={} TLP={}", total_tx, total_bundle,
        (total_tx as f64)/(total_bundle as f64));

    (total_tx, total_bundle)
}

pub fn run_scheduler() -> (usize, usize) {
    let mut scheduler = Scheduler::new();
    let mut total_bundle = 0usize;
    let mut total_tx = 0usize;
    for id in (START_HEIGHT..END_HEIGHT).step_by(10) {
        let mut file = OpenOptions::new()
            .read(true)
            .open(format!(
                "blocks/blocks_{}.json",
                id
            ))
            .expect("Could not read file");
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();

        let blocks: Vec<Block> = serde_json::from_str(&contents).unwrap();

        for blk in blocks.iter() {
            let coinbase = blk.coinbase.to_lowercase();

            if blk.tx_list.len() == 0 {
                continue;
            }

            total_tx += blk.tx_list.len();
            if MAX_TX_IN_TASK > 1 {
                let task_list = aggregate_tx(&blk.tx_list);
                for tx_vec in task_list.iter() {
                    let mut access_set = AccessSet::new();
                    let mut set_size = 0usize;
                    let mut txid = task_list[0][0].txid.clone();
                    for tx in tx_vec.iter() {
                        tx.fill_access_set(&coinbase, &mut access_set);
                        set_size += 1;
                        if set_size == MAX_TX_IN_TASK - 1 {
                            let mut old = AccessSet::new();
                            mem::swap(&mut old, &mut access_set);
                            scheduler.add_access_set(txid.clone(), old, &mut total_bundle);
                            set_size = 0;
                            txid = tx.txid.clone();
                        }
                    }
                    if set_size != 0 {
                        scheduler.add_access_set(txid.clone(), access_set, &mut total_bundle);
                    }
                }
            } else {
                for tx in blk.tx_list.iter() {
                    let access_set = tx.to_access_set(&coinbase);
                    scheduler.add_access_set(tx.txid.clone(), access_set, &mut total_bundle);
                }
            }
        }
    }
    scheduler.flush_all(&mut total_bundle);
    println!("AA total_tx={} total_bundle={} TLP={}", total_tx, total_bundle,
        (total_tx as f64)/(total_bundle as f64));
    (total_tx, total_bundle)
}
