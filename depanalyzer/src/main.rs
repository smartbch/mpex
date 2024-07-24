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
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
};

pub const WETH_ADDR: &str = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2";
//const WETH_ADDR: &str = "";

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
}

impl Tx {
    fn to_access_set(&self, coinbase: &str) -> AccessSet {
        let mut access_set = AccessSet::new();
        for rdo_addr in self.rdo_addr_list.iter() {
            let hash = hasher::hash(rdo_addr);
            access_set.rdo_k64_vec.push(BigEndian::read_u64(&hash[..8]));
            access_set.rdo_set.insert(hash);
        }
        for rnw_addr in self.rnw_addr_list.iter() {
            if rnw_addr == coinbase {
                continue;
            }
            if rnw_addr == WETH_ADDR {
                continue;
            }
            let hash = hasher::hash(rnw_addr);
            access_set.rnw_k64_vec.push(BigEndian::read_u64(&hash[..8]));
            access_set.rnw_set.insert(hash);
        }
        let mut buf = [0u8; 52];
        for rdo_slot in self.rdo_slot_list.iter() {
            let hash = hasher::hash("".to_owned() + &rdo_slot.addr + &rdo_slot.index);
            access_set.rdo_k64_vec.push(BigEndian::read_u64(&hash[..8]));
            access_set.rdo_set.insert(hash);
        }
        for rnw_slot in self.rnw_slot_list.iter() {
            let hash = hasher::hash("".to_owned() + &rnw_slot.addr + &rnw_slot.index);
            access_set.rnw_k64_vec.push(BigEndian::read_u64(&hash[..8]));
            access_set.rnw_set.insert(hash);
        }
        access_set
    }
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

struct AccessSetList<T: PBElement> {
    rdo_set_list: Vec<HashSet<[u8; 32]>>,
    rnw_set_list: Vec<HashSet<[u8; 32]>>,
    _zero: T,
}

impl<T: PBElement> AccessSetList<T> {
    pub fn new() -> Self {
        let len = T::BITS as usize;
        let mut res = Self {
            rdo_set_list: Vec::with_capacity(len),
            rnw_set_list: Vec::with_capacity(len),
            _zero: T::zero(),
        };
        for _ in 0..len {
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

    pub fn get_dep_mask(&self, access_set: &AccessSet) -> T {
        let mut result = T::zero();
        let mut mask = T::one();
        for idx in 0..(T::BITS as usize) {
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
        for idx in 0..(T::BITS as usize) {
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

fn is_access_set_collision(a: &AccessSet, b: &AccessSet) -> bool {
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

pub const BUNDLE_COUNT: usize = 64;

struct Scheduler {
    pb: ParaBloom<u64>,
    //pb: AccessSetList<u128>,
    bundles: Vec<VecDeque<(String, AccessSet)>>,
}

impl Scheduler {
    pub fn new() -> Scheduler {
        let mut bundles = Vec::with_capacity(BUNDLE_COUNT);
        for _ in 0..BUNDLE_COUNT {
            bundles.push(VecDeque::with_capacity(MAX_TASKS_LEN_IN_BUNDLE));
        }
        Scheduler {
            pb: ParaBloom::new(),
            //pb: AccessSetList::new(),
            bundles,
        }
    }

    pub fn largest_bundle(&self) -> usize {
        let mut largest_size = 0;
        let mut largest_id = 0;
        for i in 0..BUNDLE_COUNT {
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
        for bundle_id in 0..BUNDLE_COUNT {
            let target = self.bundles.get_mut(bundle_id).unwrap();
            while target.len() != 0 {
                let (_txid, access_set) = target.pop_front().unwrap();
                if is_access_set_collision(&bundle_set, &access_set) {
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

    fn add_tx(&mut self, tx: &Tx, coinbase: &str, total_bundle: &mut usize) {
        let access_set = tx.to_access_set(coinbase);
        let mask = self.pb.get_dep_mask(&access_set);
        println!("depmask={:#016x}", mask);
        let mut bundle_id = mask.trailing_ones() as usize;
        // if we cannot find a bundle to insert task because
        // it collide with all the bundles
        if bundle_id == BUNDLE_COUNT {
            bundle_id = self.largest_bundle();
            self.pb.clear(bundle_id);
            self.flush_bundle(bundle_id);
            *total_bundle += 1;
        }

        // now the task can be inserted into a bundle
        self.pb.add(bundle_id, &access_set);

        let target = self.bundles.get_mut(bundle_id).unwrap();
        target.push_back((tx.txid.clone(), access_set));

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

    fn add_tx(&mut self, tx: &Tx, coinbase: &str, total_bundle: &mut usize) {
        let access_set = tx.to_access_set(coinbase);
        if is_access_set_collision(&self.bundle_set, &access_set) {
            println!("BundleSize {}", self.bundle_size);
            self.bundle_set.rdo_set.clear();
            self.bundle_set.rnw_set.clear();
            self.bundle_size = 0;
            *total_bundle += 1;
        } else {
            merge_access_set(&mut self.bundle_set, &access_set, self.only_rnw);
            self.bundle_size += 1;
        }
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

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

fn main() {
    run_scheduler();
    //run_serial_issuer();
}

pub fn run_serial_issuer() -> (usize, usize) {
    let mut serial_issuer = SerialIssuer::new(false);
    let mut total_tx = 0;
    let mut total_bundle = 0;
    for id in (20338810..20338850).step_by(10) {
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
                serial_issuer.add_tx(tx, &coinbase, &mut total_bundle);
            }
        }
    }
    if serial_issuer.bundle_size != 0 {
        total_bundle += 1;
    }
    println!("total_tx={} total_bundle={}", total_tx, total_bundle);

    (total_tx, total_bundle)
}

pub fn run_scheduler() -> (usize, usize) {
    let mut scheduler = Scheduler::new();
    let mut total_bundle = 0usize;
    let mut total_tx = 0usize;
    for id in (20338810..20338850).step_by(10) {
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

        //count_addr(&blocks);

        for blk in blocks.iter() {
            let coinbase = blk.coinbase.to_lowercase();
            for tx in blk.tx_list.iter() {
                total_tx += 1;
                scheduler.add_tx(tx, &coinbase, &mut total_bundle);
            }
        }
    }
    println!("AA total_tx={}", total_tx);
    scheduler.flush_all(&mut total_bundle);
    println!("AA total_bundle={}", total_bundle);
    (total_tx, total_bundle)

    //let lines = read_lines("./blocks.txt").unwrap();
    //for line in lines.flatten() {
    //    let blk: Block = serde_json::from_str(&line).unwrap();
    //    for tx in blk.tx_list.iter() {
    //        scheduler.add_tx(tx);
    //    }
    //}
}
