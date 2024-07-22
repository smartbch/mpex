use std::collections::HashMap;
use std::collections::VecDeque;
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
};
use std::io::{self, BufRead};
use std::path::Path;
use serde::Deserialize;
use exepipe::exetask::AccessSet;
use exepipe::scheduler::{ParaBloom, MAX_TASKS_LEN_IN_BUNDLE, SET_MAX_SIZE};
use mpads::utils::hasher;
use byteorder::{BigEndian, ByteOrder};


#[derive(Deserialize, Debug)]
struct Slot {
    addr: String,
    index: String,
}

#[derive(Deserialize, Debug)]
struct Tx {
    txid: String,
    rdo_addr_list: Vec<String>,
    rnw_addr_list: Vec<String>,
    rdo_slot_list: Vec<Slot>,
    rnw_slot_list: Vec<Slot>,
}

impl Tx {
    fn to_access_set(&self) -> AccessSet {
        let mut access_set = AccessSet::new();
        for rdo_addr in self.rdo_addr_list.iter() {
            let hash = hasher::hash(rdo_addr);
            access_set.rdo_k64_vec.push(BigEndian::read_u64(&hash[..8]));
        }
        for rnw_addr in self.rnw_addr_list.iter() {
            if rnw_addr == "0x95222290dd7278aa3ddd389cc1e1d165cc4bafe5" {
                println!("ignore beaverbuild");
                continue; //ignore beaverbuild
            }
            if rnw_addr == "0x4838b106fce9647bdf1e7877bf73ce8b0bad5f97" {
                println!("ignore Titan Build");
                continue; //ignore Titan Build
            }
            if rnw_addr == "0x7e2a2fa2a064f693f0a55c5639476d913ff12d05" {
                continue; //MEV builder
            }
            let hash = hasher::hash(rnw_addr);
            access_set.rnw_k64_vec.push(BigEndian::read_u64(&hash[..8]));
        }
        let mut buf = [0u8; 52];
        for rdo_slot in self.rdo_slot_list.iter() {
            let hash = hasher::hash("".to_owned()+&rdo_slot.addr+&rdo_slot.index);
            access_set.rdo_k64_vec.push(BigEndian::read_u64(&hash[..8]));
        }
        for rnw_slot in self.rnw_slot_list.iter() {
            let hash = hasher::hash("".to_owned()+&rnw_slot.addr+&rnw_slot.index);
            access_set.rnw_k64_vec.push(BigEndian::read_u64(&hash[..8]));
        }
        access_set
    }
}

#[derive(Deserialize, Debug)]
struct Block {
    height: i64,
    tx_list: Vec<Tx>,
}

#[derive(Deserialize, Debug)]
struct User {
    fingerprint: String,
    location: String,
}

pub const BUNDLE_COUNT: usize = 128;

struct Scheduler {
    pb: ParaBloom<u128>,
    bundles: Vec<VecDeque<String>>,
}

impl Scheduler {
    pub fn new() -> Scheduler {
        let mut bundles = Vec::with_capacity(BUNDLE_COUNT);
        for _ in 0..BUNDLE_COUNT {
            bundles.push(VecDeque::with_capacity(MAX_TASKS_LEN_IN_BUNDLE));
        }
        Scheduler {
            pb: ParaBloom::new(),
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

    fn flush_all(&mut self) {
        println!("AA flush_all");
        for i in 0..BUNDLE_COUNT {
            if self.bundles[i].len() == 0 {
                continue;
            }
            self.flush_bundle(i);
        }
    }

    fn flush_bundle(&mut self, bundle_id: usize) {
        let target = self.bundles.get_mut(bundle_id).unwrap();
        println!("AA BeginBundle size={}", target.len()); 
        while target.len() != 0 {
            let txid = target.pop_front().unwrap();
            println!("{:?}", txid);
        }
        println!("EndBundle"); 
    }

    fn add_tx(&mut self, tx: &Tx) {
        let access_set = tx.to_access_set();
        let mask = self.pb.get_dep_mask(&access_set);
        println!("depmask={:#016x}", mask);
        let mut bundle_id = mask.trailing_ones() as usize;
        // if we cannot find a bundle to insert task because
        // it conflicts with all the bundles
        if bundle_id == BUNDLE_COUNT {
            bundle_id = self.largest_bundle();
            self.pb.clear(bundle_id);
            self.flush_bundle(bundle_id);
        }

        // now the task can be inserted into a bundle
        self.pb.add(bundle_id, &access_set);

        let target = self.bundles.get_mut(bundle_id).unwrap();
        target.push_back("".to_owned()+&tx.txid);

        // flush the bundle if it's large enough
        if self.pb.get_rdo_set_size(bundle_id) > SET_MAX_SIZE
            || self.pb.get_rnw_set_size(bundle_id) > SET_MAX_SIZE
            || target.len() >= MAX_TASKS_LEN_IN_BUNDLE
        {
            self.pb.clear(bundle_id);
            self.flush_bundle(bundle_id);
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
where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

fn main() {
    let mut scheduler = Scheduler::new();
    let mut file = OpenOptions::new()
        .read(true)
        .open("blocks.json")
        .expect("Could not read file");
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    
    let blocks: Vec<Block> = serde_json::from_str(&contents).unwrap();

    //count_addr(&blocks);
 
    for blk in blocks.iter() {
        for tx in blk.tx_list.iter() {
            scheduler.add_tx(tx);
        }
    }
    scheduler.flush_all();

    //let lines = read_lines("./blocks.txt").unwrap();
    //for line in lines.flatten() {
    //    let blk: Block = serde_json::from_str(&line).unwrap();
    //    for tx in blk.tx_list.iter() {
    //        scheduler.add_tx(tx);
    //    }
    //}
}
