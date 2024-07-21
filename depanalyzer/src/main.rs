use std::collections::VecDeque;
use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
use serde::Deserialize;
use exepipe::exetask::AccessSet;
use exepipe::scheduler::{ParaBloom, BUNDLE_COUNT, MAX_TASKS_LEN_IN_BUNDLE, SET_MAX_SIZE};
use mpads::utils::hasher;
use byteorder::{BigEndian, ByteOrder};


#[derive(Deserialize, Debug)]
struct Slot {
    addr: [u8; 20],
    index: [u8; 32],
}

#[derive(Deserialize, Debug)]
struct Tx {
    txid: [u8; 32],
    rdo_addr_list: Vec<[u8; 20]>,
    rnw_addr_list: Vec<[u8; 20]>,
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
            let hash = hasher::hash(rnw_addr);
            access_set.rnw_k64_vec.push(BigEndian::read_u64(&hash[..8]));
        }
        let mut buf = [0u8; 52];
        for rdo_slot in self.rdo_slot_list.iter() {
            buf[..20].copy_from_slice(&rdo_slot.addr[..]);
            buf[20..].copy_from_slice(&rdo_slot.index[..]);
            let hash = hasher::hash(buf);
            access_set.rdo_k64_vec.push(BigEndian::read_u64(&hash[..8]));
        }
        for rnw_slot in self.rnw_slot_list.iter() {
            buf[..20].copy_from_slice(&rnw_slot.addr[..]);
            buf[20..].copy_from_slice(&rnw_slot.index[..]);
            let hash = hasher::hash(buf);
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

struct Scheduler {
    pb: ParaBloom,
    bundles: Vec<VecDeque<[u8; 32]>>,
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

    fn flush_bundle(&mut self, bundle_id: usize) {
        let target = self.bundles.get_mut(bundle_id).unwrap();
        println!("BeginBundle size={}", target.len()); 
        while target.len() != 0 {
            let txid = target.pop_front().unwrap();
            println!("{:?}", txid);
        }
        println!("EndBundle"); 
    }

    fn add_tx(&mut self, tx: &Tx) {
        let access_set = tx.to_access_set();
        let mask = self.pb.get_dep_mask(&access_set);
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
        target.push_back(tx.txid);

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

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

fn main() {
    let mut scheduler = Scheduler::new();
    let lines = read_lines("./blocks.txt").unwrap();
    for line in lines.flatten() {
        let blk: Block = serde_json::from_str(&line).unwrap();
        for tx in blk.tx_list.iter() {
            scheduler.add_tx(tx);
        }
    }
}
