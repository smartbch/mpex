use std::collections::HashMap;
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::{
    mpsc::{self, sync_channel},
    Arc,
};

use byteorder::{BigEndian, ByteOrder};
use mpads::entry::{EntryBz, Hash32};
use mpads::test_helper::EntryBuilder;
use mpads::utils::hasher;
use mpads::{bptaskhub::BlockPairTaskHub, entrycache, test_helper::SimpleTask};
use mpads::{AdsCore, SharedAdsWrap, ADS};
use revm::primitives::{AccountInfo, Address, Bytecode, B256, U256};
use threadpool::ThreadPool;

use crate::exetask::ACC_INFO_LEN;
use crate::scheduler::EarlyExeInfo;

pub fn generate_ads_wrap(
    dir: &str,
) -> (
    SharedAdsWrap,
    Arc<ThreadPool>,
    mpsc::SyncSender<EarlyExeInfo>,
    mpsc::Receiver<EarlyExeInfo>,
    SyncSender<i32>,
    Receiver<i32>,
) {
    let tpool = Arc::new(ThreadPool::new(128));
    let (sender, receiver) = sync_channel(1024);
    AdsCore::init_dir(dir, 64 * 1024);
    let task_hub = Arc::new(BlockPairTaskHub::<SimpleTask>::new());
    let (ads, _, _) = AdsCore::new(task_hub, dir, 8 * 1024, 64 * 1024);
    let shared_ads_wrap =
        SharedAdsWrap::new(Arc::new(ads), Arc::new(entrycache::EntryCache::new()));
    let (s, r) = mpsc::sync_channel(8192);
    (shared_ads_wrap, tpool, sender, receiver, s, r)
}

// ------ ADS Mock ------
pub struct MockADS {
    entry_map: HashMap<Hash32, Vec<u8>>,
    code_map: HashMap<Hash32, Vec<u8>>,
    addr_to_entry: HashMap<Address, Hash32>,
    // task_list: Vec<i64>,
}

impl MockADS {
    pub fn new() -> Self {
        MockADS {
            entry_map: HashMap::new(),
            code_map: HashMap::new(),
            addr_to_entry: HashMap::new(),
            // task_list: Vec::new(),
        }
    }

    pub fn add_entry(&mut self, entry_data: Vec<u8>) -> Hash32 {
        let entry_bz = EntryBz { bz: &entry_data };
        let k = entry_bz.key_hash();
        self.entry_map.insert(k, entry_data);
        k
    }

    pub fn add_code(&mut self, addr: &Address, code: Bytecode) -> Hash32 {
        let code_data = bincode::serialize(&Option::Some(code)).unwrap();
        let code_hash = hasher::hash(&code_data);
        self.code_map.insert(code_hash, code_data);

        let mut v = [0u8; 32 + 8 + 32];
        v[32 + 8..].copy_from_slice(&code_hash);
        let entry = EntryBuilder::kv(&addr[..], &v).build_and_dump(&[]);
        self.add_entry(entry);

        code_hash
    }

    pub fn add_account(&mut self, addr: &Address, info: &AccountInfo) -> Hash32 {
        if let Some(code) = &info.code {
            if code != &Bytecode::new() {
                let code_data = bincode::serialize(&Option::Some(code)).unwrap();
                let code_hash = hasher::hash(&code_data);
                self.code_map.insert(code_hash, code_data);

                assert_eq!(info.code_hash, B256::new(code_hash));
            }
        }

        let v = encode_account_info(info);
        let entry = EntryBuilder::kv(&addr[..], &v).build_and_dump(&[]);
        let h = self.add_entry(entry);
        self.addr_to_entry.insert(addr.clone(), h);
        h
    }

    pub fn get_account(&self, addr: &Address) -> Option<AccountInfo> {
        let h = self.addr_to_entry.get(addr);
        if h.is_none() {
            return Option::None;
        }

        let v = self.entry_map.get(h.unwrap());
        if v.is_none() {
            return Option::None;
        }

        let bz = EntryBz { bz: v.unwrap() };
        let acc_data = bz.value();
        Option::Some(AccountInfo {
            balance: U256::from_be_slice(&acc_data[0..32]),
            nonce: BigEndian::read_u64(&acc_data[32..32 + 8]),
            code_hash: B256::from_slice(&acc_data[32 + 8..]),
            code: Option::None,
        })
    }
}

impl ADS for MockADS {
    fn read_entry(&self, key_hash: &[u8], _key: &[u8], buf: &mut [u8]) -> (usize, bool) {
        let mut k: Hash32 = [0u8; 32];
        k.copy_from_slice(key_hash);
        match self.entry_map.get(&k) {
            None => (0, false),
            Some(v) => {
                buf[..v.len()].copy_from_slice(v);
                (v.len(), true)
            }
        }
    }

    fn read_code(&self, code_hash: &[u8], buf: &mut Vec<u8>) -> usize {
        let mut k: Hash32 = [0u8; 32];
        k.copy_from_slice(code_hash);
        match self.code_map.get(&k) {
            None => 0,
            Some(code) => {
                let n = code.len();
                if buf.len() < n {
                    buf.resize(n, 0);
                }
                buf[..n].copy_from_slice(code);
                n
            }
        }
    }

    fn add_task(&self, _task_id: i64) {
        // self.task_list.push(task_id);
    }
}

pub fn encode_account_info(account_info: &AccountInfo) -> Vec<u8> {
    let mut buf = vec![0u8; ACC_INFO_LEN];
    buf[0..32].copy_from_slice(&account_info.balance.to_be_bytes_vec()); // balance
    BigEndian::write_u64(&mut buf[32..32 + 8], account_info.nonce); // nonce
    buf[40..72].copy_from_slice(account_info.code_hash.as_ref()); // code_hash
    buf
}

pub fn calc_code_hash(code: &Bytecode) -> B256 {
    let code_data = bincode::serialize(&Option::Some(code)).unwrap();
    let code_hash = hasher::hash(&code_data);
    B256::new(code_hash)
}

pub fn addr_idx_hash(addr: &Address, idx: &U256) -> U256 {
    let mut addr_idx = [0u8; 20 + 32];
    addr_idx[..20].copy_from_slice(&addr[..]);
    addr_idx[20..].copy_from_slice(&idx.to_be_bytes_vec());
    U256::from_be_bytes(hasher::hash(&addr_idx[..]))
}
