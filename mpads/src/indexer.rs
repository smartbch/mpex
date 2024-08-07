use crate::def::{OP_CREATE, OP_DELETE, OP_WRITE, SHARD_COUNT, SHARD_DIV};
use rand_core::{OsRng, RngCore};
use std::collections::BTreeSet;
use std::ops::Bound;
use std::ops::Bound::{Excluded, Included};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::RwLock;
use std::vec::Vec;
use xxhash_rust::xxh3::xxh3_64_with_secret;

extern "C" {
    fn cppbtree_new() -> usize;
    fn cppbtree_delete(tree: usize);
    fn cppbtree_size(tree: usize) -> usize;

    fn cppbtree_insert(tree: usize, key: u64, value: i64) -> bool;
    fn cppbtree_erase(tree: usize, key: u64, value: i64) -> bool;
    fn cppbtree_change(tree: usize, key: u64, old_v: i64, new_v: i64) -> bool;

    fn cppbtree_seek(tree: usize, key: u64) -> usize;

    fn iter_next(iter: usize);
    fn iter_prev(iter: usize);
    fn is_first(tree: usize, iter: usize) -> bool;
    fn iter_valid(tree: usize, iter: usize) -> bool;
    fn iter_key(iter: usize) -> u64;
    fn iter_value(iter: usize) -> i64;
    fn iter_delete(iter: usize);
}

pub struct BTreeIndexerCpp {
    bt: Vec<RwLock<usize>>,
    sizes: Vec<AtomicUsize>,
}

impl BTreeIndexerCpp {
    pub fn new(n: usize) -> Self {
        let mut res = Self {
            bt: Vec::with_capacity(n),
            sizes: Vec::with_capacity(SHARD_COUNT),
        };
        for _ in 0..n {
            res.bt.push(RwLock::new(unsafe { cppbtree_new() }));
        }
        for _ in 0..SHARD_COUNT {
            res.sizes.push(AtomicUsize::new(0));
        }
        res
    }

    pub fn len(&self, shard_id: usize) -> usize {
        self.sizes[shard_id].load(Ordering::SeqCst)
    }

    fn _add_kv(tree: usize, k48: u64, v_in: i64) {
        let inserted = unsafe { cppbtree_insert(tree, k48, v_in) };
        if !inserted {
            panic!("Add Duplicated KV");
        }
    }

    fn _erase_kv(tree: usize, k48: u64, v_in: i64) {
        let existed = unsafe { cppbtree_erase(tree, k48, v_in) };
        if !existed {
            panic!("Cannot Erase Non-existent KV");
        }
    }

    fn _change_kv(tree: usize, k48: u64, old_v: i64, new_v: i64) {
        let existed = unsafe { cppbtree_change(tree, k48, old_v, new_v) };
        if !existed {
            panic!("Cannot Erase Non-existent KV");
        }
    }

    pub fn add_kv(&self, k_in: u64, v_in: i64) {
        if v_in % 8 != 0 {
            panic!("value not 8x");
        }
        let v_in = v_in / 8;
        let idx = (k_in >> 48) as usize % self.bt.len();
        let k48 = (k_in << 16) >> 16;
        let tree_guard = self.bt[idx].write().unwrap();
        let tree: usize = *tree_guard;
        Self::_add_kv(tree, k48, v_in);
        self.sizes[idx / SHARD_DIV].fetch_add(1, Ordering::SeqCst);
    }

    pub fn erase_kv(&self, k_in: u64, v_in: i64) {
        if v_in % 8 != 0 {
            panic!("value not 8x");
        }
        let v_in = v_in / 8;
        let idx = (k_in >> 48) as usize % self.bt.len();
        let k48 = (k_in << 16) >> 16;
        let tree_guard = self.bt[idx].write().unwrap();
        let tree: usize = *tree_guard;
        Self::_erase_kv(tree, k48, v_in);
        self.sizes[idx / SHARD_DIV].fetch_sub(1, Ordering::SeqCst);
    }

    pub fn change_kv(&self, k_in: u64, v_old: i64, v_new: i64) {
        if v_old % 8 != 0 {
            panic!("value not 8x");
        }
        let v_old = v_old / 8;
        if v_new % 8 != 0 {
            panic!("value not 8x");
        }
        let v_new = v_new / 8;
        let idx = (k_in >> 48) as usize % self.bt.len();
        let k48 = (k_in << 16) >> 16;
        let tree_guard = self.bt[idx].write().unwrap();
        let tree: usize = *tree_guard;
        Self::_change_kv(tree, k48, v_old, v_new);
    }

    pub fn for_each<F>(&self, op: u8, k_in: u64, mut access: F)
    where
        F: FnMut(u64, i64) -> bool,
    {
        if op == OP_CREATE || op == OP_DELETE {
            self.for_each_adjacent_value::<F>(k_in, access);
        } else if op == OP_WRITE {
            self.for_each_value(k_in, |offset| access(0, offset));
        }
    }

    pub fn for_each_value<F>(&self, k_in: u64, mut access: F)
    where
        F: FnMut(i64) -> bool,
    {
        let idx = (k_in >> 48) as usize % self.bt.len();
        let k48 = (k_in << 16) >> 16;
        let tree_guard = self.bt[idx].read().unwrap();
        let tree: usize = *tree_guard;
        let iter = unsafe { cppbtree_seek(tree, k48) };
        loop {
            if unsafe { !iter_valid(tree, iter) || iter_key(iter) != k48 } {
                break;
            }
            let v = unsafe { iter_value(iter) * 8 };
            if access(v) {
                break;
            }
            unsafe {
                iter_next(iter);
            }
        }
        unsafe {
            iter_delete(iter);
        }
    }

    pub fn for_each_adjacent_value<F>(&self, k_in: u64, mut access: F)
    where
        F: FnMut(u64, i64) -> bool,
    {
        let idx = (k_in >> 48) as usize % self.bt.len();
        let k48 = (k_in << 16) >> 16;
        let tree_guard = self.bt[idx].read().unwrap();
        let tree: usize = *tree_guard;
        if unsafe { cppbtree_size(tree) } == 0 {
            return;
        }
        let iter = unsafe { cppbtree_seek(tree, k48) };
        loop {
            if unsafe { !iter_valid(tree, iter) || iter_key(iter) != k48 } {
                break;
            }
            let v = unsafe { iter_value(iter) * 8 };
            if access(k_in, v) {
                break;
            }
            unsafe {
                iter_next(iter);
            }
        }
        unsafe {
            iter_delete(iter);
        }

        let iter = unsafe { cppbtree_seek(tree, k48) };
        if unsafe { !is_first(tree, iter) } {
            let k = unsafe { iter_prev(iter); iter_key(iter) };
            let k_with_idx = ((idx << 48) as u64) | k;
            loop {
                if unsafe { iter_key(iter) } == k {
                    let v = unsafe { iter_value(iter) * 8 };
                    if access(k_with_idx, v) {
                        return;
                    }
                } else {
                    break;
                }
                if unsafe { is_first(tree, iter) } {
                    break;
                } else {
                    unsafe {
                        iter_prev(iter);
                    }
                }
            }
        }
        unsafe {
            iter_delete(iter);
        }
    }

    pub fn key_exists(&self, k64: u64, file_pos: i64) -> bool {
        let mut exists = false;
        self.for_each_value(k64, |offset| -> bool {
            if offset == file_pos {
                exists = true;
            }
            false // do not exit loop
        });
        exists
    }
}

// ==================

type Bits96 = [u32; 3];

fn make_bits96(k: u64, v: i64) -> Bits96 {
    [
        (k >> 16)/*high32 of k*/ as u32,
        ((k&0xFFFF)<<16) as u32/*low16 of k*/ | (v>>32)/*high16 of v*/ as u32,
        v/*low32 of v*/ as u32,
    ]
}

fn u64_to_max_bits96(n: u64) -> Bits96 {
    make_bits96(n, 0xFFFF_FFFF_FFFF)
}

fn u64_to_min_bits96(n: u64) -> Bits96 {
    make_bits96(n, 0)
}

fn last48_of_bits96(v: Bits96) -> i64 {
    let n = (v[1]&0xFFFF) as i64/*low16 of v[1]*/;
    (n << 32) | (v[2] as i64)
}

fn first48_of_bits96(v: Bits96) -> u64 {
    ((v[0] as u64) << 16) | ((v[1]>>16)/*high16 of v[1]*/ as u64)
}
pub struct BTreeIndexerRust {
    bt: Vec<RwLock<BTreeSet<Bits96>>>,
    sizes: Vec<AtomicUsize>,
}

impl BTreeIndexerRust {
    pub fn new(n: usize) -> Self {
        let mut res = Self {
            bt: Vec::with_capacity(n),
            sizes: Vec::with_capacity(SHARD_COUNT),
        };
        for _ in 0..n {
            res.bt.push(RwLock::new(BTreeSet::new()));
        }
        for _ in 0..SHARD_COUNT {
            res.sizes.push(AtomicUsize::new(0));
        }
        res
    }

    pub fn len(&self, shard_id: usize) -> usize {
        self.sizes[shard_id].load(Ordering::SeqCst)
    }

    fn _add_kv(tree: &mut BTreeSet<Bits96>, k_in: u64, v_in: i64) {
        let target = make_bits96(k_in, v_in);
        let existed = !tree.insert(target);
        if existed {
            panic!("Add Duplicated KV");
        }
    }

    fn _erase_kv(tree: &mut BTreeSet<Bits96>, k_in: u64, v_in: i64) {
        let target = make_bits96(k_in, v_in);
        let existed = tree.remove(&target);
        if !existed {
            panic!("Cannot Erase Non-existent KV");
        }
    }

    pub fn add_kv(&self, k_in: u64, v_in: i64) {
        if v_in % 8 != 0 {
            panic!("value not 8x");
        }
        let v_in = v_in / 8;
        let idx = (k_in >> 48) as usize % self.bt.len();
        let tree = &mut self.bt[idx].write().unwrap();
        Self::_add_kv(tree, k_in, v_in);
        self.sizes[idx / SHARD_DIV].fetch_add(1, Ordering::SeqCst);
    }

    pub fn erase_kv(&self, k_in: u64, v_in: i64) {
        if v_in % 8 != 0 {
            panic!("value not 8x");
        }
        let v_in = v_in / 8;
        let idx = (k_in >> 48) as usize % self.bt.len();
        let tree = &mut self.bt[idx].write().unwrap();
        Self::_erase_kv(tree, k_in, v_in);
        self.sizes[idx / SHARD_DIV].fetch_sub(1, Ordering::SeqCst);
    }

    pub fn change_kv(&self, k_in: u64, v_old: i64, v_new: i64) {
        if v_old % 8 != 0 {
            panic!("value not 8x");
        }
        let v_old = v_old / 8;
        if v_new % 8 != 0 {
            panic!("value not 8x");
        }
        let v_new = v_new / 8;
        let idx = (k_in >> 48) as usize % self.bt.len();
        let tree = &mut self.bt[idx].write().unwrap();
        Self::_erase_kv(tree, k_in, v_old);
        Self::_add_kv(tree, k_in, v_new);
    }

    pub fn for_each<F>(&self, op: u8, k_in: u64, mut access: F)
    where
        F: FnMut(u64, i64) -> bool,
    {
        if op == OP_CREATE || op == OP_DELETE {
            self.for_each_adjacent_value::<F>(k_in, access);
        } else if op == OP_WRITE {
            self.for_each_value(k_in, |offset| access(0, offset));
        }
    }

    pub fn for_each_value<F>(&self, k_in: u64, mut access: F)
    where
        F: FnMut(i64) -> bool,
    {
        let idx = (k_in >> 48) as usize % self.bt.len();
        let tree = &mut self.bt[idx].read().unwrap();
        let start = u64_to_min_bits96(k_in);
        let end = u64_to_max_bits96(k_in);
        let range = (Included(&start), Included(&end));
        for &elem in tree.range::<Bits96, (Bound<&Bits96>, Bound<&Bits96>)>(range) {
            if access(last48_of_bits96(elem) * 8) {
                break;
            }
        }
    }

    pub fn for_each_adjacent_value<F>(&self, k_in: u64, mut access: F)
    where
        F: FnMut(u64, i64) -> bool,
    {
        let idx = (k_in >> 48) as usize % self.bt.len();
        let tree = &mut self.bt[idx].read().unwrap();
        if tree.len() == 0 {
            return;
        }
        let mut start = u64_to_min_bits96(k_in);
        let mut end = u64_to_max_bits96(k_in);
        let mut range = (Included(&start), Included(&end));
        for &elem in tree.range::<Bits96, (Bound<&Bits96>, Bound<&Bits96>)>(range) {
            if access(k_in, last48_of_bits96(elem) * 8) {
                return;
            }
        }
        end = start;
        start = u64_to_min_bits96(0);
        range = (Included(&start), Excluded(&end));
        let last = tree
            .range::<Bits96, (Bound<&Bits96>, Bound<&Bits96>)>(range)
            .last();
        if let Some(last_elem) = last {
            let k = first48_of_bits96(*last_elem);
            let k_with_idx = ((idx << 48) as u64) | k;
            for &elem in tree
                .range::<Bits96, (Bound<&Bits96>, Bound<&Bits96>)>(range)
                .rev()
            {
                if first48_of_bits96(elem) == k {
                    if access(k_with_idx, last48_of_bits96(elem) * 8) {
                        return;
                    }
                } else {
                    break;
                }
            }
        }
    }

    pub fn key_exists(&self, k64: u64, file_pos: i64) -> bool {
        let mut exists = false;
        self.for_each_value(k64, |offset| -> bool {
            if offset == file_pos {
                exists = true;
            }
            false // do not exit loop
        });
        exists
    }
}

pub type BTreeIndexer = BTreeIndexerCpp;

pub struct CodeIndexer {
    bti: BTreeIndexer,
    secret: [u8; 136],
}

impl CodeIndexer {
    pub fn new() -> Self {
        let mut secret = [0u8; 136];
        OsRng.fill_bytes(secret.as_mut());

        Self {
            // only 256 shards are needed because we clear high 8 bits of k64
            bti: BTreeIndexer::new(256),
            secret,
        }
    }

    pub fn add_kv(&self, key: &[u8], v_in: i64) {
        let k64 = xxh3_64_with_secret(key, &self.secret[..]) >> 8;
        self.bti.add_kv(k64, v_in);
    }

    pub fn for_each_value<F>(&self, key: &[u8], access: F)
    where
        F: FnMut(i64) -> bool,
    {
        let k64 = xxh3_64_with_secret(key, &self.secret[..]) >> 8;
        self.bti.for_each_value(k64, access);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_all_values(bt: &BTreeIndexer, k_in: u64) -> Vec<i64> {
        let mut res = Vec::new();
        bt.for_each_value(k_in, |v| -> bool {
            res.push(v);
            false
        });
        res
    }

    fn get_all_adjacent_values(bt: &BTreeIndexer, k_in: u64) -> Vec<(u64, i64)> {
        let mut kv_out = Vec::new();
        bt.for_each_adjacent_value(k_in, |k, v| -> bool {
            kv_out.push((k, v));
            false
        });
        kv_out
    }

    #[test]
    #[should_panic(expected = "Add Duplicated KV")]
    fn test_panic_duplicate_add_kv() {
        let bt = BTreeIndexer::new(32);
        bt.add_kv(0x0004000300020001, 0x10);
        bt.add_kv(0x0004000300020001, 0x10);
    }

    #[test]
    #[should_panic(expected = "Cannot Erase Non-existent KV")]
    fn test_panic_erase_non_existent_kv() {
        let bt = BTreeIndexer::new(32);
        bt.add_kv(0x0004000300020001, 0x10);
        bt.erase_kv(0x0004000300020000, 0x10);
    }

    #[test]
    fn test_add_kv() {
        let bt = BTreeIndexer::new(65535);
        bt.add_kv(0x1111_2222_3333_4444, 888);
        bt.erase_kv(0x1111_2222_3333_4444, 888);
    }

    #[test]
    fn test_btree() {
        let bt = BTreeIndexer::new(32);
        assert_eq!(0, bt.len(0));
        bt.add_kv(0x0004000300020001, 0x10);
        assert_eq!(1, bt.len(0));
        bt.add_kv(0x0005000300020001, 0x10);
        assert_eq!(2, bt.len(0));
        bt.add_kv(0x0004000300020001, 0x00);

        assert_eq!(
            [
                (0x0004000300020001, 0x10),
                (0x0004000300020001, 0),
            ],
            get_all_adjacent_values(&bt, 0x0004000300020001).as_slice()
        );

        bt.add_kv(0x0004000300020000, 0x20);
        bt.add_kv(0x0004000300020000, 0x30);
        assert_eq!(5, bt.len(0));

        assert_eq!(
            [0x10, 0x0],
            get_all_values(&bt, 0x0004000300020001).as_slice()
        );
        assert_eq!(
            [0x20, 0x30],
            get_all_values(&bt, 0x0004000300020000).as_slice()
        );
        assert_eq!(
            [
                (0x0004000300020001, 0x10),
                (0x0004000300020001, 0),
                (0x0004000300020000, 0x30),
                (0x0004000300020000, 0x20)
            ],
            get_all_adjacent_values(&bt, 0x0004000300020001).as_slice()
        );

        assert_eq!(
            [(0x0004000300020000, 0x20), (0x0004000300020000, 0x30)],
            get_all_adjacent_values(&bt, 0x0004000300020000).as_slice()
        );

        bt.add_kv(0x0004000300020001, 0x100);
        bt.add_kv(0x0004000300020001, 0x110);
        bt.add_kv(0x0004000300020001, 0x120);
        bt.add_kv(0x0004000300020001, 0x130);
        bt.add_kv(0x0004000300020001, 0x140);
        bt.add_kv(0x0004000300020001, 0x150);
        bt.add_kv(0x0004000300020001, 0x160);
        bt.add_kv(0x0004000300020001, 0x170);
        assert_eq!(
            [0x10, 0, 0x100, 0x110, 0x120, 0x130, 0x140, 0x150, 0x160, 0x170],
            get_all_values(&bt, 0x0004000300020001).as_slice()
        );
        bt.change_kv(0x0004000300020001, 0x170, 0x710);
        assert_eq!(
            [0x10, 0, 0x100, 0x110, 0x120, 0x130, 0x140, 0x150, 0x160, 0x710],
            get_all_values(&bt, 0x0004000300020001).as_slice()
        );
        bt.add_kv(0x0004000300020002, 0x180);
        assert_eq!(
            [
                (0x0004000300020002, 0x180),
                (0x0004000300020001, 0x710),
                (0x0004000300020001, 0x160),
                (0x0004000300020001, 0x150),
                (0x0004000300020001, 0x140),
                (0x0004000300020001, 0x130),
                (0x0004000300020001, 0x120),
                (0x0004000300020001, 0x110),
                (0x0004000300020001, 0x100),
                (0x0004000300020001, 0),
                (0x0004000300020001, 0x10)
            ],
            get_all_adjacent_values(&bt, 0x0004000300020002).as_slice()
        );
        bt.erase_kv(0x0004000300020001, 0x150);
        assert_eq!(
            [
                (0x0004000300020001, 0x10),
                (0x0004000300020001, 0),
                (0x0004000300020001, 0x100),
                (0x0004000300020001, 0x110),
                (0x0004000300020001, 0x120),
                (0x0004000300020001, 0x130),
                (0x0004000300020001, 0x140),
                (0x0004000300020001, 0x160),
                (0x0004000300020001, 0x710),
                (0x0004000300020000, 0x30),
                (0x0004000300020000, 0x20)
            ],
            get_all_adjacent_values(&bt, 0x0004000300020001).as_slice()
        );
        bt.add_kv(0x000400030001FFFF, 0x150);
        assert_eq!(
            [
                (0x0004000300020001, 0x10),
                (0x0004000300020001, 0),
                (0x0004000300020001, 0x100),
                (0x0004000300020001, 0x110),
                (0x0004000300020001, 0x120),
                (0x0004000300020001, 0x130),
                (0x0004000300020001, 0x140),
                (0x0004000300020001, 0x160),
                (0x0004000300020001, 0x710),
                (0x0004000300020000, 0x30),
                (0x0004000300020000, 0x20)
            ],
            get_all_adjacent_values(&bt, 0x0004000300020001).as_slice()
        );
    }

    #[test]
    fn test_key_exists() {
        let indexer = BTreeIndexer::new(32);
        indexer.add_kv(111, 88);
        indexer.add_kv(222, 888);

        assert_eq!(false, indexer.key_exists(333, 123));
        assert_eq!(false, indexer.key_exists(111, 123));
        assert_eq!(false, indexer.key_exists(111, 888));
        assert_eq!(true, indexer.key_exists(111, 88));
        assert_eq!(true, indexer.key_exists(222, 888));
    }

    #[test]
    fn test_code_indexer() {
        let bt = CodeIndexer::new();
        let key = "key".as_bytes();
        bt.add_kv(key, 0x10);
        bt.for_each_value(key, |v_in| {
            assert_eq!(0x10, v_in);
            true
        });
    }
}
