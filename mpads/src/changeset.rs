use crate::def::{CODE_SHARD_ID, SHARD_COUNT};
use crate::refdb::{byte0_to_shard_id, OpRecord};
use crate::utils::hasher;
use std::cmp::Ordering;

#[derive(Debug)]
pub struct ChangeSet {
    pub data: Vec<u8>,
    pub op_list: Vec<ChangeOp>,
    shard_starts: [u32; SHARD_COUNT + 1],
}

#[derive(Debug)]
pub struct ChangeOp {
    pub op_type: u8,
    shard_id: u8,
    key_start: u32,
    value_start: u32,
    key_hash_start: u32,
    rec: Option<Box<OpRecord>>,
}

impl ChangeSet {
    pub fn new() -> Self {
        Self {
            data: Vec::with_capacity(1000),
            op_list: Vec::with_capacity(10),
            shard_starts: [u32::MAX; SHARD_COUNT + 1],
        }
    }

    pub fn add_op_rec(&mut self, mut rec: OpRecord) {
        let rec_box = Box::new(rec);
        let key_hash = hasher::hash(&rec_box.key[..]);
        let (k, v) = (&rec_box.key[..], &rec_box.value[..]);

        let shard_id = byte0_to_shard_id(key_hash[0]) as u8;
        if shard_id != rec_box.shard_id as u8 {
            panic!(
                "mismatch: shard_id={} key_hash={:?} rec={:?}",
                shard_id, key_hash, *rec_box
            );
        }

        let key_start = self.data.len();
        self.data.extend_from_slice(k);
        let value_start = self.data.len();
        self.data.extend_from_slice(v);
        let key_hash_start = self.data.len();
        self.data.extend_from_slice(&key_hash[..]);
        let op_type = rec_box.op_type;
        self.op_list.push(ChangeOp {
            op_type,
            shard_id,
            key_start: key_start as u32,
            value_start: value_start as u32,
            key_hash_start: key_hash_start as u32,
            rec: Some(rec_box),
        });
    }

    pub fn add_op(
        &mut self,
        op_type: u8,
        shard_id: u8,
        key_hash: &[u8; 32],
        k: &[u8],
        v: &[u8],
        rec: Option<Box<OpRecord>>,
    ) {
        let key_start = self.data.len();
        self.data.extend_from_slice(k);
        let value_start = self.data.len();
        self.data.extend_from_slice(v);
        let key_hash_start = self.data.len();
        self.data.extend_from_slice(&key_hash[..]);
        self.op_list.push(ChangeOp {
            op_type,
            shard_id,
            key_start: key_start as u32,
            value_start: value_start as u32,
            key_hash_start: key_hash_start as u32,
            rec,
        });
    }

    pub fn sort(&mut self) {
        self.op_list.sort_by(|a, b| {
            let mut res = a.shard_id.cmp(&b.shard_id);
            if res == Ordering::Equal {
                let x = &self.data[a.key_hash_start as usize..a.key_hash_start as usize + 32];
                let y = &self.data[b.key_hash_start as usize..b.key_hash_start as usize + 32];
                res = x.cmp(&y);
            }
            res
        });
        for idx in 0..self.op_list.len() {
            let shard_id = self.op_list[idx].shard_id as usize;
            if self.shard_starts[shard_id] == u32::MAX {
                self.shard_starts[shard_id] = idx as u32;
            }
        }
    }

    pub fn apply_op_in_range<F>(&self, mut access: F)
    where
        F: FnMut(u8, &[u8; 32], &[u8], &[u8], Option<&Box<OpRecord>>),
    {
        let mut key_hash = [0u8; 32];
        for op in self.op_list.iter() {
            if op.shard_id as usize == CODE_SHARD_ID {
                continue;
            }
            let kh_start = op.key_hash_start as usize;
            key_hash[..].copy_from_slice(&self.data[kh_start..kh_start + 32]);
            let key = &self.data[op.key_start as usize..op.value_start as usize];
            let value = &self.data[op.value_start as usize..kh_start];
            access(op.op_type, &key_hash, key, value, op.rec.as_ref());
        }
    }

    pub fn run_in_shard<F>(&self, shard_id: usize, mut access: F)
    where
        F: FnMut(u8, &[u8; 32], &[u8], &[u8], Option<&Box<OpRecord>>),
    {
        let shard_start = self.shard_starts[shard_id];
        if shard_start == u32::MAX {
            return;
        }
        let mut key_hash = [0u8; 32];
        let mut idx = shard_start as usize;
        while idx < self.op_list.len() {
            let op = &self.op_list[idx as usize];
            if shard_id != op.shard_id as usize {
                break;
            }
            let kh_start = op.key_hash_start as usize;
            key_hash[..].copy_from_slice(&self.data[kh_start..kh_start + 32]);
            let key = &self.data[op.key_start as usize..op.value_start as usize];
            let value = &self.data[op.value_start as usize..kh_start];
            access(op.op_type, &key_hash, key, value, op.rec.as_ref());
            idx += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::refdb::OpRecord;

    #[test]
    fn test_changeset_new() {
        let changeset = ChangeSet::new();
        assert_eq!(changeset.data.capacity(), 1000);
        assert_eq!(changeset.op_list.capacity(), 10);
        assert_eq!(changeset.shard_starts.len(), SHARD_COUNT + 1);
        assert!(changeset
            .shard_starts
            .iter()
            .all(|&start| start == u32::MAX));
    }

    #[test]
    fn test_add_op() {
        let mut changeset = ChangeSet::new();
        let key_hash = [1u8; 32];
        let key = vec![1, 2, 3];
        let value = vec![4, 5, 6];
        let rec = Some(Box::new(OpRecord::new(1)));

        changeset.add_op(1, 0, &key_hash, &key, &value, rec.clone());

        assert_eq!(
            changeset.data,
            vec![
                1, 2, 3, 4, 5, 6, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1
            ]
        );
        assert_eq!(changeset.op_list.len(), 1);

        let op = &changeset.op_list[0];
        assert_eq!(op.op_type, 1);
        assert_eq!(op.shard_id, 0);
        assert_eq!(op.key_start, 0);
        assert_eq!(op.value_start, 3);
        assert_eq!(op.key_hash_start, 6);
        assert_eq!(op.rec, rec);
    }

    // #[test]
    // fn test_get_sizes_and_rewind() {
    //     let mut changeset = ChangeSet::new();
    //     assert_eq!(changeset.get_sizes(), (0, 0));

    //     let key_hash = [1u8; 32];
    //     let key = vec![1, 2, 3];
    //     let value = vec![4, 5, 6];
    //     let rec = Some(Box::new(OpRecord::new(1)));

    //     changeset.add_op(1, 0, &key_hash, &key, &value, rec.clone());

    //     assert_eq!(changeset.get_sizes(), (38, 1));
    // }

    #[test]
    fn test_apply_op_in_range() {
        let mut changeset = ChangeSet::new();
        let key_hash = [1u8; 32];
        let key = vec![1, 2, 3];
        let value = vec![4, 5, 6];
        let rec = Some(Box::new(OpRecord::new(1)));

        changeset.add_op(0, 0, &key_hash, &key, &value, rec.clone());
        changeset.add_op(1, 0, &key_hash, &key, &value, rec.clone());

        let mut count = 0;
        changeset.apply_op_in_range(|op_type, kh, k, v, r| {
            assert_eq!(op_type, count);
            assert_eq!(kh, &key_hash);
            assert_eq!(k, &key[..]);
            assert_eq!(v, &value[..]);
            assert_eq!(r, rec.as_ref());
            count += 1;
        });
        assert_eq!(count, 2);
    }

    #[test]
    fn test_sort() {
        let mut changeset = ChangeSet::new();
        let key_hash1 = [1u8; 32];
        let key1 = vec![1, 2, 3];
        let value1 = vec![4, 5, 6];
        let key_hash2 = [2u8; 32];
        let key2 = vec![7, 8, 9];
        let value2 = vec![10, 11, 12];

        changeset.add_op(1, 1, &key_hash2, &key2, &value2, None);
        changeset.add_op(1, 0, &key_hash1, &key1, &value1, None);

        changeset.sort();

        assert_eq!(changeset.op_list.len(), 2);
        assert_eq!(changeset.op_list[0].shard_id, 0);
        assert_eq!(changeset.op_list[1].shard_id, 1);
    }

    #[test]
    fn test_run_in_shard() {
        let mut changeset = ChangeSet::new();
        let mut accessed = false;
        changeset.run_in_shard(0, |_, _, _, _, _| {
            accessed = true;
        });
        assert!(!accessed);

        let key_hash = [1u8; 32];
        let key = vec![1, 2, 3];
        let value = vec![4, 5, 6];
        let rec = Some(Box::new(OpRecord::new(1)));

        changeset.add_op(1, 1, &key_hash, &key, &value, rec.clone());
        changeset.add_op(2, 0, &key_hash, &key, &value, rec.clone());
        changeset.add_op(3, 0, &[0u8; 32], &key, &value, rec.clone());
        changeset.sort();
        assert_eq!(changeset.op_list[0].shard_id, 0);
        assert_eq!(changeset.op_list[0].op_type, 3);
        assert_eq!(changeset.op_list[1].shard_id, 0);
        assert_eq!(changeset.op_list[1].op_type, 2);
        assert_eq!(changeset.op_list[2].shard_id, 1);
        assert_eq!(changeset.op_list[2].op_type, 1);

        changeset.run_in_shard(1, |op_type, kh, k, v, r| {
            assert_eq!(op_type, 1);
            assert_eq!(kh, &key_hash);
            assert_eq!(k, &key[..]);
            assert_eq!(v, &value[..]);
            assert_eq!(r, rec.as_ref());
            accessed = true;
        });

        assert!(accessed);
    }
}
