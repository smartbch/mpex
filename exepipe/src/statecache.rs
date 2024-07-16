use byteorder::{BigEndian, ByteOrder};
use dashmap::DashMap;
use mpads::bytescache::BytesCache;
use mpads::changeset::ChangeSet;
use mpads::def::{BYTES_CACHE_SHARD_COUNT, OP_CREATE, OP_DELETE, OP_WRITE};
use revm::precompile::primitives::{Bytecode, FixedBytes};
use std::sync::Arc;

pub type CodeMap = DashMap<FixedBytes<32>, Bytecode>;
pub type DataMap = BytesCache<[u8; 32]>;

pub struct StateCache {
    // used both for warmup and post-update
    pub bytecode_map: Arc<CodeMap>,
    // used only for post-update of world state
    // entrycache is used for warmup of world state
    data_map: DataMap,
}

impl StateCache {
    pub fn new() -> Self {
        Self {
            bytecode_map: Arc::new(DashMap::new()),
            data_map: BytesCache::new(),
        }
    }

    pub fn insert_data(&self, key_hash: &[u8; 32], data: &[u8]) {
        let idx = BigEndian::read_u64(&key_hash[..8]) as usize;
        let idx = idx % BYTES_CACHE_SHARD_COUNT;
        self.data_map.insert(key_hash, idx, data);
    }

    pub fn lookup_data(&self, key_hash: &[u8; 32], data: &mut [u8]) -> bool {
        let idx = BigEndian::read_u64(&key_hash[..8]) as usize;
        let idx = idx % BYTES_CACHE_SHARD_COUNT;
        self.data_map.lookup(key_hash, idx, |bz| {
            data.copy_from_slice(&bz[..data.len()]);
        })
    }

    pub fn insert_code(&self, code_hash: &FixedBytes<32>, bc: &Bytecode) {
        self.bytecode_map.insert(*code_hash, bc.clone());
    }

    pub fn lookup_code(&self, code_hash: &FixedBytes<32>) -> Option<Bytecode> {
        match self.bytecode_map.get(code_hash) {
            None => None,
            Some(bc) => Some(bc.clone()),
        }
    }

    pub fn apply_change(&self, change_set: &ChangeSet) {
        change_set.apply_op_in_range(|op, key_hash, _k, v, _rec| {
            if op == OP_CREATE || op == OP_WRITE || op == OP_DELETE {
                self.insert_data(key_hash, v);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use mpads::def::OP_READ;
    use revm::primitives::{hex::FromHex, Bytes};

    use super::*;

    fn new_bytecode<T: AsRef<[u8]>>(code: T) -> Bytecode {
        Bytecode::new_raw(Bytes::from_hex(code).unwrap())
    }

    #[test]
    fn test_data() {
        let test_data = [
            (&[0x01u8; 32], "aaa".as_bytes()),
            (&[0x02u8; 32], "bbb".as_bytes()),
            (&[0x03u8; 32], "ccc".as_bytes()),
            (&[0x04u8; 32], "ddd".as_bytes()),
        ];

        let cache = StateCache::new();
        for (k, v) in test_data {
            cache.insert_data(k, v);
        }

        let mut buf = [0u8; 3];
        for (k, v) in test_data {
            assert!(cache.lookup_data(k, &mut buf));
            assert_eq!(v, &buf[..]);
        }

        let k5 = &[0x05u8; 32];
        assert_eq!(false, cache.lookup_data(k5, &mut buf));
    }

    #[test]
    fn test_code() {
        let test_data = [
            (&FixedBytes::new([0x01u8; 32]), &new_bytecode("aaaaaaaa")),
            (&FixedBytes::new([0x02u8; 32]), &new_bytecode("bbbbbbbb")),
            (&FixedBytes::new([0x03u8; 32]), &new_bytecode("cccccccc")),
            (&FixedBytes::new([0x04u8; 32]), &new_bytecode("dddddddd")),
        ];

        let cache = StateCache::new();
        for (k, v) in test_data {
            cache.insert_code(k, v);
        }

        for (k, v) in test_data {
            assert_eq!(Option::Some(v), cache.lookup_code(k).as_ref());
        }

        let k5 = &FixedBytes::new([0x05u8; 32]);
        assert_eq!(Option::None, cache.lookup_code(k5));
    }

    #[test]
    fn test_apply_change() {
        let test_data = [
            (OP_CREATE, &[0x11u8; 32], &[0xaau8; 32]),
            (OP_WRITE, &[0x22u8; 32], &[0xbbu8; 32]),
            (OP_DELETE, &[0x33u8; 32], &[0xccu8; 32]),
            (OP_READ, &[0x44u8; 32], &[0xddu8; 32]),
        ];

        let mut cs = ChangeSet::new();
        for (op, kh, v) in test_data {
            cs.add_op(op, 123, kh, kh, v, Option::None);
        }

        let cache = StateCache::new();
        cache.apply_change(&cs);

        let mut buf = [0u8; 32];
        for (op, kh, v) in test_data {
            if op == OP_READ {
                assert_eq!(false, cache.lookup_data(kh, &mut buf));
            } else {
                assert_eq!(true, cache.lookup_data(kh, &mut buf));
                assert_eq!(v, &buf);
            }
        }
    }
}
