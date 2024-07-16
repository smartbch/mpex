use crate::def::{ENTRY_FIXED_LENGTH, NULL_ENTRY_VERSION, SHARD_COUNT};
use crate::utils::hasher;
use byteorder::{BigEndian, ByteOrder, LittleEndian};

#[derive(Debug)]
pub struct Entry<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub next_key_hash: &'a [u8],
    pub version: i64,
    pub last_version: i64,
    pub serial_number: u64,
}

pub struct EntryBz<'a> {
    pub bz: &'a [u8],
}

pub type Hash32 = [u8; 32];

pub const ZERO_HASH32: Hash32 = [0u8; 32];

impl<'a> Entry<'a> {
    pub fn from_bz(e: &'a EntryBz) -> Entry<'a> {
        Self {
            key: e.key(),
            value: e.value(),
            next_key_hash: e.next_key_hash(),
            version: e.version(),
            last_version: e.last_version(),
            serial_number: e.serial_number(),
        }
    }

    pub fn get_serialized_len(&self, deactived_sn_count: usize) -> usize {
        let length = ENTRY_FIXED_LENGTH + self.key.len() + self.value.len();
        ((length + 7) / 8) * 8 + deactived_sn_count * 8
    }

    // 1B KeyLength
    // 3B-valueLength
    // 1B DeactivedSNList length
    // Key
    // Value
    // padding-zero-bytes
    // 32B NextKey
    // 8B Height
    // 8B LastHeight
    // 8B SerialNumber
    // DeactivedSerialNumList (list of 8B-int)
    pub fn dump(&self, b: &mut [u8], deactived_sn_list: &[u64]) -> usize {
        if b.len() < self.get_serialized_len(deactived_sn_list.len()) {
            panic!("Not enough space for dumping");
        }
        let first32 = (self.value.len() * 256 + self.key.len()) as u32;
        b[4] = deactived_sn_list.len() as u8;
        LittleEndian::write_u32(&mut b[..4], first32);
        let mut i = 5;
        b[i..i + self.key.len()].copy_from_slice(self.key);
        i += self.key.len();
        b[i..i + self.value.len()].copy_from_slice(self.value);
        i += self.value.len();
        while i % 8 != 0 {
            b[i] = 0;
            i += 1;
        }

        if self.next_key_hash.len() != 32 {
            panic!("NextKeyHash is not 32-byte");
        }
        b[i..i + 32].copy_from_slice(self.next_key_hash);
        i += 32;
        LittleEndian::write_i64(&mut b[i..i + 8], self.version);
        i += 8;
        LittleEndian::write_i64(&mut b[i..i + 8], self.last_version);
        i += 8;
        LittleEndian::write_u64(&mut b[i..i + 8], self.serial_number);
        i += 8;

        for &sn in deactived_sn_list {
            LittleEndian::write_u64(&mut b[i..i + 8], sn);
            i += 8;
        }

        i
    }
}

pub fn sentry_entry(shard_id: usize, sn: u64, bz: &mut [u8]) -> EntryBz {
    if shard_id >= SHARD_COUNT || (sn as usize) >= (1 << 16) / SHARD_COUNT {
        panic!("SentryEntry Overflow");
    }
    let first16 = (shard_id * (1 << 16) / SHARD_COUNT | (sn as usize)) as u16;
    let mut key: [u8; 32] = [0; 32];
    BigEndian::write_u16(&mut key[0..2], first16);
    let mut next_key_hash: [u8; 32];
    if first16 == 0xFFFF {
        next_key_hash = [0xFF; 32];
    } else {
        next_key_hash = [0; 32];
        BigEndian::write_u16(&mut next_key_hash[0..2], first16 + 1);
    }
    let e = Entry {
        key: &key[..],
        value: &[] as &[u8],
        next_key_hash: &next_key_hash[..],
        version: 0,
        last_version: -1,
        serial_number: sn,
    };
    let i = e.dump(bz, &[] as &[u64]);
    EntryBz { bz: &bz[..i] }
}

pub fn null_entry(bz: &mut [u8]) -> EntryBz {
    let next_key_hash: [u8; 32] = [0; 32];
    let e = Entry {
        key: &[] as &[u8],
        value: &[] as &[u8],
        next_key_hash: &next_key_hash[..],
        version: NULL_ENTRY_VERSION,
        last_version: NULL_ENTRY_VERSION,
        serial_number: u64::MAX,
    };
    let i = e.dump(bz, &[] as &[u64]);
    EntryBz { bz: &bz[..i] }
}

pub fn entry_to_bytes<'a>(
    e: &'a Entry<'a>,
    deactived_sn_list: &'a [u64],
    bz: &'a mut [u8],
) -> EntryBz<'a> {
    let total_len = ((ENTRY_FIXED_LENGTH + e.key.len() + e.value.len() + 7) / 8) * 8
        + deactived_sn_list.len() * 8;
    e.dump(&mut bz[..], deactived_sn_list);
    EntryBz {
        bz: &bz[..total_len],
    }
}

pub fn get_kv_len(len_bytes: &[u8]) -> (usize, usize) {
    let first32 = LittleEndian::read_u32(&len_bytes[..4]);
    let key_len = (first32 & 0xff) as usize;
    let value_len = (first32 >> 8) as usize;
    (key_len, value_len)
}

impl<'a> EntryBz<'a> {
    pub fn get_entry_len(len_bytes: &[u8]) -> usize {
        let (key_len, value_len) = get_kv_len(len_bytes);
        let dsn_count = len_bytes[4] as usize;
        ((ENTRY_FIXED_LENGTH + key_len + value_len + 7) / 8) * 8 + dsn_count * 8
    }

    pub fn len(&self) -> usize {
        self.bz.len()
    }

    pub fn hash(&self) -> Hash32 {
        hasher::hash(self.bz)
    }

    pub fn value(&self) -> &[u8] {
        let (key_len, value_len) = get_kv_len(self.bz);
        &self.bz[5 + key_len..5 + key_len + value_len]
    }

    pub fn key(&self) -> &[u8] {
        let key_len = self.bz[0] as usize;
        &self.bz[5..5 + key_len]
    }

    pub fn key_hash(&self) -> Hash32 {
        if self.value().len() == 0 {
            let mut res: Hash32 = [0; 32];
            res[0] = self.bz[5]; // first byte of key
            res[1] = self.bz[6]; // second byte of key
            return res;
        }
        return hasher::hash(self.key());
    }

    pub fn k64(&self) -> u64 {
        let key_hash = self.key_hash();
        BigEndian::read_u64(&key_hash[0..8])
    }

    fn next_key_hash_start(&self) -> usize {
        let (key_len, value_len) = get_kv_len(&self.bz[0..4]);
        ((5 + key_len + value_len + 7) / 8) * 8
    }

    pub fn next_key_hash(&self) -> &[u8] {
        let start = self.next_key_hash_start();
        &self.bz[start..start + 32]
    }

    pub fn version(&self) -> i64 {
        let start = self.next_key_hash_start() + 32;
        LittleEndian::read_i64(&self.bz[start..start + 8])
    }

    pub fn last_version(&self) -> i64 {
        let start = self.next_key_hash_start() + 40;
        LittleEndian::read_i64(&self.bz[start..start + 8])
    }

    pub fn serial_number(&self) -> u64 {
        let start = self.next_key_hash_start() + 48;
        LittleEndian::read_u64(&self.bz[start..start + 8])
    }

    pub fn dsn_count(&self) -> usize {
        self.bz[4] as usize
    }

    pub fn size_wo_dsn(&self) -> usize {
        self.len() - self.dsn_count() * 8
    }

    pub fn get_deactived_sn(&self, n: usize) -> u64 {
        let start = self.len() - (self.dsn_count() - n) * 8;
        LittleEndian::read_u64(&self.bz[start..start + 8])
    }

    pub fn copy_head_to(&self, buf: &mut [u8]) {
        buf.copy_from_slice(&self.bz[0..buf.len()]);
    }
}

#[cfg(test)]
mod entry_bz_tests {
    use super::*;

    #[test]
    fn test_dump() {
        let key = "key".as_bytes();
        let val = "value".as_bytes();
        let next_key_hash: Hash32 = [0xab; 32];
        let deactived_sn_list: [u64; 4] = [0xf1, 0xf2, 0xf3, 0xf4];

        let entry = Entry {
            key: key,
            value: val,
            next_key_hash: &next_key_hash,
            version: 12345,
            last_version: 11111,
            serial_number: 99999,
        };

        let mut buf: [u8; 1024] = [0; 1024];
        let n = entry.dump(&mut buf, &deactived_sn_list);
        assert_eq!(104, n);

        #[rustfmt::skip]
        assert_eq!(
            hex::encode(&buf[0..n]),
            [
                "03",         // key len
                "050000",     // val len
                "04",         // deactived_sn_list len
                "6b6579",     // key
                "76616c7565", // val
                "000000",     // padding
                "abababababababababababababababababababababababababababababababab", // next key hash
                "3930000000000000", // version
                "672b000000000000", // last version
                "9f86010000000000", // serial number
                "f100000000000000", // deactived_sn_list
                "f200000000000000",
                "f300000000000000",
                "f400000000000000",
            ].join(""),
        );
    }
}
