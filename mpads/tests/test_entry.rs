#[cfg(test)]
mod tests {
    use super::*;
    use mpads::def::{ENTRY_FIXED_LENGTH, NULL_ENTRY_VERSION};
    use mpads::entry::{self, Entry, EntryBz};
    use sha2::{Digest, Sha256};

    const DUMMY_ENTRY_VERSION: i64 = -2;

    fn dummy_entry(sn: u64, bz: &mut [u8]) -> EntryBz {
        let next_key_hash: [u8; 32] = [0; 32];
        let e = Entry {
            key: &b"dummy"[..],
            value: &b"dummy"[..],
            next_key_hash: &next_key_hash[..],
            version: DUMMY_ENTRY_VERSION,
            last_version: DUMMY_ENTRY_VERSION,
            serial_number: sn,
        };
        let i = e.dump(bz, &[] as &[u64]);
        EntryBz { bz: &bz[..i] }
    }

    #[test]
    fn test_entry() {
        let len_bytes = [7, 6, 5, 4];
        let (key_len, value_len) = entry::get_kv_len(&len_bytes);
        assert_eq!(0x7, key_len);
        assert_eq!(0x040506, value_len);

        let mut sentry_entry_buf: [u8; 101] = [0; ENTRY_FIXED_LENGTH + 32 + 8];
        let sentry_entry = entry::sentry_entry(2, 5, &mut sentry_entry_buf);
        assert_eq!(5, sentry_entry.serial_number());
        let mut hash: [u8; 32] = [0; 32];
        hash[0] = 0x20;
        hash[1] = 0x05;
        let mut next_hash = [0x0; 32];
        next_hash[0] = 0x20;
        next_hash[1] = 0x06;
        assert_eq!(hash.as_slice(), sentry_entry.key());
        assert_eq!(hash, sentry_entry.key_hash());
        assert_eq!(next_hash.as_slice(), sentry_entry.next_key_hash());
        assert_eq!(0, sentry_entry.value().len());
        let x: [u8; 32] = Sha256::digest(sentry_entry.bz).into();
        assert_eq!(x, sentry_entry.hash());
        assert_eq!(0, sentry_entry.version());
        assert_eq!(-1, sentry_entry.last_version());
        assert_eq!(5, sentry_entry.serial_number());
        assert_eq!(0, sentry_entry.dsn_count());

        let mut dummy_entry_buf: [u8; 101] = [0; ENTRY_FIXED_LENGTH + 32 + 8];
        let dummy_entry: EntryBz = dummy_entry(100, &mut dummy_entry_buf);
        let zero32 = [0u8; 32];
        assert_eq!(b"dummy".as_ref(), dummy_entry.key());
        let x: [u8; 32] = Sha256::digest(b"dummy").into();
        assert_eq!(x, dummy_entry.key_hash());
        assert_eq!(zero32.as_slice(), dummy_entry.next_key_hash());
        assert_eq!(b"dummy".as_ref(), dummy_entry.value());
        let x: [u8; 32] = Sha256::digest(dummy_entry.bz).into();
        assert_eq!(x, dummy_entry.hash());
        assert_eq!(DUMMY_ENTRY_VERSION, dummy_entry.version());
        assert_eq!(DUMMY_ENTRY_VERSION, dummy_entry.last_version());
        assert_eq!(100, dummy_entry.serial_number());
        assert_eq!(0, dummy_entry.dsn_count());

        let mut null_entry_buf = [0; ENTRY_FIXED_LENGTH + 32 + 8];
        let null_entry = entry::null_entry(&mut null_entry_buf);
        assert_eq!(0, null_entry.key().len());
        let x = [0u8; 32];
        assert_eq!(x, null_entry.key_hash());
        assert_eq!(zero32.as_slice(), null_entry.next_key_hash());
        assert_eq!(0, null_entry.value().len());
        let x: [u8; 32] = Sha256::digest(null_entry.bz).into();
        assert_eq!(x, null_entry.hash());
        assert_eq!(NULL_ENTRY_VERSION, null_entry.version());
        assert_eq!(NULL_ENTRY_VERSION, null_entry.last_version());
        assert_eq!(u64::MAX, null_entry.serial_number());
        assert_eq!(0, null_entry.dsn_count());

        let next_key_hash = Sha256::digest(String::from("nextkey").as_bytes());
        let x: [u8; 32] = next_key_hash.into();
        let key = String::from("key");
        let value = String::from("value");
        let entry = entry::Entry {
            key: key.as_bytes(),
            value: value.as_bytes(),
            next_key_hash: &x,
            version: 10000,
            last_version: 9000,
            serial_number: 800,
        };
        let mut b = vec![0; 200];
        let deactived_serial_num_list = vec![0, 1, 2, 3];
        let entry_bz_size = entry.dump(&mut b, &deactived_serial_num_list);
        let entry_bz = entry::EntryBz {
            bz: &b[..entry_bz_size],
        };
        assert_eq!(entry.key, entry_bz.key());
        let x: [u8; 32] = Sha256::digest(&entry.key).into();
        assert_eq!(x, entry_bz.key_hash());
        assert_eq!(entry.next_key_hash, entry_bz.next_key_hash());
        assert_eq!(entry.value, entry_bz.value());
        let x: [u8; 32] = Sha256::digest(&entry_bz.bz).into();
        assert_eq!(x, entry_bz.hash());
        assert_eq!(entry.version, entry_bz.version());
        assert_eq!(entry.last_version, entry_bz.last_version());
        assert_eq!(entry.serial_number, entry_bz.serial_number());
        assert_eq!(deactived_serial_num_list.len(), entry_bz.dsn_count());
        for (i, &deactived_sn) in deactived_serial_num_list.iter().enumerate() {
            assert_eq!(deactived_sn, entry_bz.get_deactived_sn(i));
        }
    }
}
