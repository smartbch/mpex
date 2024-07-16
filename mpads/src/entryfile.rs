use crate::def::DEFAULT_ENTRY_SIZE;
use crate::entry::EntryBz;
use crate::hpfile::{HPFile, PreReader};
use std::sync::Arc;

pub struct EntryFile {
    hp_file: HPFile,
}

impl EntryFile {
    pub fn new(buffer_size: usize, block_size: i64, dir_name: String) -> EntryFile {
        EntryFile {
            hp_file: HPFile::new(buffer_size as i64, block_size, dir_name).unwrap(),
        }
    }

    pub fn size(&self) -> i64 {
        self.hp_file.size()
    }

    fn read_entry_with_pre_reader(&self, off: i64, buf: &mut Vec<u8>, pre_reader: &mut PreReader) {
        // first we get the exact size
        self.hp_file
            .read_at_with_pre_reader(buf, 5, off, pre_reader);
        let size = EntryBz::get_entry_len(&buf[0..5]);
        if buf.len() < size {
            buf.resize(size, 0);
        }
        // then we copy the exact bytes out from pre-reader
        self.hp_file
            .read_at_with_pre_reader(buf, size, off, pre_reader)
    }

    pub fn read_entry(&self, off: i64, buf: &mut [u8]) -> usize {
        // we try to read the disk file only once
        self.hp_file.read_at(&mut buf[..DEFAULT_ENTRY_SIZE], off);
        let size = EntryBz::get_entry_len(&buf[0..5]);
        if size > DEFAULT_ENTRY_SIZE && size <= buf.len() {
            // we must read the disk file twice if not lucky
            self.hp_file.read_at(&mut buf[..size], off);
        }
        size
    }

    fn append(&self, e: &EntryBz, buffer: &mut Vec<u8>) -> i64 {
        self.hp_file.append(e.bz, buffer)
    }

    pub fn truncate(&self, size: i64) {
        if let Some(err) = self.hp_file.truncate(size) {
            panic!("{}", err)
        }
    }

    pub fn close(&self) {
        self.hp_file.close();
    }

    pub fn prune_head(&self, off: i64) {
        if let Some(err) = self.hp_file.prune_head(off) {
            panic!("{}", err);
        }
    }
}

pub struct EntryFileWithPreReader {
    entry_file: Arc<EntryFile>,
    pre_reader: PreReader,
}

impl EntryFileWithPreReader {
    pub fn new(ef: &Arc<EntryFile>) -> Self {
        Self {
            entry_file: ef.clone(),
            pre_reader: PreReader::new(),
        }
    }

    pub fn read_entry(&mut self, off: i64, buf: &mut Vec<u8>) {
        self.entry_file
            .read_entry_with_pre_reader(off, buf, &mut self.pre_reader);
    }

    pub fn scan_entries_lite<F>(&mut self, start_pos: i64, mut access: F)
    where
        F: FnMut(u64, &[u8], i64, u64),
    {
        let mut pos = start_pos;
        let mut last_pos = start_pos;
        let size = self.entry_file.hp_file.size();
        let total = size - start_pos;
        let step = total / 20;
        let mut buf = Vec::with_capacity(DEFAULT_ENTRY_SIZE);

        while pos < size {
            if (pos - start_pos) / step != (last_pos - start_pos) / step {
                println!(
                    "ScanEntriesLite {:.2} {}/{}",
                    (pos - start_pos) as f64 / total as f64,
                    pos - start_pos,
                    total
                );
            }
            last_pos = pos;
            self.read_entry(pos, &mut buf);
            let entry = EntryBz { bz: &buf[..] };
            access(
                entry.k64(),
                entry.next_key_hash(),
                pos,
                entry.serial_number(),
            );
            pos += entry.len() as i64;
        }
    }
}

pub struct EntryFileWriter {
    pub entry_file: Arc<EntryFile>,
    wrbuf: Vec<u8>,
}

impl EntryFileWriter {
    pub fn new(entry_file: Arc<EntryFile>, buffer_size: usize) -> EntryFileWriter {
        return EntryFileWriter {
            entry_file,
            wrbuf: Vec::with_capacity(buffer_size),
        };
    }

    pub fn temp_clone(&self) -> EntryFileWriter {
        EntryFileWriter {
            entry_file: self.entry_file.clone(),
            wrbuf: Vec::with_capacity(0),
        }
    }

    pub fn append(&mut self, e: &EntryBz) -> i64 {
        self.entry_file.append(e, &mut self.wrbuf)
    }

    pub fn flush(&mut self) {
        self.entry_file.hp_file.flush(&mut self.wrbuf);
    }
}

#[cfg(test)]
mod entry_file_tests {
    use super::*;
    use crate::def::LEAF_COUNT_IN_TWIG;
    use crate::entry::{entry_to_bytes, Entry};
    use crate::test_helper::TempDir;

    fn pad32(bz: &[u8]) -> [u8; 32] {
        let mut res = [0; 32];
        let l = bz.len();
        let mut i = 0;
        while i < l {
            res[i] = bz[i];
            i += 1;
        }
        return res;
    }

    fn make_entries(next_key_hashes: &[[u8; 32]; 3]) -> Box<[Entry]> {
        let e0 = Entry {
            key: "Key0Key0Key0Key0Key0Key0Key0Key0Key0".as_bytes(),
            value: "Value0Value0Value0Value0Value0Value0".as_bytes(),
            next_key_hash: next_key_hashes[0].as_slice(),
            version: 0,
            last_version: 0,
            serial_number: 0,
        };
        let e1 = Entry {
            key: "Key1Key ILOVEYOU 1Key1Key1".as_bytes(),
            value: "Value1Value1".as_bytes(),
            next_key_hash: next_key_hashes[1].as_slice(),
            version: 10,
            last_version: 3,
            serial_number: 1,
        };
        let e2 = Entry {
            key: "Key2Key2Key2 ILOVEYOU Key2".as_bytes(),
            value: "Value2 ILOVEYOU Value2".as_bytes(),
            next_key_hash: next_key_hashes[2].as_slice(),
            version: 20,
            last_version: 12,
            serial_number: 2,
        };
        let null_entry = Entry {
            key: &[],
            value: &[],
            next_key_hash: &[0; 32],
            version: -2,
            last_version: -2,
            serial_number: u64::MAX,
        };
        return Box::new([e0, e1, e2, null_entry]);
    }

    fn equal_entry(e: &Entry, s_list: &[u64], entry_bz: &EntryBz) {
        assert_eq!(e.key, entry_bz.key());
        assert_eq!(e.value, entry_bz.value());
        assert_eq!(e.next_key_hash, entry_bz.next_key_hash());
        assert_eq!(e.version, entry_bz.version());
        assert_eq!(e.last_version, entry_bz.last_version());
        assert_eq!(e.serial_number, entry_bz.serial_number());
        assert_eq!(s_list.len(), entry_bz.dsn_count());
        let mut i = 0;
        while i < s_list.len() {
            assert_eq!(s_list[i], entry_bz.get_deactived_sn(i));
            i += 1;
        }
    }

    #[test]
    fn entry_file() {
        let hashes: [[u8; 32]; 3] = [
            pad32("NextKey0".as_bytes()),
            pad32("NextKey1".as_bytes()),
            pad32("NextKey2".as_bytes()),
        ];
        let entries = make_entries(&hashes);
        let _dir = TempDir::new("./entryF");

        let d_snl0 = [1, 2, 3, 4];
        let d_snl1 = [5];
        let d_snl2 = [];
        let d_snl3 = [10, 1];

        let mut ef = EntryFile::new(8 * 1024, 128 * 1024, "./entryF".to_string());
        let total_len = ((crate::def::ENTRY_FIXED_LENGTH
            + &entries[0].key.len()
            + &entries[0].value.len()
            + 7)
            / 8)
            * 8
            + &d_snl0.len() * 8;
        let mut bz0 = vec![0; total_len];
        entry_to_bytes(&entries[0], d_snl0.as_slice(), &mut bz0);
        let mut buffer = vec![];
        let mut pos0 = 0;
        pos0 = ef.append(&EntryBz { bz: &bz0 }, &mut buffer);
        let total_len = ((crate::def::ENTRY_FIXED_LENGTH
            + &entries[1].key.len()
            + &entries[1].value.len()
            + 7)
            / 8)
            * 8
            + &d_snl1.len() * 8;
        let mut bz1 = vec![0; total_len];
        entry_to_bytes(&entries[1], d_snl1.as_slice(), &mut bz1);
        let mut pos1 = 0;
        pos1 = ef.append(&EntryBz { bz: &bz1 }, &mut buffer);
        let total_len = ((crate::def::ENTRY_FIXED_LENGTH
            + &entries[2].key.len()
            + &entries[2].value.len()
            + 7)
            / 8)
            * 8
            + &d_snl2.len() * 8;
        let mut bz2 = vec![0; total_len];
        entry_to_bytes(&entries[2], d_snl2.as_slice(), &mut bz2);
        let mut pos2 = 0;
        pos2 = ef.append(&EntryBz { bz: &bz2 }, &mut buffer);
        let total_len = ((crate::def::ENTRY_FIXED_LENGTH
            + &entries[3].key.len()
            + &entries[3].value.len()
            + 7)
            / 8)
            * 8
            + &d_snl3.len() * 8;
        let mut bz3 = vec![0; total_len];
        entry_to_bytes(&entries[3], d_snl3.as_slice(), &mut bz3);
        let mut pos3: i64 = 0;
        pos3 = ef.append(&EntryBz { bz: &bz3 }, &mut buffer);

        let mut i = 0;
        while i < LEAF_COUNT_IN_TWIG {
            _ = ef.append(&EntryBz { bz: &bz0 }, &mut buffer);
            _ = ef.append(&EntryBz { bz: &bz1 }, &mut buffer);
            _ = ef.append(&EntryBz { bz: &bz2 }, &mut buffer);
            _ = ef.append(&EntryBz { bz: &bz3 }, &mut buffer);
            i += 4;
        }
        ef.hp_file.flush(&mut buffer);
        ef.close();

        let mut ef = EntryFile::new(8 * 1024, 128 * 1024, "./entryF".to_string());
        let mut buf = vec![0; 300];
        let size = ef.read_entry(pos0, &mut buf);
        let entry_bz;
        if size <= buf.len() {
            entry_bz = EntryBz { bz: &buf[..size] };
        } else {
            buf = vec![0; size];
            ef.read_entry(pos0, &mut buf[..]);
            entry_bz = EntryBz { bz: &buf[..size] };
        }
        equal_entry(&entries[0], &d_snl0, &entry_bz);
        assert_eq!(pos1, pos0 + entry_bz.len() as i64);

        let mut buf = vec![0; 300];
        let size = ef.read_entry(pos1, &mut buf);
        let entry_bz;
        if size <= buf.len() {
            entry_bz = EntryBz { bz: &buf[..size] };
        } else {
            buf = vec![0; size];
            ef.read_entry(pos1, &mut buf[..]);
            entry_bz = EntryBz { bz: &buf[..size] };
        }
        equal_entry(&entries[1], &d_snl1, &entry_bz);
        assert_eq!(pos2, pos1 + entry_bz.len() as i64);

        let mut buf = vec![0; 300];
        let size = ef.read_entry(pos2, &mut buf);
        let entry_bz;
        if size <= buf.len() {
            entry_bz = EntryBz { bz: &buf[..size] };
        } else {
            buf = vec![0; size];
            ef.read_entry(pos2, &mut buf[..]);
            entry_bz = EntryBz { bz: &buf[..size] };
        }
        equal_entry(&entries[2], &d_snl2, &entry_bz);
        assert_eq!(pos3, pos2 + entry_bz.len() as i64);

        let mut buf = vec![0; 300];
        let entry_bz;
        let size = ef.read_entry(pos3, &mut buf);
        if size <= buf.len() {
            entry_bz = EntryBz { bz: &buf[..size] };
        } else {
            buf = vec![0; size];
            ef.read_entry(pos3, &mut buf[..]);
            entry_bz = EntryBz { bz: &buf[..size] };
        }
        equal_entry(&entries[3], &d_snl3, &entry_bz);

        ef.close();
    }
}
