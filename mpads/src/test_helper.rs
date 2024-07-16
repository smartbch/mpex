use crate::bptaskhub::Task;
use crate::changeset::ChangeSet;
use crate::def::{DEFAULT_FILE_SIZE, ENTRY_FIXED_LENGTH, LEAF_COUNT_IN_TWIG, SMALL_BUFFER_SIZE};
use crate::entry::{self, Entry};
use crate::tree::Tree;
use std::fs::{create_dir, metadata, read_dir, remove_dir_all, File};
use std::path::Path;
use std::sync::Arc;

pub struct TempDir {
    dir: String,
}

impl TempDir {
    pub fn new(dir: &str) -> Self {
        remove_dir_all(dir).unwrap_or(()); // ignore error
        create_dir(dir).unwrap_or(()); // ignore error
        Self {
            dir: dir.to_string(),
        }
    }

    pub fn to_string(&self) -> String {
        self.dir.clone()
    }

    pub fn list(&self) -> Vec<String> {
        TempDir::list_dir(&self.dir)
    }

    pub fn list_dir(dir: &str) -> Vec<String> {
        let mut result = vec![];
        let paths = std::fs::read_dir(Path::new(dir)).unwrap();
        for path in paths {
            result.push(path.unwrap().path().to_str().unwrap().to_string());
        }
        result.sort();
        result
    }

    pub fn create_file(&self, name: &str) {
        let file_path = Path::new(&self.dir).join(Path::new(name));
        File::create_new(file_path).unwrap();
    }

    pub fn list_all(path: &Path) -> Vec<String> {
        let mut vec = Vec::new();
        TempDir::_list_files(&mut vec, path);
        vec.sort();
        vec
    }

    fn _list_files(vec: &mut Vec<String>, path: &Path) {
        if metadata(&path).unwrap().is_dir() {
            let paths = read_dir(&path).unwrap();
            for path_result in paths {
                let full_path = path_result.unwrap().path();
                if metadata(&full_path).unwrap().is_dir() {
                    TempDir::_list_files(vec, &full_path);
                } else {
                    vec.push(String::from(full_path.to_str().unwrap()));
                }
            }
        }
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        remove_dir_all(&self.dir).unwrap_or(()); // ignore error
    }
}

pub fn pad32(bz: &[u8]) -> [u8; 32] {
    let mut res = [0; 32];
    res[..bz.len()].copy_from_slice(bz);
    res
}

pub fn build_test_tree(
    dir_name: &str,
    deact_sn_list: &Vec<u64>,
    count_before: i32,
    count_after: i32,
) -> (Tree, Vec<i64>, u64) {
    let mut tree = Tree::new(
        0,
        SMALL_BUFFER_SIZE as usize,
        DEFAULT_FILE_SIZE,
        dir_name.to_string(),
        "".to_string(),
    );
    let mut entry = entry::Entry {
        key: &b"key".as_slice(),
        value: &b"value".as_slice(),
        next_key_hash: &pad32(b"nextkey".as_slice()),
        version: 100,
        last_version: 99,
        serial_number: 0,
    };

    let total_len0 = ((ENTRY_FIXED_LENGTH + &entry.key.len() + &entry.value.len() + 7) / 8) * 8;
    let mut bz = vec![0u8; total_len0];
    let entry_bz = entry::entry_to_bytes(&entry, &[], &mut bz);

    let mut pos_list = Vec::with_capacity((LEAF_COUNT_IN_TWIG + 10) as usize);
    pos_list.push(tree.append_entry(&entry_bz));

    for i in 1..count_before {
        entry.serial_number = i as u64;
        let entry_bz = entry::entry_to_bytes(&entry, &[], &mut bz);
        pos_list.push(tree.append_entry(&entry_bz));
    }
    for sn in deact_sn_list {
        tree.deactive_entry(*sn);
    }

    let total_len1 = total_len0 + &deact_sn_list.len() * 8;
    let mut bz1 = vec![0u8; total_len1];
    entry.serial_number = count_before as u64;
    let entry_bz = entry::entry_to_bytes(&entry, deact_sn_list.as_slice(), &mut bz1);
    pos_list.push(tree.append_entry(&entry_bz));

    for _ in 0..count_after - 1 {
        entry.serial_number += 1;
        let entry_bz = entry::entry_to_bytes(&entry, &[], &mut bz);
        pos_list.push(tree.append_entry(&entry_bz));
    }

    (tree, pos_list, entry.serial_number)
}

pub struct EntryBuilder {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub next_key_hash: [u8; 32],
    pub version: i64,
    pub last_version: i64,
    pub serial_number: u64,
}

impl EntryBuilder {
    pub fn kv<T: AsRef<[u8]>>(k: T, v: T) -> Self {
        EntryBuilder {
            key: Vec::from(k.as_ref()),
            value: Vec::from(v.as_ref()),
            next_key_hash: [0; 32],
            version: 0,
            last_version: 0,
            serial_number: 0,
        }
    }

    pub fn ver(&mut self, v: i64) -> &Self {
        self.version = v;
        self
    }
    pub fn last_ver(&mut self, v: i64) -> &Self {
        self.last_version = v;
        self
    }
    pub fn sn(&mut self, v: u64) -> &Self {
        self.serial_number = v;
        self
    }
    pub fn next_kh(&mut self, kh: [u8; 32]) -> &Self {
        self.next_key_hash = kh;
        self
    }

    pub fn build(&self) -> Entry {
        Entry {
            key: &self.key[..],
            value: &self.value[..],
            next_key_hash: &self.next_key_hash[..],
            version: self.version,
            last_version: self.last_version,
            serial_number: self.serial_number,
        }
    }

    pub fn build_and_dump(&self, dsn_list: &[u64]) -> Vec<u8> {
        let entry = self.build();
        let size = entry.get_serialized_len(dsn_list.len());
        let mut bz = Vec::with_capacity(size);
        bz.resize(size, 0);
        entry.dump(&mut bz, dsn_list);
        bz
    }
}

pub struct SimpleTask {
    pub change_sets: Arc<Vec<ChangeSet>>,
}

impl Task for SimpleTask {
    fn get_change_sets(&self) -> Arc<Vec<ChangeSet>> {
        return self.change_sets.clone();
    }
}

impl SimpleTask {
    pub fn new(changesets: Vec<ChangeSet>) -> Self {
        Self {
            change_sets: Arc::new(changesets),
        }
    }
}
