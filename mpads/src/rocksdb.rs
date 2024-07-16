use core::panic;
use rocksdb::{WriteBatch, WriteBatchWithTransaction, WriteOptions, DB};
use std::path::Path;

pub struct RocksDB {
    db: DB,
}

pub struct RocksBatch {
    batch: WriteBatchWithTransaction<false>,
}

impl RocksDB {
    pub fn new(name: &str, dir: &str) -> Self {
        let path = Path::new(dir).join(name.to_owned() + ".db");
        RocksDB {
            db: DB::open_default(path).unwrap(),
        }
    }

    pub fn close(&self) {
        // self.db.close();
    }

    pub fn set<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, val: V) {
        if key.as_ref().len() == 0 {
            panic!("Empty Key")
        }
        self.db.put(key, val).unwrap()
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<Vec<u8>> {
        if key.as_ref().len() == 0 {
            panic!("Empty Key")
        }
        self.db.get(key).unwrap()
    }

    pub fn has<K: AsRef<[u8]>>(&self, key: K) -> bool {
        self.get(key).is_some()
    }

    pub fn batch_write(&mut self, batch: RocksBatch) {
        self.db.write(batch.batch).unwrap();
    }

    pub fn batch_write_sync(&mut self, batch: RocksBatch) {
        let mut write_options = WriteOptions::default();
        write_options.set_sync(true);
        self.db.write_opt(batch.batch, &write_options).unwrap();
    }
}

impl RocksBatch {
    pub fn new() -> Self {
        return Self {
            batch: WriteBatch::default(),
        };
    }

    pub fn set<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, val: V) {
        if key.as_ref().is_empty() {
            panic!("Empty Key");
        }
        self.batch.put(key, val);
    }

    pub fn delete<K: AsRef<[u8]>>(&mut self, key: K) {
        if key.as_ref().is_empty() {
            panic!("Empty Key");
        }
        self.batch.delete(key);
    }
}

#[cfg(test)]
mod tests {
    use crate::test_helper::TempDir;

    use super::*;
    use serial_test::serial;
    use std::sync::Arc;
    use std::thread;
    use std::thread::JoinHandle;

    fn create_db() -> (RocksDB, TempDir) {
        let dir = TempDir::new("./testdb.db");
        let db = RocksDB::new("testdb", ".");
        (db, dir)
    }

    #[test]
    #[serial]
    fn test_get_set_has() {
        let (db, _dir) = create_db();
        let key = "hello";
        let val = "world";

        db.set(key, val);
        assert_eq!(val.to_owned().into_bytes(), db.get(key).unwrap());
        assert_eq!(true, db.get(val).is_none());
        assert_eq!(true, db.has(key));
        assert_eq!(false, db.has("nihao"));
    }

    #[test]
    #[serial]
    #[should_panic(expected = "Empty Key")]
    fn test_set_empty_key() {
        let (db, _dir) = create_db();
        db.set("", "val");
    }

    #[test]
    #[serial]
    #[should_panic(expected = "Empty Key")]
    fn test_get_empty_key() {
        let (db, _dir) = create_db();
        db.get("");
    }

    #[test]
    #[serial]
    fn test_batch() {
        let (mut db, _dir) = create_db();
        db.set("key1", "val1");
        db.set("key2", "val2");
        db.set("key3", "val3");

        let mut batch = RocksBatch::new();
        batch.set("key4", "val4");
        batch.set("key5", "val5");
        batch.delete("key2");
        db.batch_write(batch);

        assert_eq!("val1".to_owned().into_bytes(), db.get("key1").unwrap());
        assert_eq!("val3".to_owned().into_bytes(), db.get("key3").unwrap());
        assert_eq!("val4".to_owned().into_bytes(), db.get("key4").unwrap());
        assert_eq!("val5".to_owned().into_bytes(), db.get("key5").unwrap());
        assert_eq!(false, db.has("key2"));
    }

    #[test]
    #[serial]
    #[should_panic(expected = "Empty Key")]
    fn test_batch_set_empty_key() {
        let mut batch = RocksBatch::new();
        batch.set("", "val");
    }

    #[test]
    #[serial]
    #[should_panic(expected = "Empty Key")]
    fn test_batch_delete_empty_key() {
        let mut batch = RocksBatch::new();
        batch.delete("");
    }

    #[test]
    #[serial]
    fn test_set_multithread() {
        let (db, _dir) = create_db();
        let rocksdb: Arc<DB> = Arc::new(db.db);

        let mut handles: Vec<JoinHandle<()>> = vec![];
        for i in 0..20 {
            let _rocksdb = rocksdb.clone();
            let key = [i, i, i];
            let val = [i, i, i, i, i];
            let handle = thread::spawn(move || {
                _rocksdb.put(key, val).unwrap();
                println!("{:?}={:?}", key, val);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        for i in 0..20 {
            assert_eq!(
                vec![i, i, i, i, i],
                rocksdb.get(vec![i, i, i]).unwrap().unwrap()
            );
        }
    }
}
