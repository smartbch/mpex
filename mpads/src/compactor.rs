use crate::def::DEFAULT_ENTRY_SIZE;
use crate::entry;
use crate::entryfile::EntryFile;
use crate::indexer::BTreeIndexer;
use crate::utils::ringchannel::Producer;
use std::sync::Arc;
use std::thread;
use std::time;

#[derive(Clone)]
pub struct CompactJob {
    // pub in_buf_pos: i64,
    pub old_pos: i64,
    // pub k64: u64,
    pub entry_bz: Vec<u8>,
}

pub struct Compactor {
    shard_id: usize,
    compact_trigger: usize,
    entry_file: Arc<EntryFile>,
    indexer: Arc<BTreeIndexer>,
    // compact_buf_wr: EntryBufferWriter, // to simulate a virtual in-memory EntryFile
    compact_producer: Producer<CompactJob>,
}

impl Compactor {
    pub fn new(
        shard_id: usize,
        compact_trigger: usize,
        entry_file: Arc<EntryFile>,
        indexer: Arc<BTreeIndexer>,
        compact_producer: Producer<CompactJob>,
    ) -> Self {
        Self {
            shard_id,
            compact_trigger,
            entry_file,
            indexer,
            compact_producer,
        }
    }

    pub fn fill_compact_chan(&mut self, file_pos: i64) {
        let mut file_pos = file_pos;
        let mut bz: Vec<u8> = vec![0; DEFAULT_ENTRY_SIZE];
        loop {
            while self.indexer.len(self.shard_id) < self.compact_trigger {
                thread::sleep(time::Duration::from_millis(500));
            }
            let file_size = self.entry_file.size();
            if file_size <= file_pos + 5
            /*entry length size*/
            {
                //println!("no entry here!");
                thread::sleep(time::Duration::from_millis(500));
                continue;
            }
            let size = self.entry_file.read_entry(file_pos, &mut bz[..]);
            if bz.len() < size {
                bz.resize(size, 0);
                self.entry_file.read_entry(file_pos, &mut bz[..]);
            }
            let entry_bz = entry::EntryBz { bz: &bz[..size] };
            if self.indexer.key_exists(entry_bz.k64(), file_pos) {
                let mut job = self.compact_producer.receive_returned();
                job.old_pos = file_pos;
                job.entry_bz.resize(0, 0);
                job.entry_bz.extend_from_slice(entry_bz.bz);
                self.compact_producer.produce(job);
            }
            file_pos = file_pos + entry_bz.len() as i64;
        }
    }
}

#[cfg(test)]
mod compactor_tests {
    use std::fs::create_dir_all;

    use crate::bptaskhub::BlockPairTaskHub;
    use crate::def::{ENTRIES_PATH, TWIG_PATH};
    use crate::entry::{entry_to_bytes, Entry};
    use crate::entryfile::EntryFileWriter;
    use crate::test_helper::{SimpleTask, TempDir};
    use crate::twigfile::TwigFile;
    use crate::updater::Updater;
    use crate::utils::ringchannel;
    use crate::{entrybuffer, indexer::BTreeIndexer};

    use super::*;
    #[test]
    fn test_compact() {
        let dir_name = "test_compactor";
        let _temp_dir = TempDir::new(dir_name);
        let suffix = ".test";
        let indexer = BTreeIndexer::new(2);
        let dir_entry = format!("{}/{}{}", dir_name, ENTRIES_PATH, suffix);
        create_dir_all(&dir_entry).unwrap();
        let entry_file = Arc::new(EntryFile::new(1024, 2048i64, dir_entry));
        let mut entry_file_w = EntryFileWriter::new(entry_file.clone(), 1024);
        let dir_twig = format!("{}/{}{}", dir_name, TWIG_PATH, suffix);
        create_dir_all(&dir_twig).unwrap();
        let _twig_file = TwigFile::new(1024, 2048i64, dir_twig);
        let e0 = Entry {
            key: "Key0Key0Key0Key0Key0Key0Key0Key0Key0".as_bytes(),
            value: "Value0Value0Value0Value0Value0Value0".as_bytes(),
            next_key_hash: [1; 32].as_slice(),
            version: 0,
            last_version: 0,
            serial_number: 0,
        };
        let e1 = Entry {
            key: "Key1Key ILOVEYOU 1Key1Key1".as_bytes(),
            value: "Value1Value1".as_bytes(),
            next_key_hash: [2; 32].as_slice(),
            version: 10,
            last_version: 3,
            serial_number: 1,
        };
        let mut buf = [0; 1024];
        let bz0 = entry_to_bytes(&e0, &[], &mut buf);
        let mut buf = [0; 1024];
        let bz1 = entry_to_bytes(&e1, &[], &mut buf);
        let _pos0 = entry_file_w.append(&bz0);
        let pos1 = entry_file_w.append(&bz1);
        entry_file_w.flush();
        let k64 = bz1.k64();
        indexer.add_kv(k64, pos1);
        let mut exists = false;
        indexer.for_each_value(k64, |offset| -> bool {
            if offset == pos1 {
                exists = true;
            }
            false
        });
        assert_eq!(exists, true);
        let indexer = Arc::new(indexer);
        let entry_file_size = entry_file.size();

        let job = CompactJob {
            old_pos: 0,
            entry_bz: Vec::new(),
        };
        let (producer, consumer) = ringchannel::new(100, &job);

        let mut compactor = Compactor {
            shard_id: 0,
            compact_trigger: 1,
            entry_file: entry_file.clone(),
            indexer: indexer.clone(),
            compact_producer: producer,
        };
        thread::spawn(move || {
            compactor.fill_compact_chan(pos1);
        });
        let (u_eb_wr, _) = entrybuffer::new(entry_file_size, 1024);

        let sn_end = 1;
        let mut updater = Updater::new(
            0,
            Arc::new(BlockPairTaskHub::<SimpleTask>::new()),
            u_eb_wr,
            entry_file.clone(),
            indexer.clone(),
            0,
            0,
            sn_end,
            consumer,
            0,
            1,
            1,
            0,
            0,
        );
        updater.try_compact(None, 0);
        // assert_eq!(updater.sn_start, bz1.serial_number() + 1);
        // assert_eq!(updater.sn_end, sn_end + 1);
        assert_eq!(indexer.len(0), 1);
        let mut new_pos = 0;
        indexer.for_each_value(k64, |offset| -> bool {
            new_pos = offset;
            true
        });
        assert_eq!(new_pos, pos1 + bz1.len() as i64);
    }
}
