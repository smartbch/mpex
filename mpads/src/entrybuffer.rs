use crate::def::{BIG_BUF_SIZE, DEFAULT_ENTRY_SIZE};
use crate::entry::{Entry, EntryBz};
use crate::utils::{new_big_buf_boxed, BigBuf};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, RwLock};

const BIG_BUF_SIZE_I64: i64 = BIG_BUF_SIZE as i64;

// It contains content of the new range [Start, End] of EntryFile
// This content can be read from DRAM before it is flushed to EntryFile,
// One updater appends entries to it, and then one flusher pops entries out from it.

pub struct EntryBufferWriter {
    pub entry_buffer: Arc<EntryBuffer>,
    curr_buf: Option<Box<BigBuf>>,
}

impl EntryBufferWriter {
    pub fn append(&mut self, entry: &Entry, deactived_serial_num_list: &[u64]) -> i64 {
        let (pos, curr_buf) = self.entry_buffer.append(
            entry,
            deactived_serial_num_list,
            self.curr_buf.take().unwrap(),
        );
        self.curr_buf = Some(curr_buf);
        pos
    }

    pub fn end_block(&mut self, compact_done_pos: i64, compact_done_sn: u64, sn_end: u64) {
        let curr_buf: &Box<BigBuf> = self.curr_buf.as_ref().unwrap();
        let curr_buf_clone = curr_buf.clone();
        self.entry_buffer.end_block(
            compact_done_pos,
            compact_done_sn,
            sn_end,
            self.curr_buf.take().unwrap(),
        );
        self.curr_buf = Some(curr_buf_clone);
    }

    pub fn get_entry_bz_at<F>(&mut self, file_pos: i64, access: F) -> (bool, bool)
    where
        F: FnMut(EntryBz),
    {
        let (in_disk, have_accessed);
        let curr_buf = self.curr_buf.take().unwrap();
        (in_disk, have_accessed, self.curr_buf) =
            self.entry_buffer
                .get_entry_bz_at(file_pos, Some(curr_buf), access);
        (in_disk, have_accessed)
    }
}

pub struct EntryBuffer {
    start: AtomicI64,
    end: AtomicI64,
    buf_map: RwLock<HashMap<i64, Arc<BigBuf>>>,
    free_list: RwLock<Vec<Arc<BigBuf>>>,
    pos_sender: SyncSender<i64>,
}

pub fn new(start: i64, buf_margin: usize) -> (EntryBufferWriter, EntryBufferReader) {
    let (pos_sender, pos_receiver) = sync_channel(2048);

    let entry_buffer = EntryBuffer {
        start: AtomicI64::new(start),
        end: AtomicI64::new(start),
        buf_map: RwLock::new(HashMap::<i64, Arc<BigBuf>>::new()),
        free_list: RwLock::new(Vec::<Arc<BigBuf>>::new()),
        pos_sender,
    };

    let arc_entry_buffer = Arc::new(entry_buffer);

    let entry_buffer_reader = EntryBufferReader {
        entry_buffer: arc_entry_buffer.clone(),
        curr_buf: None,
        scratch_pad: Vec::with_capacity(DEFAULT_ENTRY_SIZE),
        end_pos: start,
        curr_pos: start,
        buf_margin,
        pos_receiver,
    };

    let entry_buffer_writer = EntryBufferWriter {
        entry_buffer: arc_entry_buffer.clone(),
        curr_buf: Some(new_big_buf_boxed()),
    };

    (entry_buffer_writer, entry_buffer_reader)
}

impl EntryBuffer {
    fn end_block(
        &self,
        compact_done_pos: i64,
        compact_done_sn: u64,
        sn_end: u64,
        curr_buf: Box<BigBuf>,
    ) {
        let end = self.end.load(Ordering::SeqCst);
        let arc = Arc::from(curr_buf);
        {
            let mut buf_map = self.buf_map.write().unwrap();
            buf_map.insert(end / BIG_BUF_SIZE_I64, arc);
        }
        self.pos_sender.send(end).unwrap();
        self.pos_sender.send(i64::MIN).unwrap();
        self.pos_sender.send(compact_done_pos).unwrap();
        self.pos_sender.send(compact_done_sn as i64).unwrap();
        self.pos_sender.send(sn_end as i64).unwrap();
    }

    fn allocate_big_buf(&self) -> Box<BigBuf> {
        let mut free_list = self.free_list.write().unwrap();
        let mut idx = usize::MAX;
        for (i, arc) in free_list.iter().enumerate() {
            if Arc::strong_count(arc) == 1 && Arc::weak_count(arc) == 0 {
                idx = i;
                break;
            }
        }
        if idx != usize::MAX {
            let arc = free_list.swap_remove(idx);
            return Box::new(Arc::into_inner(arc).unwrap());
        }
        new_big_buf_boxed()
    }

    fn append(
        &self,
        entry: &Entry,
        deactived_serial_num_list: &[u64],
        mut curr_buf: Box<BigBuf>,
    ) -> (i64, Box<BigBuf>) {
        let size = entry.get_serialized_len(deactived_serial_num_list.len());
        if size > BIG_BUF_SIZE {
            panic!("Entry too large {} vs {}", size, BIG_BUF_SIZE);
        }
        let file_pos = self.end.load(Ordering::SeqCst);
        let idx = file_pos / BIG_BUF_SIZE_I64;
        let offset = file_pos % BIG_BUF_SIZE_I64;
        if offset + (size as i64) < BIG_BUF_SIZE_I64 {
            // curr_buf is large enough
            entry.dump(&mut curr_buf[offset as usize..], deactived_serial_num_list);
            self.end.fetch_add(size as i64, Ordering::SeqCst);
            return (file_pos, curr_buf);
        }
        let mut new_buf = self.allocate_big_buf();
        let total_length = entry.dump(&mut new_buf[..], deactived_serial_num_list);
        let copied_length = BIG_BUF_SIZE - offset as usize;
        curr_buf[offset as usize..].copy_from_slice(&new_buf[..copied_length]);
        new_buf.copy_within(copied_length..total_length, 0);
        let arc = Arc::<BigBuf>::from(curr_buf);
        self.buf_map.write().unwrap().insert(idx, arc);
        self.end.fetch_add(size as i64, Ordering::SeqCst);
        self.pos_sender.send(file_pos).unwrap();
        (file_pos, new_buf)
    }

    fn get_and_remove_buf(&self, idx: i64, remove_idx: i64) -> Arc<BigBuf> {
        let mut buf_map = self.buf_map.write().unwrap();
        let res = buf_map.get(&idx).unwrap().clone();
        if remove_idx > -1 {
            let new_start = (remove_idx + 1) * BIG_BUF_SIZE as i64;
            let old_start = self.start.load(Ordering::SeqCst);
            if old_start < new_start {
                self.start.store(new_start, Ordering::SeqCst);
            }
            if let Some(buf) = buf_map.remove(&remove_idx) {
                let mut free_list = self.free_list.write().unwrap();
                free_list.push(buf);
            }
        }
        res
    }

    pub fn get_entry_bz<F>(&self, file_pos: i64, access: F) -> (bool, bool)
    where
        F: FnMut(EntryBz),
    {
        let (in_disk, accessed, _) = self.get_entry_bz_at(file_pos, Option::None, access);
        (in_disk, accessed)
    }

    fn get_entry_bz_at<F>(
        &self,
        file_pos: i64,
        curr_buf: Option<Box<BigBuf>>,
        mut access: F,
    ) -> (bool, bool, Option<Box<BigBuf>>)
    where
        F: FnMut(EntryBz),
    {
        let (idx, offset) = (file_pos / BIG_BUF_SIZE_I64, file_pos % BIG_BUF_SIZE_I64);
        let start = self.start.load(Ordering::SeqCst);
        let end = self.end.load(Ordering::SeqCst);
        if file_pos < start {
            return (true /*inDisk*/, false /*HaveAccessed*/, curr_buf);
        }
        if file_pos >= end {
            panic!("Read past end");
        }
        let curr_buf_start = (end / BIG_BUF_SIZE_I64) * BIG_BUF_SIZE_I64; //round-down
        if file_pos >= curr_buf_start && curr_buf.is_none() {
            return (false /*inDisk*/, false /*HaveAccessed*/, None);
        }
        if file_pos >= curr_buf_start && curr_buf.is_some() {
            // reading currBuf is enough
            let curr_buf = curr_buf.unwrap();
            let len_bytes = &curr_buf[offset as usize..offset as usize + 5];
            let size = EntryBz::get_entry_len(len_bytes) as i64;
            let entry_bz = EntryBz {
                bz: &curr_buf[offset as usize..(offset + size) as usize],
            };
            access(entry_bz);
            return (
                false, /*inDisk*/
                true,  /*HaveAccessed*/
                Some(curr_buf),
            );
        }
        let buf_map = self.buf_map.read().unwrap();
        let target = buf_map.get(&idx);
        if target.is_none() {
            // check if start has just been changed
            let start = self.start.load(Ordering::SeqCst);
            if file_pos < start {
                return (true /*inDisk*/, false /*HaveAccessed*/, curr_buf);
            }
        }
        let target = target.unwrap();
        let len_bytes = &target[offset as usize..offset as usize + 5];
        let size = EntryBz::get_entry_len(len_bytes) as i64;
        if offset + size <= BIG_BUF_SIZE as i64 {
            // only need to read target
            let entry_bz = EntryBz {
                bz: &target[offset as usize..(offset + size) as usize],
            };
            access(entry_bz);
            return (false /*inDisk*/, true /*HaveAccessed*/, curr_buf);
        }
        if file_pos + size > curr_buf_start as i64 && curr_buf.is_none() {
            // need to read curr_buf but curr_buf is not provided
            return (false /*inDisk*/, false /*HaveAccessed*/, curr_buf);
        }
        let mut bz = Vec::with_capacity(size as usize);
        let copied_length = BIG_BUF_SIZE - offset as usize;
        bz.extend_from_slice(&target[offset as usize..]);
        if file_pos + size > curr_buf_start as i64 && curr_buf.is_some() {
            let curr_buf = curr_buf.as_ref().unwrap();
            bz.extend_from_slice(&curr_buf[..(size as usize - copied_length)]);
        } else {
            let next_buf = buf_map.get(&(idx + 1)).unwrap();
            bz.extend_from_slice(&next_buf[..(size as usize - copied_length)]);
        }
        let entry_bz = EntryBz { bz: &bz };
        access(entry_bz);
        return (false /*inDisk*/, true /*HaveAccessed*/, curr_buf);
    }
}

pub struct EntryBufferReader {
    entry_buffer: Arc<EntryBuffer>,
    curr_buf: Option<Arc<BigBuf>>,
    scratch_pad: Vec<u8>,
    end_pos: i64,
    curr_pos: i64,
    buf_margin: usize,
    pos_receiver: Receiver<i64>,
}

impl EntryBufferReader {
    pub fn read_next_entry<F>(&mut self, mut access: F) -> (/*endOfBlock*/ bool, /*file_pos*/ i64)
    where
        F: FnMut(EntryBz),
    {
        let file_pos = self.curr_pos;
        // At the beginning/ending of blocks, 'recv' may return duplicated pos, so we
        // need a while-loop to make sure 'curr_pos' increases
        while self.curr_pos >= self.end_pos {
            let pos = self.pos_receiver.recv().unwrap();
            if pos == i64::MIN {
                self.curr_buf = None; // clear the partial buffer
                return (true, file_pos);
            }
            self.end_pos = pos;
            if self.curr_buf.is_none() {
                // re-fetch currBuf because it was cleared
                let arc = self
                    .entry_buffer
                    .get_and_remove_buf(self.curr_pos / BIG_BUF_SIZE_I64, -1);
                self.curr_buf = Some(arc);
            }
        }
        let (idx, offset) = (
            self.curr_pos / BIG_BUF_SIZE_I64,
            self.curr_pos % BIG_BUF_SIZE_I64,
        );
        let curr_buf = self.curr_buf.as_ref().unwrap();
        let size = EntryBz::get_entry_len(&curr_buf[offset as usize..]) as i64;
        self.curr_pos += size;
        if offset + size < BIG_BUF_SIZE_I64 {
            let entry_bz = EntryBz {
                bz: &curr_buf[offset as usize..(offset + size) as usize],
            };
            access(entry_bz);
            return (false, file_pos);
        }
        let remove_idx = idx - (self.buf_margin / BIG_BUF_SIZE) as i64 - 1;
        let next_buf = self.entry_buffer.get_and_remove_buf(idx + 1, remove_idx);
        self.scratch_pad.clear();
        let copied_length = BIG_BUF_SIZE - offset as usize;
        self.scratch_pad
            .extend_from_slice(&curr_buf[offset as usize..]);
        self.scratch_pad
            .extend_from_slice(&next_buf[..(size as usize - copied_length)]);
        self.curr_buf = Some(next_buf);
        let entry_bz = EntryBz {
            bz: &self.scratch_pad[..],
        };
        access(entry_bz);
        (false, file_pos)
    }

    pub fn read_extra_info(&self) -> (i64, u64, u64) {
        let compact_done_pos = self.pos_receiver.recv().unwrap();
        let compact_done_sn = self.pos_receiver.recv().unwrap() as u64;
        let sn_end = self.pos_receiver.recv().unwrap() as u64;
        (compact_done_pos, compact_done_sn, sn_end)
    }
}

#[cfg(test)]
mod test_entry_buffer {
    use std::sync::atomic::Ordering;

    use crate::{
        def::BIG_BUF_SIZE,
        entry::{entry_to_bytes, Entry},
        entrybuffer::{new, BIG_BUF_SIZE_I64},
    };

    #[test]
    fn test_append_panic() {
        let (mut writer, reader) = new(0, 3 * BIG_BUF_SIZE);
        assert!(std::panic::catch_unwind(move || {
            writer.append(
                &Entry {
                    key: "key".as_bytes(),
                    value: vec!["value"; 20000].concat().as_bytes(),
                    next_key_hash: &[0xab; 32],
                    version: 12345,
                    last_version: 11111,
                    serial_number: 99999,
                },
                &[],
            );
        })
        .is_err());
    }

    #[test]
    fn test_append() {
        let (mut writer, reader) = new(0, 3 * BIG_BUF_SIZE);
        let value = vec!["value"; 8000].concat();
        let entry = Entry {
            key: "key".as_bytes(),
            value: value.as_bytes(),
            next_key_hash: &[0xab; 32],
            version: 12345,
            last_version: 11111,
            serial_number: 99999,
        };
        let deactived_sn_list = [1];

        let file_pos = writer.append(&entry, &deactived_sn_list);
        assert_eq!(0, file_pos);
        let total_size = entry.get_serialized_len(deactived_sn_list.len());
        assert_eq!(
            total_size,
            writer.entry_buffer.end.load(Ordering::SeqCst) as usize
        );

        let file_pos = writer.append(&entry, &deactived_sn_list);
        assert_eq!(total_size, file_pos as usize);
        assert_eq!(
            total_size * 2,
            writer.entry_buffer.end.load(Ordering::SeqCst) as usize
        );
        assert_eq!(reader.pos_receiver.recv().unwrap(), file_pos);
        let buf_map = writer.entry_buffer.buf_map.read().unwrap();
        assert_eq!(buf_map.len(), 1);
    }

    #[test]
    fn test_get_entry_bz_at() {
        let (mut writer, reader) = new(0, 3 * BIG_BUF_SIZE);

        // ---  if file_pos < self.start.load(Ordering::SeqCst) {
        let (in_disk, have_accessed) = writer.get_entry_bz_at(-1, |_| {});
        assert!(in_disk);
        assert!(!have_accessed);

        let value = vec!["value"; 10000].concat();
        let entry = Entry {
            key: "key".as_bytes(),
            value: value.as_bytes(),
            next_key_hash: &[0xab; 32],
            version: 12345,
            last_version: 11111,
            serial_number: 99999,
        };
        let deactived_sn_list = [1];
        let total_size: usize = entry.get_serialized_len(deactived_sn_list.len());
        let mut bz = vec![0u8; total_size];
        let entry_bytes = entry_to_bytes(&entry, &deactived_sn_list, &mut bz).bz;

        writer.append(&entry, &deactived_sn_list);

        // ---file_pos >= curr_buf_start && curr_buf.is_some()
        let (in_disk, have_accessed) = writer.get_entry_bz_at(0, |entry_bz| {
            assert_eq!(entry_bytes, entry_bz.bz);
        });
        assert!(!in_disk);
        assert!(have_accessed);

        writer.append(&entry, &deactived_sn_list);
        //----  if offset + size <= BIG_BUF_SIZE as i64
        let (in_disk, have_accessed) = writer.get_entry_bz_at(0, |entry_bz| {
            assert_eq!(entry_bytes, entry_bz.bz);
        });
        assert!(!in_disk);
        assert!(have_accessed);

        // --- last casew3
        writer.append(&entry, &deactived_sn_list);
        let (in_disk, have_accessed) = writer.get_entry_bz_at(total_size as i64, |entry_bz| {
            assert_eq!(entry_bytes, entry_bz.bz);
        });
        assert!(!in_disk);
        assert!(have_accessed);
    }

    #[test]
    fn test_end_block_and_read_extra_info() {
        let (mut writer, reader) = new(0, 3 * BIG_BUF_SIZE);
        let value = vec!["value"; 10000].concat();
        let entry = Entry {
            key: "key".as_bytes(),
            value: value.as_bytes(),
            next_key_hash: &[0xab; 32],
            version: 12345,
            last_version: 11111,
            serial_number: 99999,
        };
        let deactived_sn_list = [1];

        writer.append(&entry, &deactived_sn_list);
        writer.end_block(1, 2, 3);

        assert_eq!(writer.entry_buffer.buf_map.read().unwrap().len(), 1);

        assert_eq!(
            reader.pos_receiver.recv().unwrap(),
            (entry.get_serialized_len(deactived_sn_list.len()) as i64)
        );
        assert_eq!(reader.pos_receiver.recv().unwrap(), i64::MIN);

        let (compact_done_pos, compact_done_sn, sn_end) = reader.read_extra_info();
        assert_eq!(compact_done_pos, 1);
        assert_eq!(compact_done_sn, 2);
        assert_eq!(sn_end, 3);
    }

    #[test]
    fn test_read_next_entry() {
        let (mut writer, mut reader) = new(0, 1 * BIG_BUF_SIZE);
        let value = vec!["value"; 10000].concat();
        let entry = Entry {
            key: "key".as_bytes(),
            value: value.as_bytes(),
            next_key_hash: &[0xab; 32],
            version: 12345,
            last_version: 11111,
            serial_number: 99999,
        };
        let deactived_sn_list = [1];
        let total_size: usize = entry.get_serialized_len(deactived_sn_list.len());
        let mut bz = vec![0u8; total_size];
        let entry_bytes = entry_to_bytes(&entry, &deactived_sn_list, &mut bz).bz;

        writer.append(&entry, &deactived_sn_list);
        writer.end_block(1, 2, 3);

        // ----  offset + size <= BIG_BUF_SIZE_I64
        let (end_of_block, file_pos) = reader.read_next_entry(|entry_bz| {
            assert_eq!(entry_bytes, entry_bz.bz);
        });
        assert_eq!(end_of_block, false);
        assert_eq!(file_pos, 0);
        assert_eq!(reader.entry_buffer.buf_map.read().unwrap().len(), 1);

        // --- if pos == i64::MIN
        let (end_of_block, file_pos) = reader.read_next_entry(|_| {});
        assert_eq!(end_of_block, true);
        assert_eq!(file_pos, total_size as i64);
        assert_eq!(reader.entry_buffer.buf_map.read().unwrap().len(), 1);

        reader.read_extra_info();

        // ---- last case
        writer.append(&entry, &deactived_sn_list);
        writer.append(&entry, &deactived_sn_list);
        writer.append(&entry, &deactived_sn_list);
        writer.end_block(1, 2, 3);

        let (end_of_block, file_pos) = reader.read_next_entry(|entry_bz| {
            assert_eq!(entry_bytes, entry_bz.bz);
        });
        assert_eq!(end_of_block, false);
        assert_eq!(file_pos, total_size as i64);
        assert_eq!(reader.entry_buffer.buf_map.read().unwrap().len(), 4);

        let (end_of_block, file_pos) = reader.read_next_entry(|entry_bz| {
            assert_eq!(entry_bytes, entry_bz.bz);
        });
        assert_eq!(end_of_block, false);
        assert_eq!(file_pos, (total_size * 2) as i64);
        assert_eq!(reader.entry_buffer.buf_map.read().unwrap().len(), 4);

        let (end_of_block, file_pos) = reader.read_next_entry(|entry_bz| {
            assert_eq!(entry_bytes, entry_bz.bz);
        });

        // remove buf_map[0]
        assert_eq!(end_of_block, false);
        assert_eq!(file_pos, (total_size * 3) as i64);
        assert_eq!(reader.entry_buffer.buf_map.read().unwrap().len(), 3);
    }

    #[test]
    fn test_threshold_size() {
        let start = BIG_BUF_SIZE_I64 - 50072; // total_size
        let (mut writer, mut reader) = new(start, 1 * BIG_BUF_SIZE);
        let value = vec!["value"; 10000].concat();
        let entry = Entry {
            key: "key".as_bytes(),
            value: value.as_bytes(),
            next_key_hash: &[0xab; 32],
            version: 12345,
            last_version: 11111,
            serial_number: 99999,
        };
        let deactived_sn_list = [1];
        let total_size: usize = entry.get_serialized_len(deactived_sn_list.len());
        let mut bz = vec![0u8; total_size];
        let entry_bytes = entry_to_bytes(&entry, &deactived_sn_list, &mut bz).bz;

        writer.append(&entry, &deactived_sn_list);
        writer.append(&entry, &deactived_sn_list);

        for i in 0..2 {
            writer.get_entry_bz_at(start as i64 + i as i64 * total_size as i64, |entry_bz| {
                assert_eq!(entry_bytes, entry_bz.bz);
            });
        }

        writer.end_block(1, 2, 3);

        for _ in 0..2 {
            reader.read_next_entry(|entry_bz| {
                assert_eq!(entry_bytes, entry_bz.bz);
            });
        }
    }
}
