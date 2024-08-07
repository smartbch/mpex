use byteorder::{BigEndian, ByteOrder};
use mpads::{
    def::{DEFAULT_ENTRY_SIZE, SHARD_COUNT},
    entry::EntryBz,
    multiproof::{get_witness, MultiProofNode},
};

use crate::SeqAds;

impl SeqAds {
    pub fn get_witness_arr(
        &self,
        sns_arr: &[Vec<u64>; SHARD_COUNT],
    ) -> [Vec<MultiProofNode>; SHARD_COUNT] {
        let mut witness_arr = Vec::with_capacity(SHARD_COUNT);
        for i in 0..SHARD_COUNT {
            let sns = &sns_arr[i];
            let entry_flusher = &self.entry_flusher.lock().unwrap();
            let shard = &*entry_flusher.shards[i];
            let witness = get_witness(sns, &shard.tree);
            witness_arr.push(witness);
        }
        witness_arr.try_into().unwrap()
    }

    pub fn read_entry(
        &self,
        key_hash: &[u8],
        key: &[u8],
        // cache: Option<&EntryCache>,
        buf: &mut [u8],
    ) -> (usize, bool) {
        let k64 = BigEndian::read_u64(&key_hash[0..8]);
        let shard_id = (key_hash[0] as usize) >> 4;
        let mut size = 0;
        let mut found_it = false;
        self.indexer.for_each_value(k64, |file_pos| -> bool {
            let mut buf_too_small = false;
            // if cache.is_some() {
            //     cache.unwrap().lookup(shard_id, file_pos, |entry_bz| {
            //         found_it = mpads::AdsCore::check_entry(key_hash, key, &entry_bz);
            //         if found_it {
            //             size = entry_bz.len();
            //             if buf.len() < size {
            //                 buf_too_small = true;
            //             } else {
            //                 buf[..size].copy_from_slice(entry_bz.bz);
            //             }
            //         }
            //     });
            // }
            if found_it || buf_too_small {
                return true; //stop loop if key matches or buf is too small
            }
            size = self.entry_files[shard_id].read_entry(file_pos, buf);
            if buf.len() < size {
                return true; //stop loop if buf is too small
            }
            let entry_bz = EntryBz { bz: &buf[..size] };
            found_it = mpads::AdsCore::check_entry(key_hash, key, &entry_bz);
            // if found_it && cache.is_some() {
            //     cache.unwrap().insert(shard_id, file_pos, &entry_bz);
            // }
            found_it // stop loop if key matches
        });
        (size, found_it)
    }

    pub fn read_code(&self, code_hash: &[u8], buf: &mut Vec<u8>) -> usize {
        if buf.len() < DEFAULT_ENTRY_SIZE {
            panic!("buf.len() less than DEFAULT_ENTRY_SIZE");
        }
        let mut size = 0;
        self.code_indexer
            .for_each_value(code_hash, |file_pos| -> bool {
                size = self.code_file.read_entry(file_pos, &mut buf[..]);
                if buf.len() < size {
                    buf.resize(size, 0);
                    self.code_file.read_entry(file_pos, &mut buf[..]);
                }
                let entry_bz = EntryBz { bz: &buf[..size] };
                let match_code_hash = entry_bz.next_key_hash() == code_hash;
                if !match_code_hash {
                    size = 0;
                }
                match_code_hash // stop loop if code_hash matches
            });
        size
    }
}
