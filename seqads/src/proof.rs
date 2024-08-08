use std::vec;

use mpads::{
    bptaskhub::Task,
    changeset::ChangeSet,
    def::{
        DEFAULT_ENTRY_SIZE, LEAF_COUNT_IN_TWIG, OP_CREATE, OP_DELETE, OP_WRITE, SHARD_COUNT,
        TWIG_SHIFT,
    },
    entry::EntryBz,
    multiproof::{encode_witness, get_witness},
    tree::get_shard_idx_and_key,
    twig, ADS,
};

use crate::SeqAdsWrap;

impl<T: Task + ADS + 'static> SeqAdsWrap<T> {
    fn get_compact_sns(&self, shard_id: usize, max_compact_count: u64, sn_end: u64) -> Vec<u64> {
        let entry_flusher = &self.ads.entry_flusher.lock().unwrap();
        let shard = &*entry_flusher.shards[shard_id];
        let tree = &shard.tree;

        let oldest_active_sn = entry_flusher
            .meta
            .read()
            .unwrap()
            .get_oldest_active_sn(shard_id);
        let mut compact_active_sn_list = vec![];
        let mut twig_id = oldest_active_sn >> TWIG_SHIFT;
        loop {
            let (s, k) = get_shard_idx_and_key(twig_id);
            let active_bits = tree.active_bit_shards[s]
                .get(&k)
                .unwrap_or(&twig::NULL_ACTIVE_BITS);
            let sn_start = twig_id << TWIG_SHIFT;
            for offset in 0..LEAF_COUNT_IN_TWIG {
                let active = active_bits.get_bit(offset);
                if active {
                    compact_active_sn_list.push(sn_start + offset as u64);
                }
                if compact_active_sn_list.len() == max_compact_count as usize {
                    twig_id = u64::MAX;
                    break;
                }
            }
            twig_id += 1;
            if twig_id >= (sn_end >> TWIG_SHIFT) + 1 || twig_id == u64::MAX {
                break;
            }
        }
        compact_active_sn_list
    }

    fn get_change_and_compact_count(&self, shard_id: usize, change_set: &ChangeSet) -> (u64, u64) {
        let mut count = (0, 0);
        change_set.run_in_shard(shard_id, |op, key_hash: &[u8; 32], _k, _v, _r| match op {
            OP_CREATE => {
                count.0 += 1;
                count.1 += 2;
            }
            OP_WRITE => {
                count.0 += 1;
                count.1 += 1;
            }
            OP_DELETE => {
                count.0 += 1;
            }
            _ => {}
        });
        count
    }

    pub fn get_proof(
        &self,
        tx_input_key_hash_list: Vec<[u8; 32]>,
        change_set: &ChangeSet,
    ) -> (Vec<Vec<u8>>, Vec<Vec<u64>>) {
        let mut proofs = vec![];
        let mut compact_sns_arr = vec![];

        for shard_id in 0..SHARD_COUNT {
            let _key_hash_list = tx_input_key_hash_list
                .iter()
                .filter(|key_hash| key_hash[0] as usize >> 4 == shard_id)
                .map(|key_hash| key_hash.clone())
                .collect::<Vec<[u8; 32]>>();
            let mut bufs = (0..SHARD_COUNT)
                .map(|_| vec![0; DEFAULT_ENTRY_SIZE])
                .collect::<Vec<Vec<u8>>>();
            for (idx, key_hash) in _key_hash_list.iter().enumerate() {
                let (size, found_it) = self.read_entry(key_hash, &[], bufs.get_mut(idx).unwrap());
                if found_it {
                    bufs[0].truncate(size);
                } else {
                    bufs[0].truncate(0);
                }
            }

            let mut touched_sns = vec![];
            let mut entries = vec![];
            // append old_entry sn
            for buf in &bufs {
                let entry_bz = EntryBz { bz: buf };
                touched_sns.push(entry_bz.serial_number());
                entries.push(entry_bz);
            }

            let entry_flusher = &self.ads.entry_flusher.lock().unwrap();
            let shard = &*entry_flusher.shards[shard_id];

            // append(change_and_compact) entry sn
            let updater = shard.updater.lock().unwrap();
            let sn_end = updater.sn_end;
            let change_and_compact_count = self.get_change_and_compact_count(shard_id, change_set);
            let compact_count = change_and_compact_count.1;
            let append_count = change_and_compact_count.0 + compact_count;
            touched_sns.extend((0..append_count).map(|i| sn_end + i));

            // compact entry sn
            let compact_sns = self.get_compact_sns(shard_id, compact_count, sn_end);
            touched_sns.extend(compact_sns.iter());

            // scanned entry sn when compact
            let only_active_bits_sn_list: Vec<u64> = if compact_sns.is_empty() {
                vec![]
            } else {
                (compact_sns[0]..*compact_sns.last().unwrap())
                    .map(|i| i)
                    .collect::<Vec<u64>>()
            };
            let witness = get_witness(&touched_sns, &only_active_bits_sn_list, &shard.tree);
            compact_sns_arr.push(compact_sns);
            proofs.push(encode_witness(&witness, &entries));
        }
        (proofs, compact_sns_arr)
    }
}
