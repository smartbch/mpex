use crate::def::TWIG_SHIFT;
use crate::entry::EntryBz;
use crate::tree::{calc_max_level, Tree};
use crate::twig::{NULL_ACTIVE_BITS, NULL_MT_FOR_TWIG, NULL_NODE_IN_HIGHER_TREE, NULL_TWIG};
use crate::utils::hasher;
use std::collections::{HashMap, HashSet};
use std::vec;

pub type Witness = Vec<MultiProofNode>;

#[derive(Debug, Clone, PartialEq)]
pub struct MultiProofNode {
    old_value: [u8; 32],
    new_value: [u8; 32],
    level: u8,
    nth: u64,
}

impl MultiProofNode {
    pub fn new(level: u8, nth: u64) -> Self {
        MultiProofNode {
            old_value: [0; 32],
            new_value: [0; 32],
            level,
            nth,
        }
    }

    pub fn is_leaf(&self) -> bool {
        self.is_leaf_entry() || self.is_leaf_activebits()
    }

    pub fn is_leaf_entry(&self) -> bool {
        if self.level != 0 {
            return false;
        }
        if self.nth % 4096 >= 2048 {
            panic!("nth is not at the left tree of a twig");
        }
        true
    }

    pub fn is_leaf_activebits(&self) -> bool {
        self.level == 8 && self.nth % 16 >= 8
    }

    pub fn get_entry_sn(&self, bit_idx: u8) -> u64 {
        if !self.is_leaf_activebits() {
            panic!("not a leaf activebits");
        }
        let twig_id = self.nth / 16;
        twig_id * 2048 + ((self.nth % 16) - 8) * 256 + bit_idx as u64
    }
}

fn sn_to_leaf(sn: u64) -> u64 {
    let (high, low) = (sn / 2048, sn % 2048);
    high * 2048 * 2 + low
}

fn leaf_to_nth_for_activebits(leaf: u64) -> u64 {
    let twig_id = leaf / (2 * 2048);
    8 * 2 * twig_id + 8 + ((leaf / 256) % 8)
}

// ====== Produce the witness data =========

// get all the leaf nodes and internal nodes that must be included in
// the witness data to prove the accessed entries
// sns should include the serial number of last entry
pub fn get_witness(sn_list: &Vec<u64>, tree: &Tree) -> Witness {
    let max_sn = sn_list.iter().max().unwrap();
    let max_level = calc_max_level(max_sn >> TWIG_SHIFT) as u8;
    let leaves: Vec<u64> = sn_list.iter().map(|&sn| sn_to_leaf(sn)).collect();

    let mut pos_set = HashSet::<(u8, u64)>::new();
    for &leaf in leaves.iter() {
        if leaf % 4096 >= 2048 {
            panic!("not at the left tree of a twig");
        }
        pos_set.insert((0, leaf));
        _get_pos_list(max_level - 1, &mut pos_set, 0, leaf);
        let activebits_leaf_nth = leaf_to_nth_for_activebits(leaf);
        pos_set.insert((8, activebits_leaf_nth));
        _get_pos_list(max_level - 1, &mut pos_set, 8, activebits_leaf_nth);
    }
    //  clean up the nodes that are not necessary
    for leaf in leaves {
        for level in 1..=max_level {
            pos_set.remove(&(level, leaf / u64::pow(2, level as u32)));
        }
        let mut activebits_leaf_nth = leaf_to_nth_for_activebits(leaf);
        for level in 9..=max_level {
            activebits_leaf_nth = activebits_leaf_nth / 2;
            pos_set.remove(&(level, activebits_leaf_nth));
        }
    }

    let pos_list = _sort_pos_list(&pos_set);
    let hash_list = tree.get_hashes_by_pos_list(&pos_list);
    let mut witness = Vec::<MultiProofNode>::with_capacity(pos_list.len());
    for (i, pos) in pos_list.iter().enumerate() {
        let mut p = MultiProofNode::new(pos.0, pos.1);
        p.old_value.copy_from_slice(&hash_list[i]);
        witness.push(p);
    }
    witness
}

pub fn encode_witness(witness: &Witness, entries: &Vec<EntryBz>) -> Vec<u8> {
    let mut leaf_to_index_list = HashMap::new();
    for (i, entry) in entries.iter().enumerate() {
        let sn = entry.serial_number() as u64;
        leaf_to_index_list.insert(sn_to_leaf(sn), i as u64);
    }

    let mut bz = vec![];
    for item in witness {
        bz.extend(item.level.to_be_bytes());
        bz.extend(item.nth.to_be_bytes());
        if item.old_value == get_null_hash_by_pos(item.level, item.nth) {
            bz.push(WitnessOldValueType::Null as u8);
        } else if let Some(idx) = leaf_to_index_list.get(&item.nth) {
            bz.push(WitnessOldValueType::EntryIndex as u8);
            bz.extend(idx.to_be_bytes());
        } else {
            bz.push(WitnessOldValueType::Hash as u8);
            bz.extend(&item.old_value);
        }
    }
    bz
}

// add siblings and ancestors from the leaf to the root
fn _get_pos_list(
    max_level: u8,
    included_nodes: &mut HashSet<(u8, u64)>,
    leaf_level: u8,
    leaf_nth: u64,
) {
    included_nodes.insert((leaf_level, leaf_nth));
    let mut nth = leaf_nth;
    for level in leaf_level..=max_level {
        let sib_nth = nth ^ 1;
        included_nodes.insert((level, sib_nth));
        nth = nth / 2;
    }
}

fn _sort_pos_list(nodes_set: &HashSet<(u8, u64)>) -> Vec<(u8, u64)> {
    let leaf_to_nodes_map: HashMap<u64, (u8, u64)> =
        nodes_set.iter().fold(HashMap::new(), |mut acc, &pos| {
            let leaf = pos.1 as u64 * u64::pow(2, pos.0 as u32);
            acc.insert(leaf, pos);
            acc
        });

    let mut vec: Vec<_> = leaf_to_nodes_map.into_iter().collect();
    vec.sort_by_key(|&(key, _)| key);
    vec.into_iter().map(|(_, value)| value).collect()
}

// For each leaf, where to find its own witness and its activebit's witness?
fn get_witness_offsets(witness: &Witness, leaves: &Vec<u64>) -> Vec<(usize, usize)> {
    let mut node2idx = HashMap::<(u8, u64), Vec<usize>>::new();
    for (i, leaf) in leaves.iter().enumerate() {
        let entry_key = (0, *leaf);
        if let Some(v) = node2idx.get_mut(&entry_key) {
            v.push(i);
        } else {
            node2idx.insert(entry_key, vec![i]);
        }
        let activebits_key = (8, leaf_to_nth_for_activebits(*leaf));
        if let Some(v) = node2idx.get_mut(&activebits_key) {
            v.push(i);
        } else {
            node2idx.insert(activebits_key, vec![i]);
        }
    }

    let mut witness_offsets = vec![(0, 0); leaves.len()];
    for (i, item) in witness.iter().enumerate() {
        if let Some(idx) = node2idx.get(&(item.level, item.nth)) {
            for idx in idx {
                if item.level == 0 {
                    witness_offsets[*idx].0 = i;
                } else if item.level == 8 {
                    witness_offsets[*idx].1 = i;
                }
            }
        }
    }
    // idx active_idx
    witness_offsets
}

// ====== Consume the witness data =========

// check the integrity and correctness of witness
pub fn verify_witness(witness: &Witness, old_root: &[u8; 32], new_root: &[u8; 32]) -> bool {
    let mut stack = Vec::<MultiProofNode>::new();
    let mut idx = 0;
    loop {
        let mut two_children_at_top = false;
        if stack.len() >= 2 {
            let a = &stack[stack.len() - 1];
            let b = &stack[stack.len() - 2];
            two_children_at_top = a.level == b.level && (a.nth ^ b.nth) == 1;
        }
        // if we have two children at the stack top, we can
        // pop them out to calculate the parent
        if two_children_at_top {
            let right = stack.pop().unwrap();
            let left = stack.pop().unwrap();
            let mut parent = MultiProofNode::new(left.level + 1, left.nth / 2);
            println!(
                "----combine_level_{}: {:?} + {:?} = {}-{}",
                left.level, left.nth, right.nth, parent.level, parent.nth
            );
            hasher::node_hash_inplace(
                left.level,
                &mut parent.old_value,
                left.old_value,
                right.old_value,
            );
            hasher::node_hash_inplace(
                left.level,
                &mut parent.new_value,
                left.new_value,
                right.new_value,
            );
            stack.push(parent);
        } else if idx >= witness.len() {
            break;
        } else {
            let e = &witness[idx];
            idx += 1;
            // TODO
            // if !e.is_leaf() && e.old_value != e.new_value {
            //     return false;
            // }
            stack.push(e.clone());
        }
    }
    if stack.len() != 1 {
        return false;
    }
    let root = stack.pop().unwrap();
    println!("verify_witness root: {:?}", root);
    root.old_value == *old_root && root.new_value == *new_root
}

// verify the entries' correctness
pub fn verify_entries(
    start_sn_for_new_entry: u64,
    entries: &Vec<EntryBz>,
    witness_offsets: &Vec<(usize, usize)>,
    witness: &Witness,
) -> bool {
    if entries.len() != witness_offsets.len() {
        return false;
    }
    for (i, entry) in entries.iter().enumerate() {
        let (offset, activebit_offset) = witness_offsets[i];

        if offset >= witness.len() {
            return false;
        }
        let w = &witness[offset];
        let sn = entry.serial_number();
        let leaf = sn_to_leaf(sn);
        if w.level != 0 || w.nth != leaf {
            return false; // NodePos of witness is wrong
        }
        let for_old = sn < start_sn_for_new_entry;
        let v = if for_old { &w.old_value } else { &w.new_value };
        if *v != entry.hash() {
            return false; // hash mismatch
        }

        if activebit_offset >= witness.len() {
            return false;
        }
        let w = &witness[activebit_offset];
        let activebit_leaf = leaf_to_nth_for_activebits(leaf);
        if w.level != 8 || w.nth != activebit_leaf {
            return false; // NodePos of witness is wrong
        }
        let n = (leaf % 256) as usize;
        let v = if for_old { &w.old_value } else { &w.new_value };
        if ((v[n / 8] >> (n % 8)) & 1) == 0 {
            return false; // activebit is not set
        }
    }
    true
}

pub fn get_changed_sn(witness: &Witness) -> (Vec<u64>, Vec<u64>) {
    let mut actived_sn_vec = Vec::<u64>::new();
    let mut deactived_sn_vec = Vec::<u64>::new();
    for item in witness {
        if item.is_leaf_activebits() {
            for i in 0..256 {
                let old_bit = (item.old_value[i / 8] >> (i % 8)) & 1;
                let new_bit = (item.new_value[i / 8] >> (i % 8)) & 1;

                let sn = item.get_entry_sn(i as u8);
                if old_bit == 1 && new_bit == 0 {
                    deactived_sn_vec.push(sn);
                } else if old_bit == 0 && new_bit == 1 {
                    actived_sn_vec.push(sn);
                }
            }
        }
    }
    (actived_sn_vec, deactived_sn_vec)
}

// ----- encode & decode ----
// level(8) + nth(56) + WitnessOldValueType(1) + old_value(0/32/8)
enum WitnessOldValueType {
    Null = 0,
    Hash,
    EntryIndex,
}

impl TryFrom<u8> for WitnessOldValueType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(WitnessOldValueType::Null),
            1 => Ok(WitnessOldValueType::Hash),
            2 => Ok(WitnessOldValueType::EntryIndex),
            _ => Err(()),
        }
    }
}

fn get_witness_bz_slice(witness_bz: &Vec<u8>, start: usize, end: usize) -> Result<&[u8], String> {
    if end > witness_bz.len() {
        return Err(format!("end({}) >= witness_bz.len()", end));
    }
    Ok(&witness_bz[start..end])
}

pub fn decode_witness(
    witness_bz: &Vec<u8>,
    entries: &Vec<EntryBz>,
) -> Result<Vec<MultiProofNode>, String> {
    let mut witness = vec![];
    let mut idx = 0;
    loop {
        let level = u8::from_be_bytes(
            get_witness_bz_slice(witness_bz, idx, idx + 1)?
                .try_into()
                .unwrap(),
        );
        idx += 1;
        let nth = u64::from_be_bytes(
            get_witness_bz_slice(witness_bz, idx, idx + 8)?
                .try_into()
                .unwrap(),
        );
        idx += 8;

        let old_value_type = get_witness_bz_slice(witness_bz, idx, idx + 1)?[0];
        idx += 1;
        let old_value = match WitnessOldValueType::try_from(old_value_type) {
            Ok(WitnessOldValueType::Null) => get_null_hash_by_pos(level, nth),
            Ok(WitnessOldValueType::Hash) => {
                let mut hash = [0u8; 32];
                hash.copy_from_slice(get_witness_bz_slice(witness_bz, idx, idx + 32)?);
                idx += 32;
                hash
            }
            Ok(WitnessOldValueType::EntryIndex) => {
                let mut entry_idx = [0u8; 8];
                entry_idx.copy_from_slice(get_witness_bz_slice(witness_bz, idx, idx + 8)?);
                idx += 8;
                let entry_idx = u64::from_be_bytes(entry_idx);
                entries[entry_idx as usize].hash()
            }
            _ => return Err("Invalid old_value_type".to_string()),
        };
        witness.push(MultiProofNode {
            old_value,
            new_value: old_value.clone(),
            level,
            nth,
        });
        if idx >= witness_bz.len() {
            break;
        }
    }
    Ok(witness)
}

fn get_null_hash_by_pos(level: u8, nth: u64) -> [u8; 32] {
    if level > 12 {
        return NULL_NODE_IN_HIGHER_TREE[level as usize];
    }

    let stride = 1 << (12 - level) as usize;
    let _nth = nth as usize % stride;
    if level == 12 {
        return NULL_TWIG.twig_root;
    }
    if level == 11 && _nth == 0 {
        return NULL_TWIG.left_root;
    }
    if _nth >= stride / 2 {
        if level == 8 {
            return NULL_ACTIVE_BITS.get_bits(_nth - 8, 32).try_into().unwrap();
        }
        if level == 9 {
            return NULL_TWIG.active_bits_mtl1[_nth - 4];
        }
        if level == 10 {
            return NULL_TWIG.active_bits_mtl2[_nth - 2];
        }
        if level == 11 {
            return NULL_TWIG.active_bits_mtl3;
        }
    }
    return NULL_MT_FOR_TWIG[stride / 2 + _nth];
}

#[cfg(test)]
mod tests {
    use core::hash;
    use std::collections::HashMap;

    use crate::{
        check,
        def::{DEFAULT_ENTRY_SIZE, ENTRY_FIXED_LENGTH, SENTRY_COUNT, TWIG_MASK},
        entry::{self, Entry, EntryBz},
        entryfile::EntryFileWithPreReader,
        multiproof::{
            decode_witness, encode_witness, get_changed_sn, get_witness, get_witness_offsets,
            sn_to_leaf, verify_entries, verify_witness,
        },
        test_helper::{build_test_tree, pad32, TempDir},
        tree::{NodePos, Tree},
        twig::{NULL_ACTIVE_BITS, NULL_MT_FOR_TWIG, NULL_NODE_IN_HIGHER_TREE, NULL_TWIG},
    };

    use super::{get_null_hash_by_pos, MultiProofNode};

    #[test]
    fn test_verify_witness() {
        let dir_name = "./DataTree";
        let _tmp_dir = TempDir::new(dir_name);
        let (tree, _, _, _) = create_tree(dir_name, true);

        let sns = vec![0, 9787];
        let witness = get_witness(&sns, &tree);

        let b = verify_witness(
            &witness,
            &[
                146, 244, 47, 25, 163, 218, 83, 101, 123, 7, 222, 196, 246, 23, 245, 186, 18, 94,
                137, 34, 7, 241, 128, 117, 146, 38, 41, 57, 224, 112, 100, 55,
            ],
            &[
                241, 14, 44, 193, 141, 198, 155, 170, 45, 152, 247, 141, 70, 241, 149, 222, 117,
                111, 153, 149, 212, 25, 207, 82, 212, 20, 117, 216, 193, 236, 219, 73,
            ],
        );
        assert!(b);

        //TODO different entry_bzs[8188]
        // let mut entry_file = EntryFileWithPreReader::new(&tree.entry_file_wr.entry_file);
        // let mut buff = vec![];
        // entry_file.read_entry(poslist[8188 as usize], &mut buff);
        // println!("----{:?}", buff);
    }

    #[test]
    fn test_customer() {
        let dir_name = "./DataTree";
        let _tmp_dir = TempDir::new(dir_name);

        let sns = vec![8188, 9787, 9788, 9789];

        let mut witness_bz = vec![];
        let mut input_entry_bz_list = vec![];
        {
            // produce witness
            let (mut tree, mut pos_list, mut serial_number, mut entry_bzs) =
                create_tree(dir_name, true);
            let witness = get_witness(&sns, &tree);
            for sn in &sns[0..2] {
                input_entry_bz_list.push(entry_bzs[*sn as usize].clone());
            }
            let input_entries = input_entry_bz_list
                .iter()
                .map(|bz| EntryBz { bz: &bz })
                .collect();
            witness_bz = encode_witness(&witness, &input_entries);
        }

        let mut new_entry_bz_list = vec![];
        let witness: Vec<MultiProofNode>;
        {
            // customer
            let new_sns = vec![9788, 9789];
            let (mut tree, mut pos_list, mut serial_number, _) = create_tree(dir_name, false);
            for i in 0..new_sns.len() {
                let mut entry = Entry {
                    key: &b"key".as_slice(),
                    value: &b"value".as_slice(),
                    next_key_hash: &pad32(b"nextkey".as_slice()),
                    version: 100,
                    last_version: 99,
                    serial_number: 0,
                };
                serial_number += 1;
                entry.serial_number = serial_number;
                let deactived_sn_list = if i == 0 { vec![9787] } else { vec![] };
                for sn in &deactived_sn_list {
                    tree.deactive_entry(*sn);
                }
                let total_len0 =
                    ((ENTRY_FIXED_LENGTH + &entry.key.len() + &entry.value.len() + 7) / 8) * 8;
                let mut bz = vec![0u8; total_len0 + 8 * deactived_sn_list.len()];
                let entry_bz = entry::entry_to_bytes(&entry, &deactived_sn_list, &mut bz);
                pos_list.push(tree.append_entry(&entry_bz));
                new_entry_bz_list.push(entry_bz.bz.to_vec());
            }
            let n_list = tree.flush_files(0, 0);
            let n_list = tree.upper_tree.evict_twigs(n_list, 0, 0);
            tree.upper_tree
                .sync_upper_nodes(n_list, tree.youngest_twig_id);
            check::check_hash_consistency(&tree);

            // decode witness
            let input_entries = input_entry_bz_list
                .iter()
                .map(|bz| EntryBz { bz: &bz })
                .collect();
            let mut _witness = decode_witness(&witness_bz, &input_entries).unwrap();
            // fill witness new_value
            let pos_list: Vec<(u8, u64)> =
                _witness.iter().map(|item| (item.level, item.nth)).collect();
            let hash_list = tree.get_hashes_by_pos_list(&pos_list);
            for (i, item) in _witness.iter_mut().enumerate() {
                item.new_value.copy_from_slice(&hash_list[i]);
            }
            witness = _witness;
        }

        // verify
        verify_witness(&witness, &[0; 32], &[0; 32]);

        let leaves = sns.iter().map(|&sn| sn_to_leaf(sn)).collect();
        let witness_offsets = get_witness_offsets(&witness, &leaves);
        let mut entry_bz_list = vec![];
        entry_bz_list.extend(input_entry_bz_list);
        entry_bz_list.extend(new_entry_bz_list);
        let entries = entry_bz_list.iter().map(|bz| EntryBz { bz: &bz }).collect();
        let b = verify_entries(9787 as u64 + 1, &entries, &witness_offsets, &witness);
        assert!(b);

        let (actived_sn_vec, deactived_sn_vec) = get_changed_sn(&witness);
        println!("{:?}", actived_sn_vec);
        println!("{:?}", deactived_sn_vec);
    }

    #[test]
    fn test_witness_serialize() {
        let dir_name = "./DataTree";
        let _tmp_dir = TempDir::new(dir_name);
        let (tree, _, _, entry_bzs) = create_tree(dir_name, true);

        let sns = vec![9787];
        let witness = get_witness(&sns, &tree);

        let mut entries = vec![];
        for sn in sns {
            let entry = EntryBz {
                bz: &entry_bzs[sn as usize],
            };
            entries.push(entry);
        }
        let bz = encode_witness(&witness, &entries);
        let witness2 = decode_witness(&bz, &entries).unwrap();
        for (i, item) in witness.iter().enumerate() {
            assert_eq!(item.old_value, witness2[i].old_value);
            assert_eq!(item.old_value, witness2[i].new_value);
            assert_eq!(item.level, witness2[i].level);
            assert_eq!(item.nth, witness2[i].nth);
        }
    }

    fn create_tree(dir_name: &str, sync: bool) -> (Tree, Vec<i64>, u64, Vec<Vec<u8>>) {
        let deact_sn_list: Vec<u64> = (0..2048)
            .chain(vec![5000, 5500, 5700, 5813, 6001])
            .collect();
        let (mut tree, pos_list, serial_number, entry_bzs) =
            build_test_tree(dir_name, &deact_sn_list, TWIG_MASK as i32 * 4, 1600);
        if sync {
            let n_list = tree.flush_files(0, 0);
            let n_list = tree.upper_tree.evict_twigs(n_list, 0, 0);
            tree.upper_tree
                .sync_upper_nodes(n_list, tree.youngest_twig_id);
            check::check_hash_consistency(&tree);
        }
        (tree, pos_list, serial_number, entry_bzs)
    }
}
