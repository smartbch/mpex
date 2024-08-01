use crate::entry;
use crate::utils::hasher;
use crate::{def::ENTRY_FIXED_LENGTH, entry::EntryBz};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
struct IncludedNode {
    old_value: [u8; 32],
    new_value: [u8; 32],
    level: u8,
    nth: u64,
}

impl IncludedNode {
    pub fn new(level: u8, nth: u64) -> Self {
        let mut bz = [0u8; ENTRY_FIXED_LENGTH + 8];
        let null_hash = entry::null_entry(&mut bz[..]).hash();
        IncludedNode {
            old_value: null_hash.clone(),
            new_value: null_hash,
            level,
            nth,
        }
    }
    pub fn is_leaf(&self) -> bool {
        if self.level == 8 && self.nth % 16 >= 8 {
            return true;
        }
        self.level == 0 && self.nth % 4096 < 2048
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
pub fn get_included_nodes(max_level: u8, leaves: &Vec<u64>) -> HashSet<(u8, u64)> {
    let mut included_nodes = HashSet::<(u8, u64)>::new();
    for &leaf in leaves.iter() {
        if leaf % 4096 >= 2048 {
            panic!("not at the left tree of a twig");
        }
        included_nodes.insert((0, leaf));
        _get_included_nodes(max_level - 1, &mut included_nodes, 0, leaf);
        let activebits_leaf_nth = leaf_to_nth_for_activebits(leaf);
        included_nodes.insert((8, activebits_leaf_nth));
        _get_included_nodes(max_level - 1, &mut included_nodes, 8, activebits_leaf_nth);
    }
    //  clean up the nodes that are not necessary
    for &leaf in leaves {
        for level in 1..=max_level {
            included_nodes.remove(&(level, leaf / u64::pow(2, level as u32)));
        }
        let mut activebits_leaf_nth = leaf_to_nth_for_activebits(leaf);
        for level in 9..=max_level {
            activebits_leaf_nth = activebits_leaf_nth / 2;
            included_nodes.remove(&(level, activebits_leaf_nth));
        }
    }
    included_nodes
}

// add siblings and ancestors from the leaf to the root
pub fn _get_included_nodes(
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

// Sort the included nodes in a post-order traversal
pub fn get_witness(included_nodes: &HashSet<(u8, u64)>, max_level: u8) -> Vec<IncludedNode> {
    let mut witness = Vec::<IncludedNode>::with_capacity(included_nodes.len());
    let mut target_node_pos = included_nodes
        .iter()
        .filter(|&&(u, _)| u == 0)
        .min_by_key(|&&(_, v)| v)
        .copied()
        .unwrap();
    let mut included_nodes_set = included_nodes.clone();
    let mut stack = Vec::<(u8, u64)>::new();
    loop {
        let mut last_node_pos = (u8::MAX, u64::MAX);
        if let Some(node) = stack.last() {
            last_node_pos = node.clone();
        }

        // Has got to the root
        if last_node_pos.0 == max_level {
            break;
        }

        // can combine
        if let Some(&(level, nth)) = stack.get(stack.len().wrapping_sub(2)) {
            if (level, nth ^ 1) == last_node_pos {
                // println!("combine!!!!!!!!");
                let mut right = stack.pop().unwrap();
                let mut left = stack.pop().unwrap();
                if left.1 & 1 != 0 {
                    (left, right) = (right, left);
                }
                let parent = (right.0 + 1, right.1 / 2);
                // println!("----combine: {:?} + {:?} = {:?}", left, right, parent);
                target_node_pos = parent.clone();
                stack.push(parent);
                continue;
            }
        }

        // need push sibling to stack
        if included_nodes_set.contains(&target_node_pos) || last_node_pos == target_node_pos {
            if included_nodes_set.contains(&target_node_pos) {
                included_nodes_set.remove(&target_node_pos);
                witness.push(IncludedNode::new(target_node_pos.0, target_node_pos.1)); // TODO use tree to get value
                stack.push(target_node_pos);
            }
            target_node_pos = (target_node_pos.0, target_node_pos.1 ^ 1);
            continue;
        }

        // calc target from children
        target_node_pos = (target_node_pos.0 - 1, target_node_pos.1 * 2);
        continue;
    }
    if !included_nodes_set.is_empty() {
        panic!("included_nodes_set should empty");
    }
    witness
}

// For each leaf, where to find its own witness and its activebit's witness?
fn get_witness_offsets(witness: &Vec<IncludedNode>, leaves: &Vec<u64>) -> Vec<(usize, usize)> {
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
pub fn verify_witness(
    witness: &Vec<IncludedNode>,
    old_root: &[u8; 32],
    new_root: &[u8; 32],
) -> bool {
    let mut stack = Vec::<IncludedNode>::new();
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
            let mut a = stack.pop().unwrap();
            let mut b = stack.pop().unwrap();
            if a.nth & 1 != 0 {
                (a, b) = (b, a);
            }
            let mut parent = IncludedNode::new(a.level + 1, a.nth / 2);
            println!(
                "----combine_level_{}: {:?} + {:?} = {}-{}",
                a.level, a.nth, b.nth, parent.level, parent.nth
            );
            hasher::node_hash_inplace(a.level, &mut parent.old_value, a.old_value, b.old_value);
            hasher::node_hash_inplace(a.level, &mut parent.new_value, a.new_value, b.new_value);
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
    witness: &Vec<IncludedNode>,
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

pub fn get_changed_sn(witness: &Vec<IncludedNode>) -> (Vec<u64>, Vec<u64>) {
    let mut actived_sn_vec = Vec::<u64>::new();
    let mut deactived_sn_vec = Vec::<u64>::new();
    todo!();
    (actived_sn_vec, deactived_sn_vec)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{
        check,
        def::{DEFAULT_ENTRY_SIZE, ENTRY_FIXED_LENGTH, SENTRY_COUNT, TWIG_MASK},
        entry::{self, Entry, EntryBz},
        entryfile::EntryFileWithPreReader,
        multiproof::{
            get_changed_sn, get_included_nodes, get_witness, get_witness_offsets, sn_to_leaf,
            verify_entries, verify_witness,
        },
        test_helper::{build_test_tree, pad32, TempDir},
        tree::{NodePos, Tree},
        twig::{NULL_ACTIVE_BITS, NULL_MT_FOR_TWIG, NULL_NODE_IN_HIGHER_TREE, NULL_TWIG},
    };

    use super::IncludedNode;

    #[test]
    fn test_get_included_nodes() {
        let sns = vec![0, 2031, 2060];
        let leaves = sns.iter().map(|&sn| sn_to_leaf(sn)).collect();
        let max_level = 15;
        let included_nodes = get_included_nodes(max_level, &leaves);
        println!("{:?}", included_nodes);
    }

    #[test]
    fn test_get_witness() {
        let sns = vec![0, 2031, 2060];
        let leaves = sns.iter().map(|&sn| sn_to_leaf(sn)).collect();
        let max_level = 15;
        let included_nodes = get_included_nodes(max_level, &leaves);

        let mut witness = get_witness(&included_nodes, max_level);
        //TODO replace value
        let dir_name = "./DataTree";
        let _tmp_dir = TempDir::new(dir_name);
        let (tree, _, _, _) = create_tree(dir_name, true);
        let mut hash_map = HashMap::new();
        for sn in sns {
            tree.get_proof_map(sn, &mut hash_map);
        }
        for node in &mut witness {
            println!("{:?}", node);
            let v = hash_map
                .get(&NodePos::pos(node.level as u64, node.nth))
                .unwrap();
            node.new_value = v.clone();
            node.old_value = v.clone();
        }

        println!("{:?}", witness);
    }

    #[test]
    fn test_verify_witness() {
        let sns = vec![0, 2031, 2060];
        let leaves = sns.iter().map(|&sn| sn_to_leaf(sn)).collect();
        let max_level = 15;
        let included_nodes = get_included_nodes(max_level, &leaves);

        let mut witness = get_witness(&included_nodes, max_level);
        //TODO replace value
        let dir_name = "./DataTree";
        let _tmp_dir = TempDir::new(dir_name);
        let (tree, _, _, _) = create_tree(dir_name, true);
        let mut hash_map = HashMap::new();
        tree.get_proof_map_by_sns(&sns, &mut hash_map);
        for node in &mut witness {
            let v = hash_map
                .get(&NodePos::pos(node.level as u64, node.nth))
                .unwrap();
            node.new_value = v.clone();
            node.old_value = v.clone();
        }

        let b = verify_witness(
            &witness,
            &[
                146, 244, 47, 25, 163, 218, 83, 101, 123, 7, 222, 196, 246, 23, 245, 186, 18, 94,
                137, 34, 7, 241, 128, 117, 146, 38, 41, 57, 224, 112, 100, 55,
            ],
            &[
                146, 244, 47, 25, 163, 218, 83, 101, 123, 7, 222, 196, 246, 23, 245, 186, 18, 94,
                137, 34, 7, 241, 128, 117, 146, 38, 41, 57, 224, 112, 100, 55,
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
    fn test_verify_entries() {
        let dir_name = "./DataTree";
        let _tmp_dir = TempDir::new(dir_name);

        let pre_sns = vec![8188, 9787, 9788, 9789]; // include post_sns
        let mut pre_proof_map = HashMap::new();
        let mut pre_max_level = 0;
        {
            let (mut tree, mut pos_list, mut serial_number, mut entry_bzs) =
                create_tree(dir_name, true);
            pre_max_level = tree.get_proof_map_by_sns(&pre_sns, &mut pre_proof_map) as u8;
        }

        let post_sns = vec![9788, 9789];
        let mut post_max_level = 0;
        let mut post_proof_map = HashMap::new();
        let entry_bzs: Vec<Vec<u8>>;
        {
            let (mut tree, mut pos_list, mut serial_number, mut _entry_bzs) =
                create_tree(dir_name, false);
            // change tree
            for i in 0..2 {
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
                _entry_bzs.push(entry_bz.bz.to_vec());
            }

            let n_list = tree.flush_files(0, 0);
            let n_list = tree.upper_tree.evict_twigs(n_list, 0, 0);
            tree.upper_tree
                .sync_upper_nodes(n_list, tree.youngest_twig_id);
            check::check_hash_consistency(&tree);

            post_max_level = tree.get_proof_map_by_sns(&post_sns, &mut post_proof_map) as u8;
            entry_bzs = _entry_bzs;
        }

        let mut sns = vec![];
        sns.extend(pre_sns.iter());
        // sns.extend(post_sns);
        let leaves = sns.iter().map(|&sn| sn_to_leaf(sn)).collect();
        let included_nodes = get_included_nodes(15, &leaves);
        // TODO level 不一样
        let mut witness = get_witness(&included_nodes, 15);
        merge_witness(&mut witness, pre_proof_map, post_proof_map);

        verify_witness(&witness, &[0; 32], &[0; 32]);
        let mut entries = vec![];
        for sn in sns {
            let entry = EntryBz {
                bz: &entry_bzs[sn as usize],
            };
            entries.push(entry);
        }

        let witness_offsets = get_witness_offsets(&witness, &leaves);
        let b = verify_entries(9787 as u64 + 1, &entries, &witness_offsets, &witness);
        assert!(b);
    }

    #[test]
    fn test() {
        let leaves = vec![0];
        let included_nodes = get_included_nodes(15, &leaves);
        println!("{:?}", included_nodes);

        let witness = get_witness(&included_nodes, 15);
        println!("{:?}", included_nodes);
        verify_witness(&witness, &[0; 32], &[0; 32]);

        let entries = vec![];
        let witness_offsets = get_witness_offsets(&witness, &leaves);
        verify_entries(4000, &entries, &witness_offsets, &witness);

        let changed_sn = get_changed_sn(&witness);
        println!("{:?}", changed_sn);
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

    fn get_null_hash_by_level(level: u8, nth: u64) -> [u8; 32] {
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

    fn merge_witness(
        witness: &mut Vec<IncludedNode>,
        pre_proof_map: HashMap<NodePos, [u8; 32]>,
        post_proof_map: HashMap<NodePos, [u8; 32]>,
    ) {
        for item in witness {
            let node_pos = NodePos::pos(item.level as u64, item.nth);
            // println!("node_pos: {:?}", node_pos);
            if let Some(old_value) = pre_proof_map.get(&node_pos) {
                item.old_value.copy_from_slice(old_value);
                // println!("old_value1: {:?}", old_value);
            } else {
                let old_value = get_null_hash_by_level(item.level, item.nth);
                // println!("old_value2: {:?}", old_value);
                item.old_value.copy_from_slice(&old_value);
            }
            let new_value = post_proof_map
                .get(&node_pos)
                .unwrap_or_else(|| pre_proof_map.get(&node_pos).unwrap());
            // println!("new_value: {:?}", new_value);
            item.new_value.copy_from_slice(new_value);
        }
    }
}
