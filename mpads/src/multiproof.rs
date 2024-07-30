use crate::utils::hasher;
use crate::entry::EntryBz;
use std::collections::{HashSet, HashMap};

#[derive(Debug, Clone)]
struct IncludedNode {
    old_value: [u8; 32],
    new_value: [u8; 32],
    level: u8,
    nth: u64,
}

impl IncludedNode {
    pub fn new(level: u8, nth: u64) -> Self {
        IncludedNode {
            old_value: [0u8; 32],
            new_value: [0u8; 32],
            level,
            nth,
        }
    }
}

fn sn_to_leaf(sn: u64) -> u64 {
    let (high, low) = (sn / 2048, sn % 2048);
    (high << 1) | low
}

fn get_activebit_leaf(leaf: u64) -> u64 {
    (leaf / 256) ^ 16
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
        _get_included_nodes(max_level, &mut included_nodes, 0, leaf);
        let activebit_leaf = get_activebit_leaf(leaf);
        _get_included_nodes(max_level, &mut included_nodes, 8, activebit_leaf);
    }
    included_nodes
}

// add siblings and ancestors from the leaf to the root
pub fn _get_included_nodes(
    max_level: u8,
    included_nodes: &mut HashSet<(u8, u64)>,
    mut level: u8,
    mut nth: u64,
) {
    while level <= max_level {
        //add sibling
        let sib_nth = nth ^ 1;
        included_nodes.insert((level, nth ^ 1));

        //add parent
        level += 1;
        nth /= 2;
        if included_nodes.contains(&(level, nth)) {
            break; // already added parent
        }
        included_nodes.insert((level, sib_nth));
    }
}

// Sort the included nodes in a post-order traversal
pub fn get_witness(included_nodes: &HashSet<(u8, u64)>, max_level: u8) -> Vec<IncludedNode> {
    let mut witness = Vec::<IncludedNode>::with_capacity(included_nodes.len());
    _get_witness(included_nodes, &mut witness, max_level, 0);
    witness
}

pub fn _get_witness(
    included_nodes: &HashSet<(u8, u64)>,
    witness: &mut Vec<IncludedNode>,
    level: u8,
    nth: u64,
) {
    let (l, mut n) = (level - 1, nth * 2);
    //the left child
    if included_nodes.contains(&(l, n)) {
        _get_witness(included_nodes, witness, l, n);
    }
    n += 1;
    //the right child
    if included_nodes.contains(&(l, n)) {
        _get_witness(included_nodes, witness, l, n);
    }
    //the parent itself
    witness.push(IncludedNode::new(level, nth));
    return;
}

// For each leaf, where to find its own witness and its activebit's witness?
pub fn get_witness_offsets(witness: &Vec<IncludedNode>, leaves: &Vec<u64>) -> Vec<(usize, usize)> {
    let mut node2idx = HashMap::<(u8, u64), usize>::new();
    for i in 0..witness.len() {
        node2idx.insert((witness[i].level, witness[i].nth), i);
    }
    let mut witness_offsets = Vec::new();
    for &leaf in leaves.iter() {
        let idx = node2idx.get(&(0, leaf)).unwrap();
        let activebit_leaf = get_activebit_leaf(leaf);
        let activebit_idx = node2idx.get(&(8, activebit_leaf)).unwrap();
        witness_offsets.push((*idx, *activebit_idx));
    }
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
            let a = stack.pop().unwrap();
            let b = stack.pop().unwrap();
            let mut parent = IncludedNode::new(a.level + 1, a.nth / 2);
            hasher::node_hash_inplace(a.level, &mut parent.old_value, a.old_value, b.old_value);
            hasher::node_hash_inplace(a.level, &mut parent.new_value, a.new_value, b.new_value);
            stack.push(parent);
        } else if idx >= witness.len() {
            break;
        } else {
            let e = &witness[idx];
            idx += 1;
            stack.push(e.clone());
        }
    }
    if stack.len() != 1 {
        return false;
    }
    let root = stack.pop().unwrap();
    root.old_value == *old_root && root.new_value == *new_root
}

// verify the entries' correctness
pub fn verify_entries(
    for_old: bool,
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
        let leaf = sn_to_leaf(entry.serial_number());
        if w.level != 0 || w.nth != leaf {
            return false; // NodePos of witness is wrong
        }
        let v = if for_old {&w.old_value} else {&w.new_value};
        if *v != entry.hash() {
            return false; // hash mismatch
        }

        if activebit_offset >= witness.len() {
            return false;
        }
        let w = &witness[activebit_offset];
        let activebit_leaf = get_activebit_leaf(leaf);
        if w.level != 8 || w.nth != activebit_leaf {
            return false; // NodePos of witness is wrong
        }
        let n = (leaf % 256) as usize;
        let v = if for_old {&w.old_value} else {&w.new_value};
        if ((v[n/8] >> (n%8)) & 1) == 0 {
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

