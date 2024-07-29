use crate::utils::hasher;
use std::collections::HashSet;

#[derive(Debug, Clone)]
struct Element {
    old_value: [u8; 32],
    new_value: [u8; 32],
    level: u8,
    nth: u64,
}

impl Element {
    pub fn new(level: u8, nth: u64) -> Self {
        Element {
            old_value: [0u8; 32],
            new_value: [0u8; 32],
            level,
            nth,
        }
    }
}

pub fn get_max_level(leaves: &Vec<u64>) -> u8 {
    let mut min_leading_zeros = 64u32;
    for &leaf in leaves.iter() {
        let lz = leaf.leading_zeros();
        if min_leading_zeros < lz {
            min_leading_zeros = lz;
        }
    }
    (64 - min_leading_zeros) as u8
}

pub fn get_subtree_nodes(
    max_level: u8,
    data_leaves: &Vec<u64>,
    activebits_leaves: &Vec<u64>,
) -> HashSet<(u8, u64)> {
    let mut subtree_nodes = HashSet::<(u8, u64)>::new();
    for &leaf in data_leaves.iter() {
        _get_subtree_nodes(max_level, &mut subtree_nodes, 0, leaf);
    }
    for &leaf in activebits_leaves.iter() {
        _get_subtree_nodes(max_level, &mut subtree_nodes, 8, leaf);
    }
    subtree_nodes
}

pub fn _get_subtree_nodes(
    max_level: u8,
    subtree_nodes: &mut HashSet<(u8, u64)>,
    mut level: u8,
    mut nth: u64,
) {
    while level <= max_level {
        let sib_nth = nth ^ 1;
        //add sibling
        subtree_nodes.insert((level, nth ^ 1));
        level += 1;
        nth /= 2;
        if subtree_nodes.contains(&(level, nth)) {
            break; // already added parent
        }
        //add parent
        subtree_nodes.insert((level, sib_nth));
    }
}

pub fn get_witness(subtree_nodes: &HashSet<(u8, u64)>, max_level: u8) -> Vec<Element> {
    let mut witness = Vec::<Element>::with_capacity(subtree_nodes.len());
    _get_witness(subtree_nodes, &mut witness, max_level, 0);
    witness
}

pub fn _get_witness(
    subtree_nodes: &HashSet<(u8, u64)>,
    witness: &mut Vec<Element>,
    level: u8,
    nth: u64,
) {
    let (l, mut n) = (level - 1, nth * 2);
    //the left child
    if subtree_nodes.contains(&(l, n)) {
        _get_witness(subtree_nodes, witness, l, n);
    }
    n += 1;
    //the right child
    if subtree_nodes.contains(&(l, n)) {
        _get_witness(subtree_nodes, witness, l, n);
    }
    witness.push(Element::new(level, nth));
    return;
}

pub fn verify(witness: &Vec<Element>) -> Option<Element> {
    let mut stack = Vec::<Element>::new();
    let mut idx = 0;
    loop {
        let mut both_children_ready = false;
        if stack.len() >= 2 {
            let a = &stack[stack.len() - 1];
            let b = &stack[stack.len() - 2];
            both_children_ready = a.level == b.level && (a.nth ^ b.nth) == 1;
        }
        if both_children_ready {
            let a = stack.pop().unwrap();
            let b = stack.pop().unwrap();
            let mut parent = Element::new(a.level + 1, a.nth / 2);
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
    if stack.len() == 1 {
        return Some(stack.pop().unwrap());
    }
    None
}
