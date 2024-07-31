use rand_core::le;
use std::{collections::HashMap, hash::Hash, mem};

use crate::{tree::NodePos, utils::hasher::hash2};

#[derive(Debug, Clone)]
struct Triple {
    old_value: [u8; 32],
    new_value: [u8; 32],
    node_pos: NodePos,
}

struct StackMachine {
    pre_map: HashMap<NodePos, [u8; 32]>,
    post_map: HashMap<NodePos, [u8; 32]>,

    stack: Vec<Triple>,

    max_level: u64,
}

fn get_parent_pos(pos: &NodePos) -> NodePos {
    let level = pos.level();
    let nth = pos.nth();
    let parent_nth = nth / 2;
    NodePos::pos(level + 1, parent_nth)
}

fn get_sibling_pos(pos: &NodePos) -> NodePos {
    let level = pos.level();
    let mut nth: u64 = pos.nth();
    let sibling_nth = nth ^ 1;
    NodePos::pos(level, sibling_nth)
}

fn get_children_pos_list(pos: &NodePos) -> (NodePos, NodePos) {
    let level = pos.level();
    if level == 0 {
        panic!("leaf node has no children");
    }
    let nth = pos.nth();
    let left_nth = nth * 2;
    let right_nth = nth * 2 + 1;
    (
        NodePos::pos(level - 1, left_nth),
        NodePos::pos(level - 1, right_nth),
    )
}

impl StackMachine {
    fn new(
        pre_map: HashMap<NodePos, [u8; 32]>,
        post_map: HashMap<NodePos, [u8; 32]>,
        max_level: u64,
    ) -> Self {
        let mut machine = StackMachine {
            pre_map,
            post_map,
            stack: Vec::new(),
            max_level,
        };
        machine.clear_map();
        machine
    }

    fn clear_map(&mut self) {
        let nth_arr_at_level0: Vec<u64> = self
            .pre_map
            .keys()
            .filter(|pos| pos.level() == 0)
            .enumerate()
            .map(|(_, key)| key.nth())
            .collect();
        for nth in nth_arr_at_level0 {
            for level in 1..self.max_level {
                self.pre_map
                    .remove(&NodePos::pos(level, nth / u64::pow(2, level as u32)));
                self.post_map
                    .remove(&NodePos::pos(level, nth / u64::pow(2, level as u32)));
            }
        }
        // println!("pre_map: {:?}", self.pre_map);
    }

    fn combine(&mut self) {
        if self.stack.len() < 2 {
            panic!("stack len < 2");
        }

        let mut right = self.stack.pop().unwrap();
        let mut left = self.stack.pop().unwrap();
        if left.node_pos.nth() & 1 != 0 {
            (left, right) = (right, left);
        }
        println!(
            "-------------combine: {:?} + {:?}--------------",
            left.node_pos, right.node_pos
        );

        if get_sibling_pos(&left.node_pos) != right.node_pos {
            panic!("siblings can't combine");
        }

        let parent_pos = get_parent_pos(&right.node_pos);

        let combined_old_value =
            hash2(left.node_pos.level() as u8, left.old_value, right.old_value);
        let combined_new_value =
            hash2(left.node_pos.level() as u8, left.new_value, right.new_value);

        let parent = Triple {
            old_value: combined_old_value,
            new_value: combined_new_value,
            node_pos: parent_pos,
        };
        // println!(
        //     "combine: {:?} + {:?} = {:?}",
        //     left.old_value, right.old_value, combined_new_value
        // );
        self.stack.push(parent);
    }

    fn execute(&mut self, operations: Vec<&str>) {
        for op in operations {
            match op {
                "combine" => self.combine(),
                _ => panic!("unknown operation"),
            }
        }
    }

    fn push_by_node_pos(&mut self, node_pos: &NodePos) {
        let pre_value = self.pre_map.remove(node_pos).unwrap();
        let post_value = self.post_map.remove(node_pos).unwrap_or(pre_value);
        let triple = Triple {
            old_value: pre_value,
            new_value: post_value,
            node_pos: node_pos.clone(),
        };
        self.stack.push(triple);
    }

    fn make_stack_or_execute(&mut self, _target_node_pos: &NodePos) -> Result<(), &'static str> {
        let mut target_node_pos = _target_node_pos.clone();
        loop {
            // println!("loop: {:?}", target_node_pos);

            let mut last_triple_node_pos = NodePos::new(u64::MAX);
            if let Some(triple) = self.stack.last() {
                last_triple_node_pos = triple.node_pos.clone();
            }

            // Has got to the root
            if last_triple_node_pos.level() == self.max_level {
                break;
            }

            // can combine
            if let Some(triple) = self.stack.get(self.stack.len().wrapping_sub(2)) {
                if get_sibling_pos(&triple.node_pos) == last_triple_node_pos {
                    // println!("combine!!!!!!!!");
                    self.combine();
                    target_node_pos = self.stack.last().unwrap().node_pos.clone();
                    continue;
                }
            }

            // need push sibling to stack
            if self.pre_map.contains_key(&target_node_pos)
                || last_triple_node_pos == target_node_pos
            {
                // println!("sibling");
                if self.pre_map.contains_key(&target_node_pos) {
                    self.push_by_node_pos(&target_node_pos);
                }
                let sibling = get_sibling_pos(&target_node_pos);
                target_node_pos = sibling;
                continue;
            }

            // calc target from children
            // println!("children");
            let children = get_children_pos_list(&target_node_pos);
            target_node_pos = children.0.clone();
            continue;
        }
        Ok(())
    }

    pub fn run(&mut self) -> Triple {
        let min_left_node_nth_at_level0 = self
            .pre_map
            .keys()
            .filter(|pos| pos.level() == 0)
            .map(|pos| pos.nth())
            .min()
            .unwrap();
        let _ = self.make_stack_or_execute(&NodePos::pos(0, min_left_node_nth_at_level0));
        if self.pre_map.len() != 0 {
            panic!("pre_map not empty");
        }
        self.stack.pop().unwrap()
    }
}

#[cfg(test)]
mod compare_tests {
    use std::collections::HashMap;

    use crate::{
        check,
        def::TWIG_MASK,
        test_helper::{self, build_test_tree, TempDir},
        tree::NodePos,
    };

    use super::StackMachine;

    #[test]
    fn test1() {
        let twig_of_left_max_level = 3;

        let mut pre_map = HashMap::<NodePos, [u8; 32]>::new();

        // origin entry
        pre_map.insert(NodePos::pos(0, 0), [0; 32]);
        for level in 0..twig_of_left_max_level {
            pre_map.insert(NodePos::pos(level, 1), [0; 32]);
        }
        // origin entry
        let level0_stride = 1 << twig_of_left_max_level;
        // every level start from 0
        pre_map.insert(NodePos::pos(0, level0_stride - 1), [0; 32]);
        for level in 0..twig_of_left_max_level {
            let stride = 1 << twig_of_left_max_level - level;
            pre_map.insert(NodePos::pos(level, stride - 1 - 1), [0; 32]);
        }

        let mut post_map = HashMap::<NodePos, [u8; 32]>::new();
        // change
        post_map.insert(NodePos::pos(0, level0_stride), [1; 32]);

        let mut machine = StackMachine::new(pre_map, post_map, twig_of_left_max_level);
        let root = machine.run();
        // println!("root: {:?}", root);
    }

    #[test]
    fn test2() {
        let dir_name = "./DataTree";
        let _tmp_dir = TempDir::new(dir_name);

        let deact_sn_list: Vec<u64> = (0..2048)
            .chain(vec![5000, 5500, 5700, 5813, 6001])
            .collect();

        let (mut tree, _, _) =
            build_test_tree(dir_name, &deact_sn_list, TWIG_MASK as i32 * 4, 1600);
        let n_list = tree.flush_files(0, 0);
        let n_list = tree.upper_tree.evict_twigs(n_list, 0, 0);

        tree.upper_tree
            .sync_upper_nodes(n_list, tree.youngest_twig_id);
        check::check_hash_consistency(&tree);

        // let mut x = tree.get_proof(2031);
        // x.check(true).unwrap();

        {
            let mut pre_map = HashMap::<NodePos, [u8; 32]>::new();
            let max_level = tree.get_proof_map(0, &mut pre_map);
            let mut machine = StackMachine::new(pre_map, HashMap::new(), max_level as u64);
            let root = machine.run();
            assert_eq!(root.new_value[0..2], [146, 244]);
        }
        {
            let mut pre_map = HashMap::<NodePos, [u8; 32]>::new();
            let max_level = tree.get_proof_map(2031, &mut pre_map); // total level is max_level + 1
            println!("get_proof_map {:?}", pre_map.keys());
            let mut machine = StackMachine::new(pre_map, HashMap::new(), max_level as u64);
            let root = machine.run();
            assert_eq!(root.new_value[0..2], [146, 244]);
        }
        {
            let mut pre_map = HashMap::<NodePos, [u8; 32]>::new();
            let max_level = tree.get_proof_map(2060, &mut pre_map); // total level is max_level + 1
            println!("get_proof_map {:?}", pre_map.keys());
            let mut machine = StackMachine::new(pre_map, HashMap::new(), max_level as u64);
            let root = machine.run();
            assert_eq!(root.new_value[0..2], [146, 244]);
        }
    }
}
