use rand_core::le;
use std::{collections::HashMap, hash::Hash};

use crate::tree::NodePos;

#[derive(Debug, Clone)]
struct Triple {
    old_value: String,
    new_value: String,
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
    let sibling_nth = {
        if nth % 2 == 0 {
            nth += 1
        } else {
            nth -= 1
        }
        nth
    };
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
            .map(|(_, key)| key.nth()) // Assuming you want to collect the keys
            .collect();
        for nth in nth_arr_at_level0 {
            for level in 1..self.max_level {
                self.pre_map
                    .remove(&NodePos::pos(level, nth / u64::pow(2, level as u32)));
                self.post_map
                    .remove(&NodePos::pos(level, nth / u64::pow(2, level as u32)));
            }
        }
        println!("pre_map: {:?}", self.pre_map);
    }

    fn combine(&mut self) {
        if self.stack.len() < 2 {
            panic!("stack len < 2");
        }

        let right = self.stack.pop().unwrap();
        let left = self.stack.pop().unwrap();
        println!("combine: {:?} + {:?}", left.node_pos, right.node_pos);

        if get_sibling_pos(&left.node_pos) != right.node_pos {
            panic!("siblings can't combine");
        }

        let parent_pos = get_parent_pos(&right.node_pos);

        let combined_old_value = format!("H({}+{})", left.old_value, right.old_value);
        let combined_new_value = format!("H({}+{})", left.new_value, right.new_value);

        let parent = Triple {
            old_value: combined_old_value,
            new_value: combined_new_value,
            node_pos: parent_pos,
        };

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
            old_value: format!("{:?}", pre_value),
            new_value: format!("{:?}", post_value),
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
            if last_triple_node_pos.level() == self.max_level - 1 {
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
        self.stack.pop().unwrap()
    }
}

#[cfg(test)]
mod compare_tests {
    use std::collections::HashMap;

    use crate::tree::NodePos;

    use super::StackMachine;

    #[test]
    fn test1() {
        let twig_of_left_max_level = 4;

        let mut pre_map = HashMap::<NodePos, [u8; 32]>::new();

        // origin entry
        pre_map.insert(NodePos::pos(0, 0), [0; 32]);
        for level in 0..twig_of_left_max_level - 1 {
            pre_map.insert(NodePos::pos(level, 1), [0; 32]);
        }
        // origin entry
        let level0_stride = 1 << twig_of_left_max_level - 1;
        // every level start from 0
        pre_map.insert(NodePos::pos(0, level0_stride - 1), [0; 32]);
        for level in 0..twig_of_left_max_level - 1 {
            let stride = 1 << twig_of_left_max_level - level - 1;
            pre_map.insert(NodePos::pos(level, stride - 1 - 1), [0; 32]);
        }

        let mut post_map = HashMap::<NodePos, [u8; 32]>::new();
        // change
        post_map.insert(NodePos::pos(0, level0_stride), [1; 32]);

        let mut machine = StackMachine::new(pre_map, post_map, twig_of_left_max_level);
        let root = machine.run();
        println!("root: {:?}", root);
    }
}
