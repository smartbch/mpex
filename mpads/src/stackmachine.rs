use std::{collections::HashMap, hash::Hash};

use rand_core::le;

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
        StackMachine {
            pre_map,
            post_map,
            stack: Vec::new(),
            max_level,
        }
    }

    fn combine(&mut self) {
        if self.stack.len() < 2 {
            panic!("stack len < 2");
        }

        let right = self.stack.pop().unwrap();
        let left = self.stack.pop().unwrap();

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

    fn push_by_node_pos(&mut self, node_pos: NodePos) {
        let pre_value = self.pre_map.remove(&node_pos).unwrap();
        let post_value = self.post_map.remove(&node_pos).unwrap_or(pre_value);
        let triple = Triple {
            old_value: format!("{:?}", pre_value),
            new_value: format!("{:?}", post_value),
            node_pos: node_pos,
        };
        self.stack.push(triple);
    }

    fn make_stack_or_execute(&mut self) -> Result<(), &'static str> {
        let cur_triple = self.stack.last().unwrap();
        // println!(
        //     "execute_node start-----cur_triple : {:?}",
        //     cur_triple.node_pos
        // );

        // Has got to the root
        if cur_triple.node_pos.level() == self.max_level - 1 {
            return Ok(());
        }

        let sibling_pos = get_sibling_pos(&cur_triple.node_pos);
        // println!("sibling_pos : {:?}", sibling_pos);

        // last two elements have the same parent
        if let Some(triple) = self.stack.get(self.stack.len().wrapping_sub(2)) {
            if sibling_pos == triple.node_pos {
                println!(
                    "----------execute_node: combine {:?} {:?}",
                    sibling_pos, cur_triple.node_pos
                );
                self.combine();
                self.make_stack_or_execute()?;
                return Ok(());
            }
        }

        if sibling_pos.level() != 0 {
            let children = get_children_pos_list(&sibling_pos);
            //  use children to calculate
            if let Some(_) = self.pre_map.get(&children.0) {
                // println!("execute_node: 2");
                self.push_by_node_pos(children.0);
                self.make_stack_or_execute()?;
                return Ok(());
            }
        }

        // println!("execute_node: 3");
        // find sibling then combine
        self.push_by_node_pos(sibling_pos);
        self.make_stack_or_execute()?;
        return Ok(());
    }

    fn run(&mut self) -> Triple {
        let min_left_node_nth_at_level0 = self
            .pre_map
            .keys()
            .filter(|pos| pos.level() == 0)
            .map(|pos| pos.nth())
            .min()
            .unwrap();
        self.push_by_node_pos(NodePos::pos(0, min_left_node_nth_at_level0));
        let _ = self.make_stack_or_execute();
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
