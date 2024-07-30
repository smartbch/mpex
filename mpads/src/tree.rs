use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::{fmt, mem};
use std::{fs, thread};

use crate::def::{
    ENTRIES_PATH, FIRST_LEVEL_ABOVE_TWIG, LEAF_COUNT_IN_TWIG, MAX_TREE_LEVEL, MIN_PRUNE_COUNT,
    NODE_SHARD_COUNT, TWIG_MASK, TWIG_PATH, TWIG_ROOT_LEVEL, TWIG_SHARD_COUNT, TWIG_SHIFT,
};
use crate::entryfile::{EntryFile, EntryFileWriter};
use crate::twig::{ActiveBits, TwigMT, NULL_NODE_IN_HIGHER_TREE};
use crate::twigfile::{TwigFile, TwigFileWriter};
use crate::utils::hasher;
use crate::{entry, recover, twig};
use crate::{proof, twigfile};

/*
             ____TwigRoot___                   Level_12
            /               \
           /                 \
1       leftRoot              activeBitsMTL3   Level_11
2       Level_10        2     activeBitsMTL2
4       Level_9         4     activeBitsMTL1
8       Level_8    8*32bytes  activeBits
16      Level_7
32      Level_6
64      Level_5
128     Level_4
256     Level_3
512     Level_2
1024    Level_1
2048    Level_0
*/

/*         1
     2             3
  4     5       6     7
 8 9   a b     c d   e f
*/

#[derive(Copy, Clone, Eq, Hash, PartialEq)]
pub struct NodePos(u64);

impl NodePos {
    pub fn new(pos: u64) -> NodePos {
        NodePos(pos)
    }
    pub fn pos(level: u64, n: u64) -> NodePos {
        if level >= u8::MAX as u64 {
            panic!("level is too large");
        }
        if n >= (1 << 56) {
            panic!("n is too large");
        }
        NodePos((level << 56) | n)
    }
    pub fn level(&self) -> u64 {
        self.0 >> 56 // extract the high 8 bits
    }
    pub fn nth(&self) -> u64 {
        (self.0 << 8) >> 8 // extract the low 56 bits
    }
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl fmt::Debug for NodePos {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "NodePos: {:?} {{ level: {}, nth: {} }}",
            self.as_u64(),
            self.level(),
            self.nth()
        )
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EdgeNode {
    pub pos: NodePos,
    pub value: [u8; 32],
}

#[derive(Clone)]
pub struct UpperTree {
    // the nodes in high level tree (higher than twigs)
    // this variable can be recovered from saved edge nodes and activeTwigs
    pub nodes: Vec<Vec<HashMap<NodePos, [u8; 32]>>>, //MaxUpperLevel*NodeShardCount maps
    // this variable can be recovered from entry file
    pub active_twig_shards: Vec<HashMap<u64, twig::Twig>>, //TwigShardCount maps
}

impl UpperTree {
    pub fn empty() -> Self {
        Self {
            nodes: Vec::with_capacity(0),
            active_twig_shards: Vec::with_capacity(0),
        }
    }

    pub fn new() -> Self {
        let node_shards = vec![HashMap::<NodePos, [u8; 32]>::new(); NODE_SHARD_COUNT];
        let nodes = vec![node_shards; MAX_TREE_LEVEL];
        let active_twig_shards = vec![HashMap::<u64, twig::Twig>::new(); TWIG_SHARD_COUNT];

        Self {
            nodes,
            active_twig_shards,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.len() == 0
    }

    pub fn add_twigs(&mut self, twig_map: HashMap<u64, twig::Twig>) {
        for (twig_id, twig) in twig_map {
            let (shard_idx, key) = get_shard_idx_and_key(twig_id);
            self.active_twig_shards[shard_idx].insert(key, twig);
        }
    }

    pub fn get_twig(&mut self, twig_id: u64) -> Option<&mut twig::Twig> {
        let (shard_idx, key) = get_shard_idx_and_key(twig_id);
        self.active_twig_shards[shard_idx].get_mut(&key)
    }

    pub fn get_twig_root(&self, n: u64) -> Option<&[u8; 32]> {
        let (shard_idx, key) = get_shard_idx_and_key(n);
        let twig_option = self.active_twig_shards[shard_idx].get(&key);
        match twig_option {
            Some(v) => Some(&v.twig_root),
            None => {
                // the twig has been evicted
                let pos = NodePos::pos(TWIG_ROOT_LEVEL as u64, n);
                self.get_node(pos)
            }
        }
    }

    pub fn set_node_copy(&mut self, pos: NodePos, node: &[u8; 32]) {
        let mut n = [0; 32];
        n.copy_from_slice(node);
        // self.nodes[pos.level() as usize][pos.nth() as usize % NODE_SHARD_COUNT].insert(pos, n);
        self.set_node(pos, n);
    }

    pub fn set_node(&mut self, pos: NodePos, node: [u8; 32]) {
        self.nodes[pos.level() as usize][pos.nth() as usize % NODE_SHARD_COUNT].insert(pos, node);
    }

    pub fn get_node(&self, pos: NodePos) -> Option<&[u8; 32]> {
        self.nodes[pos.level() as usize][pos.nth() as usize % NODE_SHARD_COUNT].get(&pos)
    }

    fn delete_node(&mut self, pos: NodePos) {
        self.nodes[pos.level() as usize][pos.nth() as usize % NODE_SHARD_COUNT].remove(&pos);
    }

    pub fn prune_nodes(&mut self, start: u64, end: u64, youngest_twig_id: u64) -> Vec<u8> {
        let max_level = calc_max_level(youngest_twig_id);
        self.remove_useless_nodes(start, end, max_level);
        recover::edge_nodes_to_bytes(&self.get_edge_nodes(end, max_level))
    }

    fn remove_useless_nodes(&mut self, start: u64, end: u64, max_level: i64) {
        let mut cur_start = start;
        let mut cur_end = end;
        for level in TWIG_ROOT_LEVEL..=max_level {
            let mut end_back = cur_end;
            if cur_end % 2 != 0 && level != TWIG_ROOT_LEVEL {
                end_back -= 1;
            }

            let mut start_back = cur_start;
            if start_back > 0 {
                // minus 1 from start to delete some margin nodes
                start_back -= 1;
            }
            for i in start_back..end_back {
                let pos = NodePos::pos(level as u64, i);
                self.delete_node(pos);
            }
            cur_start >>= 1;
            cur_end >>= 1;
        }
    }

    fn get_edge_nodes(&self, end: u64, max_level: i64) -> Vec<EdgeNode> {
        let mut cur_end = end;
        let mut new_edge_nodes = Vec::new();
        for level in TWIG_ROOT_LEVEL..=max_level {
            let mut end_back = cur_end;
            if cur_end % 2 != 0 && level != TWIG_ROOT_LEVEL {
                end_back -= 1;
            }
            let pos = NodePos::pos(level as u64, end_back);
            if let Some(v) = self.get_node(pos) {
                new_edge_nodes.push(EdgeNode { pos, value: *v });
            } else {
                panic!("What? can not find {}-{}", level, end);
            }
            cur_end >>= 1;
        }
        new_edge_nodes
    }

    pub fn sync_nodes_by_level(
        &mut self,
        level: i64,
        n_list: Vec<u64>,
        youngest_twig_id: u64,
    ) -> Vec<u64> {
        let max_n = max_n_at_level(youngest_twig_id, level);
        let pos = NodePos::pos(level as u64, max_n);
        self.set_node_copy(pos, &NULL_NODE_IN_HIGHER_TREE[level as usize]);
        let pos = NodePos::pos(level as u64, max_n + 1);
        self.set_node_copy(pos, &NULL_NODE_IN_HIGHER_TREE[level as usize]);

        // take written_nodes out from self.nodes
        self.nodes.push(Vec::new()); // push a placeholder that will be removed
        let mut written_nodes = self.nodes.swap_remove(level as usize);

        let mut new_list = Vec::with_capacity(n_list.len());
        thread::scope(|s| {
            let mut node_threads = Vec::with_capacity(NODE_SHARD_COUNT);
            for shard_id in (0..NODE_SHARD_COUNT).rev() {
                let nodes = written_nodes.pop().unwrap(); //taken out from written_nodes
                let n_list = &n_list[..];
                let upper_tree = &*self; // change a mutable borrow to an immutable borrow
                node_threads
                    .push(s.spawn(move || do_sync_job(upper_tree, nodes, level, shard_id, n_list)));
            }
            for i in &n_list {
                if new_list.is_empty() || *new_list.last().unwrap() != i / 2 {
                    new_list.push(i / 2);
                }
            }
            for _ in 0..NODE_SHARD_COUNT {
                let node_thread = node_threads.pop().unwrap();
                written_nodes.push(node_thread.join().unwrap()); // return back to written_nodes
            }
        });

        // return written_nodes back to self.nodes
        self.nodes.push(written_nodes);
        self.nodes.swap_remove(level as usize); // the placeholder is removed

        new_list
    }

    pub fn sync_upper_nodes(
        &mut self,
        mut n_list: Vec<u64>,
        youngest_twig_id: u64,
    ) -> (Vec<u64>, [u8; 32]) {
        let max_level = calc_max_level(youngest_twig_id);
        for level in FIRST_LEVEL_ABOVE_TWIG..=max_level {
            n_list = self.sync_nodes_by_level(level, n_list, youngest_twig_id);
        }
        let root = *self.get_node(NodePos::pos(max_level as u64, 0)).unwrap();
        (n_list, root)
    }

    pub fn evict_twigs(
        &mut self,
        n_list: Vec<u64>,
        twig_evict_start: u64,
        twig_evict_end: u64,
    ) -> Vec<u64> {
        let new_list = self.sync_mt_for_active_bits_phase2(n_list);
        // run the pending twig-eviction jobs
        // they were not evicted earlier because sync_mt_for_active_bits_phase2 needs their content
        for twig_id in twig_evict_start..twig_evict_end {
            // evict the twig and store its twigRoot in nodes
            let pos = NodePos::pos(TWIG_ROOT_LEVEL as u64, twig_id);
            let twig_root = self.get_twig(twig_id).unwrap().twig_root.clone();
            self.set_node_copy(pos, &twig_root);
            let (shard_idx, key) = get_shard_idx_and_key(twig_id);
            self.active_twig_shards[shard_idx].remove(&key);
        }
        new_list
    }

    pub fn sync_mt_for_active_bits_phase2(&mut self, mut n_list: Vec<u64>) -> Vec<u64> {
        let mut new_list = Vec::with_capacity(n_list.len());
        thread::scope(|s| {
            let mut twig_threads = Vec::with_capacity(TWIG_SHARD_COUNT);
            for shard_id in (0..TWIG_SHARD_COUNT).rev() {
                let n_list = &n_list;
                // we take twig_shard out from active_twig_shards
                let mut twig_shard = self.active_twig_shards.pop().unwrap();
                twig_threads.push(s.spawn(move || {
                    for i in n_list {
                        let twig_id = i >> 1;
                        let (s, k) = get_shard_idx_and_key(twig_id);
                        if s != shard_id {
                            continue;
                        };
                        twig_shard.get_mut(&k).unwrap().sync_l2((i & 1) as i32);
                    }
                    twig_shard
                }));
            }

            for i in &n_list {
                if new_list.is_empty() || *new_list.last().unwrap() != i / 2 {
                    new_list.push(i / 2);
                }
            }
            // we return twig_shard back into active_twig_shards
            for _ in 0..TWIG_SHARD_COUNT {
                let twig_thread = twig_threads.pop().unwrap();
                self.active_twig_shards.push(twig_thread.join().unwrap());
            }
        });

        mem::swap(&mut new_list, &mut n_list);
        new_list.clear();
        thread::scope(|s| {
            let mut twig_threads = Vec::with_capacity(TWIG_SHARD_COUNT);
            for shard_id in (0..TWIG_SHARD_COUNT).rev() {
                let n_list = &n_list;
                // we take twig_shard out from active_twig_shards
                let mut twig_shard = self.active_twig_shards.pop().unwrap();
                twig_threads.push(s.spawn(move || {
                    for twig_id in n_list {
                        let (s, k) = get_shard_idx_and_key(*twig_id);
                        if s != shard_id {
                            continue;
                        };
                        twig_shard.get_mut(&k).unwrap().sync_l3();
                        twig_shard.get_mut(&k).unwrap().sync_top();
                    }
                    twig_shard
                }));
            }

            for i in &n_list {
                if new_list.is_empty() || *new_list.last().unwrap() != i / 2 {
                    new_list.push(i / 2);
                }
            }
            // we return twig_shard back into active_twig_shards
            for _ in 0..TWIG_SHARD_COUNT {
                let twig_thread = twig_threads.pop().unwrap();
                self.active_twig_shards.push(twig_thread.join().unwrap());
            }
        });

        new_list
    }
}

fn do_sync_job(
    upper_tree: &UpperTree,
    mut nodes: HashMap<NodePos, [u8; 32]>,
    level: i64,
    shard_id: usize,
    n_list: &[u64],
) -> HashMap<NodePos, [u8; 32]> {
    let child_nodes = upper_tree.nodes.get((level - 1) as usize).unwrap();
    for &i in n_list {
        if i as usize % NODE_SHARD_COUNT != shard_id {
            continue;
        }
        let pos = NodePos::pos(level as u64, i);
        if level == FIRST_LEVEL_ABOVE_TWIG {
            let left_option = upper_tree.get_twig_root(2 * i);
            let left = match left_option {
                Some(v) => v,
                None => panic!("Cannot find left twig root {}", 2 * i),
            };
            let right_option = upper_tree.get_twig_root(2 * i + 1);
            let mut right = [0u8; 32];
            match right_option {
                Some(v) => {
                    right.copy_from_slice(v);
                }
                None => {
                    right.copy_from_slice(&twig::NULL_TWIG.twig_root[..]);
                }
            };
            let mut hash = [0u8; 32];
            hasher::node_hash_inplace(level as u8 - 1, &mut hash, left, &right);
            nodes.insert(pos, hash);
        } else {
            let node_pos_l = NodePos::pos((level - 1) as u64, 2 * i);
            let node_pos_r = NodePos::pos((level - 1) as u64, 2 * i + 1);
            let sl = node_pos_l.nth() as usize % NODE_SHARD_COUNT;
            let sr = node_pos_r.nth() as usize % NODE_SHARD_COUNT;
            let node_l = match child_nodes[sl].get(&node_pos_l) {
                Some(v) => v,
                None => {
                    panic!(
                        "Cannot find left child {}-{} {}-{} {} {:?}",
                        level,
                        i,
                        level - 1,
                        2 * i,
                        2 * i + 1,
                        node_pos_l
                    );
                }
            };

            let node_r = match child_nodes[sr].get(&node_pos_r) {
                Some(v) => v,
                None => {
                    panic!(
                        "Cannot find right child {}-{} {}-{} {} {:?}",
                        level,
                        i,
                        level - 1,
                        2 * i,
                        2 * i + 1,
                        node_pos_r
                    )
                }
            };

            let mut hash = [0u8; 32];
            hasher::node_hash_inplace(level as u8 - 1, &mut hash, node_l, node_r);
            nodes.insert(pos, hash);
        }
    }
    nodes
}

pub struct Tree {
    pub my_shard_id: usize,

    pub upper_tree: UpperTree,
    pub new_twig_map: HashMap<u64, twig::Twig>,

    pub entry_file_wr: EntryFileWriter,
    pub twig_file_wr: TwigFileWriter,
    pub dir_name: String,

    // these variables can be recovered from entry file
    pub youngest_twig_id: u64,
    // pub active_bit_shards: [HashMap<u64, [u8; 256]>; TWIG_SHARD_COUNT],
    pub active_bit_shards: Vec<HashMap<u64, ActiveBits>>,
    pub mtree_for_youngest_twig: Box<TwigMT>,

    // The following variables are only used during the execution of one block
    pub mtree_for_yt_change_start: i32,
    pub mtree_for_yt_change_end: i32,
    touched_pos_of_512b: HashSet<u64>,
}

impl Tree {
    pub fn new_blank(
        shard_id: usize,
        buffer_size: usize,
        segment_size: i64,
        dir_name: String,
        suffix: String,
    ) -> Self {
        let dir_entry = format!("{}/{}{}", dir_name, ENTRIES_PATH, suffix);
        let _ = fs::create_dir_all(&dir_entry);
        let dir_twig = format!("{}/{}{}", dir_name, TWIG_PATH, suffix);
        let _ = fs::create_dir_all(&dir_twig);
        let twig_arc = Arc::new(TwigFile::new(buffer_size, segment_size, dir_twig));
        let ef_arc = Arc::new(EntryFile::new(buffer_size, segment_size, dir_entry));

        Self {
            my_shard_id: shard_id,
            upper_tree: UpperTree::new(),
            new_twig_map: HashMap::new(),
            entry_file_wr: EntryFileWriter::new(ef_arc, buffer_size),
            twig_file_wr: TwigFileWriter::new(twig_arc, buffer_size),
            dir_name,
            youngest_twig_id: 0,
            active_bit_shards: vec![HashMap::new(); TWIG_SHARD_COUNT],
            mtree_for_youngest_twig: twig::NULL_MT_FOR_TWIG.clone(),
            mtree_for_yt_change_start: -1,
            mtree_for_yt_change_end: -1,
            touched_pos_of_512b: HashSet::new(),
        }
    }

    pub fn new(
        shard_id: usize,
        buffer_size: usize,
        segment_size: i64,
        dir_name: String,
        suffix: String,
    ) -> Self {
        let mut tree = Self::new_blank(shard_id, buffer_size, segment_size, dir_name, suffix);

        tree.new_twig_map.insert(0, twig::NULL_TWIG.clone());
        tree.upper_tree
            .set_node(NodePos::pos(FIRST_LEVEL_ABOVE_TWIG as u64, 0), [0; 32]);
        tree.upper_tree.active_twig_shards[0].insert(0, twig::NULL_TWIG.clone());
        tree.active_bit_shards[0].insert(0, twig::NULL_ACTIVE_BITS.clone());

        tree
    }

    pub fn close(&mut self) {
        // Close files
        self.entry_file_wr.entry_file.close();
        self.twig_file_wr.twig_file.close();
    }

    pub fn get_file_sizes(&self) -> (i64, i64) {
        (
            self.entry_file_wr.entry_file.size(),
            self.twig_file_wr.twig_file.hp_file.size(),
        )
    }

    pub fn truncate_files(&self, entry_file_size: i64, twig_file_size: i64) {
        self.entry_file_wr.entry_file.truncate(entry_file_size);
        self.twig_file_wr.twig_file.truncate(twig_file_size);
    }

    pub fn get_active_bits(&self, twig_id: u64) -> &ActiveBits {
        let (shard_idx, key) = get_shard_idx_and_key(twig_id);
        match self.active_bit_shards[shard_idx].get(&key) {
            Some(v) => v,
            None => panic!("cannot find twig {}", twig_id),
        }
    }

    fn get_active_bits_mut(&mut self, twig_id: u64) -> &mut ActiveBits {
        let (shard_idx, key) = get_shard_idx_and_key(twig_id);
        self.active_bit_shards[shard_idx].get_mut(&key).unwrap()
    }

    pub fn get_active_bit(&self, sn: u64) -> bool {
        let twig_id = sn >> TWIG_SHIFT;
        let pos = sn as u32 & TWIG_MASK;
        self.get_active_bits(twig_id).get_bit(pos)
    }

    pub fn set_entry_activiation(&mut self, sn: u64, active: bool) {
        let twig_id = sn >> TWIG_SHIFT;
        let pos = sn as u32 & TWIG_MASK;
        let active_bits = self.get_active_bits_mut(twig_id);
        if active {
            active_bits.set_bit(pos);
        } else {
            active_bits.clear_bit(pos);
        }
        self.touch_pos(sn);
    }

    pub fn touch_pos(&mut self, sn: u64) {
        self.touched_pos_of_512b.insert(sn / 512);
    }

    pub fn clear_touched_pos(&mut self) {
        self.touched_pos_of_512b.clear();
    }

    pub fn active_entry(&mut self, sn: u64) {
        self.set_entry_activiation(sn, true);
    }

    pub fn deactive_entry(&mut self, sn: u64) {
        self.set_entry_activiation(sn, false);
    }

    pub fn append_entry(&mut self, entry_bz: &entry::EntryBz) -> i64 {
        let sn = entry_bz.serial_number();
        self.active_entry(sn);

        let twig_id = sn >> TWIG_SHIFT;
        self.youngest_twig_id = twig_id;
        // record change_start/change_end for endblock sync
        let position = sn as u32 & TWIG_MASK;
        if self.mtree_for_yt_change_start == -1 {
            self.mtree_for_yt_change_start = position as i32;
        } else if self.mtree_for_yt_change_end + 1 != position as i32 {
            panic!("non-increasing position!");
        }
        self.mtree_for_yt_change_end = position as i32;

        let pos = self.entry_file_wr.append(&entry_bz);
        self.mtree_for_youngest_twig[(LEAF_COUNT_IN_TWIG + position) as usize]
            .copy_from_slice(entry_bz.hash().as_slice());

        if position == TWIG_MASK {
            // when this is the last entry of current twig
            // write the merkle tree of youngest twig to twig_file
            self.sync_mt_for_youngest_twig(false);
            self.twig_file_wr.append_twig(
                &self.mtree_for_youngest_twig[..],
                pos + entry_bz.len() as i64,
            );
            // allocate new twig as youngest twig
            self.youngest_twig_id += 1;
            let (s, i) = get_shard_idx_and_key(self.youngest_twig_id);
            self.new_twig_map
                .insert(self.youngest_twig_id, twig::NULL_TWIG.clone());
            self.active_bit_shards[s].insert(i, twig::NULL_ACTIVE_BITS.clone());

            self.mtree_for_youngest_twig
                .copy_from_slice(&twig::NULL_MT_FOR_TWIG[..]);
            self.touch_pos(sn + 1)
        }
        pos
    }

    pub fn prune_twigs(&mut self, start_id: u64, end_id: u64) {
        if end_id - start_id < MIN_PRUNE_COUNT {
            panic!(
                "The count of pruned twigs is too small: {}",
                end_id - start_id
            );
        }

        let end_pos = self.twig_file_wr.twig_file.get_first_entry_pos(end_id);
        self.entry_file_wr.entry_file.prune_head(end_pos);
        self.twig_file_wr
            .twig_file
            .prune_head((end_id * twigfile::TWIG_SIZE) as i64);
    }

    pub fn flush_files(&mut self, twig_delete_start: u64, twig_delete_end: u64) -> Vec<u64> {
        let mut entry_file_tmp = self.entry_file_wr.temp_clone();
        let mut twig_file_tmp = self.twig_file_wr.temp_clone();
        mem::swap(&mut entry_file_tmp, &mut self.entry_file_wr);
        mem::swap(&mut twig_file_tmp, &mut self.twig_file_wr);
        // run flushing in a threads such that sync_* won't be blocked
        let ef_handler = thread::spawn(move || {
            entry_file_tmp.flush();
            entry_file_tmp
        });
        let tf_handler = thread::spawn(move || {
            twig_file_tmp.flush();
            twig_file_tmp
        });
        self.sync_mt_for_youngest_twig(false);
        let youngest_twig = self.new_twig_map.get(&self.youngest_twig_id).unwrap();
        let mut twig_map = HashMap::new();
        twig_map.insert(self.youngest_twig_id, youngest_twig.clone());
        mem::swap(&mut self.new_twig_map, &mut twig_map);
        //add new_twig_map's old content to upper_tree
        self.upper_tree.add_twigs(twig_map);
        //now, new_twig_map only contains one member: youngest_twig.clone()

        let n_list = self.sync_mt_for_active_bits_phase1();
        for twig_id in twig_delete_start..twig_delete_end {
            let (shard_idx, key) = get_shard_idx_and_key(twig_id);
            self.active_bit_shards[shard_idx].remove(&key);
        }
        self.touched_pos_of_512b.clear();
        entry_file_tmp = ef_handler.join().unwrap();
        twig_file_tmp = tf_handler.join().unwrap();
        mem::swap(&mut entry_file_tmp, &mut self.entry_file_wr);
        mem::swap(&mut twig_file_tmp, &mut self.twig_file_wr);
        n_list
    }

    pub fn sync_mt_for_active_bits_phase1(&mut self) -> Vec<u64> {
        let mut n_list = self
            .touched_pos_of_512b
            .iter()
            .cloned()
            .collect::<Vec<u64>>();
        n_list.sort();

        let mut new_list = Vec::with_capacity(n_list.len());
        return thread::scope(|s| {
            let mut twig_threads = Vec::with_capacity(TWIG_SHARD_COUNT);
            for shard_id in (0..TWIG_SHARD_COUNT).rev() {
                let active_bits_shards = &self.active_bit_shards;
                let n_list = &n_list;
                // we take twig_shard out from active_twig_shards
                let mut twig_shard = self.upper_tree.active_twig_shards.pop().unwrap();
                twig_threads.push(s.spawn(move || {
                    for i in n_list {
                        let twig_id = i >> 2;
                        let (s, k) = get_shard_idx_and_key(twig_id);
                        if s != shard_id {
                            continue;
                        }
                        let active_bits = active_bits_shards[s].get(&k).unwrap();
                        twig_shard
                            .get_mut(&k)
                            .unwrap()
                            .sync_l1((i & 3) as i32, active_bits);
                    }
                    twig_shard
                }));
            }
            for i in &n_list {
                if new_list.is_empty() || *new_list.last().unwrap() != i / 2 {
                    new_list.push(i / 2);
                }
            }
            // we return twig_shard back into active_twig_shards
            for _ in 0..TWIG_SHARD_COUNT {
                let twig_thread = twig_threads.pop().unwrap();
                self.upper_tree
                    .active_twig_shards
                    .push(twig_thread.join().unwrap());
            }
            new_list
        });
    }

    pub fn sync_mt_for_youngest_twig(&mut self, recover_mode: bool) {
        if self.mtree_for_yt_change_start == -1 {
            return;
        }
        sync_mtree(
            &mut self.mtree_for_youngest_twig,
            self.mtree_for_yt_change_start,
            self.mtree_for_yt_change_end,
        );
        self.mtree_for_yt_change_start = -1;
        self.mtree_for_yt_change_end = 0;
        let youngest_twig;
        if recover_mode {
            youngest_twig = self.upper_tree.get_twig(self.youngest_twig_id).unwrap();
        } else {
            youngest_twig = self.new_twig_map.get_mut(&self.youngest_twig_id).unwrap();
        }
        youngest_twig
            .left_root
            .copy_from_slice(&self.mtree_for_youngest_twig[1]);
    }

    pub fn load_mt_for_non_youngest_twig(&mut self, twig_id: u64) {
        if self.mtree_for_yt_change_start == -1 {
            return;
        }
        self.mtree_for_yt_change_start = -1;
        self.mtree_for_yt_change_end = 0;
        let active_twig = self.upper_tree.get_twig(self.youngest_twig_id).unwrap();
        self.twig_file_wr
            .twig_file
            .get_hash_root(twig_id, &mut active_twig.left_root);
    }

    fn get_upper_path_and_root(&self, twig_id: u64) -> (Vec<proof::ProofNode>, [u8; 32]) {
        let max_level = calc_max_level(self.youngest_twig_id);

        let mut peer_hash = [0u8; 32];
        // use '^ 1' to flip the lowest bit to get sibling
        if let Some(v) = self.upper_tree.get_twig_root(twig_id ^ 1) {
            peer_hash.copy_from_slice(v);
        } else {
            peer_hash.copy_from_slice(&twig::NULL_TWIG.twig_root[..]);
        }

        let mut self_hash = [0u8; 32];
        if let Some(v) = self.upper_tree.get_twig_root(twig_id) {
            self_hash.copy_from_slice(v);
        } else {
            return (Vec::new(), [0; 32]);
        }

        let mut upper_path = Vec::with_capacity((max_level - FIRST_LEVEL_ABOVE_TWIG + 1) as usize);
        upper_path.push(proof::ProofNode {
            self_hash,
            peer_hash,
            peer_at_left: (twig_id & 1) != 0, //twig_id's lowest bit == 1 so the peer is at left
        });

        let mut n = twig_id >> 1;
        for level in FIRST_LEVEL_ABOVE_TWIG..max_level {
            let peer_at_left = (n & 1) != 0;

            let snode = match self.upper_tree.get_node(NodePos::pos(level as u64, n)) {
                Some(v) => *v,
                None => panic!("Cannot find node"),
            };
            let pnode = match self.upper_tree.get_node(NodePos::pos(level as u64, n ^ 1)) {
                Some(v) => *v,
                None => panic!("Cannot find node"),
            };
            upper_path.push(proof::ProofNode {
                self_hash: snode,
                peer_hash: pnode,
                peer_at_left,
            });
            n = n >> 1;
        }

        let root_option = self.upper_tree.get_node(NodePos::pos(max_level as u64, 0));
        let root = match root_option {
            Some(v) => *v,
            None => panic!("cannot find node {}-{}", max_level, 0),
        };

        (upper_path, root)
    }

    pub fn get_proof(&self, sn: u64) -> proof::ProofPath {
        let twig_id = sn >> TWIG_SHIFT;
        let mut path = proof::ProofPath::new();
        path.serial_num = sn;

        if twig_id > self.youngest_twig_id {
            panic!("twig_id > self.youngest_twig_id");
        }

        (path.upper_path, path.root) = self.get_upper_path_and_root(twig_id);
        if path.upper_path.is_empty() {
            panic!("Cannot find upper path");
        }

        if twig_id == self.youngest_twig_id {
            path.left_of_twig = proof::get_left_path_in_mem(&self.mtree_for_youngest_twig, sn);
        } else {
            path.left_of_twig =
                proof::get_left_path_on_disk(&*self.twig_file_wr.twig_file, twig_id, sn);
        }
        let (s, k) = get_shard_idx_and_key(twig_id);
        let twig = self.upper_tree.active_twig_shards[s]
            .get(&k)
            .unwrap_or(&twig::NULL_TWIG);
        let active_bits = self.active_bit_shards[s]
            .get(&k)
            .unwrap_or(&twig::NULL_ACTIVE_BITS);
        path.right_of_twig = proof::get_right_path(twig, active_bits, sn);

        path
    }

    pub fn get_proof_map(&self, sn: u64, proof_map: &mut HashMap<NodePos, [u8; 32]>) -> i64 {
        let twig_id = sn >> TWIG_SHIFT;
        let max_level_of_left_of_twig = 12;

        // left of twig
        let mut mtree_idx_and_node_pos = vec![];
        let nth_at_level0 = sn & TWIG_MASK as u64;
        mtree_idx_and_node_pos.push((
            2048 + nth_at_level0,
            NodePos::pos(0, 2048 * 2 * twig_id + nth_at_level0 as u64),
        ));
        for level in 0..(max_level_of_left_of_twig - 1) {
            let nth = (nth_at_level0 >> level) as usize;
            let peer_nth = nth ^ 1;
            let stride = 2048 >> level;
            // let node_pos = NodePos::pos(level, peer_nth as u64);
            // if proof_map.contains_key(&node_pos) {
            //     continue;
            // }
            mtree_idx_and_node_pos.push((
                stride + peer_nth as u64,
                NodePos::pos(level, stride * 2 * twig_id + peer_nth as u64),
            ));
        }
        let is_youngest_twig_id = twig_id == self.youngest_twig_id;
        let mut cache: HashMap<i64, [u8; 32]> = HashMap::with_capacity(8);
        for (idx, pos) in mtree_idx_and_node_pos {
            let mut hash = [0u8; 32];
            if is_youngest_twig_id {
                hash = self.mtree_for_youngest_twig[idx as usize];
            } else {
                self.twig_file_wr
                    .twig_file
                    .get_hash_node(twig_id, idx as i64, &mut cache, &mut hash);
            }
            proof_map.insert(pos, hash);
        }

        // right of twig
        let (s, k) = get_shard_idx_and_key(twig_id);
        let active_bits = self.active_bit_shards[s]
            .get(&k)
            .unwrap_or(&twig::NULL_ACTIVE_BITS);

        let mut self_id = sn & TWIG_MASK as u64;
        self_id = self_id / 256;
        // add self
        let hash = active_bits.get_bits(self_id as usize, 32);
        proof_map.insert(
            NodePos::pos(8, 8 * 2 * twig_id + 8 + self_id),
            hash.try_into().unwrap(),
        );
        // add peer
        let peer = self_id ^ 1;
        let hash = active_bits.get_bits(peer as usize, 32);
        proof_map.insert(
            NodePos::pos(8, 8 * 2 * twig_id + 8 + peer),
            hash.try_into().unwrap(),
        );
        // add parent peer
        let twig = self.upper_tree.active_twig_shards[s]
            .get(&k)
            .unwrap_or(&twig::NULL_TWIG);
        println!(
            "twig_{} left_root: {:?} twig_root: {:?}",
            twig_id, twig.left_root, twig.twig_root
        );
        let peer = (self_id >> 1) ^ 1;
        let hash = twig.active_bits_mtl1[peer as usize];
        proof_map.insert(
            NodePos::pos(9, 4 * 2 * twig_id + 4 + peer),
            hash.try_into().unwrap(),
        );
        // add parent'parent peer
        let peer = (self_id >> 2) ^ 1;
        let hash = twig.active_bits_mtl2[peer as usize];
        proof_map.insert(
            NodePos::pos(10, 2 * 2 * twig_id + 2 + peer),
            hash.try_into().unwrap(),
        );

        // upper tree
        let max_level = calc_max_level(self.youngest_twig_id);
        let mut peer_hash = [0u8; 32];
        let peer_twig_nth = twig_id ^ 1;
        if let Some(v) = self.upper_tree.get_twig_root(peer_twig_nth) {
            peer_hash.copy_from_slice(v);
        } else {
            peer_hash.copy_from_slice(&twig::NULL_TWIG.twig_root[..]);
        }
        // add peer twig root
        proof_map.insert(NodePos::pos(12, peer_twig_nth as u64), peer_hash);
        // add ancestor
        let mut upper_peer_nth = twig_id >> 1;
        for level in FIRST_LEVEL_ABOVE_TWIG..max_level {
            upper_peer_nth = upper_peer_nth ^ 1;
            let hash = self
                .upper_tree
                .get_node(NodePos::pos(level as u64, upper_peer_nth))
                .unwrap();
            proof_map.insert(NodePos::pos(level as u64, upper_peer_nth as u64), *hash);
            upper_peer_nth = upper_peer_nth >> 1;
        }

        println!(
            "root: {:?}",
            self.upper_tree
                .get_node(NodePos::pos(max_level as u64, 0))
                .unwrap()
        );

        max_level
    }
}

pub fn sync_mtree(mtree: &mut TwigMT, start: i32, end: i32) {
    let mut cur_start = start;
    let mut cur_end = end;
    let mut level = 0;
    let mut base = LEAF_COUNT_IN_TWIG as i32;
    let mut node = [0u8; 32];
    while base >= 2 {
        let mut end_round = cur_end;
        if cur_end % 2 == 1 {
            end_round += 1;
        };
        let mut j = (cur_start >> 1) << 1; //clear the lowest bit of cur_start
        while j <= end_round && j + 1 < base {
            let i = (base + j) as usize;
            hasher::node_hash_inplace(level, &mut node[..], &mtree[i], &mtree[i + 1]);
            mtree[i / 2].copy_from_slice(&node[..]);
            j += 2;
        }
        cur_start >>= 1;
        cur_end >>= 1;
        level += 1;
        base >>= 1;
    }
}

pub fn calc_max_level(youngest_twig_id: u64) -> i64 {
    FIRST_LEVEL_ABOVE_TWIG + 63 - youngest_twig_id.leading_zeros() as i64
}

pub fn max_n_at_level(youngest_twig_id: u64, level: i64) -> u64 {
    if level < FIRST_LEVEL_ABOVE_TWIG {
        panic!("level is too small");
    }
    let shift = level - FIRST_LEVEL_ABOVE_TWIG + 1;
    youngest_twig_id >> shift
}

pub fn get_shard_idx_and_key(twig_id: u64) -> (usize, u64) {
    let idx = twig_id as usize % TWIG_SHARD_COUNT;
    let key = twig_id / TWIG_SHARD_COUNT as u64;
    (idx, key)
}
