use anyhow::Result;
use byteorder::{ByteOrder, LittleEndian};
use std::collections::HashMap;

use crate::def::{FIRST_LEVEL_ABOVE_TWIG, TWIG_MASK, TWIG_ROOT_LEVEL};
use crate::twig::ActiveBits;
use crate::twig::{self, TwigMT};
use crate::twigfile;
use crate::utils::hasher;

#[derive(Clone, Copy, Debug)]
pub struct ProofNode {
    pub self_hash: [u8; 32],
    pub peer_hash: [u8; 32],
    pub peer_at_left: bool,
}

#[derive(Debug)]
pub struct ProofPath {
    pub left_of_twig: [ProofNode; 11],
    pub right_of_twig: [ProofNode; 3],
    pub upper_path: Vec<ProofNode>,
    pub serial_num: u64,
    pub root: [u8; 32],
}

const OTHER_NODE_COUNT: usize = 1 + 11 + 1 + 3 + 1;
impl ProofPath {
    pub fn new() -> Self {
        let empty_node = ProofNode {
            self_hash: [0; 32],
            peer_hash: [0; 32],
            peer_at_left: false,
        };
        ProofPath {
            left_of_twig: [empty_node; 11],
            right_of_twig: [empty_node; 3],
            upper_path: Vec::new(),
            serial_num: 0,
            root: [0; 32],
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut res = Vec::with_capacity(8 + (self.upper_path.len() + OTHER_NODE_COUNT) * 32);
        res.extend_from_slice(&self.serial_num.to_le_bytes()); // 8-byte
        res.extend_from_slice(&self.left_of_twig[0].self_hash); // 1
        for i in 0..self.left_of_twig.len() {
            //11
            res.extend_from_slice(&self.left_of_twig[i].peer_hash);
        }
        res.extend_from_slice(&self.right_of_twig[0].self_hash); //1
        for i in 0..self.right_of_twig.len() {
            //3
            res.extend_from_slice(&self.right_of_twig[i].peer_hash);
        }
        for i in 0..self.upper_path.len() {
            res.extend_from_slice(&self.upper_path[i].peer_hash);
        }
        res.extend_from_slice(&self.root); //1
        res
    }

    pub fn check(&mut self, complete: bool) -> Result<()> {
        for i in 0..self.left_of_twig.len() - 1 {
            let res = hasher::hash2x(
                i as u8,
                &self.left_of_twig[i].self_hash,
                &self.left_of_twig[i].peer_hash,
                self.left_of_twig[i].peer_at_left,
            );

            if complete {
                self.left_of_twig[i + 1].self_hash.copy_from_slice(&res);
            } else {
                if !res.eq(&self.left_of_twig[i + 1].self_hash) {
                    return Err(anyhow::anyhow!("Mismatch at left path, level: {}", i));
                }
            }
        }

        let leaf_mt_root = hasher::hash2x(
            10,
            &self.left_of_twig[10].self_hash,
            &self.left_of_twig[10].peer_hash,
            self.left_of_twig[10].peer_at_left,
        );

        for i in 0..2 {
            let res = hasher::hash2x(
                (i + 8) as u8,
                &self.right_of_twig[i].self_hash,
                &self.right_of_twig[i].peer_hash,
                self.right_of_twig[i].peer_at_left,
            );
            if complete {
                self.right_of_twig[i + 1].self_hash.copy_from_slice(&res);
            } else {
                if !res.eq(&self.right_of_twig[i + 1].self_hash) {
                    return Err(anyhow::anyhow!("Mismatch at right path, level: {}", i));
                }
            }
        }

        let active_bits_mt_l3 = hasher::hash2x(
            10,
            &self.right_of_twig[2].self_hash,
            &self.right_of_twig[2].peer_hash,
            self.right_of_twig[2].peer_at_left,
        );

        let twig_root = hasher::hash2(11, &leaf_mt_root, &active_bits_mt_l3);
        if complete {
            self.upper_path[0].self_hash.copy_from_slice(&twig_root);
        } else {
            if !twig_root.eq(&self.upper_path[0].self_hash) {
                return Err(anyhow::anyhow!("Mismatch at twig top"));
            }
        }

        for i in 0..self.upper_path.len() {
            let level = TWIG_ROOT_LEVEL as usize + i;
            let res = hasher::hash2x(
                level as u8,
                &self.upper_path[i].self_hash,
                &self.upper_path[i].peer_hash,
                self.upper_path[i].peer_at_left,
            );

            if i < self.upper_path.len() - 1 {
                if complete {
                    self.upper_path[i + 1].self_hash.copy_from_slice(&res);
                } else {
                    if !res.eq(&self.upper_path[i + 1].self_hash) {
                        return Err(anyhow::anyhow!("Mismatch at upper path, level: {}", level));
                    }
                }
            } else {
                if !res.eq(&self.root) {
                    return Err(anyhow::anyhow!("Mismatch at root"));
                }
            }
        }

        Ok(())
    }
}

pub fn bytes_to_proof_path(bz: &Vec<u8>) -> Result<ProofPath> {
    let n = bz.len() - 8;
    let upper_count: i32 = (n / 32 - OTHER_NODE_COUNT) as i32;
    if n % 32 != 0 || upper_count < 0 {
        return Err(anyhow::anyhow!("Invalid byte slice length: {}", bz.len()));
    }
    let mut upper_path = Vec::with_capacity(upper_count as usize);
    let empty_node = ProofNode {
        self_hash: [0; 32],
        peer_hash: [0; 32],
        peer_at_left: false,
    };
    let mut left_of_twig = [empty_node; 11];
    let mut right_of_twig = [empty_node; 3];
    let serial_num = LittleEndian::read_u64(&bz[0..8]);
    let mut bz = &bz[8..];
    left_of_twig[0].self_hash.copy_from_slice(&bz[..32]);
    bz = &bz[32..];
    for i in 0..left_of_twig.len() {
        left_of_twig[i].peer_hash.copy_from_slice(&bz[..32]);
        left_of_twig[i].peer_at_left = (serial_num >> i) & 1 == 1;
        bz = &bz[32..];
    }
    right_of_twig[0].self_hash.copy_from_slice(&bz[..32]);
    bz = &bz[32..];
    for i in 0..right_of_twig.len() {
        right_of_twig[i].peer_hash.copy_from_slice(&bz[..32]);
        right_of_twig[i].peer_at_left = (serial_num >> (8 + i)) & 1 == 1;
        bz = &bz[32..];
    }
    for i in 0..upper_count {
        let mut node = empty_node;
        node.peer_hash.copy_from_slice(&bz[..32]);
        node.peer_at_left = ((serial_num >> (FIRST_LEVEL_ABOVE_TWIG - 2 + i as i64)) & 1) == 1;
        upper_path.push(node);
        bz = &bz[32..];
    }
    let mut root = [0; 32];
    root.copy_from_slice(&bz[..32]);
    Ok(ProofPath {
        left_of_twig,
        right_of_twig,
        upper_path,
        serial_num,
        root,
    })
}

pub fn check_proof(path: &mut ProofPath) -> Result<Vec<u8>> {
    path.check(false)?;
    let bz = path.to_bytes();
    let mut path2 = bytes_to_proof_path(&bz)?;
    path2.check(true)?; //double check
    Ok(bz)
}

pub fn get_right_path(twig: &twig::Twig, active_bits: &ActiveBits, sn: u64) -> [ProofNode; 3] {
    let n = sn & TWIG_MASK as u64;
    let mut right = [ProofNode {
        self_hash: [0; 32],
        peer_hash: [0; 32],
        peer_at_left: false,
    }; 3];
    let self_id = n / 256;
    let peer = self_id ^ 1;
    right[0]
        .self_hash
        .copy_from_slice(active_bits.get_bits(self_id as usize, 32));
    right[0]
        .peer_hash
        .copy_from_slice(active_bits.get_bits(peer as usize, 32));
    right[0].peer_at_left = (peer & 1) == 0;

    let self_ = n / 512;
    let peer = self_ ^ 1;
    right[1]
        .self_hash
        .copy_from_slice(&twig.active_bits_mtl1[self_ as usize]);
    right[1]
        .peer_hash
        .copy_from_slice(&twig.active_bits_mtl1[peer as usize]);
    right[1].peer_at_left = (peer & 1) == 0;

    let self_ = n / 1024;
    let peer = self_ ^ 1;
    right[2]
        .self_hash
        .copy_from_slice(&twig.active_bits_mtl2[self_ as usize]);
    right[2]
        .peer_hash
        .copy_from_slice(&twig.active_bits_mtl2[peer as usize]);
    right[2].peer_at_left = (peer & 1) == 0;
    right
}

pub fn get_left_path<F>(sn: u64, mut get_hash: F) -> [ProofNode; 11]
where
    F: FnMut(usize) -> [u8; 32],
{
    let n = sn & TWIG_MASK as u64;
    let mut left = [ProofNode {
        self_hash: [0; 32],
        peer_hash: [0; 32],
        peer_at_left: false,
    }; 11];
    for level in 0..11 {
        let stride = 2048 >> level;
        let self_id = (n >> level) as usize;
        let peer = self_id ^ 1;
        left[level]
            .self_hash
            .copy_from_slice(&get_hash(stride + self_id));
        left[level]
            .peer_hash
            .copy_from_slice(&get_hash(stride + peer));
        left[level].peer_at_left = peer & 1 == 0;
    }
    left
}

pub fn get_left_path_in_mem(mt4twig: &TwigMT, sn: u64) -> [ProofNode; 11] {
    get_left_path(sn, |i| mt4twig[i])
}

pub fn get_left_path_on_disk(tf: &twigfile::TwigFile, twig_id: u64, sn: u64) -> [ProofNode; 11] {
    let mut cache: HashMap<i64, [u8; 32]> = HashMap::with_capacity(8);
    get_left_path(sn, |i| match cache.get(&(i as i64)) {
        Some(v) => {
            let mut res = [0; 32];
            res.copy_from_slice(v);
            res
        }
        None => {
            let mut res = [0; 32];
            tf.get_hash_node(twig_id, i as i64, &mut cache, &mut res);
            res
        }
    })
}
