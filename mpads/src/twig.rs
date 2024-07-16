use lazy_static::lazy_static;

use crate::def::{ENTRY_FIXED_LENGTH, FIRST_LEVEL_ABOVE_TWIG, LEAF_COUNT_IN_TWIG, TWIG_ROOT_LEVEL};
use crate::entry::Hash32;
use crate::entry::{self, ZERO_HASH32};
use crate::tree::sync_mtree;
use crate::utils::hasher;

// global variables
lazy_static! {
    pub static ref NULL_MT_FOR_TWIG: Box<TwigMT> = get_init_data().0;
    pub static ref NULL_TWIG: Twig = get_init_data().1;
    pub static ref NULL_NODE_IN_HIGHER_TREE: [[u8; 32]; 64] = get_init_data().2;
    pub static ref NULL_ACTIVE_BITS: ActiveBits = ActiveBits::new();
}

pub type TwigMT = [Hash32; 4096];

#[derive(Clone, Debug)]
pub struct Twig {
    pub active_bits_mtl1: [Hash32; 4],
    pub active_bits_mtl2: [Hash32; 2],
    pub active_bits_mtl3: Hash32,
    pub left_root: Hash32,
    pub twig_root: Hash32,
}

// for initializing null nodes of the merkle tree
fn get_init_data() -> (Box<TwigMT>, Twig, [Hash32; 64]) {
    // null left tree in twig:
    let null_mt_for_twig = create_null_mt_for_twig();
    // null right tree in twig:
    let null_twig = create_null_twig(null_mt_for_twig[1]);
    // null higher tree above twig:
    let null_node_in_higher_tree = create_null_node_in_higher_tree(&null_twig);

    (null_mt_for_twig, null_twig, null_node_in_higher_tree)
}

fn create_null_mt_for_twig() -> Box<TwigMT> {
    let mut bz = [0u8; ENTRY_FIXED_LENGTH + 8];
    let null_hash = entry::null_entry(&mut bz[..]).hash();

    let mut null_mt_for_twig = Box::new([ZERO_HASH32; 4096]);
    for i in 2048..4096 {
        null_mt_for_twig[i] = null_hash;
    }
    sync_mtree(&mut null_mt_for_twig, 0, 2047);

    null_mt_for_twig
}

fn create_null_twig(null_mt_for_twig: Hash32) -> Twig {
    let mut null_twig = Twig::new();

    null_twig.sync_l1(0, &NULL_ACTIVE_BITS);
    null_twig.sync_l1(1, &NULL_ACTIVE_BITS);
    null_twig.sync_l1(2, &NULL_ACTIVE_BITS);
    null_twig.sync_l1(3, &NULL_ACTIVE_BITS);
    null_twig.sync_l2(0);
    null_twig.sync_l2(1);
    null_twig.sync_l3();

    null_twig.left_root = null_mt_for_twig;
    null_twig.sync_top();

    null_twig
}

fn create_null_node_in_higher_tree(null_twig: &Twig) -> [Hash32; 64] {
    let mut null_node_in_higher_tree = [ZERO_HASH32; 64];

    null_node_in_higher_tree[FIRST_LEVEL_ABOVE_TWIG as usize] = hasher::hash2(
        TWIG_ROOT_LEVEL as u8,
        &null_twig.twig_root,
        &null_twig.twig_root,
    );

    for i in (FIRST_LEVEL_ABOVE_TWIG + 1)..64 {
        null_node_in_higher_tree[i as usize] = hasher::hash2(
            (i - 1) as u8,
            &null_node_in_higher_tree[(i - 1) as usize],
            &null_node_in_higher_tree[(i - 1) as usize],
        );
    }

    null_node_in_higher_tree
}

impl Twig {
    pub fn new() -> Self {
        Self {
            active_bits_mtl1: [[0; 32]; 4],
            active_bits_mtl2: [[0; 32]; 2],
            active_bits_mtl3: [0; 32],
            left_root: [0; 32],
            twig_root: [0; 32],
        }
    }

    pub fn sync_l1(&mut self, pos: i32, active_bits: &ActiveBits) {
        match pos {
            0 => hasher::node_hash_inplace(
                8,
                &mut self.active_bits_mtl1[0],
                active_bits.get_bits(0, 32),
                active_bits.get_bits(1, 32),
            ),
            1 => hasher::node_hash_inplace(
                8,
                &mut self.active_bits_mtl1[1],
                active_bits.get_bits(2, 32),
                active_bits.get_bits(3, 32),
            ),
            2 => hasher::node_hash_inplace(
                8,
                &mut self.active_bits_mtl1[2],
                active_bits.get_bits(4, 32),
                active_bits.get_bits(5, 32),
            ),
            3 => hasher::node_hash_inplace(
                8,
                &mut self.active_bits_mtl1[3],
                active_bits.get_bits(6, 32),
                active_bits.get_bits(7, 32),
            ),
            _ => panic!("Can not reach here!"),
        }
    }

    pub fn sync_l2(&mut self, pos: i32) {
        match pos {
            0 => hasher::node_hash_inplace(
                9,
                &mut self.active_bits_mtl2[0],
                &self.active_bits_mtl1[0],
                &self.active_bits_mtl1[1],
            ),
            1 => hasher::node_hash_inplace(
                9,
                &mut self.active_bits_mtl2[1],
                &self.active_bits_mtl1[2],
                &self.active_bits_mtl1[3],
            ),
            _ => panic!("Can not reach here!"),
        }
    }

    pub fn sync_l3(&mut self) {
        hasher::node_hash_inplace(
            10,
            &mut self.active_bits_mtl3,
            &self.active_bits_mtl2[0],
            &self.active_bits_mtl2[1],
        );
    }

    pub fn sync_top(&mut self) {
        hasher::node_hash_inplace(
            11,
            &mut self.twig_root,
            &self.left_root,
            &self.active_bits_mtl3,
        );
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActiveBits([u8; 256]);

impl ActiveBits {
    pub fn new() -> Self {
        Self([0; 256])
    }

    pub fn set_bit(&mut self, offset: u32) {
        if offset > LEAF_COUNT_IN_TWIG {
            panic!("Invalid ID");
        }
        let mask = 1 << (offset & 0x7);
        let pos = (offset >> 3) as usize;
        self.0[pos] |= mask;
    }

    pub fn clear_bit(&mut self, offset: u32) {
        if offset > LEAF_COUNT_IN_TWIG {
            panic!("Invalid ID");
        }
        let mask = 1 << (offset & 0x7);
        let pos = (offset >> 3) as usize;
        self.0[pos] &= !mask; //bit-wise not
    }

    pub fn get_bit(&self, offset: u32) -> bool {
        if offset > LEAF_COUNT_IN_TWIG {
            panic!("Invalid ID");
        }
        let mask = 1 << (offset & 0x7);
        let pos = (offset >> 3) as usize;
        (self.0[pos] & mask) != 0
    }

    pub fn get_bits(&self, page_num: usize, page_size: usize) -> &[u8] {
        &self.0[page_num * page_size..(page_num + 1) * page_size]
    }
}

#[cfg(test)]
mod active_bits_tests {
    use super::*;

    #[test]
    fn test_new_bits() {
        let bits = ActiveBits::new();
        for i in 0..255 {
            assert_eq!(0, bits.0[i]);
        }
    }

    #[test]
    fn test_set_bit() {
        let mut bits = ActiveBits::new();

        bits.set_bit(25);
        assert_eq!(0b00000010, bits.0[3]);

        bits.set_bit(70);
        assert_eq!(0b01000000, bits.0[8]);

        bits.set_bit(83);
        assert_eq!(0b00001000, bits.0[10]);

        bits.set_bit(801);
        assert_eq!(0b00000010, bits.0[100]);
    }

    #[test]
    fn test_clear_bits() {
        let mut bits = ActiveBits::new();

        bits.set_bit(2047);
        bits.set_bit(2044);
        assert_eq!(0b10010000, bits.0[255]);

        bits.clear_bit(2047);
        assert_eq!(0b00010000, bits.0[255]);
        bits.clear_bit(2044);
        assert_eq!(0b00000000, bits.0[255]);
    }

    #[test]
    fn test_get_bit() {
        let mut bits = ActiveBits::new();

        bits.set_bit(2047);
        bits.set_bit(2044);
        assert_eq!(0b10010000, bits.0[255]);

        assert_eq!(true, bits.get_bit(2047));
        assert_eq!(false, bits.get_bit(2046));
        assert_eq!(false, bits.get_bit(2045));
        assert_eq!(true, bits.get_bit(2044));
        assert_eq!(false, bits.get_bit(2043));
        assert_eq!(false, bits.get_bit(2042));
        assert_eq!(false, bits.get_bit(2041));
        assert_eq!(false, bits.get_bit(2040));
    }

    #[test]
    #[should_panic(expected = "Invalid ID")]
    fn test_set_bit_idx_out_of_range() {
        let mut bits = ActiveBits::new();
        bits.set_bit(LEAF_COUNT_IN_TWIG + 1);
    }

    #[test]
    #[should_panic(expected = "Invalid ID")]
    fn test_clear_bit_idx_out_of_range() {
        let mut bits = ActiveBits::new();
        bits.clear_bit(LEAF_COUNT_IN_TWIG + 1);
    }

    #[test]
    #[should_panic(expected = "Invalid ID")]
    fn test_get_bit_idx_out_of_range() {
        let bits = ActiveBits::new();
        bits.get_bit(LEAF_COUNT_IN_TWIG + 1);
    }

    #[test]
    fn test_get_bits() {
        let mut bits = ActiveBits::new();
        for i in 0..255 {
            bits.0[i] = i as u8;
        }

        assert_eq!(
            bits.get_bits(3, 32),
            vec![
                96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112,
                113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127
            ]
        );
    }
}

#[cfg(test)]
mod twig_tests {
    use crate::twig::{ActiveBits, Twig, NULL_MT_FOR_TWIG, NULL_NODE_IN_HIGHER_TREE, NULL_TWIG};

    #[test]
    fn test_sync() {
        let mut twig = Twig::new();
        let mut active_bits = ActiveBits::new();
        for i in 0..255 {
            active_bits.0[i] = i as u8;
        }

        twig.sync_l1(0, &active_bits);
        twig.sync_l1(1, &active_bits);
        twig.sync_l1(2, &active_bits);
        twig.sync_l1(3, &active_bits);
        assert_eq!(
            "ebdc6bccc0d70075f48ab3c602652a1787d41c05f5a0a851ffe479df0975e683",
            hex::encode(twig.active_bits_mtl1[0])
        );
        assert_eq!(
            "3eac125482e6c5682c92af7dd633d9e99d027cf3f53237b46e2507ca2c9cd599",
            hex::encode(twig.active_bits_mtl1[1])
        );
        assert_eq!(
            "e208457ddd8f66e95ea947bc1beb5c463de054daa3f0ae1c3682a973c1861a32",
            hex::encode(twig.active_bits_mtl1[2])
        );
        assert_eq!(
            "e8b9fd47cce5df56b8d4b0b098af1b49ff3ea97d0c093c8ef6eccb34ae73ac8f",
            hex::encode(twig.active_bits_mtl1[3])
        );

        twig.sync_l2(0);
        twig.sync_l2(1);
        assert_eq!(
            "cf1a0078d5a94742b42bf05d301919b5ae89c155fc1e68a08d260e7ec27c967e",
            hex::encode(twig.active_bits_mtl2[0])
        );
        assert_eq!(
            "cf1a0078d5a94742b42bf05d301919b5ae89c155fc1e68a08d260e7ec27c967e",
            hex::encode(twig.active_bits_mtl2[0])
        );

        twig.sync_l3();
        assert_eq!(
            "d911c0d3beffe478f28b2ebc7cb824ad02ff2793534f37a0c6ddaf9d84527a66",
            hex::encode(twig.active_bits_mtl3)
        );

        twig.left_root = [88; 32];
        twig.sync_top();
        assert_eq!(
            "9312922a448932555a5f1d07b98f422fc0a4259e450f7536161b8ef8ddc96e08",
            hex::encode(twig.twig_root)
        );
    }

    #[test]
    fn test_init_data() {
        let null_twigmt_hashes = [
            "067c225ad4d5b8b2ecd82f980fdc242c84257099f29bc00283c8444f34918d1f", // 1
            "37985c3895fdee091d08aebb47a0809c72fc8f518b67879689c8e40e3b3ecbfd", // 2
            "201a57cf6fa6da9806ad1c6d49fe61b59c5d8c4eacb1b820e0d5ff9c5134aaca", // 4
            "a184e7de39bc89599378544c717f2358ce84f65cf689c9d88bb9be8a3ce03426", // 8
            "4bb1792deffce7c2e73a691f899d9a115fe5b877e8e40cf3222e7b8104bccaea", // 16
            "feb66e52f626ed1bca81df0be6ae5551f0fcb227155da7c8c39238a01c5ef078", // 32
            "bf8508a9424c4ae4c9b9287d8e89a24ef6abea30045809abe25d38db8fd1820a", // 64
            "48eceb81b2d3ac38c1cd4b177fa3fbad385d2c7204427b54ef75bd32cb0d845e", // 128
            "ed1de3c3c4bf2ec3d3796a9550c2cfd24c77d72957ba21c5b11a74be2594d1bb", // 256
            "0b6239dd848bc2fd2a32a23dd4f41c25feb8aa963ba08f0eb7bf4079182b9996", // 512
            "4b2c790b4afc993d2ece8ca9889e0921d9c60980a00323977548a38ea46b83fc", // 1024
            "4e77ace5522aacf7c26b08c04f780458f5cec642b6a17eee19ee84a544024cc1", // 2048
        ];
        for i in 0..12 {
            assert_eq!(
                null_twigmt_hashes[i],
                hex::encode(NULL_MT_FOR_TWIG[2usize.pow(i as u32)])
            );
        }

        assert_eq!(
            "be461db4e49fd4bef76e293ce1a0470e64941dd36fda05e0211e329c3be5d3e6",
            hex::encode(NULL_TWIG.twig_root)
        );

        assert_eq!(
            "32a03f49a6820698a32052330d856049ca6ecf330c9d73c82881813b2cd81f38",
            hex::encode(NULL_NODE_IN_HIGHER_TREE[63])
        );
    }
}
