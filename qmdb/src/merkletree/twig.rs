use lazy_static::lazy_static;

use crate::def::{ENTRY_BASE_LENGTH, FIRST_LEVEL_ABOVE_TWIG, LEAF_COUNT_IN_TWIG, TWIG_ROOT_LEVEL};
use crate::entryfile::entry;
use crate::utils::hasher::{self, Hash32, ZERO_HASH32};

// global variables
lazy_static! {
    pub static ref NULL_MT_FOR_TWIG: Box<TwigMT> = get_init_data().0;
    pub static ref NULL_TWIG: Box<Twig> = get_init_data().1;
    pub static ref NULL_NODE_IN_HIGHER_TREE: [[u8; 32]; 64] = get_init_data().2;
    pub static ref NULL_ACTIVE_BITS: ActiveBits = ActiveBits::new();
}

pub type TwigMT = [Hash32]; // size is 4096

#[derive(Clone, Debug)]
pub struct Twig {
    pub active_bits_mtl1: [Hash32; 4],
    pub active_bits_mtl2: [Hash32; 2],
    pub active_bits_mtl3: Hash32,
    pub left_root: Hash32,
    pub twig_root: Hash32,
}

// for initializing null nodes of the merkle tree
fn get_init_data() -> (Box<TwigMT>, Box<Twig>, [Hash32; 64]) {
    // null left tree in twig:
    let null_mt_for_twig = create_null_mt_for_twig();
    // null right tree in twig:
    let null_twig = create_null_twig(null_mt_for_twig[1]);
    // null higher tree above twig:
    let null_node_in_higher_tree = create_null_node_in_higher_tree(&null_twig);

    (null_mt_for_twig, null_twig, null_node_in_higher_tree)
}

fn create_null_mt_for_twig() -> Box<TwigMT> {
    let mut bz = [0u8; ENTRY_BASE_LENGTH + 8];
    let null_hash = entry::null_entry(&mut bz[..]).hash();

    let mut null_mt_for_twig = vec![ZERO_HASH32; 4096].into_boxed_slice();
    for i in 2048..4096 {
        null_mt_for_twig[i] = null_hash;
    }
    sync_mtree(&mut null_mt_for_twig, 0, 2047);

    null_mt_for_twig
}

fn create_null_twig(null_mt_for_twig: Hash32) -> Box<Twig> {
    let mut null_twig = Box::new(Twig::new());

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

impl Default for Twig {
    fn default() -> Self {
        Self::new()
    }
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

impl Default for ActiveBits {
    fn default() -> Self {
        Self::new()
    }
}

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

        assert!(bits.get_bit(2047));
        assert!(!bits.get_bit(2046));
        assert!(!bits.get_bit(2045));
        assert!(bits.get_bit(2044));
        assert!(!bits.get_bit(2043));
        assert!(!bits.get_bit(2042));
        assert!(!bits.get_bit(2041));
        assert!(!bits.get_bit(2040));
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
    use crate::merkletree::twig::{
        ActiveBits, Twig, NULL_MT_FOR_TWIG, NULL_NODE_IN_HIGHER_TREE, NULL_TWIG,
    };

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
            "cce5498796e1da850e39978e5e7bc572779e8ddc5eca8532aa8d28eb8b9fa839", // 1
            "6625f6aa53d328b2572979b52d98b376f26d86ead0fc89b386d4ed026e944e42", // 2
            "c6085473880d2de6339201f1855d088c7a7fc74ab884c5bcc8b851d202328646", // 4
            "730bf342c9b3d3e9a5ecd86d26d9bb3333a6038a110455bd98cae0b91284a50b", // 8
            "122c8ce9fa6aaa67e3afa2e1b47a704ad12c1e6608b2a21e84fd19bd07c30713", // 16
            "692f5b1dc974510438da37d0c46c8e39946a79af1246fe6fbc3f44fc80bc40c3", // 32
            "053d9c73883c8ee7eac9cf011458c61433bbd4bba561e3ddc3f49cf76e52e288", // 64
            "7c81680ffb753a36d9e0b345f308fd818a402b0ecb5e1366cc94991a56075044", // 128
            "e4c3f379b7a5789594c9109e9896aecf749b85c4a6a0b3d26a2c697e26f36fd3", // 256
            "065ac2fd5a856e8e35e104a78235fc5f8c7e75fabbf8064cda207c4babbeb56c", // 512
            "28c0cc1650e8b10b29de7eb17201be478391272380e55745fd52d5feb8554eaa", // 1024
            "ca2337691033ab0a24c10fbc70b49bea8c5978db1a0ec6510e7e97f528301c39", // 2048
        ];
        for i in 0..12 {
            assert_eq!(
                null_twigmt_hashes[i],
                hex::encode(NULL_MT_FOR_TWIG[2usize.pow(i as u32)])
            );
        }

        assert_eq!(
            "37f6d34b5f4fe4aba10fd7411d6f58efc4bf844935c37dbe83c5686ceb62ce9d",
            hex::encode(NULL_TWIG.twig_root)
        );

        assert_eq!(
            "c787c83f6f8402c636a2f48f1bf2c02ceb31ea5ccdd4bd9e6fe6efcc3031b640",
            hex::encode(NULL_NODE_IN_HIGHER_TREE[63])
        );
    }
}
