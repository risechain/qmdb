use super::tree::{NodePos, Tree};
use super::twig::ActiveBits;
use super::twig::{self, TwigMT};
use crate::def::{FIRST_LEVEL_ABOVE_TWIG, MAX_UPPER_LEVEL, NODE_SHARD_COUNT, TWIG_SHARD_COUNT};
use crate::utils::hasher;
use std::collections::HashMap;

pub fn check_mt(mt: &TwigMT) {
    let mut level = 10;
    let mut stride = 1;
    let mut sum = [0; 32];

    while stride <= 1024 {
        for i in stride..2 * stride {
            hasher::node_hash_inplace(level, &mut sum, &mt[2 * i], &mt[2 * i + 1]);
            if !mt[i].eq(sum.as_slice()) {
                panic!("Mismatch {}-{} {} {}", level, i, 2 * i, 2 * i + 1);
            }
        }

        if stride == 1024 {
            break;
        }
        stride *= 2;
        level -= 1;
    }
}

pub fn hash_equal(tag: &str, a: &[u8], b: &[u8]) {
    if !a.eq(b) {
        println!("a: {:?}, b: {:?}", a, b);
        panic!("{}", tag.to_owned() + "Not Equal");
    }
}

pub fn check_upper_nodes(tree: &Tree) {
    for k in 0..MAX_UPPER_LEVEL {
        for s in 0..NODE_SHARD_COUNT {
            check_upper_nodes_internal(tree, &tree.upper_tree.nodes[k][s]);
        }
    }
}

fn check_upper_nodes_internal(tree: &Tree, nodes: &HashMap<NodePos, [u8; 32]>) {
    for (pos, parent_hash) in nodes {
        let level = pos.level();
        let n = pos.nth();
        let mut left_child = [0; 32];
        let mut right_child = [0; 32];
        if level == FIRST_LEVEL_ABOVE_TWIG as u64 {
            let left_child_option = tree.upper_tree.get_twig_root(2 * n);
            match left_child_option {
                Some(v) => left_child.copy_from_slice(v),
                None => continue,
            }
            let right_child_option = tree.upper_tree.get_twig_root(2 * n + 1);
            match right_child_option {
                Some(v) => right_child.copy_from_slice(v),
                None => {
                    right_child.copy_from_slice(twig::NULL_TWIG.clone().twig_root.as_slice());
                }
            }
        } else {
            let left_child_option = tree.upper_tree.get_node(NodePos::pos(level - 1, 2 * n));
            match left_child_option {
                Some(v) => left_child.copy_from_slice(v),
                None => continue,
            }
            let right_child_option = tree.upper_tree.get_node(NodePos::pos(level - 1, 2 * n + 1));
            match right_child_option {
                Some(v) => right_child.copy_from_slice(v),
                None => continue,
            }
        }
        let mut h = [0u8; 32];
        hasher::node_hash_inplace(level as u8 - 1, &mut h, &left_child, &right_child);
        if !h.eq(parent_hash) {
            panic!("Mismatch at {}-{} l:{} r:{}", level, n, 2 * n, 2 * n + 1);
        }
    }
}

pub fn check_twig(twig: &twig::Twig, active_bits: &ActiveBits) {
    hash_equal(
        "L1-0",
        &twig.active_bits_mtl1[0],
        hasher::hash1(8, active_bits.get_bits(0, 64)).as_slice(),
    );
    hash_equal(
        "L1-1",
        &twig.active_bits_mtl1[1],
        hasher::hash1(8, active_bits.get_bits(1, 64)).as_slice(),
    );
    hash_equal(
        "L1-2",
        &twig.active_bits_mtl1[2],
        hasher::hash1(8, active_bits.get_bits(2, 64)).as_slice(),
    );
    hash_equal(
        "L1-3",
        &twig.active_bits_mtl1[3],
        hasher::hash1(8, active_bits.get_bits(3, 64)).as_slice(),
    );
    hash_equal(
        "L2-0",
        &twig.active_bits_mtl2[0],
        hasher::hash2(9, &twig.active_bits_mtl1[0], &twig.active_bits_mtl1[1]).as_slice(),
    );
    hash_equal(
        "L2-1",
        &twig.active_bits_mtl2[1],
        hasher::hash2(9, &twig.active_bits_mtl1[2], &twig.active_bits_mtl1[3]).as_slice(),
    );
    hash_equal(
        "L3",
        &twig.active_bits_mtl3,
        hasher::hash2(10, &twig.active_bits_mtl2[0], &twig.active_bits_mtl2[1]).as_slice(),
    );
    hash_equal(
        "Top",
        &twig.twig_root,
        hasher::hash2(11, &twig.left_root, &twig.active_bits_mtl3).as_slice(),
    );
}

pub fn check_all_twigs(tree: &Tree) {
    for s in 0..TWIG_SHARD_COUNT {
        for (twig_id, twig) in tree.upper_tree.active_twig_shards[s].iter() {
            let active_bits = &tree.active_bit_shards[s][twig_id];
            check_twig(twig, active_bits);
        }
    }
}

pub fn check_hash_consistency(tree: &Tree) {
    check_all_twigs(tree);
    check_upper_nodes(tree);
    check_mt(&tree.mtree_for_youngest_twig);
}
