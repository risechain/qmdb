use qmdb::{
    def::{ENTRY_BASE_LENGTH, TWIG_MASK},
    entryfile::entry,
    merkletree::{
        check,
        helpers::build_test_tree,
        proof::{self, ProofPath},
    },
    test_helper::TempDir,
};

fn check_equal(pp: &ProofPath, other: &ProofPath) -> String {
    if pp.upper_path.len() != other.upper_path.len() {
        return String::from("UpperPath's length not equal");
    }
    if pp.serial_num != other.serial_num {
        return String::from("SerialNum not equal");
    }
    if pp.left_of_twig[0].self_hash != other.left_of_twig[0].self_hash {
        return String::from("LeftOfTwig.SelfHash not equal");
    }
    for i in 0..11 {
        if pp.left_of_twig[i].peer_hash != other.left_of_twig[i].peer_hash {
            return String::from("LeftOfTwig.PeerHash not equal");
        }
        if pp.left_of_twig[i].peer_at_left != other.left_of_twig[i].peer_at_left {
            return format!("LeftOfTwig.PeerAtLeft[{}] not equal", i);
        }
    }
    if pp.right_of_twig[0].self_hash != other.right_of_twig[0].self_hash {
        return String::from("RightOfTwig.SelfHash not equal");
    }
    for i in 0..3 {
        if pp.right_of_twig[i].peer_hash != other.right_of_twig[i].peer_hash {
            return String::from("RightOfTwig.PeerHash not equal");
        }
        if pp.right_of_twig[i].peer_at_left != other.right_of_twig[i].peer_at_left {
            return String::from("RightOfTwig.PeerAtLeft not equal");
        }
    }
    for i in 0..pp.upper_path.len() {
        if pp.upper_path[i].peer_hash != other.upper_path[i].peer_hash {
            return String::from("UpperPath.PeerHash not equal");
        }
        if pp.upper_path[i].peer_at_left != other.upper_path[i].peer_at_left {
            return String::from("UpperPath.PeerAtLeft not equal");
        }
    }
    if pp.root != other.root {
        return String::from("Root not equal");
    }
    String::from("")
}

#[test]
fn test_tree_proof() {
    let dir_name = "./DataTree";
    let _tmp_dir = TempDir::new(dir_name);

    let deact_sn_list: Vec<u64> = (0..2048)
        .chain(vec![5000, 5500, 5700, 5813, 6001])
        .collect();

    let (mut tree, _, _, _) = build_test_tree(dir_name, &deact_sn_list, TWIG_MASK as i32 * 4, 1600);
    let n_list = tree.flush_files(0, 0);
    let n_list = tree.upper_tree.evict_twigs(n_list, 0, 0);

    tree.upper_tree
        .sync_upper_nodes(n_list, tree.youngest_twig_id);
    check::check_hash_consistency(&tree);

    let max_sn = TWIG_MASK as i32 * 4 + 1600;
    for i in 0..max_sn {
        let mut proof_path = tree.get_proof(i as u64);
        proof_path.check(false).unwrap();

        let bz = proof_path.to_bytes();
        let mut path2 = proof::bytes_to_proof_path(&bz).unwrap();
        let r = check_equal(&proof_path, &path2);
        assert_eq!(r, String::from(""));
        path2.check(true).unwrap();
    }

    // check null entries
    let _max_sn = max_sn + 452;
    let mut bz = [0u8; ENTRY_BASE_LENGTH + 8];
    let null_hash = entry::null_entry(&mut bz[..]).hash();
    for i in max_sn.._max_sn {
        let mut proof_path = tree.get_proof(i as u64);
        assert_eq!(proof_path.left_of_twig[0].self_hash, null_hash);
        proof_path.check(false).unwrap();

        let bz = proof_path.to_bytes();
        let mut path2 = proof::bytes_to_proof_path(&bz).unwrap();
        let r = check_equal(&proof_path, &path2);
        assert_eq!(r, String::from(""));
        path2.check(true).unwrap();
    }
}
