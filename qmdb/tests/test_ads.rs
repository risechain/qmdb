use hpfile::TempDir;
use qmdb::config::Config;
use qmdb::def::{IN_BLOCK_IDX_BITS, OP_CREATE};
use qmdb::entryfile::EntryBz;
use qmdb::seqads::{SeqAds, SeqAdsWrap};
use qmdb::test_helper::SimpleTask;
use qmdb::utils::changeset::ChangeSet;
use qmdb::utils::{byte0_to_shard_id, hasher};
use qmdb::{AdsCore, ADS};
use std::fs::remove_dir_all;

#[cfg(not(feature = "tee_cipher"))]
#[test]
fn test_seqads() {
    let ads_dir = "./SEQADS";
    let _tmp_dir = TempDir::new(ads_dir);

    let config = Config {
        dir: ads_dir.to_string(),
        with_twig_file: true,
        ..Config::default()
    };
    AdsCore::init_dir(&config);
    let ads = SeqAds::new(&config);
    ads.meta_db
        .write()
        .unwrap()
        .insert_extra_data(1, "".to_owned());
    let mut change_set = ChangeSet::new();
    let address = [1; 20];
    let key_hash = hasher::hash(address);
    let shard_id = byte0_to_shard_id(key_hash[0]) as u8;
    println!("shard id:{}", shard_id);
    change_set.add_op(
        OP_CREATE,
        shard_id,
        &key_hash,    //used during sort as key_hash
        &address[..], //the key
        &[2; 20],     //the value
        None,
    );
    let address2 = [3; 20];
    let key_hash2 = hasher::hash(address2);
    let shard_id2 = byte0_to_shard_id(key_hash2[0]) as u8;
    println!("shard id2:{}", shard_id2);
    change_set.add_op(
        OP_CREATE,
        shard_id2,
        &key_hash2,    //used during sort as key_hash
        &address2[..], //the key
        &[4; 20],      //the value
        None,
    );
    change_set.sort();
    let change_sets = vec![change_set];
    let nums = ads.indexer.len(shard_id as usize);
    println!("{}", nums);
    let task_id = 1 << IN_BLOCK_IDX_BITS;
    ads.commit_tx(task_id, &change_sets);
    let mut change_set2 = ChangeSet::new();
    let address3 = [5; 20];
    let key_hash3 = hasher::hash(address3);
    let shard_id3 = byte0_to_shard_id(key_hash3[0]) as u8;
    println!("shard id3:{}", shard_id3);
    change_set2.add_op(
        OP_CREATE,
        shard_id3,
        &key_hash3,    //used during sort as key_hash
        &address3[..], //the key
        &[6; 20],      //the value
        None,
    );
    change_set2.sort();
    let change_sets = vec![change_set2];
    let task_id = (1 << IN_BLOCK_IDX_BITS) | 1;
    ads.commit_tx(task_id, &change_sets);
    let nums = ads.indexer.len(shard_id as usize);
    println!("{}", nums);
    ads.commit_block(1);
    drop(ads);
    let ads_wrap = SeqAdsWrap::<SimpleTask>::new(&config);
    let mut bz = [0; 300];
    let (size, found_it) = ads_wrap.read_entry(2, &key_hash, &address, &mut bz);
    assert!(found_it);
    let entry_bz = EntryBz { bz: &bz[..size] };
    assert_eq!(entry_bz.value(), &[2; 20]);
    let mut bz = [0; 300];
    let (size, found_it) = ads_wrap.read_entry(2, &key_hash2, &address2, &mut bz);
    assert!(found_it);
    let entry_bz = EntryBz { bz: &bz[..size] };
    assert_eq!(entry_bz.value(), &[4; 20]);
    let mut bz = [0; 300];
    let (size, found_it) = ads_wrap.read_entry(2, &key_hash3, &address3, &mut bz);
    assert!(found_it);
    let entry_bz = EntryBz { bz: &bz[..size] };
    assert_eq!(entry_bz.value(), &[6; 20]);
    let list = ads_wrap.get_shared().get_root_hash_list();
    let root = list[shard_id as usize];
    println!("root:{:?}", root);
    let root = list[shard_id2 as usize];
    println!("root2:{:?}", root);
    let root = list[shard_id3 as usize];
    println!("root3:{:?}", root);
    let _ = remove_dir_all(ads_dir);
}
