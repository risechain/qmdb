use std::sync::Arc;

use parking_lot::RwLock;
use qmdb::{
    config::Config,
    def::{DEFAULT_ENTRY_SIZE, IN_BLOCK_IDX_BITS},
    tasks::TasksManager,
    test_helper::SimpleTask,
    utils::hasher,
    AdsCore, AdsWrap, SharedAdsWrap, ADS,
};

pub static mut ADS: [Option<AdsWrap<SimpleTask>>; 2] = [None, None];

pub fn init(qmdb_dir: &str, table_id: usize) {
    // init QMDB
    let qmdb_dir_with_table_id = format!("{}-{}", qmdb_dir, table_id);
    let config = Config::from_dir(&qmdb_dir_with_table_id);
    AdsCore::init_dir(&config);
    let ads = AdsWrap::<SimpleTask>::new(&config);
    unsafe {
        ADS[table_id] = Some(ads);
    }
}

pub fn create_kv(tid: usize, height: i64, task_list: Vec<RwLock<Option<SimpleTask>>>) {
    let task_count = task_list.len() as i64;
    let last_task_id = (height << IN_BLOCK_IDX_BITS) | (task_count - 1);
    let mut ads = unsafe { ADS[tid].take().unwrap() };
    //warmup_indexer(&indexer, height, &task_list);
    //fake_indexer(&idx_file, page_count, &task_list);
    ads.start_block(height, Arc::new(TasksManager::new(task_list, last_task_id)));
    let shared_ads = ads.get_shared();
    shared_ads.insert_extra_data(height, "".to_owned());
    for idx in 0..task_count {
        let task_id = (height << IN_BLOCK_IDX_BITS) | idx;
        //println!("AA bench height={} task_id={:#08x}", height, task_id);
        shared_ads.add_task(task_id);
    }
    unsafe {
        ADS[tid] = Some(ads);
    }
}

pub fn update_kv(tid: usize, height: i64, task_list: Vec<RwLock<Option<SimpleTask>>>) {
    create_kv(tid, height, task_list);
}

pub fn delete_kv(tid: usize, height: i64, task_list: Vec<RwLock<Option<SimpleTask>>>) {
    create_kv(tid, height, task_list);
}

pub fn flush(tid: usize) {
    let mut ads = unsafe { ADS[tid].take().unwrap() };
    ads.flush();
    unsafe {
        ADS[tid] = Some(ads);
    }
}

pub fn get_ads(tid: usize) -> AdsWrap<SimpleTask> {
    unsafe { ADS[tid].take().unwrap() }
}

pub fn return_ads(tid: usize, ads: AdsWrap<SimpleTask>) {
    unsafe {
        ADS[tid] = Some(ads);
    }
}

//let shared_ads = &ads.get_shared();
pub fn read_kv(shared_ads: &SharedAdsWrap, key_list: &Vec<[u8; 52]>) {
    rayon::scope(|s| {
        s.spawn(move |_| {
            let mut buf = [0; DEFAULT_ENTRY_SIZE];
            for k in key_list.iter() {
                let kh = hasher::hash(&k[..]);
                let (_, ok) = shared_ads.read_entry(-1, &kh[..], &[], &mut buf);
                if !ok {
                    panic!("Cannot read entry kh={:?} ", kh);
                }
            }
        });
    });
}
