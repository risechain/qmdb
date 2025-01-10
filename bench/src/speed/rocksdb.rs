use std::{fs, path::Path};

use parking_lot::RwLock;
use qmdb::{
    def::{OP_CREATE, OP_DELETE, OP_READ, OP_WRITE},
    tasks::Task,
    test_helper::SimpleTask,
    utils::hasher,
};
use rayon;
use rocksdb::{Options, ReadOptions, WriteBatch, WriteBatchWithTransaction, WriteOptions, DB};

struct RocksDB {
    db: DB,
}

struct RocksBatch {
    batch: WriteBatchWithTransaction<false>,
}

impl RocksDB {
    fn new(name: &str, dir: &str) -> Self {
        let path = Path::new(dir).join(name.to_owned() + ".db");
        //let options = Self::configure_options();
        let mut options = Options::default();
        options.create_if_missing(true);

        RocksDB {
            db: DB::open(&options, path).unwrap(),
        }
    }

    // fn configure_options() -> Options {
    //     let cpu_cores = std::thread::available_parallelism()
    //         .map(|n| n.get())
    //         .unwrap_or(1);

    //     let mut options = Options::default();
    //     options.set_use_direct_reads(true);
    //     options.set_use_direct_io_for_flush_and_compaction(true);

    //     // https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning
    //     options.create_if_missing(true);
    //     options.set_max_background_jobs(cpu_cores as i32);
    //     options.set_bytes_per_sync(1_048_576);
    //     options.set_compaction_style(rocksdb::DBCompactionStyle::Universal);

    //     let mut table_options = rocksdb::BlockBasedOptions::default();
    //     table_options.set_block_size(16 * 1024);
    //     table_options.set_cache_index_and_filter_blocks(true);
    //     table_options.set_pin_l0_filter_and_index_blocks_in_cache(true);

    //     options.set_block_based_table_factory(&table_options);

    //     options
    // }

    // fn close(&self) {
    //     // self.db.close();
    // }

    // fn set<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, val: V) {
    //     if key.as_ref().len() == 0 {
    //         panic!("Empty Key")
    //     }
    //     self.db.put(key, val).unwrap()
    // }

    fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<Vec<u8>> {
        if key.as_ref().is_empty() {
            panic!("Empty Key")
        }
        self.db.get(key).unwrap()
    }

    // fn has<K: AsRef<[u8]>>(&self, key: K) -> bool {
    //     self.get(key).is_some()
    // }

    // fn batch_write(&mut self, batch: RocksBatch) {
    //     self.db.write(batch.batch).unwrap();
    // }

    fn batch_write_sync(&mut self, batch: RocksBatch) {
        let mut write_options = WriteOptions::default();
        write_options.set_sync(true);
        self.db.write_opt(batch.batch, &write_options).unwrap();
    }
}

impl RocksBatch {
    fn new() -> Self {
        Self {
            batch: WriteBatch::default(),
        }
    }

    fn set<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, val: V) {
        if key.as_ref().is_empty() {
            panic!("Empty Key");
        }
        self.batch.put(key, val);
    }

    fn delete<K: AsRef<[u8]>>(&mut self, key: K) {
        if key.as_ref().is_empty() {
            panic!("Empty Key");
        }
        self.batch.delete(key);
    }
}

static mut RKS_DB: Option<RocksDB> = None;

pub fn init(rocksdb_dir: &str) {
    if Path::new(rocksdb_dir).exists() {
        fs::remove_dir_all(rocksdb_dir).unwrap();
    }
    let rks_db = RocksDB::new("ROCKS", rocksdb_dir);
    unsafe {
        RKS_DB = Some(rks_db);
    }
}

pub fn create_kv(_: i64, task_list: Vec<RwLock<Option<SimpleTask>>>) {
    let mut rks_db = unsafe { RKS_DB.take().unwrap() };
    rocksdb_create_kv(&mut rks_db, &task_list);
    unsafe {
        RKS_DB = Some(rks_db);
    }
}

pub fn update_kv(_: i64, task_list: Vec<RwLock<Option<SimpleTask>>>) {
    let mut rks_db = unsafe { RKS_DB.take().unwrap() };
    rocksdb_update_kv(&mut rks_db, &task_list);
    unsafe {
        RKS_DB = Some(rks_db);
    }
}

pub fn delete_kv(_: i64, task_list: Vec<RwLock<Option<SimpleTask>>>) {
    let mut rks_db = unsafe { RKS_DB.take().unwrap() };
    rocksdb_update_kv(&mut rks_db, &task_list);
    unsafe {
        RKS_DB = Some(rks_db);
    }
}

pub fn read_kv(key_list: &Vec<[u8; 52]>) {
    let rks_db = unsafe { RKS_DB.take().unwrap() };
    let _rks_db = &rks_db;
    rayon::scope(|s| {
        for k in key_list.iter() {
            let _ = hasher::hash(&k[..]);
            s.spawn(move |_| {
                _rks_db.get(k).unwrap();
            });
        }
    });
    unsafe {
        RKS_DB = Some(rks_db);
    }
}

// ===========

fn rocksdb_create_kv(rks_db: &mut RocksDB, task_list: &Vec<RwLock<Option<SimpleTask>>>) {
    let mut batch = RocksBatch::new();
    for item in task_list.iter() {
        let task_opt = item.read();
        let task = task_opt.as_ref().unwrap();
        for change_set in task.get_change_sets().iter() {
            change_set.apply_op_in_range(|op, _kh, k, v, _, _r| {
                if op != OP_CREATE {
                    panic!("only OP_CREATE is allowd in first round");
                }
                batch.set(k, v);
            });
        }
    }
    rks_db.batch_write_sync(batch);
}

fn rocksdb_update_kv(rks_db: &mut RocksDB, task_list: &Vec<RwLock<Option<SimpleTask>>>) {
    const N: usize = 10000;
    let mut keys = Vec::with_capacity(N);
    let mut batch_out = RocksBatch::new();
    std::thread::scope(|s: &std::thread::Scope<'_, '_>| {
        let handler = s.spawn(|| {
            let mut batch = RocksBatch::new();
            for item in task_list.iter() {
                let task_opt = item.read();
                let task = task_opt.as_ref().unwrap();
                for change_set in task.get_change_sets().iter() {
                    change_set.apply_op_in_range(|op, _kh, k, v, _, _r| {
                        if op == OP_WRITE || op == OP_CREATE {
                            batch.set(k, v);
                        } else if op == OP_DELETE {
                            batch.delete(k);
                        }
                    });
                }
            }
            batch
        });
        for item in task_list.iter() {
            let task_opt = item.read();
            let task = task_opt.as_ref().unwrap();
            for change_set in task.get_change_sets().iter() {
                change_set.apply_op_in_range(|op, _kh, k, _v, _, _r| {
                    if op == OP_WRITE || op == OP_READ {
                        let mut key = Vec::with_capacity(k.len());
                        key.extend_from_slice(k);
                        keys.push(key);
                        if keys.len() == N {
                            let mut v = Vec::with_capacity(N);
                            std::mem::swap(&mut v, &mut keys);
                            s.spawn(|| {
                                let mut option = ReadOptions::default();
                                option.set_async_io(true);
                                rks_db.db.multi_get_opt(v, &option);
                            });
                        }
                    }
                });
            }
        }
        if !keys.is_empty() {
            s.spawn(|| {
                rks_db.db.multi_get(keys);
            });
        }
        let mut batch = handler.join().unwrap();
        std::mem::swap(&mut batch, &mut batch_out);
    });
    rks_db.batch_write_sync(batch_out);
}
