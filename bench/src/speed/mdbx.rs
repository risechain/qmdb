use std::{fs, path::Path};

use log::debug;
use parking_lot::RwLock;
use qmdb::{
    def::{OP_CREATE, OP_DELETE, OP_READ, OP_WRITE},
    tasks::Task,
    test_helper::SimpleTask,
    utils::hasher,
};
use rayon;
use rayon::iter::*;
use reth_libmdbx::{Environment, Geometry, ObjectLength, WriteFlags};

static mut MDBX_ENV: Option<Environment> = None;

pub fn init(mdbx_dir: &str) {
    // init MDBX
    if Path::new(mdbx_dir).exists() {
        fs::remove_dir_all(mdbx_dir).unwrap();
    }
    let map_size = 1024usize * 1024 * 1024 * 1024; //1TB
    let mdbx_env = Environment::builder()
        .set_geometry(Geometry {
            size: Some(0..map_size),
            ..Default::default()
        })
        .open(Path::new(mdbx_dir))
        .unwrap();
    debug!("MDBX: env.map_size={}", mdbx_env.info().unwrap().map_size());
    unsafe {
        MDBX_ENV = Some(mdbx_env);
    }
}

pub fn create_kv(_: i64, task_list: Vec<RwLock<Option<SimpleTask>>>) {
    let mut mdbx_env = unsafe { MDBX_ENV.take().unwrap() };
    mdbx_create_kv(&mut mdbx_env, &task_list);
    unsafe {
        MDBX_ENV = Some(mdbx_env);
    }
}

pub fn update_kv(_: i64, task_list: Vec<RwLock<Option<SimpleTask>>>) {
    let mut mdbx_env = unsafe { MDBX_ENV.take().unwrap() };
    mdbx_update_kv(&mut mdbx_env, &task_list);
    unsafe {
        MDBX_ENV = Some(mdbx_env);
    }
}

pub fn delete_kv(_: i64, task_list: Vec<RwLock<Option<SimpleTask>>>) {
    let mut mdbx_env = unsafe { MDBX_ENV.take().unwrap() };
    mdbx_update_kv(&mut mdbx_env, &task_list);
    unsafe {
        MDBX_ENV = Some(mdbx_env);
    }
}

pub fn read_kv(key_list: &Vec<[u8; 52]>) {
    let mdbx_env = unsafe { MDBX_ENV.take().unwrap() };
    let txn = &mdbx_env.begin_ro_txn().unwrap();
    let db = &txn.open_db(None).unwrap();
    rayon::scope(|s| {
        for k in key_list.iter() {
            let _ = hasher::hash(&k[..]);
            s.spawn(move |_| {
                txn.get::<ObjectLength>(db.dbi(), k).unwrap().unwrap();
            });
        }
    });
    unsafe {
        MDBX_ENV = Some(mdbx_env);
    }
}

// ===========
fn mdbx_create_kv(env: &mut Environment, task_list: &Vec<RwLock<Option<SimpleTask>>>) {
    let txn = env.begin_rw_txn().unwrap();
    let db = txn.open_db(None).unwrap();
    for item in task_list.iter() {
        let task_opt = item.read();
        let task = task_opt.as_ref().unwrap();
        for change_set in task.get_change_sets().iter() {
            change_set.apply_op_in_range(|op, _kh, k, v, _, _r| {
                if op != OP_CREATE {
                    panic!("only OP_CREATE is allowd in first round");
                }
                txn.put(db.dbi(), k, v, WriteFlags::empty()).unwrap();
            });
        }
    }
    txn.commit().unwrap();
}

fn mdbx_update_kv(env: &mut Environment, task_list: &Vec<RwLock<Option<SimpleTask>>>) {
    std::thread::scope(|s| {
        let handler = s.spawn(|| {
            let txn = env.begin_rw_txn().unwrap();
            let db = txn.open_db(None).unwrap();
            for item in task_list.iter() {
                let task_opt = item.read();
                let task = task_opt.as_ref().unwrap();
                for change_set in task.get_change_sets().iter() {
                    change_set.apply_op_in_range(|op, _kh, k, v, _, _r| {
                        if op == OP_WRITE || op == OP_CREATE {
                            txn.put(db.dbi(), k, v, WriteFlags::empty()).unwrap();
                        } else if op == OP_DELETE {
                            txn.del(db.dbi(), k, None).unwrap();
                        }
                    });
                }
            }
            txn
        });
        task_list.par_iter().for_each(|item| {
            let task_opt = item.read();
            let task = task_opt.as_ref().unwrap();
            let txn = env.begin_ro_txn().unwrap();
            let db = txn.open_db(None).unwrap();
            for change_set in task.get_change_sets().iter() {
                change_set.apply_op_in_range(|op, _kh, k, _v, _, _r| {
                    if op == OP_WRITE || op == OP_READ {
                        txn.get::<ObjectLength>(db.dbi(), k).unwrap().unwrap();
                    }
                });
            }
        });
        let txn = handler.join().unwrap();
        txn.commit().unwrap();
    });
}
