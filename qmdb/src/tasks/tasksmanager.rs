use super::task::Task;
use crate::utils::changeset::ChangeSet;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc,
};

pub struct TasksManager<T: Task> {
    tasks: Vec<RwLock<Option<T>>>,
    last_task_id: AtomicI64,
}

impl<T: Task> TasksManager<T> {
    pub fn default() -> Self {
        Self {
            tasks: vec![], // need include end_block task.
            last_task_id: AtomicI64::new(-1),
        }
    }

    pub fn new(tasks: Vec<RwLock<Option<T>>>, last_task_id: i64) -> Self {
        Self {
            tasks,
            last_task_id: AtomicI64::new(last_task_id),
        }
    }

    // contain end_block task.
    pub fn tasks_len(&self) -> usize {
        self.tasks.len()
    }

    pub fn get_last_task_id(&self) -> i64 {
        self.last_task_id.load(Ordering::SeqCst)
    }

    pub fn set_last_task_id(&self, id: i64) {
        self.last_task_id.store(id, Ordering::SeqCst);
    }

    pub fn get_tasks_change_sets(&self, idx: usize) -> Arc<Vec<ChangeSet>> {
        let task_opt = self.tasks[idx].read();
        let task = task_opt.as_ref().unwrap();
        task.get_change_sets()
    }

    pub fn task_for_read(&self, idx: usize) -> RwLockReadGuard<Option<T>> {
        if idx >= self.tasks.len() {
            panic!("task index out of range");
        }
        self.tasks[idx].read()
    }
    pub fn task_for_write(&self, idx: usize) -> RwLockWriteGuard<Option<T>> {
        if idx >= self.tasks.len() {
            panic!("task index out of range");
        }
        self.tasks[idx].write()
    }
    pub fn set_task(&self, idx: usize, task: T) {
        let mut out_ptr = self.task_for_write(idx);
        *out_ptr = Some(task);
    }
}
