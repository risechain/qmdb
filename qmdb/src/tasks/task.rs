use std::sync::Arc;

use crate::{entryfile::EntryCache, utils::changeset::ChangeSet};

pub trait Task: Send + Sync {
    fn get_change_sets(&self) -> Arc<Vec<ChangeSet>>;
}

pub trait TaskHub: Send + Sync {
    fn check_begin_end(&self, task_id: i64) -> (Option<Arc<EntryCache>>, bool);
    fn get_change_sets(&self, task_id: i64) -> Arc<Vec<ChangeSet>>;
}
