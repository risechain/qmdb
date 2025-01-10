use crate::def;
use crate::entryfile::{EntryBz, EntryCache, EntryFile};
use crate::indexer::Indexer;
use crate::utils::bytescache::new_cache_pos;
use crate::utils::changeset::ChangeSet;
use std::sync::Arc;
use threadpool::ThreadPool;

pub struct EntryLoader {
    shard_id: usize,
    entry_file: Arc<EntryFile>,
    cache: Arc<EntryCache>,
    indexer: Arc<Indexer>,
    thread_pool: ThreadPool,
}

impl EntryLoader {
    pub fn new(
        shard_id: usize,
        entry_file: Arc<EntryFile>,
        cache: Arc<EntryCache>,
        indexer: Arc<Indexer>,
    ) -> Self {
        let thread_pool = ThreadPool::new(3);
        Self {
            shard_id,
            entry_file,
            cache,
            indexer,
            thread_pool,
        }
    }

    fn fetch_entry_to_cache(
        entry_file: Arc<EntryFile>,
        cache: Arc<EntryCache>,
        shard_id: usize,
        file_pos: i64,
    ) {
        let entry_pos = new_cache_pos();
        // try to insert a locked entry_pos
        let cache_hit = !cache.allocate_if_missing(shard_id, file_pos, entry_pos);
        if cache_hit {
            return; // no need to fetch
        }
        let mut small = [0u8; def::DEFAULT_ENTRY_SIZE];
        let size = entry_file.read_entry(file_pos, &mut small[..]);
        let e;
        let mut buf: Vec<u8>;
        if size <= small.len() {
            e = EntryBz { bz: &small[..size] };
        } else {
            buf = Vec::with_capacity(size);
            entry_file.read_entry(file_pos, &mut buf[..]);
            e = EntryBz { bz: &buf[..size] };
        }
        cache.insert(shard_id, file_pos, &e);
    }

    pub fn run_task(&mut self, height: i64, change_sets: &Vec<ChangeSet>) {
        let indexer = self.indexer.clone();
        let mut thread_count = 0usize;
        for change_set in change_sets {
            change_set.run_in_shard(self.shard_id, |op, key_hash: &[u8; 32], _k, _v, _r| {
                indexer.for_each(height, op, key_hash, |_k, _offset| -> bool {
                    thread_count += 1;
                    false // do not exit loop
                });
            });
        }
        if thread_count == 0 {
            return;
        }
        for change_set in change_sets {
            change_set.run_in_shard(self.shard_id, |op, key_hash: &[u8; 32], _k, _v, _r| {
                indexer.for_each(height, op, key_hash, |_k, offset| -> bool {
                    let entry_file = self.entry_file.clone();
                    let cache = self.cache.clone();
                    let shard_id = self.shard_id;
                    self.thread_pool.execute(move || {
                        Self::fetch_entry_to_cache(entry_file, cache, shard_id, offset);
                    });
                    false // do not exit loop
                });
            });
        }
        self.thread_pool.join();
    }
}
