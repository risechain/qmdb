use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::Arc;
use threadpool::ThreadPool;

use crate::def::{DEFAULT_ENTRY_SIZE, IN_BLOCK_IDX_BITS, SHARD_COUNT};
use crate::entryfile::{EntryBuffer, EntryBz, EntryCache, EntryFile};
use crate::indexer::Indexer;
use crate::tasks::TaskHub;
use crate::utils::byte0_to_shard_id;
use crate::utils::bytescache::new_cache_pos;
use log::error;
pub struct JobManager {
    update_buffers: Vec<Arc<EntryBuffer>>,
    entry_files: Vec<Arc<EntryFile>>,
    done_chans: Vec<SyncSender<(i64, i64)>>,
}

impl Default for JobManager {
    fn default() -> Self {
        Self::new()
    }
}

impl JobManager {
    pub fn new() -> Self {
        JobManager {
            update_buffers: Vec::with_capacity(SHARD_COUNT),
            entry_files: Vec::with_capacity(SHARD_COUNT),
            done_chans: Vec::with_capacity(SHARD_COUNT),
        }
    }

    pub fn add_shard(
        &mut self,
        update_buffer: Arc<EntryBuffer>,
        entry_file: Arc<EntryFile>,
        done_chan: SyncSender<(i64, i64)>,
    ) {
        self.update_buffers.push(update_buffer);
        self.entry_files.push(entry_file);
        self.done_chans.push(done_chan);
    }
}

pub struct Prefetcher {
    task_hub: Arc<dyn TaskHub>,
    update_buffers: Vec<Arc<EntryBuffer>>,
    entry_files: Vec<Arc<EntryFile>>,
    cache: Arc<EntryCache>,
    indexer: Arc<Indexer>,
    done_chans: Vec<SyncSender<(i64, i64)>>,
    tpool: Arc<ThreadPool>,
}

fn fetch_entry_to_cache(
    update_buffer: &Arc<EntryBuffer>,
    entry_file: &Arc<EntryFile>,
    cache: &Arc<EntryCache>,
    shard_id: usize,
    file_pos: i64,
) {
    let entry_pos = new_cache_pos();
    // try to insert a locked entry_pos
    let cache_hit = !cache.allocate_if_missing(shard_id, file_pos, entry_pos);
    if cache_hit {
        return; // no need to fetch
    }
    // in 'get_entry_bz_at', 'curr_buf' is None because we cannot read it
    let (in_disk, have_accessed) = update_buffer.get_entry_bz(file_pos, |entry_bz| {
        cache.insert(shard_id, file_pos, &entry_bz);
    });
    if in_disk && !have_accessed {
        let mut small = [0u8; DEFAULT_ENTRY_SIZE];
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
}

impl Prefetcher {
    pub fn new(
        task_hub: Arc<dyn TaskHub>,
        cache: Arc<EntryCache>,
        indexer: Arc<Indexer>,
        tpool: Arc<ThreadPool>,
        job_man: JobManager,
    ) -> Self {
        Self {
            task_hub,
            cache,
            indexer,
            update_buffers: job_man.update_buffers,
            entry_files: job_man.entry_files,
            done_chans: job_man.done_chans,
            tpool,
        }
    }

    pub fn start_threads(mut self, task_receiver: Receiver<i64>) {
        std::thread::spawn(move || loop {
            let task_id = task_receiver.recv().unwrap_or(-1);
            if task_id == -1 {
                error!("task receiver in prefetcher exit!");
                return;
            }
            self.run_task(task_id);
        });
    }

    pub fn run_task(&mut self, task_id: i64) {
        let (cache_for_new_block, end_block) = self.task_hub.check_begin_end(task_id);
        if cache_for_new_block.is_some() {
            self.cache = cache_for_new_block.unwrap();
        }
        let mut next_task_id = task_id + 1;
        let height = task_id >> IN_BLOCK_IDX_BITS;
        if end_block {
            //next_task_id is the first task of the next block
            next_task_id = (height + 1) << IN_BLOCK_IDX_BITS;
        }
        let task_hub = self.task_hub.clone();
        let mut thread_counts = Vec::with_capacity(SHARD_COUNT);
        for shard_id in 0..SHARD_COUNT {
            let mut thread_count = 0usize;
            // first we need to know the count of threads that will be spawned
            for change_set in &*task_hub.get_change_sets(task_id) {
                thread_count += change_set.op_count_in_shard(shard_id);
            }
            // if there are no OP_*, prefetcher needs to do nothing.
            if thread_count == 0 {
                self.done_chans[shard_id]
                    .send((task_id, next_task_id))
                    .unwrap();
            }
            thread_counts.push(AtomicUsize::new(thread_count));
        }
        // spawn a thread for each OP_*
        let thread_counts = Arc::new(thread_counts);
        let change_sets = task_hub.get_change_sets(task_id).clone();
        for i in 0..change_sets.len() {
            let update_buffers = self.update_buffers.clone();
            let entry_files = self.entry_files.clone();
            let cache = self.cache.clone();
            let thread_counts = thread_counts.clone();
            let done_chans = self.done_chans.clone();
            let indexer = self.indexer.clone();
            let change_sets = change_sets.clone();
            self.tpool.execute(move || {
                change_sets[i].run_all(|op, kh: &[u8; 32], _k, _v, _r| {
                    let shard_id = byte0_to_shard_id(kh[0]);
                    indexer.for_each(height, op, &kh[..], |_k, offset| -> bool {
                        let ubuf = &update_buffers[shard_id];
                        let ef = &entry_files[shard_id];
                        fetch_entry_to_cache(ubuf, ef, &cache, shard_id, offset);
                        false // do not exit loop
                    });
                    // the last finished thread send task_id to done_chan
                    if thread_counts[shard_id].fetch_sub(1, Ordering::SeqCst) == 1 {
                        done_chans[shard_id].send((task_id, next_task_id)).unwrap();
                    }
                });
            });
        }
    }
}
