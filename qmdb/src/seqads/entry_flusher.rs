use crate::def::{LEAF_COUNT_IN_TWIG, MIN_PRUNE_COUNT, PRUNE_EVERY_NBLOCKS, TWIG_SHIFT};
use crate::merkletree::Tree;
use crate::metadb::MetaDB;
use crate::seqads::entry_updater::EntryUpdater;
use std::sync::{Arc, Mutex, RwLock};

pub struct FlusherShard {
    pub updater: Arc<Mutex<EntryUpdater>>,
    pub tree: Tree,
    last_compact_done_sn: u64,
    shard_id: usize,
    meta: Arc<RwLock<MetaDB>>,
}

impl FlusherShard {
    pub fn new(
        tree: Tree,
        oldest_active_sn: u64,
        shard_id: usize,
        updater: Arc<Mutex<EntryUpdater>>,
        meta_db: Arc<RwLock<MetaDB>>,
    ) -> Self {
        Self {
            updater,
            tree,
            last_compact_done_sn: oldest_active_sn,
            shard_id,
            meta: meta_db,
        }
    }

    pub fn flush(&mut self, prune_to_height: i64) {
        let mut updater = self.updater.lock().unwrap();
        let entry_bzs = updater.get_all_entry_bz();
        for entry_bz in entry_bzs {
            let _ = self.tree.append_entry(&entry_bz);
            for i in 0..entry_bz.dsn_count() {
                let dsn = entry_bz.get_deactived_sn(i);
                self.tree.deactive_entry(dsn);
            }
        }
        let mut start_twig_id: u64 = 0;
        let mut end_twig_id: u64 = 0;
        let compact_done_sn = updater.sn_start;
        let compact_done_pos = updater.compact_done_pos;
        let mut ef_size: i64 = 0;
        if prune_to_height > 0 && prune_to_height % PRUNE_EVERY_NBLOCKS == 0 {
            let meta = self.meta.read().unwrap();
            (start_twig_id, _) = meta.get_last_pruned_twig(self.shard_id);
            (end_twig_id, ef_size) = meta.get_first_twig_at_height(self.shard_id, prune_to_height);
            if end_twig_id == u64::MAX {
                panic!("FirstTwigAtHeight Not Found");
            }
            let mut last_evicted_twig_id = compact_done_sn / (LEAF_COUNT_IN_TWIG as u64);
            last_evicted_twig_id = last_evicted_twig_id.saturating_sub(1);
            if end_twig_id > last_evicted_twig_id {
                end_twig_id = last_evicted_twig_id;
            }
            if start_twig_id <= end_twig_id && end_twig_id < start_twig_id + MIN_PRUNE_COUNT {
                end_twig_id = start_twig_id;
            } else {
                self.tree.prune_twigs(start_twig_id, end_twig_id, ef_size);
            }
        }
        let del_start = self.last_compact_done_sn / (LEAF_COUNT_IN_TWIG as u64);
        let del_end = compact_done_sn / (LEAF_COUNT_IN_TWIG as u64);
        let tmp_list = self.tree.flush_files(del_start, del_end);
        let last_compact_done_sn = self.last_compact_done_sn;
        self.last_compact_done_sn = compact_done_sn;
        let n_list = self.tree.upper_tree.evict_twigs(
            tmp_list,
            last_compact_done_sn >> TWIG_SHIFT,
            compact_done_sn >> TWIG_SHIFT,
        );
        let (_new_n_list, root_hash) = self
            .tree
            .upper_tree
            .sync_upper_nodes(n_list, self.tree.youngest_twig_id);
        let mut edge_nodes_bytes = Vec::<u8>::with_capacity(0);
        if prune_to_height > 0
            && prune_to_height % PRUNE_EVERY_NBLOCKS == 0
            && start_twig_id < end_twig_id
        {
            edge_nodes_bytes = self.tree.upper_tree.prune_nodes(
                start_twig_id,
                end_twig_id,
                self.tree.youngest_twig_id,
            );
        }
        let mut meta = self.meta.write().unwrap();
        if !edge_nodes_bytes.is_empty() {
            meta.set_edge_nodes(self.shard_id, &edge_nodes_bytes[..]);
            meta.set_last_pruned_twig(self.shard_id, end_twig_id, ef_size);
        }
        meta.set_root_hash(self.shard_id, root_hash);
        updater.update_buffer.reset();

        meta.set_oldest_active_sn(self.shard_id, compact_done_sn);
        meta.set_oldest_active_file_pos(self.shard_id, compact_done_pos);
    }
}

pub struct EntryFlusher {
    pub shards: Vec<Box<FlusherShard>>,
    pub meta: Arc<RwLock<MetaDB>>,
    max_kept_height: i64,
}

impl EntryFlusher {
    pub fn new(shards: Vec<Box<FlusherShard>>, meta: Arc<RwLock<MetaDB>>) -> Self {
        Self {
            shards,
            meta,
            max_kept_height: 1000,
        }
    }

    pub fn flush_block(&mut self, curr_height: i64) {
        let mut meta = self.meta.write().unwrap();
        meta.set_curr_height(curr_height);
        for flusher in &self.shards {
            let shard_id = flusher.shard_id;
            let (entry_file_size, twig_file_size) = flusher.tree.get_file_sizes();
            meta.set_entry_file_size(shard_id, entry_file_size);
            meta.set_twig_file_size(shard_id, twig_file_size);
            meta.set_next_serial_num(shard_id, flusher.updater.lock().unwrap().sn_end);
            if (curr_height + 1) % PRUNE_EVERY_NBLOCKS == 0 {
                meta.set_first_twig_at_height(
                    shard_id,
                    curr_height + 1,
                    flusher.updater.lock().unwrap().sn_start / (LEAF_COUNT_IN_TWIG as u64),
                    entry_file_size,
                )
            }
        }
        meta.commit();
    }

    pub fn flush_tx(&mut self, shard_count: usize, curr_height: i64) {
        let prune_to_height = curr_height - self.max_kept_height;
        for i in (0..shard_count).rev() {
            self.shards[i].flush(prune_to_height);
        }
    }
}
