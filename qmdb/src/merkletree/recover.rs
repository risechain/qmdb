use aes_gcm::Aes256Gcm;
use byteorder::{ByteOrder, LittleEndian};
use log::debug;

use super::{
    tree::{get_shard_idx_and_key, EdgeNode, NodePos, Tree},
    twig::{NULL_ACTIVE_BITS, NULL_MT_FOR_TWIG, NULL_TWIG},
};
use crate::{
    def::{DEFAULT_ENTRY_SIZE, LEAF_COUNT_IN_TWIG, TWIG_MASK, TWIG_ROOT_LEVEL, TWIG_SHIFT},
    entryfile::{EntryBz, EntryFileWithPreReader},
    utils::hasher,
};

pub fn edge_nodes_to_bytes(edge_nodes: &Vec<EdgeNode>) -> Vec<u8> {
    let stride = 8 + 32;
    let mut res = vec![0u8; edge_nodes.len() * stride];
    for (i, node) in edge_nodes.iter().enumerate() {
        LittleEndian::write_u64(&mut res[i * stride..i * stride + 8], node.pos.as_u64());
        res[i * stride + 8..(i + 1) * stride].copy_from_slice(&node.value);
    }
    res
}

pub fn bytes_to_edge_nodes(bz: &[u8]) -> Vec<EdgeNode> {
    let stride = 8 + 32;
    if bz.len() % stride != 0 {
        panic!("Invalid byteslice length for EdgeNodes");
    }
    let len: usize = bz.len() / stride;
    let mut res = Vec::with_capacity(len);
    for i in 0..len {
        let pos = LittleEndian::read_u64(&bz[i * stride..i * stride + 8]);
        let mut edge_node = EdgeNode {
            pos: NodePos::new(pos),
            value: [0u8; 32],
        };
        edge_node
            .value
            .copy_from_slice(&bz[i * stride + 8..(i + 1) * stride]);
        res.push(edge_node);
    }
    res
}

impl Tree {
    fn recover_entry(&mut self, entry: EntryBz, oldest_active_twig_id: u64, youngest_twig_id: u64) {
        for (_, sn) in entry.dsn_iter() {
            let twig_id = sn >> TWIG_SHIFT;
            if twig_id >= oldest_active_twig_id {
                self.deactive_entry(sn);
            }
        }

        // update youngestTwigID
        let sn = entry.serial_number();
        let twig_id = sn >> TWIG_SHIFT;
        self.youngest_twig_id = twig_id;
        // mark this entry as valid
        if twig_id >= oldest_active_twig_id {
            self.active_entry(sn);
        }
        // record ChangeStart/ChangeEnd for endblock sync
        let position = (sn & TWIG_MASK as u64) as i32;
        if self.mtree_for_yt_change_start == -1 {
            self.mtree_for_yt_change_start = position;
        }
        self.mtree_for_yt_change_end = position;

        // update the corresponding leaf of merkle tree
        let twigmt_idx = LEAF_COUNT_IN_TWIG as usize + position as usize;
        self.mtree_for_youngest_twig[twigmt_idx] = entry.hash();

        if position as u32 == TWIG_MASK {
            // Only the real youngest twig's left_root
            // need this sync_mt_for_youngest_twig call. All the other twigs' left_root
            // can be loaded from TwigFile
            let empty_twig_file = self.twig_file_wr.twig_file.is_empty();
            if twig_id == youngest_twig_id || empty_twig_file {
                self.sync_mt_for_youngest_twig(true);
            } else {
                self.load_mt_for_non_youngest_twig(twig_id);
            }
            self.youngest_twig_id += 1;
            let (s, i) = get_shard_idx_and_key(self.youngest_twig_id);
            self.upper_tree.active_twig_shards[s].insert(i, NULL_TWIG.clone());
            self.active_bit_shards[s].insert(i, NULL_ACTIVE_BITS.clone());
            self.mtree_for_youngest_twig = NULL_MT_FOR_TWIG.clone();
        };
    }

    fn scan_entries(
        &mut self,
        oldest_active_twig_id: u64,
        youngest_twig_id: u64,
        ef_prune_to: i64,
    ) {
        let mut pos = ef_prune_to;
        let tf = &self.twig_file_wr.twig_file;
        if !tf.is_empty() {
            pos = tf.get_first_entry_pos(oldest_active_twig_id);
        }
        let size = self.entry_file_wr.entry_file.size();
        let total = size - pos;
        let step = total / 20;
        let start_pos = pos;
        let mut last_pos = pos;
        let mut buf = Vec::with_capacity(DEFAULT_ENTRY_SIZE);

        let mut entry_file = EntryFileWithPreReader::new(&self.entry_file_wr.entry_file);
        let mut count = 0;
        while pos < size {
            if (pos - start_pos) / step != (last_pos - start_pos) / step {
                debug!(
                    "ScanEntries {:.2} {}/{}",
                    (pos - start_pos) as f64 / total as f64,
                    pos - start_pos,
                    total
                );
            }
            last_pos = pos;
            entry_file.read_entry(pos, size, &mut buf).unwrap();
            let entry = EntryBz { bz: &buf[..] };
            self.recover_entry(entry, oldest_active_twig_id, youngest_twig_id);
            pos += buf.len() as i64;
            buf.clear();
            count += 1;
        }
        debug!("entry count in recover tree: {}", count);
    }

    pub fn scan_entries_lite<F>(&self, oldest_active_twig_id: u64, ef_prune_to: i64, access: F)
    where
        F: FnMut(&[u8], &[u8], i64, u64),
    {
        let mut pos = ef_prune_to;
        let tf = &self.twig_file_wr.twig_file;
        if !tf.is_empty() {
            pos = tf.get_first_entry_pos(oldest_active_twig_id);
        }
        let mut entry_file_rd = EntryFileWithPreReader::new(&self.entry_file_wr.entry_file);
        entry_file_rd.scan_entries_lite(pos, access);
    }

    fn recover_twigs(
        &mut self,
        oldest_active_twig_id: u64,
        youngest_twig_id: u64,
        ef_prune_to: i64,
    ) -> Vec<u64> {
        self.scan_entries(oldest_active_twig_id, youngest_twig_id, ef_prune_to);
        self.sync_mt_for_youngest_twig(true);
        let res = self.sync_mt_for_active_bits_phase1();
        let n_list = self.upper_tree.sync_mt_for_active_bits_phase2(res);
        self.clear_touched_pos();
        n_list
    }

    fn recover_upper_nodes(&mut self, edge_nodes: &Vec<EdgeNode>, n_list: Vec<u64>) -> [u8; 32] {
        for node in edge_nodes {
            self.upper_tree.set_node_copy(node.pos, &node.value);
        }
        let (_, root_hash) = self
            .upper_tree
            .sync_upper_nodes(n_list, self.youngest_twig_id);
        root_hash
    }

    fn recover_inactive_twig_roots(
        &mut self,
        last_pruned_twig_id: u64,
        oldest_active_twig_id: u64,
    ) {
        for twig_id in last_pruned_twig_id..oldest_active_twig_id {
            let mut left_root = [0; 32];
            self.twig_file_wr
                .twig_file
                .get_hash_root(twig_id, &mut left_root);
            let twig_root = hasher::hash2(11, &left_root[..], &NULL_TWIG.active_bits_mtl3[..]);
            let pos = NodePos::pos(TWIG_ROOT_LEVEL as u64, twig_id);
            self.upper_tree.set_node(pos, twig_root);
        }
    }
}

pub fn recover_tree(
    shard_id: usize,
    buffer_size: usize,
    segment_size: usize,
    with_twig_file: bool,
    dir_name: String,
    suffix: String,
    edge_nodes: &Vec<EdgeNode>,
    last_pruned_twig_id: u64,
    ef_prune_to: i64,
    oldest_active_twig_id: u64,
    youngest_twig_id: u64,
    file_sizes: &[i64],
    cipher: Option<Aes256Gcm>,
) -> (Tree, [u8; 32]) {
    let mut tree = Tree::new_blank(
        shard_id,
        buffer_size,
        segment_size as i64,
        dir_name,
        suffix,
        with_twig_file,
        cipher,
    );
    tree.youngest_twig_id = youngest_twig_id;

    if file_sizes.len() == 2 {
        debug!(
            "OldSize entryFile {} twigFile {}",
            tree.entry_file_wr.entry_file.size(),
            tree.twig_file_wr.twig_file.hp_file.size(),
        );
        debug!(
            "NewSize entryFile {} twigFile {}",
            file_sizes[0], file_sizes[1]
        );
        tree.truncate_files(file_sizes[0], file_sizes[1]);
    }

    let (s, i) = get_shard_idx_and_key(oldest_active_twig_id);
    tree.upper_tree.active_twig_shards[s].insert(i, NULL_TWIG.clone());
    tree.active_bit_shards[s].insert(i, NULL_ACTIVE_BITS.clone());
    tree.mtree_for_youngest_twig = NULL_MT_FOR_TWIG.clone();

    let mut starting_inactive_twig_id = last_pruned_twig_id;
    if last_pruned_twig_id == u64::MAX {
        starting_inactive_twig_id = 0;
    };
    if starting_inactive_twig_id % 2 == 1 {
        starting_inactive_twig_id -= 1;
    }
    // when twig_file is empty, we scan entries from 'ef_prune_to' instead of the oldest active
    // twig, so the inactive twig roots will be recovered in 'recover_twigs' instead of here
    if !tree.twig_file_wr.twig_file.is_empty() {
        tree.recover_inactive_twig_roots(starting_inactive_twig_id, oldest_active_twig_id);
    }

    for edge_node in edge_nodes {
        if edge_node.pos.level() == TWIG_ROOT_LEVEL as u64
            && starting_inactive_twig_id < edge_node.pos.nth()
        {
            starting_inactive_twig_id = edge_node.pos.nth();
        }
    }

    let mut n_list0 = calc_n_list(starting_inactive_twig_id, oldest_active_twig_id);
    let n_list = tree.recover_twigs(oldest_active_twig_id, youngest_twig_id, ef_prune_to);
    let new_list = if !n_list0.is_empty() && !n_list.is_empty() && n_list0.last() == n_list.first()
    {
        n_list0.extend_from_slice(&n_list[1..]);
        n_list0
    } else {
        n_list0.extend_from_slice(&n_list);
        n_list0
    };

    let youngest_twig = tree.upper_tree.get_twig(tree.youngest_twig_id).unwrap();
    tree.new_twig_map
        .insert(tree.youngest_twig_id, youngest_twig.clone());

    let root_hash = tree.recover_upper_nodes(edge_nodes, new_list);
    (tree, root_hash)
}

fn calc_n_list(starting_inactive_twig_id: u64, oldest_active_twig_id: u64) -> Vec<u64> {
    let mut n_list0 =
        Vec::with_capacity(1 + (oldest_active_twig_id - starting_inactive_twig_id) as usize / 2);
    for twig_id in starting_inactive_twig_id..oldest_active_twig_id {
        if n_list0.is_empty() || *n_list0.last().unwrap() != twig_id / 2 {
            n_list0.push(twig_id / 2);
        }
    }
    n_list0
}

#[cfg(test)]
mod tests {
    use super::calc_n_list;

    #[test]
    fn test_calc_n_list() {
        let n_list = calc_n_list(123, 234);
        assert_eq!(
            n_list,
            vec![
                61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81,
                82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101,
                102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116
            ]
        );
    }
}
