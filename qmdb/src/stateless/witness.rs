use std::collections::HashMap;

use crate::{
    entryfile::{EntryBz, EntryVec},
    merkletree::twig::{NULL_ACTIVE_BITS, NULL_MT_FOR_TWIG, NULL_NODE_IN_HIGHER_TREE, NULL_TWIG},
    utils::{hasher, slice::try_get_slice},
};

use super::{
    node::{Node, ValuePair},
    utils::{level0nth_to_level8nth, sn_to_level0nth, sn_to_level8nth},
};

// level(8) + nth(64) + NodeValueType(1) + old_value(0/32/8)
enum NodeValueType {
    Null = 0,
    ActiveBits,
    MerkleOnly,
    Calculated,
}

impl TryFrom<u8> for NodeValueType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(NodeValueType::Null),
            1 => Ok(NodeValueType::ActiveBits),
            2 => Ok(NodeValueType::MerkleOnly),
            3 => Ok(NodeValueType::Calculated),
            _ => Err(()),
        }
    }
}

fn get_null_hash_by_pos(level: u8, nth: u64) -> [u8; 32] {
    if level > 12 {
        return NULL_NODE_IN_HIGHER_TREE[level as usize];
    }

    let stride = 1 << (12 - level) as usize;
    let _nth = nth as usize % stride;
    if level == 12 {
        return NULL_TWIG.twig_root;
    }
    if level == 11 && _nth == 0 {
        return NULL_TWIG.left_root;
    }
    if _nth >= stride / 2 {
        if level == 8 {
            return NULL_ACTIVE_BITS.get_bits(_nth - 8, 32).try_into().unwrap();
        }
        if level == 9 {
            return NULL_TWIG.active_bits_mtl1[_nth - 4];
        }
        if level == 10 {
            return NULL_TWIG.active_bits_mtl2[_nth - 2];
        }
        if level == 11 {
            return NULL_TWIG.active_bits_mtl3;
        }
    }
    NULL_MT_FOR_TWIG[stride / 2 + _nth]
}

// Witness can help proving:
// 1) old data against the pre-root
// 2) new data lead to the post-root
// initially it only contains old data, the stateless validator adds
// new data after execution
pub struct Witness {
    shard_id: usize,
    nodes: Vec<Node>,
    level0map: HashMap<u64, usize>,
    level8map: HashMap<u64, usize>,
}

impl Witness {
    pub fn new(shared_id: usize, nodes: Vec<Node>) -> Self {
        let mut level0map = HashMap::new();
        let mut level8map = HashMap::new();
        for (idx, node) in nodes.iter().enumerate() {
            if node.level == 0 {
                level0map.insert(node.nth, idx);
            } else if node.level == 8 {
                level8map.insert(node.nth, idx);
            }
        }
        Witness {
            shard_id: shared_id,
            nodes,
            level0map,
            level8map,
        }
    }

    // from encoded witness bytes
    pub fn try_from_witness_bz(
        shared_id: usize,
        witness_bz: &Vec<u8>,
        entry_vec: &EntryVec,
    ) -> Result<Witness, String> {
        let mut nodes = vec![];
        let mut idx = 0;
        loop {
            let level =
                u8::from_be_bytes(try_get_slice(witness_bz, idx, idx + 1)?.try_into().unwrap());
            idx += 1;
            let nth =
                u64::from_be_bytes(try_get_slice(witness_bz, idx, idx + 8)?.try_into().unwrap());
            idx += 8;

            let old_value_type = try_get_slice(witness_bz, idx, idx + 1)?[0];
            idx += 1;
            match NodeValueType::try_from(old_value_type) {
                Ok(NodeValueType::Null) => {
                    let mut node = Node::new(level, nth, true);
                    let null_hash = get_null_hash_by_pos(level, nth);
                    node.set_old_and_new(&null_hash[..]);
                    nodes.push(node);
                }
                Ok(NodeValueType::ActiveBits) => {
                    let mut node = Node::new(level, nth, true);
                    let hash = try_get_slice(witness_bz, idx, idx + 32)?;
                    idx += 32;
                    node.set_old_and_new(hash);
                    nodes.push(node);
                }
                Ok(NodeValueType::MerkleOnly) => {
                    let node = Node::new(level, nth, false);
                    nodes.push(node);
                }
                Ok(NodeValueType::Calculated) => {
                    let mut node = Node::new(level, nth, true);
                    let mut entry_idx = [0u8; 8];
                    entry_idx.copy_from_slice(try_get_slice(witness_bz, idx, idx + 8)?);
                    idx += 8;
                    let entry_idx = u64::from_be_bytes(entry_idx);
                    let hash = entry_vec.get_entry(shared_id, entry_idx as usize).hash();
                    node.set_old_and_new(&hash[..]);
                    nodes.push(node);
                }
                _ => return Err("Invalid old_value_type".to_string()),
            }

            if idx >= witness_bz.len() {
                break;
            }
        }
        let witness = Witness::new(shared_id, nodes);
        if !witness.verify_entries(entry_vec) {
            return Err("Inactive entry".to_string());
        }
        Ok(witness)
    }

    // check the integrity and correctness of witness
    pub fn verify_witness(
        &self,
        merkle_nodes: &[u8],
        old_root: &[u8; 32],
        new_root: &[u8; 32],
    ) -> bool {
        let mut stack = Vec::<Node>::new();
        let mut idx = 0;
        let mut off = 0;
        loop {
            let mut two_children_at_top = false;
            if stack.len() >= 2 {
                let a = &stack[stack.len() - 1];
                let b = &stack[stack.len() - 2];
                two_children_at_top = a.level == b.level && (a.nth ^ b.nth) == 1;
            }
            // if we have two children at the stack top, we can
            // pop them out to calculate the parent
            if two_children_at_top {
                let right = stack.pop().unwrap();
                let left = stack.pop().unwrap();
                let mut parent = Node::new(left.level + 1, left.nth / 2, true);
                // println!(
                //     "----combine_level_{}: {:?} + {:?} = {}-{}",
                //     left.level, left.nth, right.nth, parent.level, parent.nth
                // );
                hasher::node_hash_inplace(
                    left.level,
                    &mut parent.value.as_mut().unwrap().old,
                    left.get_old(),
                    right.get_old(),
                );
                hasher::node_hash_inplace(
                    left.level,
                    &mut parent.value.as_mut().unwrap().new,
                    left.get_new(),
                    right.get_new(),
                );
                // println!(
                //     "verify_witness/new_value: {:?} + {:?} = {:?}",
                //     left.get_new(),
                //     right.get_new(),
                //     parent.get_new()
                // );
                stack.push(parent);
            } else if idx >= self.len() {
                break;
            } else {
                let mut e = self.get(idx).clone();
                if e.value.is_none() {
                    e.value = Some(Box::new(ValuePair {
                        old: [0u8; 32],
                        new: [0u8; 32],
                    }));
                    e.set_old_and_new(&merkle_nodes[off..off + 32]);
                    off += 32;
                }
                idx += 1;
                stack.push(e);
            }
        }
        if stack.len() != 1 {
            return false;
        }
        let root = stack.pop().unwrap();
        root.get_old() == *old_root && root.get_new() == *new_root
    }

    // encode the witness data into bytes
    pub fn encode_witness(&self, entries: &EntryVec) -> (Vec<u8>, Vec<u8>) {
        let mut level0nth_to_entry_idx = HashMap::new();
        for (i, entry) in entries.enumerate(self.shard_id) {
            let sn = entry.serial_number();
            level0nth_to_entry_idx.insert(sn_to_level0nth(sn), i as u64);
        }

        let mut bz = vec![];
        let mut merkle_nodes = vec![];
        for item in self.iter() {
            bz.extend(item.level.to_be_bytes());
            bz.extend(item.nth.to_be_bytes());

            if item.get_old() == get_null_hash_by_pos(item.level, item.nth) {
                bz.push(NodeValueType::Null as u8);
                continue;
            }
            if item.level == 0 {
                if let Some(idx) = level0nth_to_entry_idx.get(&item.nth) {
                    bz.push(NodeValueType::Calculated as u8);
                    bz.extend(idx.to_be_bytes());
                    continue;
                }
            }

            if item.level == 8 {
                bz.push(NodeValueType::ActiveBits as u8);
                bz.extend(&item.get_old());
            } else {
                bz.push(NodeValueType::MerkleOnly as u8);
                merkle_nodes.extend(&item.get_old());
            }
        }
        (bz, merkle_nodes)
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Node> {
        self.nodes.iter()
    }

    pub fn get(&self, idx: usize) -> &Node {
        &self.nodes[idx]
    }

    pub fn get_active_bit(&self, sn: u64) -> Option<bool> {
        let level8nth = sn_to_level8nth(sn);
        if let Some(&idx) = self.level8map.get(&level8nth) {
            return Some(self.nodes[idx].actived(false, sn % 256));
        }
        None
    }

    // find the next active sn after the given sn
    pub fn find_next_active_sn(&self, mut start_sn: u64) -> Option<u64> {
        loop {
            let level8nth = sn_to_level8nth(start_sn);
            if let Some(&idx) = self.level8map.get(&level8nth) {
                let node = &self.nodes[idx];
                for bit_idx in (start_sn % 256)..256 {
                    if node.actived(false, bit_idx) {
                        return Some(node.get_entry_sn(bit_idx as u8));
                    }
                }
            } else {
                return None;
            }
            start_sn = (start_sn + 256) / 256 * 256;
        }
    }

    // the stateless validator adds new data (newly created entries) using 'add_entry'
    pub fn add_entry(&mut self, entry: &EntryBz) -> bool {
        let level0nth = sn_to_level0nth(entry.serial_number());
        if let Some(&idx) = self.level0map.get(&level0nth) {
            self.nodes[idx].set_new(&entry.hash());
        } else {
            return false;
        }

        let level8nth = level0nth_to_level8nth(level0nth);
        if let Some(&idx) = self.level8map.get(&(level8nth)) {
            self.nodes[idx].set_actived((level0nth % 256) as usize, true);
        } else {
            return false;
        }

        for i in 0..entry.dsn_count() {
            let sn = entry.get_deactived_sn(i);
            let level8nth = sn_to_level8nth(sn);
            if let Some(&idx) = self.level8map.get(&(level8nth)) {
                self.nodes[idx].set_actived((sn % 256) as usize, false);
            } else {
                return false;
            }
        }
        true
    }

    // verify the entries' activebits
    fn verify_entries(&self, entries: &EntryVec) -> bool {
        for (_, entry) in entries.enumerate(self.shard_id) {
            let sn = entry.serial_number();
            let level8nth = sn_to_level8nth(sn);
            if let Some(&idx) = self.level8map.get(&level8nth) {
                if !self.nodes[idx].actived(true, sn) {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    }

    // debug
    pub fn print(&self) {
        for node in self.iter() {
            print!("L:{}, N:{},", node.level, node.nth);
            if node.value.is_some() {
                let pair = node.value.as_ref().unwrap();
                println!(
                    "  OV:{}, NV:{}",
                    hex::encode(pair.old),
                    hex::encode(pair.new)
                );
            } else {
                println!("  NULL");
            }
        }
    }
}
