use std::collections::HashMap;
use std::sync::Arc;

use byteorder::{ByteOrder, LittleEndian};

use crate::def::HPFILE_RANGE;
use crate::utils::hasher::hash2;
use hpfile::HPFile;
use xxhash_rust::xxh32;

/*
We store top levels(1~255) and the leaves(2048~4095), while the middle levels are ignored to save disk
When we need to read the nodes in ignored levels, we must compute their value on-the-fly from leaves
Level_11  =  1
Level_10  =  2~3
Level_9   =  4~7
Level_8   =  8~15
Level_7   =  16~31
Level_6   =  32~63
Level_5   =  64~127
Level_4   =  128~255
Level_3   X  256~511
Level_2   X  512~1023
Level_1   X  1024~2047
Level_0   =  2048~4095
*/

const IGNORED_COUNT: u64 = 2048 - 256; // the count of ignored nodes
const TWIG_FULL_LENGTH: u64 = 4095; // the total count of all the internal nodes and the leaves
const TWIG_ENTRY_COUNT: u64 = TWIG_FULL_LENGTH - IGNORED_COUNT;
pub const TWIG_SIZE: u64 = 12 + TWIG_ENTRY_COUNT * 32;

#[derive(Debug)]
pub struct TwigFile {
    pub hp_file: HPFile,
}

impl TwigFile {
    pub fn new(buffer_size: usize, segment_size: i64, dir_name: String) -> TwigFile {
        TwigFile {
            hp_file: HPFile::new(buffer_size as i64, segment_size, dir_name, false).unwrap(),
        }
    }

    pub fn empty() -> TwigFile {
        TwigFile {
            hp_file: HPFile::empty(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.hp_file.is_empty()
    }

    fn append_twig(&self, m_tree: &[[u8; 32]], last_entry_end_pos: i64, buffer: &mut Vec<u8>) {
        if self.hp_file.is_empty() {
            return;
        }
        if last_entry_end_pos < 0 {
            panic!("Invalid last entry end position: {}", last_entry_end_pos);
        }
        if m_tree.len() != TWIG_FULL_LENGTH as usize + 1 {
            panic!("len(m_tree): {} != {}", m_tree.len(), TWIG_FULL_LENGTH);
        }
        // last_entry_end_pos and its 32b hash need 12 bytes
        let mut buf: [u8; 12] = [0; 12];
        LittleEndian::write_i64(&mut buf[0..8], last_entry_end_pos);
        let digest = xxh32::xxh32(&buf[0..8], 0);
        LittleEndian::write_u32(&mut buf[8..], digest);
        _ = self.hp_file.append(&buf[..], buffer);
        // only write the higher levels and leaf nodes, middle levels are ignored
        for i in 1..256 {
            _ = self.hp_file.append(&m_tree[i as usize][..], buffer);
        }
        for i in 2048..TWIG_FULL_LENGTH + 1 {
            _ = self.hp_file.append(&m_tree[i as usize][..], buffer);
        }
    }

    pub fn get_first_entry_pos(&self, mut twig_id: u64) -> i64 {
        if twig_id == 0 {
            return 0;
        }
        // the end pos of previous twig's last entry is the
        // pos of current twig's first entry
        twig_id -= 1;
        let mut buf: [u8; 12] = [0; 12];
        self.hp_file
            .read_at(&mut buf[..], (twig_id * TWIG_SIZE) as i64 % HPFILE_RANGE)
            .unwrap();
        let mut digest = [0; 4];
        LittleEndian::write_u32(&mut digest, xxh32::xxh32(&buf[0..8], 0));
        assert_eq!(buf[8..], digest[..], "Checksum Error!");
        LittleEndian::read_i64(&buf[0..8])
    }

    // for the ignored middle layer, we must calculate the node's value from leaves in a range
    pub fn get_leaf_range(hash_id: i64) -> (i64, i64) {
        if (256..512).contains(&hash_id) {
            // level_3 : 256~511
            (hash_id * 8, hash_id * 8 + 8)
        } else if hash_id < 1024 {
            //level_2 : 512~1023
            return ((hash_id / 2) * 8, (hash_id / 2) * 8 + 8);
        } else if hash_id < 2048 {
            //level_1 : 1024~2047
            return ((hash_id / 4) * 8, (hash_id / 4) * 8 + 8);
        } else {
            panic!("Invalid hashID")
        }
    }

    pub fn get_hash_node_in_ignore_range(
        &self,
        twig_id: u64,
        hash_id: i64,
        cache: &mut HashMap<i64, [u8; 32]>,
        out: &mut [u8; 32],
    ) {
        let (start, end) = Self::get_leaf_range(hash_id);
        let mut buf = [0; 32 * 8];
        let offset =
            twig_id * TWIG_SIZE + 12 + (start as u64 - 1/*because hash_id starts from 1*/) * 32
                - IGNORED_COUNT * 32;
        let offset = (offset as i64) % HPFILE_RANGE;
        let num_bytes_read = self.hp_file.read_at(&mut buf[..], offset).unwrap_or(0);
        if num_bytes_read != buf.len() {
            // Cannot read them in one call because of file-crossing
            for i in 0..8 {
                //read them in 8 steps in case of file-crossing
                self.hp_file
                    .read_at(&mut buf[i * 32..i * 32 + 32], offset + (i * 32) as i64)
                    .unwrap();
            }
        }
        // recover a little cone with 8 leaves into cache
        // this little cone will be queried by 'get_proof'
        let mut level = 0;
        for i in start / 2..end / 2 {
            let off = ((i - start / 2) * 64) as usize;
            let v = hash2(level, &buf[off..off + 32], &buf[off + 32..off + 64]);
            cache.insert(i, v);
        }
        level = 1;
        for i in start / 4..end / 4 {
            let v = hash2(
                level,
                cache.get(&(i * 2)).unwrap(),
                cache.get(&(i * 2 + 1)).unwrap(),
            );
            cache.insert(i, v);
        }
        level = 2;
        let id = start / 8;
        let v = hash2(
            level,
            cache.get(&(id * 2)).unwrap(),
            cache.get(&(id * 2 + 1)).unwrap(),
        );
        cache.insert(id, v);
        out.copy_from_slice(cache.get(&hash_id).unwrap().as_slice());
    }

    pub fn get_hash_root(&self, twig_id: u64, buf: &mut [u8; 32]) {
        self.get_hash_node(twig_id, 1, &mut HashMap::new(), buf)
    }

    pub fn get_hash_node<'a>(
        &'a self,
        twig_id: u64,
        hash_id: i64,
        cache: &'a mut HashMap<i64, [u8; 32]>,
        buf: &mut [u8; 32],
    ) {
        if hash_id <= 0 || hash_id >= 4096 {
            panic!("Invalid hashID: {}", hash_id);
        }
        if (256..2048).contains(&hash_id) {
            return self.get_hash_node_in_ignore_range(twig_id, hash_id, cache, buf);
        }
        let mut offset =
            twig_id * TWIG_SIZE + 12 + (hash_id as u64 - 1/*because hash_id starts from 1*/) * 32;
        if hash_id >= 2048 {
            offset -= IGNORED_COUNT * 32;
        }
        let offset = (offset as i64) % HPFILE_RANGE;
        self.hp_file
            .read_at(&mut buf[..], offset)
            .expect("TODO: panic message");
    }

    pub fn truncate(&self, size: i64) {
        if let Err(err) = self.hp_file.truncate(size) {
            panic!("{}", err)
        }
    }

    pub fn close(&self) {
        self.hp_file.close();
    }

    pub fn prune_head(&self, off: i64) {
        if let Err(err) = self.hp_file.prune_head(off) {
            panic!("{}", err)
        }
    }
}

pub struct TwigFileWriter {
    pub twig_file: Arc<TwigFile>,
    wrbuf: Vec<u8>,
}

impl TwigFileWriter {
    pub fn new(twig_file: Arc<TwigFile>, buffer_size: usize) -> TwigFileWriter {
        TwigFileWriter {
            twig_file,
            wrbuf: Vec::with_capacity(buffer_size),
        }
    }

    pub fn temp_clone(&self) -> TwigFileWriter {
        TwigFileWriter {
            twig_file: self.twig_file.clone(),
            wrbuf: Vec::with_capacity(0),
        }
    }

    pub fn append_twig(&mut self, m_tree: &[[u8; 32]], last_entry_end_pos: i64) {
        self.twig_file
            .append_twig(m_tree, last_entry_end_pos, &mut self.wrbuf);
    }

    pub fn flush(&mut self) {
        self.twig_file
            .hp_file
            .flush(&mut self.wrbuf, false)
            .unwrap();
    }
}

#[cfg(test)]
mod twig_file_test {
    use super::*;
    use crate::{
        merkletree::twig::{sync_mtree, TwigMT},
        test_helper::TempDir,
    };

    fn generate_twig(mut rand_num: u32, twig: &mut TwigMT) {
        // let mut rand_num = rand_num;
        for i in 2048..4096 {
            let mut j = 0;
            while j + 4 < 32 {
                LittleEndian::write_u32(&mut twig[i][j..j + 4], rand_num);
                rand_num += 257;
                j += 4;
            }
        }
        sync_mtree(twig, 0, 2048);
    }

    #[test]
    fn twig_file_all() {
        let dir = TempDir::new("./twig");

        let tf = TwigFile::new(64 * 1024, 1024 * 1024, dir.to_str());

        let mut twigs = [[[0; 32]; 4096]; 3];
        generate_twig(1000, &mut twigs[0]);
        generate_twig(1111111, &mut twigs[1]);
        generate_twig(2222222, &mut twigs[2]);
        let mut buffer = vec![];
        tf.append_twig(&twigs[0][..], 789, &mut buffer);
        tf.append_twig(&twigs[1][..], 1000789, &mut buffer);
        tf.append_twig(&twigs[2][..], 2000789, &mut buffer);

        let _ = tf.hp_file.flush(&mut buffer, false);
        tf.close();

        let tf = TwigFile::new(64 * 1024, 1024 * 1024, dir.to_str());
        assert_eq!(0, tf.get_first_entry_pos(0));
        assert_eq!(789, tf.get_first_entry_pos(1));
        assert_eq!(1000789, tf.get_first_entry_pos(2));
        assert_eq!(2000789, tf.get_first_entry_pos(3));

        for twig_id in 0..3 {
            for i in 1..4096 {
                let mut cache = HashMap::<i64, [u8; 32]>::new();
                let mut buf = [0; 32];
                tf.get_hash_node(twig_id, i, &mut cache, &mut buf);
                assert_eq!(buf[..], twigs[twig_id as usize][i as usize][..]);
            }
        }
        for twig_id in 0..3 {
            let mut cache = HashMap::<i64, [u8; 32]>::new();
            for i in 1..4096 {
                if cache.contains_key(&i) {
                    let bz = cache.get(&i).unwrap();
                    assert_eq!(&twigs[twig_id as usize][i as usize][..], bz.as_slice());
                } else {
                    let mut buf = [0; 32];
                    tf.get_hash_node(twig_id, i, &mut cache, &mut buf);
                    assert_eq!(buf[..], twigs[twig_id as usize][i as usize][..]);
                }
            }
        }
        tf.close();
    }
}
