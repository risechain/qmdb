use super::entry::EntryBz;
use crate::def::{BYTES_CACHE_SHARD_COUNT, SHARD_COUNT};
use crate::utils::bytescache::BytesCache;

pub struct EntryCache {
    bc: Vec<BytesCache<i64>>,
}

impl Default for EntryCache {
    fn default() -> Self {
        Self::new()
    }
}

impl EntryCache {
    pub fn new() -> Self {
        let mut bc = Vec::with_capacity(SHARD_COUNT);
        for _ in 0..SHARD_COUNT {
            bc.push(BytesCache::<i64>::new());
        }
        EntryCache { bc }
    }

    pub fn clear(&self) {
        for cache in self.bc.iter() {
            cache.clear();
        }
    }

    pub fn new_uninit() -> Self {
        EntryCache {
            bc: Vec::with_capacity(0),
        }
    }

    fn pos2idx(file_pos: i64) -> usize {
        // make the low 9 bits have more randomness
        let tmp = ((file_pos >> 8) ^ file_pos) as usize;
        tmp % BYTES_CACHE_SHARD_COUNT
    }

    pub fn allocate_if_missing(&self, shard_id: usize, file_pos: i64, entry_pos: u64) -> bool {
        let idx = Self::pos2idx(file_pos);
        self.bc[shard_id].allocate_if_missing(&file_pos, idx, entry_pos)
    }

    pub fn insert(&self, shard_id: usize, file_pos: i64, entry_bz: &EntryBz) {
        let idx = Self::pos2idx(file_pos);
        self.bc[shard_id].insert(&file_pos, idx, entry_bz.bz)
    }

    pub fn lookup<F>(&self, shard_id: usize, file_pos: i64, mut access: F) -> bool
    where
        F: FnMut(EntryBz),
    {
        let idx = Self::pos2idx(file_pos);
        self.bc[shard_id].lookup(&file_pos, idx, |cache_bz: &[u8]| {
            let size = EntryBz::get_entry_len(cache_bz);
            access(EntryBz {
                bz: &cache_bz[..size],
            });
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helper::EntryBuilder;

    fn lookup_entry(cache: &EntryCache, shard_id: usize, pos: i64) -> Vec<u8> {
        let mut found: Vec<u8> = Vec::new();
        cache.lookup(shard_id, pos, |cache_bz| {
            found.resize(cache_bz.len(), 0);
            found.copy_from_slice(cache_bz.bz);
        });
        found
    }

    #[test]
    fn test_insert_lookup() {
        let cache = EntryCache::new();
        let shard_id = 7;

        let entry1 = EntryBuilder::kv("key1", "val1").build_and_dump(&[1]);
        let entry2 = EntryBuilder::kv("key2", "val2").build_and_dump(&[2]);
        let entry3 = EntryBuilder::kv("key3", "val3").build_and_dump(&[3]);
        cache.insert(shard_id, 1234, &EntryBz { bz: &entry1[..] });
        cache.insert(shard_id, 2345, &EntryBz { bz: &entry2[..] });
        cache.insert(shard_id, 1746, &EntryBz { bz: &entry3[..] });

        assert_eq!(entry1, lookup_entry(&cache, shard_id, 1234));
        assert_eq!(entry2, lookup_entry(&cache, shard_id, 2345));
        assert_eq!(entry3, lookup_entry(&cache, shard_id, 1746));

        assert!(cache.lookup(shard_id, 1234, |_| {}));
        assert!(!cache.lookup(shard_id, 4321, |_| {}));
        assert!(!cache.lookup(shard_id, 8888, |_| {}));
    }
}
