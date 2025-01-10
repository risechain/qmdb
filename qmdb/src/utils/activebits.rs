use dashmap::DashMap;
use std::sync::atomic::{AtomicU32, Ordering};

const U32_COUNT: u64 = 1024;
const BIT_COUNT: u64 = 32 * U32_COUNT;

struct Segment {
    count: AtomicU32,
    arr: [AtomicU32; U32_COUNT as usize],
}

const ZERO: AtomicU32 = AtomicU32::new(0);

impl Segment {
    fn new() -> Self {
        Self {
            count: ZERO,
            arr: [ZERO; U32_COUNT as usize],
        }
    }

    fn get(&self, n: usize) -> bool {
        let (i, j) = (n / 32, n % 32);
        let mask = 1u32 << j;
        let old = self.arr[i].load(Ordering::SeqCst);
        (old & mask) != 0
    }

    fn set(&self, n: usize) {
        let (i, j) = (n / 32, n % 32);
        let mask = 1u32 << j;
        let old = self.arr[i].fetch_or(mask, Ordering::SeqCst);
        if (old & mask) == 0 {
            self.count.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn clear(&self, n: usize) -> bool {
        let (i, j) = (n / 32, n % 32);
        let mask = 1u32 << j;
        let old = self.arr[i].fetch_and(!mask, Ordering::SeqCst);
        if (old & mask) != 0 {
            let old_count = self.count.fetch_sub(1, Ordering::SeqCst);
            return old_count == 1; //need to remove myself
        }
        false
    }
}

pub struct ActiveBits {
    m: DashMap<u64, Box<Segment>>,
}

impl ActiveBits {
    pub fn with_capacity(n: usize) -> Self {
        Self {
            m: DashMap::with_capacity(n),
        }
    }

    pub fn get(&self, n: u64) -> bool {
        let (i, j) = (n / BIT_COUNT, n % BIT_COUNT);
        if let Some(seg) = self.m.get(&i) {
            return seg.get(j as usize);
        }
        false
    }

    pub fn set(&self, n: u64) {
        let (i, j) = (n / BIT_COUNT, n % BIT_COUNT);
        let need_allocate = {
            if let Some(seg) = self.m.get(&i) {
                seg.set(j as usize);
                false
            } else {
                true
            }
        };
        if need_allocate {
            let seg = Box::new(Segment::new());
            seg.set(j as usize);
            self.m.insert(i, seg);
        }
    }

    pub fn clear(&self, n: u64) {
        let (i, j) = (n / BIT_COUNT, n % BIT_COUNT);
        let need_remove = {
            if let Some(seg) = self.m.get(&i) {
                seg.clear(j as usize)
            } else {
                false
            }
        };
        if need_remove {
            self.m.remove(&i);
        }
    }
}

#[cfg(test)]
mod segments_tests {
    use super::*;

    #[test]
    fn test_segment() {
        let segment = Segment::new();

        // Test set and get
        assert!(!segment.get(0));
        segment.set(0);
        assert!(segment.get(0));
        assert!(!segment.get(1));

        // Test clear
        assert!(!segment.clear(1)); // Clearing unset bit
        assert!(segment.clear(0)); // Clearing last set bit
        assert!(!segment.get(0));

        // Test multiple bits
        segment.set(31);
        segment.set(32);
        assert!(segment.get(31));
        assert!(segment.get(32));
        assert!(!segment.get(33));

        // Test count
        assert_eq!(segment.count.load(Ordering::SeqCst), 2);
        segment.clear(31);
        assert_eq!(segment.count.load(Ordering::SeqCst), 1);
        segment.clear(32);
        assert_eq!(segment.count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_segment_edge_cases() {
        let segment = Segment::new();

        // Test first and last bits
        segment.set(0);
        segment.set(BIT_COUNT as usize - 1);
        assert!(segment.get(0));
        assert!(segment.get(BIT_COUNT as usize - 1));
        assert_eq!(segment.count.load(Ordering::SeqCst), 2);

        // Test setting already set bit
        segment.set(0);
        assert_eq!(segment.count.load(Ordering::SeqCst), 2);

        // Test clearing unset bit
        assert!(!segment.clear(1));
        assert_eq!(segment.count.load(Ordering::SeqCst), 2);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_set_clear() {
        let ab = ActiveBits::with_capacity(10);

        // Test set and get
        ab.set(42);
        assert!(ab.get(42));
        assert!(!ab.get(43));

        // Test clear
        ab.clear(42);
        assert!(!ab.get(42));
    }

    #[test]
    fn test_large_numbers() {
        let ab = ActiveBits::with_capacity(10);

        let large_num = u64::MAX - 1;
        ab.set(large_num);
        assert!(ab.get(large_num));
        assert!(!ab.get(large_num - 1));
        assert!(!ab.get(large_num + 1));

        ab.clear(large_num);
        assert!(!ab.get(large_num));
    }

    #[test]
    fn test_multiple_segments() {
        let ab = ActiveBits::with_capacity(10);

        let num1 = BIT_COUNT - 1;
        let num2 = BIT_COUNT;
        let num3 = BIT_COUNT + 1;

        ab.set(num1);
        ab.set(num2);
        ab.set(num3);

        assert!(ab.get(num1));
        assert!(ab.get(num2));
        assert!(ab.get(num3));

        ab.clear(num2);
        assert!(ab.get(num1));
        assert!(!ab.get(num2));
        assert!(ab.get(num3));
    }
}
