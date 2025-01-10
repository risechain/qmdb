#[derive(Debug, Clone, PartialEq)]
pub struct ValuePair {
    pub old: [u8; 32],
    pub new: [u8; 32],
}

#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    pub value: Option<Box<ValuePair>>,
    pub level: u8,
    pub nth: u64,
}

impl Node {
    pub fn new(level: u8, nth: u64, initial_value: bool) -> Self {
        Node {
            value: if initial_value {
                Some(Box::new(ValuePair {
                    old: [0u8; 32],
                    new: [0u8; 32],
                }))
            } else {
                None
            },
            level,
            nth,
        }
    }

    pub fn get_old(&self) -> [u8; 32] {
        self.value.as_ref().unwrap().old
    }

    pub fn get_new(&self) -> [u8; 32] {
        self.value.as_ref().unwrap().new
    }

    pub fn get_pair_mut(&mut self) -> &mut ValuePair {
        self.value.as_mut().unwrap()
    }

    pub fn set_old_and_new(&mut self, v: &[u8]) {
        let pair = self.value.as_mut().unwrap();
        pair.old.copy_from_slice(v);
        pair.new.copy_from_slice(v);
    }

    pub fn set_old(&mut self, v: &[u8; 32]) {
        let pair = self.value.as_mut().unwrap();
        pair.old.copy_from_slice(&v[..]);
    }

    pub fn set_new(&mut self, v: &[u8; 32]) {
        let pair = self.value.as_mut().unwrap();
        pair.new.copy_from_slice(&v[..]);
    }

    fn is_activebits(&self) -> bool {
        self.level == 8 && self.nth % 16 >= 8
    }

    pub fn get_entry_sn(&self, bit_idx: u8) -> u64 {
        if !self.is_activebits() {
            panic!("not a leaf activebits");
        }
        let twig_id = self.nth / 16;
        twig_id * 2048 + ((self.nth % 16) - 8) * 256 + bit_idx as u64
    }

    pub fn actived(&self, for_old: bool, sn_or_bit_idx: u64) -> bool {
        let bit_idx = sn_or_bit_idx % 256;
        let activebits = if for_old {
            self.get_old()
        } else {
            self.get_new()
        };
        ((activebits[bit_idx as usize / 8] >> (bit_idx % 8)) & 1) == 1
    }

    pub fn set_actived(&mut self, bit_idx: usize, actived: bool) {
        let mask = 1u8 << (bit_idx % 8);
        let pair = self.get_pair_mut();
        if actived {
            pair.new[bit_idx / 8] |= mask;
        } else {
            pair.new[bit_idx / 8] &= !mask;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node() {
        let mut node = Node::new(5, 10, false);

        // Test initial state
        assert_eq!(node.level, 5);
        assert_eq!(node.nth, 10);
        assert!(node.value.is_none());

        // Set old and new values
        let old_value = [1u8; 32];
        let new_value = [2u8; 32];
        node.value = Some(Box::new(ValuePair {
            old: old_value,
            new: new_value,
        }));

        // Test get_old and get_new
        assert_eq!(node.get_old(), old_value);
        assert_eq!(node.get_new(), new_value);

        // Test set_old and set_new
        let updated_old = [3u8; 32];
        let updated_new = [4u8; 32];
        node.set_old(&updated_old);
        node.set_new(&updated_new);
        assert_eq!(node.get_old(), updated_old);
        assert_eq!(node.get_new(), updated_new);

        // Test is_leaf methods
        let leaf_entry = Node::new(0, 1000, false);
        assert!(!leaf_entry.is_activebits());

        let activebits = Node::new(8, 24, false);
        assert!(activebits.is_activebits());

        // Test get_entry_sn
        assert_eq!(activebits.get_entry_sn(0), 8 * 256);
        assert_eq!(activebits.get_entry_sn(255), 8 * 256 + 255);

        // Test actived and set_actived
        let mut node_with_activebits = Node::new(8, 24, false);
        node_with_activebits.value = Some(Box::new(ValuePair {
            old: [0u8; 32],
            new: [0u8; 32],
        }));

        assert!(!node_with_activebits.actived(false, 0));
        node_with_activebits.set_actived(0, true);
        assert!(node_with_activebits.actived(false, 0));

        assert!(!node_with_activebits.actived(false, 7));
        node_with_activebits.set_actived(7, true);
        assert!(node_with_activebits.actived(false, 7));
    }
}

// ... rest of the existing code ...
