pub mod hybrid;
pub mod inmem;

#[cfg(feature = "use_hybridindexer")]
pub type Indexer = hybrid::HybridIndexer;

#[cfg(not(feature = "use_hybridindexer"))]
pub type Indexer = inmem::InMemIndexer;

#[cfg(test)]
mod tests {
    use crate::test_helper::to_k80;

    use super::*;

    fn get_all_values(bt: &Indexer, k80: &[u8]) -> Vec<i64> {
        let mut res = Vec::new();
        bt.for_each_value(-1, k80, |v| -> bool {
            res.push(v);
            false
        });
        res
    }

    fn get_all_adjacent_values(bt: &Indexer, k80: &[u8]) -> Vec<([u8; 10], i64)> {
        let mut kv_out = Vec::new();
        bt.for_each_adjacent_value(-1, k80, |k, v| -> bool {
            let mut _k = [0u8; 10];
            _k.copy_from_slice(k);
            kv_out.push((_k, v));
            false
        });
        kv_out
    }

    //#[test]
    //#[should_panic(expected = "Add Duplicated KV")]
    //fn test_panic_duplicate_add_kv() {
    //    let bt = Indexer::new(32);
    //    bt.add_kv(&to_k80(0x0004000300020001), 0x10, 0);
    //    bt.add_kv(&to_k80(0x0004000300020001), 0x10, 0);
    //}

    #[test]
    #[should_panic(expected = "Cannot Erase Non-existent KV")]
    fn test_panic_erase_non_existent_kv() {
        let bt = Indexer::new(32);
        bt.add_kv(&to_k80(0x0004000300020001), 0x10, 0);
        bt.erase_kv(&to_k80(0x0004000300020000), 0x10, 0);
    }

    #[test]
    fn test_add_kv() {
        let bt = Indexer::new(65535);
        bt.add_kv(&to_k80(0x1111_2222_3333_4444), 888, 0);
        bt.erase_kv(&to_k80(0x1111_2222_3333_4444), 888, 0);
    }

    #[test]
    fn test_btree() {
        let bt = Indexer::new(32);
        assert_eq!(0, bt.len(0));
        bt.add_kv(&to_k80(0x0004000300020001), 0x10, 0);
        assert_eq!(1, bt.len(0));
        bt.add_kv(&to_k80(0x0005000300020001), 0x10, 0);
        assert_eq!(2, bt.len(0));
        bt.add_kv(&to_k80(0x0004000300020001), 0x00, 0);

        assert_eq!(
            [
                (to_k80(0x0004000300020001), 0),
                (to_k80(0x0004000300020001), 0x10)
            ],
            get_all_adjacent_values(&bt, &to_k80(0x0004000300020001)).as_slice()
        );

        bt.add_kv(&to_k80(0x0004000300020000), 0x20, 0);
        bt.add_kv(&to_k80(0x0004000300020000), 0x30, 0);
        assert_eq!(5, bt.len(0));

        assert_eq!(
            [0x0, 0x10],
            get_all_values(&bt, &to_k80(0x0004000300020001)).as_slice()
        );
        assert_eq!(
            [0x30, 0x20],
            get_all_values(&bt, &to_k80(0x0004000300020000)).as_slice()
        );
        assert_eq!(
            [
                (to_k80(0x0004000300020001), 0),
                (to_k80(0x0004000300020001), 0x10),
                (to_k80(0x0004000300020000), 0x20),
                (to_k80(0x0004000300020000), 0x30),
            ],
            get_all_adjacent_values(&bt, &to_k80(0x0004000300020001)).as_slice()
        );

        assert_eq!(
            [
                (to_k80(0x0004000300020000), 0x30),
                (to_k80(0x0004000300020000), 0x20),
            ],
            get_all_adjacent_values(&bt, &to_k80(0x0004000300020000)).as_slice()
        );

        bt.add_kv(&to_k80(0x0004000300020001), 0x100, 0);
        bt.add_kv(&to_k80(0x0004000300020001), 0x110, 0);
        bt.add_kv(&to_k80(0x0004000300020001), 0x120, 0);
        bt.add_kv(&to_k80(0x0004000300020001), 0x130, 0);
        bt.add_kv(&to_k80(0x0004000300020001), 0x140, 0);
        bt.add_kv(&to_k80(0x0004000300020001), 0x150, 0);
        bt.add_kv(&to_k80(0x0004000300020001), 0x160, 0);
        bt.add_kv(&to_k80(0x0004000300020001), 0x170, 0);
        assert_eq!(
            [0x170, 0x160, 0x150, 0x140, 0x130, 0x120, 0x110, 0x100, 0, 0x10],
            get_all_values(&bt, &to_k80(0x0004000300020001)).as_slice()
        );
        bt.change_kv(&to_k80(0x0004000300020001), 0x170, 0x710, 0, 0);
        assert_eq!(
            [0x710, 0x160, 0x150, 0x140, 0x130, 0x120, 0x110, 0x100, 0, 0x10],
            get_all_values(&bt, &to_k80(0x0004000300020001)).as_slice()
        );
        bt.add_kv(&to_k80(0x0004000300020002), 0x180, 0);
        assert_eq!(
            [
                (to_k80(0x0004000300020002), 0x180),
                (to_k80(0x0004000300020001), 0x10),
                (to_k80(0x0004000300020001), 0),
                (to_k80(0x0004000300020001), 0x100),
                (to_k80(0x0004000300020001), 0x110),
                (to_k80(0x0004000300020001), 0x120),
                (to_k80(0x0004000300020001), 0x130),
                (to_k80(0x0004000300020001), 0x140),
                (to_k80(0x0004000300020001), 0x150),
                (to_k80(0x0004000300020001), 0x160),
                (to_k80(0x0004000300020001), 0x710),
            ],
            get_all_adjacent_values(&bt, &to_k80(0x0004000300020002)).as_slice()
        );
        bt.erase_kv(&to_k80(0x0004000300020001), 0x150, 0);
        assert_eq!(
            [
                (to_k80(0x0004000300020001), 0x710),
                (to_k80(0x0004000300020001), 0x160),
                (to_k80(0x0004000300020001), 0x140),
                (to_k80(0x0004000300020001), 0x130),
                (to_k80(0x0004000300020001), 0x120),
                (to_k80(0x0004000300020001), 0x110),
                (to_k80(0x0004000300020001), 0x100),
                (to_k80(0x0004000300020001), 0),
                (to_k80(0x0004000300020001), 0x10),
                (to_k80(0x0004000300020000), 0x20),
                (to_k80(0x0004000300020000), 0x30),
            ],
            get_all_adjacent_values(&bt, &to_k80(0x0004000300020001)).as_slice()
        );
        bt.add_kv(&to_k80(0x000400030001FFFF), 0x150, 0);
        assert_eq!(
            [
                (to_k80(0x0004000300020001), 0x710),
                (to_k80(0x0004000300020001), 0x160),
                (to_k80(0x0004000300020001), 0x140),
                (to_k80(0x0004000300020001), 0x130),
                (to_k80(0x0004000300020001), 0x120),
                (to_k80(0x0004000300020001), 0x110),
                (to_k80(0x0004000300020001), 0x100),
                (to_k80(0x0004000300020001), 0),
                (to_k80(0x0004000300020001), 0x10),
                (to_k80(0x0004000300020000), 0x20),
                (to_k80(0x0004000300020000), 0x30),
            ],
            get_all_adjacent_values(&bt, &to_k80(0x0004000300020001)).as_slice()
        );
    }

    #[test]
    fn test_key_exists() {
        let indexer = Indexer::new(32);
        indexer.add_kv(&to_k80(111), 88, 0);
        indexer.add_kv(&to_k80(222), 888, 0);

        assert!(!indexer.key_exists(&to_k80(333), 123, 0));
        assert!(!indexer.key_exists(&to_k80(111), 123, 0));
        assert!(!indexer.key_exists(&to_k80(111), 888, 0));
        assert!(indexer.key_exists(&to_k80(111), 88, 0));
        assert!(indexer.key_exists(&to_k80(222), 888, 0));
    }
}
