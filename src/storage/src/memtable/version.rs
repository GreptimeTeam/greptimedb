use std::cmp::Ordering;

use common_time::RangeMillis;

use crate::memtable::{MemtableId, MemtableRef};

/// A version of all memtables.
///
/// This structure is immutable now.
#[derive(Debug)]
pub struct MemtableVersion {
    mutable: MemtableRef,
    /// Immutable memtables.
    immutables: Vec<MemtableRef>,
}

impl MemtableVersion {
    pub fn new(mutable: MemtableRef) -> MemtableVersion {
        Self {
            mutable,
            immutables: vec![],
        }
    }

    #[inline]
    pub fn mutable_memtable(&self) -> &MemtableRef {
        &self.mutable
    }

    #[inline]
    pub fn immutable_memtables(&self) -> &[MemtableRef] {
        &self.immutables
    }

    pub fn num_memtables(&self) -> usize {
        // the last `1` is for `mutable`
        self.immutable_memtables().len() + 1
    }

    /// Clone current memtable version and freeze its mutable memtables, which moves
    /// all mutable memtables to immutable memtable list.
    pub fn freeze_mutable(&self, new_mutable: MemtableRef) -> MemtableVersion {
        let mut immutables = self.immutables.clone();
        immutables.push(self.mutable.clone());

        MemtableVersion {
            mutable: new_mutable,
            immutables,
        }
    }

    pub fn mutable_bytes_allocated(&self) -> usize {
        self.mutable.bytes_allocated()
    }

    pub fn total_bytes_allocated(&self) -> usize {
        self.immutables
            .iter()
            .map(|m| m.bytes_allocated())
            .sum::<usize>()
            + self.mutable.bytes_allocated()
    }

    /// Creates a new `MemtableVersion` that removes immutable memtables
    /// less than or equal to max_memtable_id.
    pub fn remove_immutables(&self, max_memtable_id: MemtableId) -> MemtableVersion {
        let immutables = self
            .immutables
            .iter()
            .filter(|immem| immem.id() > max_memtable_id)
            .cloned()
            .collect();

        MemtableVersion {
            mutable: self.mutable.clone(),
            immutables,
        }
    }

    pub fn memtables_to_flush(&self) -> (Option<MemtableId>, Vec<MemtableRef>) {
        let max_memtable_id = self.immutables.iter().map(|immem| immem.id()).max();
        let memtables = self.immutables.clone();
        // .iter()
        // .flat_map(|immem| immem.to_memtable_with_metas())
        // .collect();

        (max_memtable_id, memtables)
    }
}

// We use a new type to order time ranges by (end, start).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RangeKey(RangeMillis);

impl Ord for RangeKey {
    fn cmp(&self, other: &RangeKey) -> Ordering {
        self.0
            .end()
            .cmp(other.0.end())
            .then_with(|| self.0.start().cmp(other.0.start()))
    }
}

impl PartialOrd for RangeKey {
    fn partial_cmp(&self, other: &RangeKey) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use store_api::storage::OpType;

    use super::*;
    use crate::memtable::tests;
    use crate::memtable::BTreeMemtable;
    use crate::memtable::Memtable;

    #[test]
    fn test_memtableset_misc() {
        let mut set = MemtableSet::new();

        assert!(set.is_empty());
        assert_eq!(0, set.max_memtable_id());
        assert_eq!(0, set.bytes_allocated());
        assert!(set
            .get_by_range(&RangeMillis::new(0, 10).unwrap())
            .is_none());

        set.insert(
            RangeMillis::new(0, 10).unwrap(),
            Arc::new(BTreeMemtable::new(0, tests::schema_for_test())),
        );
        set.insert(
            RangeMillis::new(10, 20).unwrap(),
            Arc::new(BTreeMemtable::new(1, tests::schema_for_test())),
        );
        let memtable = Arc::new(BTreeMemtable::new(2, tests::schema_for_test()));
        // Write some test data
        tests::write_kvs(
            &*memtable,
            10, // sequence
            OpType::Put,
            &[
                (1000, 1),
                (1000, 2),
                (2002, 1),
                (2003, 1),
                (2003, 5),
                (1001, 1),
            ], // keys
            &[
                (Some(1), None),
                (Some(2), None),
                (Some(7), None),
                (Some(8), None),
                (Some(9), None),
                (Some(3), None),
            ], // values
        );

        set.insert(RangeMillis::new(20, 30).unwrap(), memtable.clone());

        for (i, (range, _)) in set.iter().enumerate() {
            assert_eq!(
                *range,
                RangeMillis::new(i as i64 * 10, i as i64 * 10 + 10).unwrap()
            );
        }

        assert!(!set.is_empty());
        assert_eq!(2, set.max_memtable_id());
        assert_eq!(memtable.bytes_allocated(), set.bytes_allocated());
        assert!(set
            .get_by_range(&RangeMillis::new(0, 10).unwrap())
            .is_some());
        assert!(set
            .get_by_range(&RangeMillis::new(10, 20).unwrap())
            .is_some());
        assert!(set
            .get_by_range(&RangeMillis::new(20, 30).unwrap())
            .is_some());
        assert!(set
            .get_by_range(&RangeMillis::new(0, 100).unwrap())
            .is_none());
    }

    fn create_test_memtableset(ids: &[MemtableId]) -> MemtableSet {
        let mut set = MemtableSet::new();

        for id in ids {
            let i = *id as i64;
            set.insert(
                RangeMillis::new(i * 10, (i + 1) * 10).unwrap(),
                Arc::new(BTreeMemtable::new(*id, tests::schema_for_test())),
            );
        }

        set
    }

    #[test]
    fn test_add_memtableset() {
        let s1 = create_test_memtableset(&[0, 1, 2]);
        let s2 = create_test_memtableset(&[3, 4, 5, 6]);

        let mut s1_memtables = s1.to_memtable_with_metas();
        let s2_memtables = s2.to_memtable_with_metas();
        s1_memtables.extend(s2_memtables);

        let empty = create_test_memtableset(&[]);
        assert_eq!(s1, s1.extend(empty));

        let s3 = s1.extend(s2);
        assert_ne!(s1, s3);

        assert_eq!(7, s3.memtables.len());
        let s3_memtables = s3.to_memtable_with_metas();
        assert_eq!(7, s3_memtables.len());

        for i in 0..7 {
            assert_eq!(s1_memtables[i].bucket, s3_memtables[i].bucket);
            assert_eq!(s1_memtables[i].memtable.id(), s3_memtables[i].memtable.id());
        }
        assert_eq!(6, s3.max_memtable_id());
    }

    #[test]
    fn test_memtableversion() {
        let s1 = create_test_memtableset(&[0, 1, 2]);
        let s2 = create_test_memtableset(&[3, 4, 5, 6]);
        let s3 = s1.extend(s2.clone());

        let v1 = MemtableVersion::new();
        assert!(v1.mutable_memtable().is_empty());
        assert_eq!(0, v1.num_memtables());

        // Add one mutable
        let v2 = v1.add_mutable(s1.clone());
        assert_ne!(v1, v2);
        let mutables = v2.mutable_memtable();
        assert_eq!(s1, *mutables);
        assert_eq!(3, v2.num_memtables());

        // Add another mutable
        let v3 = v2.add_mutable(s2);
        assert_ne!(v1, v3);
        assert_ne!(v2, v3);
        let mutables = v3.mutable_memtable();
        assert_eq!(s3, *mutables);
        assert!(v3.memtables_to_flush().1.is_empty());
        assert_eq!(7, v3.num_memtables());

        // Try to freeze s1, s2
        let v4 = v3.freeze_mutable();
        assert_ne!(v1, v4);
        assert_ne!(v2, v4);
        assert_ne!(v3, v4);
        assert!(v4.mutable_memtable().is_empty());
        assert_eq!(v4.immutables.len(), 1);
        assert_eq!(v4.immutables[0], Arc::new(s3.clone()));

        let (max_id, tables) = v4.memtables_to_flush();
        assert_eq!(6, max_id.unwrap());
        assert_eq!(7, tables.len());
        assert_eq!(7, v4.num_memtables());

        // Add another mutable
        let s4 = create_test_memtableset(&[7, 8]);
        let v5 = v4.add_mutable(s4.clone());
        let mutables = v5.mutable_memtable();
        assert_eq!(s4, *mutables);
        assert_eq!(v4.immutables, v5.immutables);

        // Try to freeze s4
        let v6 = v5.freeze_mutable();
        assert_eq!(v6.immutables.len(), 2);
        assert_eq!(v6.immutables[0], Arc::new(s3));
        assert_eq!(v6.immutables[1], Arc::new(s4.clone()));

        let (max_id, tables) = v6.memtables_to_flush();
        assert_eq!(8, max_id.unwrap());
        assert_eq!(9, tables.len());
        assert_eq!(9, v6.num_memtables());
        // verify tables
        for (i, table) in tables.iter().enumerate() {
            assert_eq!(i as u32, table.memtable.id());
            let i = i as i64;
            assert_eq!(
                table.bucket,
                RangeMillis::new(i * 10, (i + 1) * 10).unwrap()
            );
        }

        // Remove tables
        let v7 = v6.remove_immutables(6);
        assert_eq!(v7.immutables.len(), 1);
        assert_eq!(v7.immutables[0], Arc::new(s4));

        let v8 = v7.remove_immutables(8);
        assert_eq!(v8.immutables.len(), 0);
        assert_eq!(0, v8.num_memtables());
    }
}
