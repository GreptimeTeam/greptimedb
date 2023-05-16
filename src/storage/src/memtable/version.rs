// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
        self.mutable.stats().bytes_allocated()
    }

    pub fn total_bytes_allocated(&self) -> usize {
        self.immutables
            .iter()
            .map(|m| m.stats().bytes_allocated())
            .sum::<usize>()
            + self.mutable.stats().bytes_allocated()
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
    use std::sync::Arc;

    use super::*;
    use crate::memtable::{DefaultMemtableBuilder, MemtableBuilder};
    use crate::test_util::schema_util;

    #[test]
    fn test_memtable_version() {
        let memtable_builder = DefaultMemtableBuilder::default();
        let region_schema = Arc::new(schema_util::new_region_schema(1, 1));

        let memtable_1 = memtable_builder.build(region_schema.clone());
        let v1 = MemtableVersion::new(memtable_1);
        assert_eq!(1, v1.num_memtables());

        // Freeze and add new mutable.
        let memtable_2 = memtable_builder.build(region_schema.clone());
        let v2 = v1.freeze_mutable(memtable_2);
        let v2_immutables = v2.immutable_memtables();
        assert_eq!(1, v2_immutables.len());
        assert_eq!(0, v2_immutables[0].id());
        assert_eq!(1, v2.mutable_memtable().id());
        assert_eq!(2, v2.num_memtables());

        // Add another one and check immutable memtables that need flush
        let memtable_3 = memtable_builder.build(region_schema);
        let v3 = v2.freeze_mutable(memtable_3);
        let (max_table_id, immutables) = v3.memtables_to_flush();
        assert_eq!(1, max_table_id.unwrap());
        assert_eq!(2, immutables.len());

        // Remove memtables
        let v4 = v3.remove_immutables(1);
        assert_eq!(1, v4.num_memtables());
        assert_eq!(0, v4.immutable_memtables().len());
        assert_eq!(2, v4.mutable_memtable().id());
    }
}
