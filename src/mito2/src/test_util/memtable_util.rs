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

//! Memtable test utilities.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;
use table::predicate::Predicate;

use crate::error::Result;
use crate::memtable::{
    BoxedBatchIterator, KeyValues, Memtable, MemtableBuilder, MemtableId, MemtableRef,
    MemtableStats,
};

/// Empty memtable for test.
#[derive(Debug, Default)]
pub(crate) struct EmptyMemtable {
    /// Id of this memtable.
    id: MemtableId,
}

impl EmptyMemtable {
    /// Returns a new memtable with specific `id`.
    pub(crate) fn new(id: MemtableId) -> EmptyMemtable {
        EmptyMemtable { id }
    }
}

impl Memtable for EmptyMemtable {
    fn id(&self) -> MemtableId {
        self.id
    }

    fn write(&self, _kvs: &KeyValues) -> Result<()> {
        Ok(())
    }

    fn iter(
        &self,
        _projection: Option<&[ColumnId]>,
        _filters: Option<Predicate>,
    ) -> BoxedBatchIterator {
        Box::new(std::iter::empty())
    }

    fn is_empty(&self) -> bool {
        true
    }

    fn mark_immutable(&self) {}

    fn stats(&self) -> MemtableStats {
        MemtableStats::default()
    }
}

/// Empty memtable builder.
#[derive(Debug, Default)]
pub(crate) struct EmptyMemtableBuilder {
    /// Next memtable id.
    next_id: AtomicU32,
}

impl MemtableBuilder for EmptyMemtableBuilder {
    fn build(&self, _metadata: &RegionMetadataRef) -> MemtableRef {
        Arc::new(EmptyMemtable::new(
            self.next_id.fetch_add(1, Ordering::Relaxed),
        ))
    }
}
