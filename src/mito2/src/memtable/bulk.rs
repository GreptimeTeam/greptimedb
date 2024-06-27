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

//! Memtable implementation for bulk load

use std::sync::{Arc, RwLock};

use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;
use table::predicate::Predicate;

use crate::error::Result;
use crate::memtable::bulk::part::BulkPart;
use crate::memtable::key_values::KeyValue;
use crate::memtable::{
    BoxedBatchIterator, KeyValues, Memtable, MemtableId, MemtableRange, MemtableRef, MemtableStats,
};

#[allow(unused)]
pub(crate) mod part;

#[derive(Debug)]
pub struct BulkMemtable {
    id: MemtableId,
    parts: RwLock<Vec<BulkPart>>,
}

impl Memtable for BulkMemtable {
    fn id(&self) -> MemtableId {
        self.id
    }

    fn write(&self, _kvs: &KeyValues) -> Result<()> {
        unimplemented!()
    }

    fn write_one(&self, _key_value: KeyValue) -> Result<()> {
        unimplemented!()
    }

    fn write_bulk(&self, fragment: BulkPart) -> Result<()> {
        let mut parts = self.parts.write().unwrap();
        parts.push(fragment);
        Ok(())
    }

    fn iter(
        &self,
        _projection: Option<&[ColumnId]>,
        _predicate: Option<Predicate>,
    ) -> Result<BoxedBatchIterator> {
        todo!()
    }

    fn ranges(
        &self,
        _projection: Option<&[ColumnId]>,
        _predicate: Option<Predicate>,
    ) -> Vec<MemtableRange> {
        todo!()
    }

    fn is_empty(&self) -> bool {
        self.parts.read().unwrap().is_empty()
    }

    fn freeze(&self) -> Result<()> {
        Ok(())
    }

    fn stats(&self) -> MemtableStats {
        todo!()
    }

    fn fork(&self, id: MemtableId, _metadata: &RegionMetadataRef) -> MemtableRef {
        Arc::new(Self {
            id,
            parts: RwLock::new(vec![]),
        })
    }
}
