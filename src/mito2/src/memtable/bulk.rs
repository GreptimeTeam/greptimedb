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

use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, SequenceNumber};
use table::predicate::Predicate;

use crate::error::Result;
use crate::flush::WriteBufferManagerRef;
use crate::memtable::bulk::part::BulkPart;
use crate::memtable::key_values::KeyValue;
use crate::memtable::partition_tree::{PartitionTreeConfig, PartitionTreeMemtableBuilder};
use crate::memtable::{
    AllocTracker, BoxedBatchIterator, KeyValues, Memtable, MemtableBuilder, MemtableId,
    MemtableRanges, MemtableRef, MemtableStats,
};
use crate::read::Batch;
use crate::region::options::MergeMode;

#[allow(unused)]
mod context;
#[allow(unused)]
pub(crate) mod part;
mod part_reader;
mod row_group_reader;

#[derive(Debug)]
pub struct BulkMemtableBuilder {
    write_buffer_manager: Option<WriteBufferManagerRef>,
    dedup: bool,
    merge_mode: MergeMode,
    fallback_builder: PartitionTreeMemtableBuilder,
}

impl MemtableBuilder for BulkMemtableBuilder {
    fn build(&self, id: MemtableId, metadata: &RegionMetadataRef) -> MemtableRef {
        //todo(hl): create different memtables according to region type (metadata/physical)
        if metadata.primary_key_encoding == PrimaryKeyEncoding::Dense {
            self.fallback_builder.build(id, metadata)
        } else {
            Arc::new(BulkMemtable::new(
                metadata.clone(),
                id,
                self.write_buffer_manager.clone(),
                self.dedup,
                self.merge_mode,
            )) as MemtableRef
        }
    }
}

impl BulkMemtableBuilder {
    pub fn new(
        write_buffer_manager: Option<WriteBufferManagerRef>,
        dedup: bool,
        merge_mode: MergeMode,
    ) -> Self {
        let builder = PartitionTreeMemtableBuilder::new(
            PartitionTreeConfig::default(),
            write_buffer_manager.clone(),
        );

        Self {
            write_buffer_manager,
            dedup,
            merge_mode,
            fallback_builder: builder,
        }
    }
}

#[derive(Debug)]
pub struct BulkMemtable {
    id: MemtableId,
    parts: RwLock<Vec<BulkPart>>,
    region_metadata: RegionMetadataRef,
    alloc_tracker: AllocTracker,
    max_timestamp: AtomicI64,
    min_timestamp: AtomicI64,
    max_sequence: AtomicU64,
    num_rows: AtomicUsize,
    dedup: bool,
    merge_mode: MergeMode,
}

impl BulkMemtable {
    pub fn new(
        region_metadata: RegionMetadataRef,
        id: MemtableId,
        write_buffer_manager: Option<WriteBufferManagerRef>,
        dedup: bool,
        merge_mode: MergeMode,
    ) -> Self {
        Self {
            id,
            parts: RwLock::new(vec![]),
            region_metadata,
            alloc_tracker: AllocTracker::new(write_buffer_manager),
            max_timestamp: AtomicI64::new(i64::MIN),
            min_timestamp: AtomicI64::new(i64::MAX),
            max_sequence: Default::default(),
            num_rows: Default::default(),
            dedup,
            merge_mode,
        }
    }
}

struct EmptyIter;

impl Iterator for EmptyIter {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
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
        self.alloc_tracker.on_allocation(fragment.data.len());
        let mut parts = self.parts.write().unwrap();
        parts.push(fragment);
        Ok(())
    }

    fn iter(
        &self,
        _projection: Option<&[ColumnId]>,
        _predicate: Option<Predicate>,
        _sequence: Option<SequenceNumber>,
    ) -> Result<BoxedBatchIterator> {
        //todo(hl): temporarily disable reads.
        //todo(hl): we should also consider dedup and merge mode when reading bulk parts,
        Ok(Box::new(EmptyIter))
    }

    fn ranges(
        &self,
        _projection: Option<&[ColumnId]>,
        _predicate: Option<Predicate>,
        _sequence: Option<SequenceNumber>,
    ) -> MemtableRanges {
        todo!()
    }

    fn is_empty(&self) -> bool {
        self.parts.read().unwrap().is_empty()
    }

    fn freeze(&self) -> Result<()> {
        self.alloc_tracker.done_allocating();
        Ok(())
    }

    fn stats(&self) -> MemtableStats {
        let estimated_bytes = self.alloc_tracker.bytes_allocated();

        if estimated_bytes == 0 {
            // no rows ever written
            return MemtableStats {
                estimated_bytes,
                time_range: None,
                num_rows: 0,
                num_ranges: 0,
                max_sequence: 0,
            };
        }

        let ts_type = self
            .region_metadata
            .time_index_column()
            .column_schema
            .data_type
            .clone()
            .as_timestamp()
            .expect("Timestamp column must have timestamp type");
        let max_timestamp = ts_type.create_timestamp(self.max_timestamp.load(Ordering::Relaxed));
        let min_timestamp = ts_type.create_timestamp(self.min_timestamp.load(Ordering::Relaxed));
        MemtableStats {
            estimated_bytes,
            time_range: Some((min_timestamp, max_timestamp)),
            num_rows: self.num_rows.load(Ordering::Relaxed),
            num_ranges: 1, //todo(hl): we should consider bulk parts as different ranges.
            max_sequence: self.max_sequence.load(Ordering::Relaxed),
        }
    }

    fn fork(&self, id: MemtableId, metadata: &RegionMetadataRef) -> MemtableRef {
        Arc::new(Self::new(
            metadata.clone(),
            id,
            self.alloc_tracker.write_buffer_manager(),
            self.dedup,
            self.merge_mode,
        ))
    }
}
