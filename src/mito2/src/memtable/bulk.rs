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
use crate::memtable::bulk::context::BulkIterContext;
use crate::memtable::bulk::part::BulkPart;
use crate::memtable::key_values::KeyValue;
use crate::memtable::partition_tree::{PartitionTreeConfig, PartitionTreeMemtableBuilder};
use crate::memtable::{
    AllocTracker, BoxedBatchIterator, KeyValues, Memtable, MemtableBuilder, MemtableId,
    MemtableRanges, MemtableRef, MemtableStats,
};
use crate::read::dedup::{LastNonNull, LastRow};
use crate::read::sync::dedup::DedupReader;
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
        let part_metadata = fragment.metadata();
        if self.max_timestamp.load(Ordering::Relaxed) < part_metadata.max_timestamp {
            self.max_timestamp
                .store(part_metadata.max_timestamp, Ordering::Relaxed);
        }
        if self.min_timestamp.load(Ordering::Relaxed) > part_metadata.min_timestamp {
            self.min_timestamp
                .store(part_metadata.min_timestamp, Ordering::Relaxed);
        }
        if self.max_sequence.load(Ordering::Relaxed) < part_metadata.max_sequence {
            self.max_sequence
                .store(part_metadata.max_sequence, Ordering::Relaxed);
        }
        self.num_rows
            .fetch_add(part_metadata.num_rows, Ordering::Relaxed);
        parts.push(fragment);
        Ok(())
    }

    fn iter(
        &self,
        projection: Option<&[ColumnId]>,
        predicate: Option<Predicate>,
        sequence: Option<SequenceNumber>,
    ) -> Result<BoxedBatchIterator> {
        let mut readers = Vec::new();
        let parts = self.parts.read().unwrap();

        let ctx = Arc::new(BulkIterContext::new(
            self.region_metadata.clone(),
            &projection,
            predicate.clone(),
        ));
        for part in parts.as_slice() {
            if let Some(reader) = part.read(ctx.clone(), sequence).unwrap() {
                readers.push(reader);
            }
        }
        let merge_reader = crate::read::sync::merge::MergeReader::new(readers)?;
        let reader = match self.merge_mode {
            MergeMode::LastRow => {
                Box::new(DedupReader::new(merge_reader, LastRow::new(self.dedup))) as BoxedBatchIterator
            }
            MergeMode::LastNonNull => {
                Box::new(DedupReader::new(merge_reader, LastNonNull::new(self.dedup))) as BoxedBatchIterator
            }
        };
        Ok(reader )
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

#[cfg(test)]
mod tests {
    use api::helper::ColumnDataTypeWrapper;
    use api::v1::value::ValueData;
    use api::v1::{OpType, Row, Rows, SemanticType};
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use std::sync::Arc;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder, RegionMetadataRef};
    use store_api::storage::RegionId;

    use crate::memtable::bulk::part::BulkPartEncoder;
    use crate::memtable::bulk::BulkMemtable;
    use crate::memtable::{BulkPart, Memtable};
    use crate::region::options::MergeMode;

    fn metrics_region_metadata() -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(123, 456));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("k0", ConcreteDataType::binary_datatype(), false),
                semantic_type: SemanticType::Tag,
                column_id: 0,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("v0", ConcreteDataType::float64_datatype(), true),
                semantic_type: SemanticType::Field,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("v1", ConcreteDataType::float64_datatype(), true),
                semantic_type: SemanticType::Field,
                column_id: 3,
            })
            .primary_key(vec![0]);
        let region_metadata = builder.build().unwrap();
        Arc::new(region_metadata)
    }

    fn metrics_column_schema() -> Vec<api::v1::ColumnSchema> {
        let schema = metrics_region_metadata();
        schema
            .column_metadatas
            .iter()
            .map(|c| api::v1::ColumnSchema {
                column_name: c.column_schema.name.clone(),
                datatype: ColumnDataTypeWrapper::try_from(c.column_schema.data_type.clone())
                    .unwrap()
                    .datatype() as i32,
                semantic_type: c.semantic_type as i32,
                ..Default::default()
            })
            .collect()
    }

    fn build_metrics_bulk_part(
        k: &str,
        ts: &[i64],
        v0: &[Option<f64>],
        v1: &[Option<f64>],
        seq: u64,
    ) -> BulkPart {
        assert_eq!(ts.len(), v0.len());
        assert_eq!(ts.len(), v1.len());

        let rows = ts
            .iter()
            .zip(v0.iter())
            .zip(v1.iter())
            .map(|((ts, v0), v1)| Row {
                values: vec![
                    api::v1::Value {
                        value_data: Some(ValueData::BinaryValue(k.as_bytes().to_vec())),
                    },
                    api::v1::Value {
                        value_data: Some(ValueData::TimestampMillisecondValue(*ts as i64)),
                    },
                    api::v1::Value {
                        value_data: v0.map(ValueData::F64Value),
                    },
                    api::v1::Value {
                        value_data: v1.map(ValueData::F64Value),
                    },
                ],
            })
            .collect::<Vec<_>>();

        let mutation = api::v1::Mutation {
            op_type: OpType::Put as i32,
            sequence: seq,
            rows: Some(Rows {
                schema: metrics_column_schema(),
                rows,
            }),
            write_hint: None,
            bulk: Vec::new(),
        };
        let encoder = BulkPartEncoder::new(metrics_region_metadata(), true, 1024);
        encoder.encode_mutations(&[mutation]).unwrap().unwrap()
    }

    #[test]
    fn test_bulk_iter() {
        let schema = metrics_region_metadata();
        let memtable = BulkMemtable::new(schema, 0, None, true, MergeMode::LastRow);
        memtable.write_bulk(build_metrics_bulk_part("a", &[1], &[None], &[Some(1.0)], 0)).unwrap();
        // write duplicated rows
        memtable.write_bulk(build_metrics_bulk_part("a", &[1], &[None], &[Some(1.0)], 0)).unwrap();
        let iter = memtable.iter(None, None, None).unwrap();
        let total_rows = iter.map(|b| {
            b.unwrap().num_rows()
        }).sum::<usize>();
        assert_eq!(1, total_rows);
    }
}
