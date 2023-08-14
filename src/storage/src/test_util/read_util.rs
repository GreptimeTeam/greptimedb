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

use std::sync::Arc;

use api::v1::OpType;
use async_trait::async_trait;
use datatypes::prelude::ScalarVector;
use datatypes::type_id::LogicalTypeId;
use datatypes::vectors::{Int64Vector, TimestampMillisecondVector, UInt64Vector, UInt8Vector};

use crate::error::Result;
use crate::memtable::{BatchIterator, BoxedBatchIterator, RowOrdering};
use crate::metadata::RegionMetadata;
use crate::read::{Batch, BatchReader, BoxedBatchReader};
use crate::schema::{ProjectedSchema, ProjectedSchemaRef, RegionSchemaRef};
use crate::test_util::descriptor_util::RegionDescBuilder;

/// Create a new region schema (timestamp, v0).
fn new_region_schema() -> RegionSchemaRef {
    let desc = RegionDescBuilder::new("read-util")
        .push_field_column(("v0", LogicalTypeId::Int64, true))
        .build();
    let metadata: RegionMetadata = desc.try_into().unwrap();
    metadata.schema().clone()
}

/// Create a new projected schema (timestamp, v0).
pub fn new_projected_schema() -> ProjectedSchemaRef {
    let region_schema = new_region_schema();
    Arc::new(ProjectedSchema::new(region_schema, None).unwrap())
}

/// Build a new batch, with 0 sequence and op_type.
pub fn new_kv_batch(key_values: &[(i64, Option<i64>)]) -> Batch {
    let key = Arc::new(TimestampMillisecondVector::from_values(
        key_values.iter().map(|v| v.0),
    ));
    let value = Arc::new(Int64Vector::from(
        key_values.iter().map(|v| v.1).collect::<Vec<_>>(),
    ));
    let sequences = Arc::new(UInt64Vector::from_vec(vec![0; key_values.len()]));
    let op_types = Arc::new(UInt8Vector::from_vec(vec![0; key_values.len()]));

    Batch::new(vec![key, value, sequences, op_types])
}

/// Build a new batch from (key, value, sequence, op_type)
pub fn new_full_kv_batch(all_values: &[(i64, i64, u64, OpType)]) -> Batch {
    let key = Arc::new(TimestampMillisecondVector::from_values(
        all_values.iter().map(|v| v.0),
    ));
    let value = Arc::new(Int64Vector::from_values(all_values.iter().map(|v| v.1)));
    let sequences = Arc::new(UInt64Vector::from_values(all_values.iter().map(|v| v.2)));
    let op_types = Arc::new(UInt8Vector::from_values(
        all_values.iter().map(|v| v.3 as u8),
    ));

    Batch::new(vec![key, value, sequences, op_types])
}

pub async fn collect_kv_batch(reader: &mut dyn BatchReader) -> Vec<(i64, Option<i64>)> {
    let mut result = Vec::new();
    while let Some(batch) = reader.next_batch().await.unwrap() {
        let key = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondVector>()
            .unwrap();
        let value = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Vector>()
            .unwrap();

        for (k, v) in key.iter_data().zip(value.iter_data()) {
            result.push((k.unwrap().into(), v));
        }
    }

    result
}

pub type Batches<'a> = &'a [&'a [(i64, Option<i64>)]];

/// A reader for test that pop batch from Vec.
pub struct VecBatchReader {
    schema: ProjectedSchemaRef,
    batches: Vec<Batch>,
}

impl VecBatchReader {
    fn new(mut batches: Vec<Batch>) -> VecBatchReader {
        batches.reverse();

        VecBatchReader {
            schema: new_projected_schema(),
            batches,
        }
    }
}

#[async_trait]
impl BatchReader for VecBatchReader {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        Ok(self.batches.pop())
    }
}

impl Iterator for VecBatchReader {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Result<Batch>> {
        self.batches.pop().map(Ok)
    }
}

impl BatchIterator for VecBatchReader {
    fn schema(&self) -> ProjectedSchemaRef {
        self.schema.clone()
    }

    fn ordering(&self) -> RowOrdering {
        // TODO(yingwen): Allow setting the row ordering.
        RowOrdering::Key
    }
}

pub fn build_vec_reader(batches: &[&[(i64, Option<i64>)]]) -> VecBatchReader {
    let batches: Vec<_> = batches
        .iter()
        .map(|key_values| new_kv_batch(key_values))
        .collect();

    VecBatchReader::new(batches)
}

pub fn build_full_vec_reader(batches: &[&[(i64, i64, u64, OpType)]]) -> VecBatchReader {
    let batches: Vec<_> = batches
        .iter()
        .map(|key_values| new_full_kv_batch(key_values))
        .collect();

    VecBatchReader::new(batches)
}

pub fn build_boxed_reader(batches: &[&[(i64, Option<i64>)]]) -> BoxedBatchReader {
    Box::new(build_vec_reader(batches))
}

pub fn build_boxed_iter(batches: &[&[(i64, Option<i64>)]]) -> BoxedBatchIterator {
    Box::new(build_vec_reader(batches))
}
