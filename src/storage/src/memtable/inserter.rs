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

use store_api::storage::{OpType, SequenceNumber};

use super::MemtableRef;
use crate::error::Result;
use crate::memtable::KeyValues;
use crate::metrics::MEMTABLE_WRITE_ELAPSED;
use crate::write_batch::{Mutation, Payload};

/// Wraps logic of inserting key/values in [WriteBatch] to [Memtable].
pub struct Inserter {
    /// Sequence of the batch to be inserted.
    sequence: SequenceNumber,
    /// Used to calculate the start index in batch for `KeyValues`.
    index_in_batch: usize,
}

impl Inserter {
    pub fn new(sequence: SequenceNumber) -> Inserter {
        Inserter {
            sequence,
            index_in_batch: 0,
        }
    }

    /// Insert write batch payload into memtable.
    ///
    /// Won't do schema validation if not configured. Caller (mostly the [`RegionWriter`]) should ensure the
    /// schemas of `memtable` are consistent with `payload`'s.
    pub fn insert_memtable(&mut self, payload: &Payload, memtable: &MemtableRef) -> Result<()> {
        let _timer = common_telemetry::timer!(MEMTABLE_WRITE_ELAPSED);

        if payload.is_empty() {
            return Ok(());
        }

        // This function only makes effect in debug mode.
        validate_input_and_memtable_schemas(payload, memtable);

        // Enough to hold all key or value columns.
        let total_column_num = payload.schema.num_columns();
        // Reusable KeyValues buffer.
        let mut kvs = KeyValues {
            sequence: self.sequence,
            op_type: OpType::Put,
            start_index_in_batch: self.index_in_batch,
            keys: Vec::with_capacity(total_column_num),
            values: Vec::with_capacity(total_column_num),
            timestamp: None,
        };

        for mutation in &payload.mutations {
            self.write_one_mutation(mutation, memtable, &mut kvs)?;
        }

        Ok(())
    }

    fn write_one_mutation(
        &mut self,
        mutation: &Mutation,
        memtable: &MemtableRef,
        kvs: &mut KeyValues,
    ) -> Result<()> {
        let schema = memtable.schema();
        let num_rows = mutation.record_batch.num_rows();

        kvs.reset(mutation.op_type, self.index_in_batch);

        let ts_idx = schema.timestamp_index();
        kvs.timestamp = Some(mutation.record_batch.column(ts_idx).clone());
        for key_idx in 0..ts_idx {
            kvs.keys.push(mutation.record_batch.column(key_idx).clone());
        }
        for value_idx in schema.value_indices() {
            kvs.values
                .push(mutation.record_batch.column(value_idx).clone());
        }

        memtable.write(kvs)?;

        self.index_in_batch += num_rows;

        Ok(())
    }
}

fn validate_input_and_memtable_schemas(payload: &Payload, memtable: &MemtableRef) {
    if cfg!(debug_assertions) {
        let payload_schema = &payload.schema;
        let memtable_schema = memtable.schema();
        let user_schema = memtable_schema.user_schema();
        debug_assert_eq!(payload_schema.version(), user_schema.version());
        // Only validate column schemas.
        debug_assert_eq!(
            payload_schema.column_schemas(),
            user_schema.column_schemas()
        );
    }
}

/// Holds `start` and `end` indexes to get a slice `[start, end)` from the vector whose
/// timestamps belong to same time range at `range_index`.
#[derive(Debug, PartialEq)]
struct SliceIndex {
    start: usize,
    end: usize,
    /// Index in time ranges.
    range_index: usize,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use common_time::timestamp::Timestamp;
    use datatypes::type_id::LogicalTypeId;
    use datatypes::value::Value;
    use datatypes::vectors::{Int64Vector, TimestampMillisecondVector, VectorRef};
    use store_api::storage::WriteRequest;

    use super::*;
    use crate::memtable::{DefaultMemtableBuilder, IterContext, MemtableBuilder};
    use crate::metadata::RegionMetadata;
    use crate::schema::RegionSchemaRef;
    use crate::test_util::descriptor_util::RegionDescBuilder;
    use crate::test_util::write_batch_util;
    use crate::write_batch::WriteBatch;

    fn new_test_write_batch() -> WriteBatch {
        write_batch_util::new_write_batch(
            &[
                ("ts", LogicalTypeId::TimestampMillisecond, false),
                ("value", LogicalTypeId::Int64, true),
            ],
            Some(0),
            1,
        )
    }

    fn new_region_schema() -> RegionSchemaRef {
        let desc = RegionDescBuilder::new("test")
            .timestamp(("ts", LogicalTypeId::TimestampMillisecond, false))
            .push_field_column(("value", LogicalTypeId::Int64, true))
            .build();
        let metadata: RegionMetadata = desc.try_into().unwrap();

        metadata.schema().clone()
    }

    fn put_batch(batch: &mut WriteBatch, data: &[(i64, Option<i64>)]) {
        let mut put_data = HashMap::with_capacity(2);
        let ts = TimestampMillisecondVector::from_values(data.iter().map(|v| v.0));
        put_data.insert("ts".to_string(), Arc::new(ts) as VectorRef);
        let value = Int64Vector::from(data.iter().map(|v| v.1).collect::<Vec<_>>());
        put_data.insert("value".to_string(), Arc::new(value) as VectorRef);

        batch.put(put_data).unwrap();
    }

    fn check_memtable_content(
        mem: &MemtableRef,
        sequence: SequenceNumber,
        data: &[(i64, Option<i64>)],
        max_ts: i64,
        min_ts: i64,
    ) {
        let iter = mem.iter(&IterContext::default()).unwrap();
        assert_eq!(min_ts, mem.stats().min_timestamp.value());
        assert_eq!(max_ts, mem.stats().max_timestamp.value());

        let mut index = 0;
        for batch in iter {
            let batch = batch.unwrap();
            let row_num = batch.column(0).len();
            for i in 0..row_num {
                let ts = batch.column(0).get(i);
                let v = batch.column(1).get(i);
                assert_eq!(
                    Value::Timestamp(Timestamp::new_millisecond(data[index].0)),
                    ts
                );
                assert_eq!(Value::from(data[index].1), v);
                assert_eq!(Value::from(sequence), batch.column(2).get(i));

                index += 1;
            }
        }

        assert_eq!(data.len(), index);
    }

    #[test]
    fn test_inserter_put_one_memtable() {
        let sequence = 11111;
        let memtable_schema = new_region_schema();
        let mutable_memtable = DefaultMemtableBuilder::default().build(memtable_schema);
        let mut inserter = Inserter::new(sequence);

        let mut batch = new_test_write_batch();
        put_batch(&mut batch, &[(1, Some(1)), (2, None)]);
        // Also test multiple put data in one batch.
        put_batch(
            &mut batch,
            &[
                (3, None),
                (2, None), // Duplicate entries in same put data.
                (2, Some(2)),
                (4, Some(4)),
                (201, Some(201)),
                (102, None),
                (101, Some(101)),
            ],
        );

        inserter
            .insert_memtable(batch.payload(), &mutable_memtable)
            .unwrap();
        check_memtable_content(
            &mutable_memtable,
            sequence,
            &[
                (1, Some(1)),
                (2, Some(2)),
                (3, None),
                (4, Some(4)),
                (101, Some(101)),
                (102, None),
                (201, Some(201)),
            ],
            201,
            1,
        );
    }
}
