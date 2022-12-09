// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use datatypes::vectors::VectorRef;
use snafu::OptionExt;
use store_api::storage::{ColumnDescriptor, OpType, SequenceNumber};

use super::MemtableRef;
use crate::error::{self, Result};
use crate::memtable::KeyValues;
use crate::write_batch::{Mutation, PutData, WriteBatch};

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

    // TODO(yingwen): Can we take the WriteBatch?
    /// Insert write batch into memtable.
    ///
    /// Won't do schema validation if not configured. Caller (mostly the [`RegionWriter`]) should ensure the
    /// schemas of `memtable` are consistent with `batch`'s.
    pub fn insert_memtable(&mut self, batch: &WriteBatch, memtable: &MemtableRef) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        // This function only makes effect in debug mode.
        validate_input_and_memtable_schemas(batch, memtable);

        // Enough to hold all key or value columns.
        let total_column_num = batch.schema().num_columns();
        // Reusable KeyValues buffer.
        let mut kvs = KeyValues {
            sequence: self.sequence,
            op_type: OpType::Put,
            start_index_in_batch: self.index_in_batch,
            keys: Vec::with_capacity(total_column_num),
            values: Vec::with_capacity(total_column_num),
        };

        for mutation in batch {
            match mutation {
                Mutation::Put(put_data) => {
                    self.write_one_mutation(put_data, memtable, &mut kvs)?;
                }
            }
        }

        Ok(())
    }

    fn write_one_mutation(
        &mut self,
        put_data: &PutData,
        memtable: &MemtableRef,
        kvs: &mut KeyValues,
    ) -> Result<()> {
        let schema = memtable.schema();
        let num_rows = put_data.num_rows();

        kvs.reset(OpType::Put, self.index_in_batch);

        for key_col in schema.row_key_columns() {
            clone_put_data_column_to(put_data, &key_col.desc, &mut kvs.keys)?;
        }

        for value_col in schema.value_columns() {
            clone_put_data_column_to(put_data, &value_col.desc, &mut kvs.values)?;
        }

        memtable.write(kvs)?;

        self.index_in_batch += num_rows;

        Ok(())
    }
}

fn validate_input_and_memtable_schemas(batch: &WriteBatch, memtable: &MemtableRef) {
    if cfg!(debug_assertions) {
        let batch_schema = batch.schema();
        let memtable_schema = memtable.schema();
        let user_schema = memtable_schema.user_schema();
        debug_assert_eq!(batch_schema.version(), user_schema.version());
        // Only validate column schemas.
        debug_assert_eq!(batch_schema.column_schemas(), user_schema.column_schemas());
    }
}

fn clone_put_data_column_to(
    put_data: &PutData,
    desc: &ColumnDescriptor,
    target: &mut Vec<VectorRef>,
) -> Result<()> {
    let vector = put_data
        .column_by_name(&desc.name)
        .context(error::BatchMissingColumnSnafu { column: &desc.name })?;
    target.push(vector.clone());

    Ok(())
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
    use std::sync::Arc;

    use common_time::timestamp::Timestamp;
    use datatypes::prelude::ScalarVector;
    use datatypes::type_id::LogicalTypeId;
    use datatypes::value::Value;
    use datatypes::vectors::{Int64Vector, TimestampMillisecondVector};
    use store_api::storage::{PutOperation, WriteRequest};

    use super::*;
    use crate::memtable::{DefaultMemtableBuilder, IterContext, MemtableBuilder};
    use crate::metadata::RegionMetadata;
    use crate::schema::RegionSchemaRef;
    use crate::test_util::descriptor_util::RegionDescBuilder;
    use crate::test_util::write_batch_util;

    fn new_test_write_batch() -> WriteBatch {
        write_batch_util::new_write_batch(
            &[
                ("ts", LogicalTypeId::TimestampMillisecond, false),
                ("value", LogicalTypeId::Int64, true),
            ],
            Some(0),
        )
    }

    fn new_region_schema() -> RegionSchemaRef {
        let desc = RegionDescBuilder::new("test")
            .timestamp(("ts", LogicalTypeId::TimestampMillisecond, false))
            .push_value_column(("value", LogicalTypeId::Int64, true))
            .enable_version_column(false)
            .build();
        let metadata: RegionMetadata = desc.try_into().unwrap();

        metadata.schema().clone()
    }

    fn put_batch(batch: &mut WriteBatch, data: &[(i64, Option<i64>)]) {
        let mut put_data = PutData::with_num_columns(2);
        let ts = TimestampMillisecondVector::from_values(data.iter().map(|v| v.0));
        put_data.add_key_column("ts", Arc::new(ts)).unwrap();
        let value = Int64Vector::from(data.iter().map(|v| v.1).collect::<Vec<_>>());
        put_data.add_value_column("value", Arc::new(value)).unwrap();

        batch.put(put_data).unwrap();
    }

    fn check_memtable_content(
        mem: &MemtableRef,
        sequence: SequenceNumber,
        data: &[(i64, Option<i64>)],
    ) {
        let iter = mem.iter(&IterContext::default()).unwrap();

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

        inserter.insert_memtable(&batch, &mutable_memtable).unwrap();
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
        );
    }
}
