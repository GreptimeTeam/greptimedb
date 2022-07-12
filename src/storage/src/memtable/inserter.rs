use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common_time::{RangeMillis, TimestampMillis};
use datatypes::prelude::ScalarVector;
use datatypes::schema::SchemaRef;
use datatypes::vectors::{Int64Vector, NullVector, VectorRef};
use snafu::{ensure, OptionExt};
use store_api::storage::{ColumnDescriptor, SequenceNumber, ValueType};

use crate::error::{self, Result};
use crate::memtable::{KeyValues, Memtable, MemtableSet};
use crate::write_batch::{Mutation, PutData, WriteBatch};

type RangeIndexMap = HashMap<TimestampMillis, usize>;

/// Wraps logic of inserting key/values in [WriteBatch] to [Memtable].
pub struct Inserter {
    /// Sequence of the batch to be inserted.
    sequence: SequenceNumber,
    /// Time ranges of all input data.
    time_ranges: Vec<RangeMillis>,
    /// Map time range's start time to its index in time ranges.
    time_range_indexes: RangeIndexMap,
    /// Bucket duration of memtables.
    bucket_duration: Duration,
    /// Used to calculate the start index in batch for `KeyValues`.
    index_in_batch: usize,
}

impl Inserter {
    pub fn new(
        sequence: SequenceNumber,
        time_ranges: Vec<RangeMillis>,
        bucket_duration: Duration,
    ) -> Inserter {
        let time_range_indexes = time_ranges
            .iter()
            .enumerate()
            .map(|(i, range)| (*range.start(), i))
            .collect();

        Inserter {
            sequence,
            time_ranges,
            time_range_indexes,
            bucket_duration,
            index_in_batch: 0,
        }
    }

    // TODO(yingwen): Can we take the WriteBatch?
    /// Insert write batch into memtables if both `batch` and `memtables` are not empty.
    ///
    /// Won't do schema validation, caller (mostly the [`RegionWriter`]) should ensure the
    /// schemas of `memtables` are consistent with `batch`'s, and the time ranges of `memtables`
    /// are consistent with `self`'s time ranges.
    ///
    /// # Panics
    /// Panics if there is time range in `self.time_ranges` but not in `memtables`.
    pub fn insert_memtables(&mut self, batch: &WriteBatch, memtables: &MemtableSet) -> Result<()> {
        if batch.is_empty() || memtables.is_empty() {
            return Ok(());
        }

        // Enough to hold all key or value columns.
        let total_column_num = batch.schema().num_columns();
        // Reusable KeyValues buffer.
        let mut kvs = KeyValues {
            sequence: self.sequence,
            value_type: ValueType::Put,
            start_index_in_batch: self.index_in_batch,
            keys: Vec::with_capacity(total_column_num),
            values: Vec::with_capacity(total_column_num),
        };

        for mutation in batch {
            match mutation {
                Mutation::Put(put_data) => {
                    self.put_memtables(batch.schema(), put_data, memtables, &mut kvs)?;
                }
            }
        }

        Ok(())
    }

    fn put_memtables(
        &mut self,
        schema: &SchemaRef,
        put_data: &PutData,
        memtables: &MemtableSet,
        kvs: &mut KeyValues,
    ) -> Result<()> {
        if memtables.len() == 1 {
            // Fast path, only one memtable to put.
            let (_range, memtable) = memtables.iter().next().unwrap();
            return self.put_one_memtable(put_data, &**memtable, kvs);
        }

        // Split data by time range and put them into memtables.
        self.put_multiple_memtables(schema, put_data, memtables, kvs)
    }

    fn put_one_memtable(
        &mut self,
        put_data: &PutData,
        memtable: &dyn Memtable,
        kvs: &mut KeyValues,
    ) -> Result<()> {
        let schema = memtable.schema();
        let num_rows = put_data.num_rows();

        kvs.reset(ValueType::Put, self.index_in_batch);

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

    /// Put data to multiple memtables.
    fn put_multiple_memtables(
        &mut self,
        schema: &SchemaRef,
        put_data: &PutData,
        memtables: &MemtableSet,
        kvs: &mut KeyValues,
    ) -> Result<()> {
        let timestamp_schema = schema
            .timestamp_column()
            .context(error::BatchMissingTimestampSnafu)?;

        let timestamps = put_data.column_by_name(&timestamp_schema.name).context(
            error::BatchMissingColumnSnafu {
                column: &timestamp_schema.name,
            },
        )?;
        let timestamps = timestamps
            .as_any()
            .downcast_ref()
            .context(error::BatchMissingTimestampSnafu)?;
        let slice_indexes =
            compute_slice_indexes(timestamps, self.bucket_duration, &self.time_range_indexes);

        for slice_index in slice_indexes {
            let sliced_data = put_data.slice(slice_index.start, slice_index.end);
            let range = &self.time_ranges[slice_index.range_index];
            // The caller should ensure memtable for given time range is exists.
            let memtable = memtables
                .get_by_range(range)
                .expect("Memtable not found for range");

            self.put_one_memtable(&sliced_data, &**memtable, kvs)?;
        }

        Ok(())
    }
}

fn clone_put_data_column_to(
    put_data: &PutData,
    desc: &ColumnDescriptor,
    target: &mut Vec<VectorRef>,
) -> Result<()> {
    if let Some(vector) = put_data.column_by_name(&desc.name) {
        target.push(vector.clone());
    } else {
        // The write batch should have been validated before.
        ensure!(
            desc.is_nullable,
            error::BatchMissingColumnSnafu { column: &desc.name }
        );

        let num_rows = put_data.num_rows();
        target.push(Arc::new(NullVector::new(num_rows)));
    }

    Ok(())
}

struct SliceIndex {
    start: usize,
    end: usize,
    /// Index in time ranges.
    range_index: usize,
}

/// Compute `SliceIndex` for `timestamps`.
///
/// # Panics
/// Panics if the duration is too large to be represented by i64, or `timestamps` are not all
/// included by `time_range_indexes`.
fn compute_slice_indexes(
    timestamps: &Int64Vector,
    duration: Duration,
    time_range_indexes: &RangeIndexMap,
) -> Vec<SliceIndex> {
    let duration_ms = duration.as_millis().try_into().expect("Duration too large");
    let mut slice_indexes = Vec::with_capacity(time_range_indexes.len());
    // Current start and end of a valid `SliceIndex`.
    let (mut start, mut end) = (0, 0);
    // Current range index of a valid `SliceIndex`.
    let mut last_range_index = None;

    for (i, data) in timestamps.iter_data().enumerate() {
        match data {
            Some(v) => {
                let aligned = TimestampMillis::new(v).aligned_by_bucket(duration_ms);
                let current_range_index = *time_range_indexes
                    .get(&aligned)
                    .expect("Range for timestamp not found");
                end = i;

                match last_range_index {
                    Some(last_index) => {
                        if last_index != current_range_index {
                            // Found a new range, we need to push a SliceIndex for last range.
                            slice_indexes.push(SliceIndex {
                                start,
                                end,
                                range_index: last_index,
                            });
                            // Update last range index.
                            last_range_index = Some(current_range_index);
                            // Advance start.
                            start = i;
                        }
                    }
                    // No previous range index.
                    None => last_range_index = Some(current_range_index),
                }
            }
            None => {
                // Row with null timestamp will be skipped. This usually should no happen.
                if let Some(last_index) = last_range_index {
                    // Need to store SliceIndex for last range.
                    slice_indexes.push(SliceIndex {
                        start,
                        end: i,
                        range_index: last_index,
                    });
                    // Clear last range index.
                    last_range_index = None;
                }

                // Advances start and end, skips current row.
                start = i + 1;
                end = start;
            }
        }
    }

    // Process last slice index.
    if let Some(last_index) = last_range_index {
        slice_indexes.push(SliceIndex {
            start,
            // We need to use `end + 1` to include the last element.
            end: end + 1,
            range_index: last_index,
        });
    }

    slice_indexes
}

// TODO(yingwen): Add tests for MemtableInserter.
