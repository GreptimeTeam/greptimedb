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
        let time_range_indexes = new_range_index_map(&time_ranges);

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

fn new_range_index_map(time_ranges: &[RangeMillis]) -> RangeIndexMap {
    time_ranges
        .iter()
        .enumerate()
        .map(|(i, range)| (*range.start(), i))
        .collect()
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

/// Holds `start` and `end` indexes to get a slice `[start, end)` from the vector whose
/// timestamps belong to same time range at `range_index`.
#[derive(Debug, PartialEq)]
struct SliceIndex {
    start: usize,
    end: usize,
    /// Index in time ranges.
    range_index: usize,
}

/// Computes the indexes used to split timestamps into time ranges aligned by `duration`, stores
/// the indexes in [`SliceIndex`].
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
    // Time range index of the valid but unpushed `SliceIndex`.
    let mut last_range_index = None;

    // Iterate all timestamps, split timestamps by its time range.
    for (i, ts) in timestamps.iter_data().enumerate() {
        // Find index for time range of the timestamp.
        let current_range_index = ts
            .and_then(|v| TimestampMillis::new(v).aligned_by_bucket(duration_ms))
            .and_then(|aligned| time_range_indexes.get(&aligned).copied());

        match current_range_index {
            Some(current_range_index) => {
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
                // Row without timestamp or out of time range will be skipped. This usually should not happen.
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

#[cfg(test)]
mod tests {
    use datatypes::{type_id::LogicalTypeId, value::Value};
    use store_api::storage::{PutOperation, WriteRequest};

    use super::*;
    use crate::memtable::{
        DefaultMemtableBuilder, IterContext, MemtableBuilder, MemtableId, MemtableSchema,
    };
    use crate::metadata::RegionMetadata;
    use crate::test_util::descriptor_util::RegionDescBuilder;
    use crate::test_util::write_batch_util;

    fn new_time_ranges(starts: &[i64], duration: i64) -> Vec<RangeMillis> {
        let mut ranges = Vec::with_capacity(starts.len());
        for start in starts {
            assert_eq!(*start, start / duration * duration);

            ranges.push(RangeMillis::new(*start, start + duration).unwrap());
        }

        ranges
    }

    fn check_compute_slice_indexes(
        timestamps: &[Option<i64>],
        range_starts: &[i64],
        duration: i64,
        expect: &[SliceIndex],
    ) {
        assert!(duration > 0);

        let timestamps = Int64Vector::from_iter(timestamps.iter());
        let time_ranges = new_time_ranges(range_starts, duration);
        let time_range_indexes = new_range_index_map(&time_ranges);

        let slice_indexes = compute_slice_indexes(
            &timestamps,
            Duration::from_millis(duration as u64),
            &time_range_indexes,
        );

        assert_eq!(expect, slice_indexes);
    }

    #[test]
    fn test_compute_slice_indexes_valid() {
        // Test empty input.
        check_compute_slice_indexes(&[], &[], 100, &[]);

        // One valid input.
        check_compute_slice_indexes(
            &[Some(99)],
            &[0],
            100,
            &[SliceIndex {
                start: 0,
                end: 1,
                range_index: 0,
            }],
        );

        // 2 ranges.
        check_compute_slice_indexes(
            &[Some(99), Some(234)],
            &[0, 200],
            100,
            &[
                SliceIndex {
                    start: 0,
                    end: 1,
                    range_index: 0,
                },
                SliceIndex {
                    start: 1,
                    end: 2,
                    range_index: 1,
                },
            ],
        );

        // Multiple elements in first range.
        check_compute_slice_indexes(
            &[Some(99), Some(13), Some(18), Some(234)],
            &[0, 200],
            100,
            &[
                SliceIndex {
                    start: 0,
                    end: 3,
                    range_index: 0,
                },
                SliceIndex {
                    start: 3,
                    end: 4,
                    range_index: 1,
                },
            ],
        );

        // Multiple elements in last range.
        check_compute_slice_indexes(
            &[Some(99), Some(234), Some(271)],
            &[0, 200],
            100,
            &[
                SliceIndex {
                    start: 0,
                    end: 1,
                    range_index: 0,
                },
                SliceIndex {
                    start: 1,
                    end: 3,
                    range_index: 1,
                },
            ],
        );

        // Mulitple ranges.
        check_compute_slice_indexes(
            &[Some(99), Some(13), Some(234), Some(456)],
            &[0, 200, 400],
            100,
            &[
                SliceIndex {
                    start: 0,
                    end: 2,
                    range_index: 0,
                },
                SliceIndex {
                    start: 2,
                    end: 3,
                    range_index: 1,
                },
                SliceIndex {
                    start: 3,
                    end: 4,
                    range_index: 2,
                },
            ],
        );

        // Different slices with same range.
        check_compute_slice_indexes(
            &[Some(99), Some(234), Some(15)],
            &[0, 200],
            100,
            &[
                SliceIndex {
                    start: 0,
                    end: 1,
                    range_index: 0,
                },
                SliceIndex {
                    start: 1,
                    end: 2,
                    range_index: 1,
                },
                SliceIndex {
                    start: 2,
                    end: 3,
                    range_index: 0,
                },
            ],
        );
    }

    #[test]
    fn test_compute_slice_indexes_null_timestamp() {
        check_compute_slice_indexes(&[None], &[0], 100, &[]);

        check_compute_slice_indexes(
            &[None, None, Some(53)],
            &[0],
            100,
            &[SliceIndex {
                start: 2,
                end: 3,
                range_index: 0,
            }],
        );

        check_compute_slice_indexes(
            &[Some(53), None, None],
            &[0],
            100,
            &[SliceIndex {
                start: 0,
                end: 1,
                range_index: 0,
            }],
        );

        check_compute_slice_indexes(
            &[None, Some(53), None, Some(240), Some(13), None],
            &[0, 200],
            100,
            &[
                SliceIndex {
                    start: 1,
                    end: 2,
                    range_index: 0,
                },
                SliceIndex {
                    start: 3,
                    end: 4,
                    range_index: 1,
                },
                SliceIndex {
                    start: 4,
                    end: 5,
                    range_index: 0,
                },
            ],
        );
    }

    #[test]
    fn test_compute_slice_indexes_no_range() {
        check_compute_slice_indexes(
            &[Some(99), Some(234), Some(15)],
            &[0],
            100,
            &[
                SliceIndex {
                    start: 0,
                    end: 1,
                    range_index: 0,
                },
                SliceIndex {
                    start: 2,
                    end: 3,
                    range_index: 0,
                },
            ],
        );

        check_compute_slice_indexes(
            &[Some(99), Some(15), Some(234)],
            &[0],
            100,
            &[SliceIndex {
                start: 0,
                end: 2,
                range_index: 0,
            }],
        );

        check_compute_slice_indexes(
            &[Some(i64::MIN), Some(99), Some(15)],
            &[0],
            100,
            &[SliceIndex {
                start: 1,
                end: 3,
                range_index: 0,
            }],
        );
    }

    fn new_test_write_batch() -> WriteBatch {
        write_batch_util::new_write_batch(
            &[
                ("ts", LogicalTypeId::Int64, false),
                ("value", LogicalTypeId::Int64, true),
            ],
            Some(0),
        )
    }

    fn new_memtable_schema() -> MemtableSchema {
        let desc = RegionDescBuilder::new("test")
            .timestamp(("ts", LogicalTypeId::Int64, false))
            .push_value_column(("value", LogicalTypeId::Int64, true))
            .enable_version_column(false)
            .build();
        let metadata: RegionMetadata = desc.try_into().unwrap();

        MemtableSchema::new(metadata.columns_row_key)
    }

    fn put_batch(batch: &mut WriteBatch, data: &[(i64, Option<i64>)]) {
        let mut put_data = PutData::with_num_columns(2);
        let ts = Int64Vector::from_values(data.iter().map(|v| v.0));
        put_data.add_key_column("ts", Arc::new(ts)).unwrap();
        let value = Int64Vector::from_iter(data.iter().map(|v| v.1));
        put_data.add_value_column("value", Arc::new(value)).unwrap();

        batch.put(put_data).unwrap();
    }

    fn new_memtable_set(time_ranges: &[RangeMillis], schema: &MemtableSchema) -> MemtableSet {
        let mut set = MemtableSet::new();
        for (id, range) in time_ranges.iter().enumerate() {
            let mem = DefaultMemtableBuilder {}.build(id as MemtableId, schema.clone());
            set.insert(*range, mem)
        }

        set
    }

    fn check_memtable_content(
        mem: &dyn Memtable,
        sequence: SequenceNumber,
        data: &[(i64, Option<i64>)],
    ) {
        let mut iter = mem.iter(IterContext::default()).unwrap();

        let mut index = 0;
        while let Some(batch) = iter.next().unwrap() {
            let row_num = batch.keys[0].len();
            for i in 0..row_num {
                let ts = batch.keys[0].get(i);
                let v = batch.values[0].get(i);
                assert_eq!(Value::from(data[index].0), ts);
                assert_eq!(Value::from(data[index].1), v);
                assert_eq!(sequence, batch.sequences.get_data(i).unwrap());

                index += 1;
            }
        }

        assert_eq!(data.len(), index);
    }

    #[test]
    fn test_inserter_put_one_memtable() {
        let sequence = 11111;
        let bucket_duration = 100;
        let time_ranges = new_time_ranges(&[0], bucket_duration);
        let memtable_schema = new_memtable_schema();
        let memtables = new_memtable_set(&time_ranges, &memtable_schema);
        let mut inserter = Inserter::new(
            sequence,
            time_ranges,
            Duration::from_millis(bucket_duration as u64),
        );

        let mut batch = new_test_write_batch();
        put_batch(&mut batch, &[(1, Some(1)), (2, None)]);
        // Also test multiple put data in one batch.
        put_batch(
            &mut batch,
            &[
                (3, None),
                // Duplicate entries in same put data.
                (2, None),
                (2, Some(2)),
                (4, Some(4)),
            ],
        );

        inserter.insert_memtables(&batch, &memtables).unwrap();
        let mem = memtables
            .get_by_range(&RangeMillis::new(0, 100).unwrap())
            .unwrap();
        check_memtable_content(
            &**mem,
            sequence,
            &[(1, Some(1)), (2, Some(2)), (3, None), (4, Some(4))],
        );
    }

    #[test]
    fn test_inserter_put_multiple() {
        let sequence = 11111;
        let bucket_duration = 100;
        let time_ranges = new_time_ranges(&[0, 100, 200], bucket_duration);
        let memtable_schema = new_memtable_schema();
        let memtables = new_memtable_set(&time_ranges, &memtable_schema);
        let mut inserter = Inserter::new(
            sequence,
            time_ranges,
            Duration::from_millis(bucket_duration as u64),
        );

        let mut batch = new_test_write_batch();
        put_batch(
            &mut batch,
            &[
                (1, Some(1)),
                (2, None),
                (201, Some(201)),
                (102, None),
                (101, Some(101)),
            ],
        );
        put_batch(
            &mut batch,
            &[
                (180, Some(1)),
                (3, Some(3)),
                (1, None),
                (211, Some(211)),
                (180, Some(180)),
            ],
        );

        inserter.insert_memtables(&batch, &memtables).unwrap();
        let mem = memtables
            .get_by_range(&RangeMillis::new(0, 100).unwrap())
            .unwrap();
        check_memtable_content(&**mem, sequence, &[(1, None), (2, None), (3, Some(3))]);

        let mem = memtables
            .get_by_range(&RangeMillis::new(100, 200).unwrap())
            .unwrap();
        check_memtable_content(
            &**mem,
            sequence,
            &[(101, Some(101)), (102, None), (180, Some(180))],
        );

        let mem = memtables
            .get_by_range(&RangeMillis::new(200, 300).unwrap())
            .unwrap();
        check_memtable_content(&**mem, sequence, &[(201, Some(201)), (211, Some(211))]);
    }
}
