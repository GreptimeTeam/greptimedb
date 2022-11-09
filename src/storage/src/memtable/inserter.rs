use std::collections::HashMap;
use std::time::Duration;

use common_time::timestamp_millis::BucketAligned;
use common_time::{RangeMillis, TimestampMillis};
use datatypes::vectors::VectorRef;
use snafu::OptionExt;
use store_api::storage::{ColumnDescriptor, OpType, SequenceNumber};

use super::MemtableRef;
use crate::error::{self, Result};
use crate::memtable::KeyValues;
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
    /// Insert write batch into memtable.
    ///
    /// Won't do schema validation if not configured. Caller (mostly the [`RegionWriter`]) should ensure the
    /// schemas of `memtable` are consistent with `batch`'s, and the time ranges of `memtable`
    /// are consistent with `self`'s time ranges.
    pub fn insert_memtable(&mut self, batch: &WriteBatch, memtable: &MemtableRef) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        // This function only makes effect in debug mod.
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
                    // self.put_memtables(batch.schema(), put_data, memtable, &mut kvs)?;
                    self.put_one_memtable(put_data, memtable, &mut kvs)?;
                }
            }
        }

        Ok(())
    }

    fn put_one_memtable(
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

/// Computes the indexes used to split timestamps into time ranges aligned by `duration`, stores
/// the indexes in [`SliceIndex`].
///
/// # Panics
/// Panics if the duration is too large to be represented by i64, or `timestamps` are not all
/// included by `time_range_indexes`.
fn compute_slice_indices<I: Iterator<Item = Option<i64>>>(
    timestamps: I,
    duration: Duration,
    time_range_indexes: &RangeIndexMap,
) -> Vec<SliceIndex> {
    let duration_ms = duration
        .as_millis()
        .try_into()
        .unwrap_or_else(|e| panic!("Duration {:?} too large, {}", duration, e));

    let mut slice_indexes = Vec::with_capacity(time_range_indexes.len());
    // Current start and end of a valid `SliceIndex`.
    let (mut start, mut end) = (0, 0);
    // Time range index of the valid but unpushed `SliceIndex`.
    let mut last_range_index = None;

    // Iterate all timestamps, split timestamps by its time range.
    for (i, ts) in timestamps.enumerate() {
        // Find index for time range of the timestamp.

        let current_range_index = ts
            .and_then(|v| v.align_by_bucket(duration_ms))
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
    use std::sync::Arc;

    use common_time::timestamp::Timestamp;
    use datatypes::prelude::ScalarVectorBuilder;
    use datatypes::vectors::{
        Int64Vector, Int64VectorBuilder, TimestampVector, TimestampVectorBuilder,
    };
    use datatypes::{type_id::LogicalTypeId, value::Value};
    use store_api::storage::{PutOperation, WriteRequest};

    use super::*;
    use crate::memtable::{DefaultMemtableBuilder, IterContext, MemtableBuilder, MemtableId};
    use crate::metadata::RegionMetadata;
    use crate::schema::RegionSchemaRef;
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

        let mut builder = TimestampVectorBuilder::with_capacity(0);
        for v in timestamps {
            builder.push(v.map(common_time::timestamp::Timestamp::from_millis));
        }

        let ts_vec = builder.finish();

        let time_ranges = new_time_ranges(range_starts, duration);
        let time_range_indexes = new_range_index_map(&time_ranges);

        let slice_indexes = compute_slice_indices(
            ts_vec.iter_data().map(|v| v.map(|v| v.value())),
            Duration::from_millis(duration as u64),
            &time_range_indexes,
        );

        assert_eq!(expect, slice_indexes);
    }

    #[test]
    fn test_check_compute_slice_indexes_i64() {
        let timestamps = &[Some(99), Some(13), Some(18), Some(234)];
        let range_starts = &[0, 200];
        let duration = 100;

        let mut builder = Int64VectorBuilder::with_capacity(timestamps.len());
        for v in timestamps {
            builder.push(*v);
        }

        let ts_vec = builder.finish();

        let time_ranges = new_time_ranges(range_starts, duration);
        let time_range_indexes = new_range_index_map(&time_ranges);

        let slice_indexes = compute_slice_indices(
            ts_vec.iter_data(),
            Duration::from_millis(duration as u64),
            &time_range_indexes,
        );
        assert_eq!(
            vec![
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
            slice_indexes
        );
    }

    #[test]
    fn test_check_compute_slice_indexes_timestamp() {
        let timestamps = &[Some(99), Some(13), Some(18), Some(234)];
        let range_starts = &[0, 200];
        let duration = 100;

        let mut builder = TimestampVectorBuilder::with_capacity(timestamps.len());
        for v in timestamps {
            builder.push(v.map(Timestamp::from_millis));
        }

        let ts_vec = builder.finish();

        let time_ranges = new_time_ranges(range_starts, duration);
        let time_range_indexes = new_range_index_map(&time_ranges);

        let slice_indexes = compute_slice_indices(
            ts_vec.iter_data().map(|v| v.map(|v| v.value())),
            Duration::from_millis(duration as u64),
            &time_range_indexes,
        );
        assert_eq!(
            vec![
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
            slice_indexes
        );
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
            &[Some(-1), Some(99), Some(15)],
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
                ("ts", LogicalTypeId::Timestamp, false),
                ("value", LogicalTypeId::Int64, true),
            ],
            Some(0),
        )
    }

    fn new_region_schema() -> RegionSchemaRef {
        let desc = RegionDescBuilder::new("test")
            .timestamp(("ts", LogicalTypeId::Timestamp, false))
            .push_value_column(("value", LogicalTypeId::Int64, true))
            .enable_version_column(false)
            .build();
        let metadata: RegionMetadata = desc.try_into().unwrap();

        metadata.schema().clone()
    }

    fn put_batch(batch: &mut WriteBatch, data: &[(i64, Option<i64>)]) {
        let mut put_data = PutData::with_num_columns(2);
        let ts = TimestampVector::from_values(data.iter().map(|v| v.0));
        put_data.add_key_column("ts", Arc::new(ts)).unwrap();
        let value = Int64Vector::from_iter(data.iter().map(|v| v.1));
        put_data.add_value_column("value", Arc::new(value)).unwrap();

        batch.put(put_data).unwrap();
    }

    fn new_memtable_set(time_ranges: &[RangeMillis], schema: &RegionSchemaRef) -> MemtableSet {
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
        let iter = mem.iter(&IterContext::default()).unwrap();

        let mut index = 0;
        for batch in iter {
            let batch = batch.unwrap();
            let row_num = batch.column(0).len();
            for i in 0..row_num {
                let ts = batch.column(0).get(i);
                let v = batch.column(1).get(i);
                assert_eq!(Value::Timestamp(Timestamp::from_millis(data[index].0)), ts);
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
        let bucket_duration = 100;
        let time_ranges = new_time_ranges(&[0], bucket_duration);
        let memtable_schema = new_region_schema();
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
        let memtable_schema = new_region_schema();
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
