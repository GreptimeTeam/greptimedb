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

use common_time::Timestamp;
use datatypes::prelude::*;
use datatypes::timestamp::TimestampMillisecond;
use datatypes::type_id::LogicalTypeId;
use datatypes::vectors::{
    TimestampMillisecondVector, TimestampMillisecondVectorBuilder, UInt64Vector,
    UInt64VectorBuilder, UInt8Vector,
};

use super::*;
use crate::metadata::RegionMetadata;
use crate::schema::{ProjectedSchema, RegionSchemaRef};
use crate::test_util::descriptor_util::RegionDescBuilder;

// Schema for testing memtable:
// - key: Int64(timestamp), UInt64(version),
// - value: UInt64, UInt64
pub fn schema_for_test() -> RegionSchemaRef {
    // Just build a region desc and use its columns metadata.
    let desc = RegionDescBuilder::new("test")
        .push_field_column(("v0", LogicalTypeId::UInt64, true))
        .push_field_column(("v1", LogicalTypeId::UInt64, true))
        .build();
    let metadata: RegionMetadata = desc.try_into().unwrap();

    metadata.schema().clone()
}

fn kvs_for_test_with_index(
    sequence: SequenceNumber,
    op_type: OpType,
    start_index_in_batch: usize,
    keys: &[TimestampMillisecond],
    values: &[(Option<u64>, Option<u64>)],
) -> KeyValues {
    assert_eq!(keys.len(), values.len());

    let mut key_builders = TimestampMillisecondVectorBuilder::with_capacity(keys.len());
    for key in keys {
        key_builders.push(Some(*key));
    }
    let ts_col = Arc::new(key_builders.finish()) as _;

    let mut value_builders = (
        UInt64VectorBuilder::with_capacity(values.len()),
        UInt64VectorBuilder::with_capacity(values.len()),
    );
    for value in values {
        value_builders.0.push(value.0);
        value_builders.1.push(value.1);
    }
    let row_values = vec![
        Arc::new(value_builders.0.finish()) as _,
        Arc::new(value_builders.1.finish()) as _,
    ];

    let kvs = KeyValues {
        sequence,
        op_type,
        start_index_in_batch,
        keys: vec![],
        values: row_values,
        timestamp: Some(ts_col),
    };

    assert_eq!(keys.len(), kvs.len());
    assert_eq!(keys.is_empty(), kvs.is_empty());

    kvs
}

fn kvs_for_test(
    sequence: SequenceNumber,
    op_type: OpType,
    keys: &[TimestampMillisecond],
    values: &[(Option<u64>, Option<u64>)],
) -> KeyValues {
    kvs_for_test_with_index(sequence, op_type, 0, keys, values)
}

pub fn write_kvs(
    memtable: &dyn Memtable,
    sequence: SequenceNumber,
    op_type: OpType,
    keys: &[i64],
    values: &[(Option<u64>, Option<u64>)],
) {
    let keys: Vec<TimestampMillisecond> = keys.iter().map(|l| ((*l).into())).collect();

    let kvs = kvs_for_test(sequence, op_type, &keys, values);

    memtable.write(&kvs).unwrap();
}

fn check_batch_valid(batch: &Batch) {
    assert_eq!(5, batch.num_columns());
    let row_num = batch.column(0).len();
    for i in 1..5 {
        assert_eq!(row_num, batch.column(i).len());
    }
}

fn check_iter_content(
    iter: &mut dyn BatchIterator,
    keys: &[i64],
    sequences: &[u64],
    op_types: &[OpType],
    values: &[(Option<u64>, Option<u64>)],
) {
    let keys: Vec<TimestampMillisecond> = keys.iter().map(|l| (*l).into()).collect();

    let mut index = 0;
    for batch in iter {
        let batch = batch.unwrap();
        check_batch_valid(&batch);

        let row_num = batch.column(0).len();
        for i in 0..row_num {
            let k0 = batch.column(0).get(i);
            let (v0, v1) = (batch.column(1).get(i), batch.column(2).get(i));
            let sequence = batch.column(3).get(i);
            let op_type = batch.column(4).get(i);

            assert_eq!(Value::from(keys[index]), k0);
            assert_eq!(Value::from(values[index].0), v0);
            assert_eq!(Value::from(values[index].1), v1);
            assert_eq!(Value::from(sequences[index]), sequence);
            assert_eq!(Value::from(op_types[index] as u8), op_type);

            index += 1;
        }
    }

    assert_eq!(keys.len(), index);
}

struct MemtableTester {
    schema: RegionSchemaRef,
    builders: Vec<MemtableBuilderRef>,
}

impl Default for MemtableTester {
    fn default() -> MemtableTester {
        MemtableTester::new()
    }
}

impl MemtableTester {
    fn new() -> MemtableTester {
        let schema = schema_for_test();
        let builders = vec![Arc::new(DefaultMemtableBuilder::default()) as _];

        MemtableTester { schema, builders }
    }

    fn new_memtables(&self) -> Vec<MemtableRef> {
        self.builders
            .iter()
            .map(|b| b.build(self.schema.clone()))
            .collect()
    }

    fn run_testcase<F>(&self, testcase: F)
    where
        F: Fn(TestContext),
    {
        for memtable in self.new_memtables() {
            let test_ctx = TestContext {
                schema: self.schema.clone(),
                memtable,
            };

            testcase(test_ctx);
        }
    }
}

struct TestContext {
    schema: RegionSchemaRef,
    memtable: MemtableRef,
}

fn write_iter_memtable_case(ctx: &TestContext) {
    // Test iterating an empty memtable.
    let mut iter = ctx.memtable.iter(IterContext::default()).unwrap();
    assert!(iter.next().is_none());
    // Poll the empty iterator again.
    assert!(iter.next().is_none());
    assert_eq!(0, ctx.memtable.stats().bytes_allocated());

    // Init test data.
    write_kvs(
        &*ctx.memtable,
        10, // sequence
        OpType::Put,
        &[1000, 1000, 2002, 2003, 2003, 1001], // keys
        &[
            (Some(1), None),
            (Some(2), None),
            (Some(7), None),
            (Some(8), None),
            (Some(9), None),
            (Some(3), None),
        ], // values
    );
    write_kvs(
        &*ctx.memtable,
        11, // sequence
        OpType::Put,
        &[1002, 1003, 1004],                            // keys
        &[(None, None), (Some(5), None), (None, None)], // values
    );

    // 9 key value pairs (6 + 3).
    assert_eq!(576, ctx.memtable.stats().bytes_allocated());

    let batch_sizes = [1, 4, 8, consts::READ_BATCH_SIZE];
    for batch_size in batch_sizes {
        let iter_ctx = IterContext {
            batch_size,
            ..Default::default()
        };
        let mut iter = ctx.memtable.iter(iter_ctx.clone()).unwrap();
        assert_eq!(
            ctx.schema.user_schema(),
            iter.schema().projected_user_schema()
        );
        assert_eq!(RowOrdering::Key, iter.ordering());

        check_iter_content(
            &mut *iter,
            &[1000, 1001, 1002, 1003, 1004, 2002, 2003], // keys
            &[10, 10, 11, 11, 11, 10, 10],               // sequences
            &[
                OpType::Put,
                OpType::Put,
                OpType::Put,
                OpType::Put,
                OpType::Put,
                OpType::Put,
                OpType::Put,
            ], // op_types
            &[
                (Some(2), None),
                (Some(3), None),
                (None, None),
                (Some(5), None),
                (None, None),
                (Some(7), None),
                (Some(9), None),
            ], // values
        );
    }
}

#[test]
fn test_iter_context_default() {
    let ctx = IterContext::default();
    assert_eq!(SequenceNumber::MAX, ctx.visible_sequence);
}

#[test]
fn test_write_iter_memtable() {
    let tester = MemtableTester::default();
    tester.run_testcase(|ctx| {
        write_iter_memtable_case(&ctx);
    });
}

fn check_iter_batch_size(iter: &mut dyn BatchIterator, total: usize, batch_size: usize) {
    let mut remains = total;
    for batch in iter {
        let batch = batch.unwrap();
        check_batch_valid(&batch);

        let row_num = batch.column(0).len();
        if remains >= batch_size {
            assert_eq!(batch_size, row_num);
            remains -= batch_size;
        } else {
            assert_eq!(remains, row_num);
            remains = 0;
        }
    }

    assert_eq!(0, remains);
}

#[test]
fn test_iter_batch_size() {
    let tester = MemtableTester::default();
    tester.run_testcase(|ctx| {
        write_kvs(
            &*ctx.memtable,
            10, // sequence
            OpType::Put,
            &[1000, 1000, 1001, 2002, 2003, 2003], // keys
            &[
                (Some(1), None),
                (Some(2), None),
                (Some(3), None),
                (Some(4), None),
                (None, None),
                (None, None),
            ], // values
        );

        let total = 4;
        // Batch size [less than, equal to, greater than] total
        let batch_sizes = [1, 6, 8];
        for batch_size in batch_sizes {
            let iter_ctx = IterContext {
                batch_size,
                ..Default::default()
            };

            let mut iter = ctx.memtable.iter(iter_ctx.clone()).unwrap();
            check_iter_batch_size(&mut *iter, total, batch_size);
        }
    });
}

#[test]
fn test_duplicate_key_across_batch() {
    let tester = MemtableTester::default();
    tester.run_testcase(|ctx| {
        write_kvs(
            &*ctx.memtable,
            10, // sequence
            OpType::Put,
            &[1000, 1001, 2000, 2001], // keys
            &[(Some(1), None), (None, None), (None, None), (None, None)], // values
        );

        write_kvs(
            &*ctx.memtable,
            11, // sequence
            OpType::Put,
            &[1000, 2001],                             // keys
            &[(Some(1231), None), (Some(1232), None)], // values
        );

        let batch_sizes = [1, 2, 3, 4, 5];
        for batch_size in batch_sizes {
            let iter_ctx = IterContext {
                batch_size,
                ..Default::default()
            };

            let mut iter = ctx.memtable.iter(iter_ctx.clone()).unwrap();
            check_iter_content(
                &mut *iter,
                &[1000, 1001, 2000, 2001], // keys
                &[11, 10, 10, 11],         // sequences
                &[OpType::Put, OpType::Put, OpType::Put, OpType::Put], // op_types
                &[
                    (Some(1231), None),
                    (None, None),
                    (None, None),
                    (Some(1232), None),
                ], // values
            );
        }
    });
}

#[test]
fn test_duplicate_key_in_batch() {
    let tester = MemtableTester::default();
    tester.run_testcase(|ctx| {
        write_kvs(
            &*ctx.memtable,
            10, // sequence
            OpType::Put,
            &[1000, 1000, 1001, 2001], // keys
            &[(None, None), (None, None), (Some(1234), None), (None, None)], // values
        );

        let batch_sizes = [1, 2, 3, 4, 5];
        for batch_size in batch_sizes {
            let iter_ctx = IterContext {
                batch_size,
                ..Default::default()
            };

            let mut iter = ctx.memtable.iter(iter_ctx.clone()).unwrap();
            check_iter_content(
                &mut *iter,
                &[1000, 1001, 2001],                               // keys
                &[10, 10, 10],                                     // sequences
                &[OpType::Put, OpType::Put, OpType::Put],          // op_types
                &[(None, None), (Some(1234), None), (None, None)], // values
            );
        }
    });
}

#[test]
fn test_sequence_visibility() {
    let tester = MemtableTester::default();
    tester.run_testcase(|ctx| {
        write_kvs(
            &*ctx.memtable,
            10, // sequence
            OpType::Put,
            &[1000, 1000],                       // keys
            &[(Some(1), None), (Some(2), None)], // values
        );

        write_kvs(
            &*ctx.memtable,
            11, // sequence
            OpType::Put,
            &[1000, 1000],                         // keys
            &[(Some(11), None), (Some(12), None)], // values
        );

        write_kvs(
            &*ctx.memtable,
            12, // sequence
            OpType::Put,
            &[1000, 1000],                         // keys
            &[(Some(21), None), (Some(22), None)], // values
        );

        {
            let iter_ctx = IterContext {
                batch_size: 1,
                visible_sequence: 9,
                projected_schema: None,
                time_range: None,
            };

            let mut iter = ctx.memtable.iter(iter_ctx).unwrap();
            check_iter_content(
                &mut *iter,
                &[], // keys
                &[], // sequences
                &[], // op_types
                &[], // values
            );
        }

        {
            let iter_ctx = IterContext {
                batch_size: 1,
                visible_sequence: 10,
                projected_schema: None,
                time_range: None,
            };

            let mut iter = ctx.memtable.iter(iter_ctx).unwrap();
            check_iter_content(
                &mut *iter,
                &[1000],                     // keys
                &[10],                       // sequences
                &[OpType::Put, OpType::Put], // op_types
                &[(Some(2), None)],          // values
            );
        }

        {
            let iter_ctx = IterContext {
                batch_size: 1,
                visible_sequence: 11,
                projected_schema: None,
                time_range: None,
            };

            let mut iter = ctx.memtable.iter(iter_ctx).unwrap();
            check_iter_content(
                &mut *iter,
                &[1000],                     // keys
                &[11],                       // sequences
                &[OpType::Put, OpType::Put], // op_types
                &[(Some(12), None)],         // values
            );
        }
    });
}

#[test]
fn test_iter_after_none() {
    let tester = MemtableTester::default();
    tester.run_testcase(|ctx| {
        write_kvs(
            &*ctx.memtable,
            10, // sequence
            OpType::Put,
            &[1000, 1001, 1002],                                  // keys
            &[(Some(0), None), (Some(1), None), (Some(2), None)], // values
        );

        let iter_ctx = IterContext {
            batch_size: 4,
            ..Default::default()
        };

        let mut iter = ctx.memtable.iter(iter_ctx).unwrap();
        let _ = iter.next().unwrap();
        assert!(iter.next().is_none());
        assert!(iter.next().is_none());
    });
}

#[test]
fn test_filter_memtable() {
    let tester = MemtableTester::default();
    tester.run_testcase(|ctx| {
        write_kvs(
            &*ctx.memtable,
            10, // sequence
            OpType::Put,
            &[1000, 1001, 1002],                                  // keys
            &[(Some(0), None), (Some(1), None), (Some(2), None)], // values
        );

        let iter_ctx = IterContext {
            batch_size: 4,
            time_range: Some(
                TimestampRange::new(
                    Timestamp::new_millisecond(0),
                    Timestamp::new_millisecond(1001),
                )
                .unwrap(),
            ),
            ..Default::default()
        };

        let mut iter = ctx.memtable.iter(iter_ctx).unwrap();
        let batch = iter.next().unwrap().unwrap();
        assert_eq!(5, batch.columns.len());
        assert_eq!(
            Arc::new(TimestampMillisecondVector::from_slice([1000])) as Arc<_>,
            batch.columns[0]
        );
    });
}

#[test]
fn test_memtable_projection() {
    let tester = MemtableTester::default();
    // Only need v0, but row key columns and internal columns would also be read.
    let projected_schema =
        Arc::new(ProjectedSchema::new(tester.schema.clone(), Some(vec![2])).unwrap());

    tester.run_testcase(|ctx| {
        write_kvs(
            &*ctx.memtable,
            9, // sequence
            OpType::Put,
            &[1000, 1001, 1002], // keys
            &[
                (Some(10), Some(20)),
                (Some(11), Some(21)),
                (Some(12), Some(22)),
            ], // values
        );

        let iter_ctx = IterContext {
            batch_size: 4,
            projected_schema: Some(projected_schema.clone()),
            ..Default::default()
        };

        let mut iter = ctx.memtable.iter(iter_ctx).unwrap();
        let batch = iter.next().unwrap().unwrap();
        assert!(iter.next().is_none());

        assert_eq!(4, batch.num_columns());
        let k0 = Arc::new(TimestampMillisecondVector::from_slice([1000, 1001, 1002])) as VectorRef;
        let v0 = Arc::new(UInt64Vector::from_slice([20, 21, 22])) as VectorRef;
        let sequences = Arc::new(UInt64Vector::from_slice([9, 9, 9])) as VectorRef;
        let op_types = Arc::new(UInt8Vector::from_slice([1, 1, 1])) as VectorRef;

        assert_eq!(k0, *batch.column(0));
        assert_eq!(v0, *batch.column(1));
        assert_eq!(sequences, *batch.column(2));
        assert_eq!(op_types, *batch.column(3));
    });
}
