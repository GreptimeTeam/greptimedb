use common_time::timestamp::Timestamp;
use datatypes::arrow;
use datatypes::arrow::array::{Int64Array, PrimitiveArray, UInt64Array, UInt8Array};
use datatypes::prelude::*;
use datatypes::type_id::LogicalTypeId;
use datatypes::vectors::{TimestampVectorBuilder, UInt64VectorBuilder};

use super::*;
use crate::metadata::RegionMetadata;
use crate::schema::{ProjectedSchema, RegionSchemaRef};
use crate::test_util::descriptor_util::RegionDescBuilder;

// For simplicity, all memtables in test share same memtable id.
const MEMTABLE_ID: MemtableId = 1;

// Schema for testing memtable:
// - key: Int64(timestamp), UInt64(version),
// - value: UInt64, UInt64
pub fn schema_for_test() -> RegionSchemaRef {
    // Just build a region desc and use its columns metadata.
    let desc = RegionDescBuilder::new("test")
        .enable_version_column(true)
        .push_value_column(("v0", LogicalTypeId::UInt64, true))
        .push_value_column(("v1", LogicalTypeId::UInt64, true))
        .build();
    let metadata: RegionMetadata = desc.try_into().unwrap();

    metadata.schema().clone()
}

fn kvs_for_test_with_index(
    sequence: SequenceNumber,
    op_type: OpType,
    start_index_in_batch: usize,
    keys: &[(Timestamp, u64)],
    values: &[(Option<u64>, Option<u64>)],
) -> KeyValues {
    assert_eq!(keys.len(), values.len());

    let mut key_builders = (
        TimestampVectorBuilder::with_capacity(keys.len()),
        UInt64VectorBuilder::with_capacity(keys.len()),
    );
    for key in keys {
        key_builders.0.push(Some(key.0));
        key_builders.1.push(Some(key.1));
    }
    let row_keys = vec![
        Arc::new(key_builders.0.finish()) as _,
        Arc::new(key_builders.1.finish()) as _,
    ];

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
        keys: row_keys,
        values: row_values,
    };

    assert_eq!(keys.len(), kvs.len());
    assert_eq!(keys.is_empty(), kvs.is_empty());

    kvs
}

fn kvs_for_test(
    sequence: SequenceNumber,
    op_type: OpType,
    keys: &[(Timestamp, u64)],
    values: &[(Option<u64>, Option<u64>)],
) -> KeyValues {
    kvs_for_test_with_index(sequence, op_type, 0, keys, values)
}

pub fn write_kvs(
    memtable: &dyn Memtable,
    sequence: SequenceNumber,
    op_type: OpType,
    keys: &[(i64, u64)],
    values: &[(Option<u64>, Option<u64>)],
) {
    let keys: Vec<(Timestamp, u64)> = keys.iter().map(|(l, r)| ((*l).into(), *r)).collect();

    let kvs = kvs_for_test(sequence, op_type, &keys, values);

    memtable.write(&kvs).unwrap();
}

fn check_batch_valid(batch: &Batch) {
    assert_eq!(6, batch.num_columns());
    let row_num = batch.column(0).len();
    for i in 1..6 {
        assert_eq!(row_num, batch.column(i).len());
    }
}

fn check_iter_content(
    iter: &mut dyn BatchIterator,
    keys: &[(i64, u64)],
    sequences: &[u64],
    op_types: &[OpType],
    values: &[(Option<u64>, Option<u64>)],
) {
    let keys: Vec<(Timestamp, u64)> = keys.iter().map(|(l, r)| ((*l).into(), *r)).collect();

    let mut index = 0;
    for batch in iter {
        let batch = batch.unwrap();
        check_batch_valid(&batch);

        let row_num = batch.column(0).len();
        for i in 0..row_num {
            let (k0, k1) = (batch.column(0).get(i), batch.column(1).get(i));
            let (v0, v1) = (batch.column(2).get(i), batch.column(3).get(i));
            let sequence = batch.column(4).get(i);
            let op_type = batch.column(5).get(i);

            assert_eq!(Value::from(keys[index].0), k0);
            assert_eq!(Value::from(keys[index].1), k1);
            assert_eq!(Value::from(values[index].0), v0);
            assert_eq!(Value::from(values[index].1), v1);
            assert_eq!(Value::from(sequences[index]), sequence);
            assert_eq!(Value::from(op_types[index].as_u8()), op_type);

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
        let builders = vec![Arc::new(DefaultMemtableBuilder {}) as _];

        MemtableTester { schema, builders }
    }

    fn new_memtables(&self) -> Vec<MemtableRef> {
        self.builders
            .iter()
            .map(|b| b.build(MEMTABLE_ID, self.schema.clone()))
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
    let mut iter = ctx.memtable.iter(&IterContext::default()).unwrap();
    assert!(iter.next().is_none());
    // Poll the empty iterator again.
    assert!(iter.next().is_none());
    assert_eq!(0, ctx.memtable.bytes_allocated());

    // Init test data.
    write_kvs(
        &*ctx.memtable,
        10, // sequence
        OpType::Put,
        &[
            (1000, 1),
            (1000, 2),
            (2002, 1),
            (2003, 1),
            (2003, 5),
            (1001, 1),
        ], // keys
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
        &[(1002, 1), (1003, 1), (1004, 1)],             // keys
        &[(None, None), (Some(5), None), (None, None)], // values
    );

    // 9 key value pairs (6 + 3).
    assert_eq!(288, ctx.memtable.bytes_allocated());

    let batch_sizes = [1, 4, 8, consts::READ_BATCH_SIZE];
    for batch_size in batch_sizes {
        let iter_ctx = IterContext {
            batch_size,
            ..Default::default()
        };
        let mut iter = ctx.memtable.iter(&iter_ctx).unwrap();
        assert_eq!(
            ctx.schema.user_schema(),
            iter.schema().projected_user_schema()
        );
        assert_eq!(RowOrdering::Key, iter.ordering());

        check_iter_content(
            &mut *iter,
            &[
                (1000, 1),
                (1000, 2),
                (1001, 1),
                (1002, 1),
                (1003, 1),
                (1004, 1),
                (2002, 1),
                (2003, 1),
                (2003, 5),
            ], // keys
            &[10, 10, 10, 11, 11, 11, 10, 10, 10], // sequences
            &[
                OpType::Put,
                OpType::Put,
                OpType::Put,
                OpType::Put,
                OpType::Put,
                OpType::Put,
                OpType::Put,
                OpType::Put,
                OpType::Put,
            ], // op_types
            &[
                (Some(1), None),
                (Some(2), None),
                (Some(3), None),
                (None, None),
                (Some(5), None),
                (None, None),
                (Some(7), None),
                (Some(8), None),
                (Some(9), None),
            ], // values
        );
    }
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
            &[
                (1000, 1),
                (1000, 2),
                (1001, 1),
                (2002, 1),
                (2003, 1),
                (2003, 5),
            ], // keys
            &[
                (Some(1), None),
                (Some(2), None),
                (Some(3), None),
                (Some(4), None),
                (None, None),
                (None, None),
            ], // values
        );

        let total = 6;
        // Batch size [less than, equal to, greater than] total
        let batch_sizes = [1, 6, 8];
        for batch_size in batch_sizes {
            let iter_ctx = IterContext {
                batch_size,
                ..Default::default()
            };

            let mut iter = ctx.memtable.iter(&iter_ctx).unwrap();
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
            &[(1000, 1), (1000, 2), (2000, 1), (2001, 2)], // keys
            &[(Some(1), None), (None, None), (None, None), (None, None)], // values
        );

        write_kvs(
            &*ctx.memtable,
            11, // sequence
            OpType::Put,
            &[(1000, 1), (2001, 2)],                   // keys
            &[(Some(1231), None), (Some(1232), None)], // values
        );

        let batch_sizes = [1, 2, 3, 4, 5];
        for batch_size in batch_sizes {
            let iter_ctx = IterContext {
                batch_size,
                ..Default::default()
            };

            let mut iter = ctx.memtable.iter(&iter_ctx).unwrap();
            check_iter_content(
                &mut *iter,
                &[(1000, 1), (1000, 2), (2000, 1), (2001, 2)], // keys
                &[11, 10, 10, 11],                             // sequences
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
            &[(1000, 1), (1000, 2), (1000, 1), (2001, 2)], // keys
            &[(None, None), (None, None), (Some(1234), None), (None, None)], // values
        );

        let batch_sizes = [1, 2, 3, 4, 5];
        for batch_size in batch_sizes {
            let iter_ctx = IterContext {
                batch_size,
                ..Default::default()
            };

            let mut iter = ctx.memtable.iter(&iter_ctx).unwrap();
            check_iter_content(
                &mut *iter,
                &[(1000, 1), (1000, 2), (2001, 2)],       // keys
                &[10, 10, 10],                            // sequences
                &[OpType::Put, OpType::Put, OpType::Put], // op_types
                &[(Some(1234), None), (None, None), (None, None), (None, None)], // values
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
            &[(1000, 1), (1000, 2)],             // keys
            &[(Some(1), None), (Some(2), None)], // values
        );

        write_kvs(
            &*ctx.memtable,
            11, // sequence
            OpType::Put,
            &[(1000, 1), (1000, 2)],               // keys
            &[(Some(11), None), (Some(12), None)], // values
        );

        write_kvs(
            &*ctx.memtable,
            12, // sequence
            OpType::Put,
            &[(1000, 1), (1000, 2)],               // keys
            &[(Some(21), None), (Some(22), None)], // values
        );

        {
            let iter_ctx = IterContext {
                batch_size: 1,
                visible_sequence: 9,
                for_flush: false,
                projected_schema: None,
            };

            let mut iter = ctx.memtable.iter(&iter_ctx).unwrap();
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
                for_flush: false,
                projected_schema: None,
            };

            let mut iter = ctx.memtable.iter(&iter_ctx).unwrap();
            check_iter_content(
                &mut *iter,
                &[(1000, 1), (1000, 2)],             // keys
                &[10, 10],                           // sequences
                &[OpType::Put, OpType::Put],         // op_types
                &[(Some(1), None), (Some(2), None)], // values
            );
        }

        {
            let iter_ctx = IterContext {
                batch_size: 1,
                visible_sequence: 11,
                for_flush: false,
                projected_schema: None,
            };

            let mut iter = ctx.memtable.iter(&iter_ctx).unwrap();
            check_iter_content(
                &mut *iter,
                &[(1000, 1), (1000, 2)],               // keys
                &[11, 11],                             // sequences
                &[OpType::Put, OpType::Put],           // op_types
                &[(Some(11), None), (Some(12), None)], // values
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
            &[(1000, 0), (1001, 1), (1002, 2)], // keys
            &[(Some(0), None), (Some(1), None), (Some(2), None)], // values
        );

        let iter_ctx = IterContext {
            batch_size: 4,
            ..Default::default()
        };

        let mut iter = ctx.memtable.iter(&iter_ctx).unwrap();
        assert!(iter.next().is_some());
        assert!(iter.next().is_none());
        assert!(iter.next().is_none());
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
            &[(1000, 0), (1001, 1), (1002, 2)], // keys
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

        let mut iter = ctx.memtable.iter(&iter_ctx).unwrap();
        let batch = iter.next().unwrap().unwrap();
        assert!(iter.next().is_none());

        assert_eq!(5, batch.num_columns());
        let k0 = Int64Array::from_slice(&[1000, 1001, 1002]);
        let k0 = PrimitiveArray::new(
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            k0.values().clone(),
            k0.validity().cloned(),
        );

        let k1 = UInt64Array::from_slice(&[0, 1, 2]);
        let v0 = UInt64Array::from_slice(&[10, 11, 12]);
        let sequences = UInt64Array::from_slice(&[9, 9, 9]);
        let op_types = UInt8Array::from_slice(&[0, 0, 0]);

        assert_eq!(k0, &*batch.column(0).to_arrow_array());
        assert_eq!(k1, &*batch.column(1).to_arrow_array());
        assert_eq!(v0, &*batch.column(2).to_arrow_array());
        assert_eq!(sequences, &*batch.column(3).to_arrow_array());
        assert_eq!(op_types, &*batch.column(4).to_arrow_array());
    });
}
