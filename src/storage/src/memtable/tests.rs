use std::path::PathBuf;

use datatypes::arrow::array::{Array, Int64Array, UInt64Array, UInt8Array};
use datatypes::arrow::io::parquet::read::FileReader;
use datatypes::prelude::*;
use datatypes::type_id::LogicalTypeId;
use datatypes::vectors::{Int64VectorBuilder, UInt64VectorBuilder};

use super::*;
use crate::flush_task::{Backend, FlushConfig, FlushTask};
use crate::metadata::RegionMetadata;
use crate::test_util::descriptor_util::RegionDescBuilder;

// For simplicity, all memtables in test share same memtable id.
const MEMTABLE_ID: MemtableId = 1;

// Schema for testing memtable:
// - key: Int64(timestamp), UInt64(version),
// - value: UInt64
fn schema_for_test() -> MemtableSchema {
    // Just build a region desc and use its columns_row_key metadata.
    let desc = RegionDescBuilder::new("test")
        .push_value_column(("v1", LogicalTypeId::UInt64, true))
        .build();
    let metadata: RegionMetadata = desc.try_into().unwrap();

    MemtableSchema::new(metadata.columns_row_key)
}

fn kvs_for_test_with_index(
    sequence: SequenceNumber,
    value_type: ValueType,
    start_index_in_batch: usize,
    keys: &[(i64, u64)],
    values: &[Option<u64>],
) -> KeyValues {
    assert_eq!(keys.len(), values.len());

    let mut key_builders = (
        Int64VectorBuilder::with_capacity(keys.len()),
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

    let mut value_builder = UInt64VectorBuilder::with_capacity(values.len());
    for value in values {
        value_builder.push(*value);
    }
    let row_values = vec![Arc::new(value_builder.finish()) as _];

    let kvs = KeyValues {
        sequence,
        value_type,
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
    value_type: ValueType,
    keys: &[(i64, u64)],
    values: &[Option<u64>],
) -> KeyValues {
    kvs_for_test_with_index(sequence, value_type, 0, keys, values)
}

fn write_kvs(
    memtable: &dyn Memtable,
    sequence: SequenceNumber,
    value_type: ValueType,
    keys: &[(i64, u64)],
    values: &[Option<u64>],
) {
    let kvs = kvs_for_test(sequence, value_type, keys, values);

    memtable.write(&kvs).unwrap();
}

fn check_batch_valid(batch: &Batch) {
    assert_eq!(2, batch.keys.len());
    assert_eq!(1, batch.values.len());
    let row_num = batch.keys[0].len();
    assert_eq!(row_num, batch.keys[1].len());
    assert_eq!(row_num, batch.sequences.len());
    assert_eq!(row_num, batch.value_types.len());
    assert_eq!(row_num, batch.values[0].len());
}

fn check_iter_content(
    iter: &mut dyn BatchIterator,
    keys: &[(i64, u64)],
    sequences: &[u64],
    value_types: &[ValueType],
    values: &[Option<u64>],
) {
    let mut index = 0;
    while let Some(batch) = iter.next().unwrap() {
        check_batch_valid(&batch);

        let row_num = batch.keys[0].len();
        for i in 0..row_num {
            let (k0, k1) = (batch.keys[0].get(i), batch.keys[1].get(i));
            let sequence = batch.sequences.get_data(i).unwrap();
            let value_type = batch.value_types.get_data(i).unwrap();
            let v = batch.values[0].get(i);

            assert_eq!(Value::from(keys[index].0), k0);
            assert_eq!(Value::from(keys[index].1), k1);
            assert_eq!(sequences[index], sequence);
            assert_eq!(value_types[index].as_u8(), value_type);
            assert_eq!(Value::from(values[index]), v);

            index += 1;
        }
    }

    assert_eq!(keys.len(), index);
}

// TODO(yingwen): Check size of the returned batch.

struct MemtableTester {
    schema: MemtableSchema,
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
    schema: MemtableSchema,
    memtable: MemtableRef,
}

fn write_iter_memtable_case(ctx: &TestContext) {
    // Test iterating an empty memtable.
    let mut iter = ctx.memtable.iter(IterContext::default()).unwrap();
    assert!(iter.next().unwrap().is_none());
    assert_eq!(0, ctx.memtable.bytes_allocated());

    // Init test data.
    write_kvs(
        &*ctx.memtable,
        10, // sequence
        ValueType::Put,
        &[
            (1000, 1),
            (1000, 2),
            (2002, 1),
            (2003, 1),
            (2003, 5),
            (1001, 1),
        ], // keys
        &[Some(1), Some(2), Some(7), Some(8), Some(9), Some(3)], // values
    );
    write_kvs(
        &*ctx.memtable,
        11, // sequence
        ValueType::Put,
        &[(1002, 1), (1003, 1), (1004, 1)], // keys
        &[None, Some(5), None],             // values
    );

    assert_eq!(216, ctx.memtable.bytes_allocated());

    let batch_sizes = [1, 4, 8, consts::READ_BATCH_SIZE];
    for batch_size in batch_sizes {
        let iter_ctx = IterContext {
            batch_size,
            ..Default::default()
        };
        let mut iter = ctx.memtable.iter(iter_ctx).unwrap();
        assert_eq!(ctx.schema, *iter.schema());
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
                ValueType::Put,
                ValueType::Put,
                ValueType::Put,
                ValueType::Put,
                ValueType::Put,
                ValueType::Put,
                ValueType::Put,
                ValueType::Put,
                ValueType::Put,
            ], // value types
            &[
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
                None,
                Some(7),
                Some(8),
                Some(9),
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
    while let Some(batch) = iter.next().unwrap() {
        check_batch_valid(&batch);

        let row_num = batch.keys[0].len();
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
            ValueType::Put,
            &[
                (1000, 1),
                (1000, 2),
                (1001, 1),
                (2002, 1),
                (2003, 1),
                (2003, 5),
            ], // keys
            &[Some(1), Some(2), Some(3), Some(4), None, None], // values
        );

        let total = 6;
        // Batch size [less than, equal to, greater than] total
        let batch_sizes = [1, 6, 8];
        for batch_size in batch_sizes {
            let iter_ctx = IterContext {
                batch_size,
                ..Default::default()
            };

            let mut iter = ctx.memtable.iter(iter_ctx).unwrap();
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
            ValueType::Put,
            &[(1000, 1), (1000, 2), (2000, 1), (2001, 2)], // keys
            &[Some(1), None, None, None],                  // values
        );

        write_kvs(
            &*ctx.memtable,
            11, // sequence
            ValueType::Put,
            &[(1000, 1), (2001, 2)],   // keys
            &[Some(1231), Some(1232)], // values
        );

        let batch_sizes = [1, 2, 3, 4, 5];
        for batch_size in batch_sizes {
            let iter_ctx = IterContext {
                batch_size,
                ..Default::default()
            };

            let mut iter = ctx.memtable.iter(iter_ctx).unwrap();
            check_iter_content(
                &mut *iter,
                &[(1000, 1), (1000, 2), (2000, 1), (2001, 2)], // keys
                &[11, 10, 10, 11],                             // sequences
                &[
                    ValueType::Put,
                    ValueType::Put,
                    ValueType::Put,
                    ValueType::Put,
                ], // value types
                &[Some(1231), None, None, Some(1232)],         // values
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
            ValueType::Put,
            &[(1000, 1), (1000, 2), (1000, 1), (2001, 2)], // keys
            &[None, None, Some(1234), None],               // values
        );

        let batch_sizes = [1, 2, 3, 4, 5];
        for batch_size in batch_sizes {
            let iter_ctx = IterContext {
                batch_size,
                ..Default::default()
            };

            let mut iter = ctx.memtable.iter(iter_ctx).unwrap();
            check_iter_content(
                &mut *iter,
                &[(1000, 1), (1000, 2), (2001, 2)], // keys
                &[10, 10, 10],                      // sequences
                &[ValueType::Put, ValueType::Put, ValueType::Put], // value types
                &[Some(1234), None, None, None],    // values
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
            ValueType::Put,
            &[(1000, 1), (1000, 2)], // keys
            &[Some(1), Some(2)],     // values
        );

        write_kvs(
            &*ctx.memtable,
            11, // sequence
            ValueType::Put,
            &[(1000, 1), (1000, 2)], // keys
            &[Some(11), Some(12)],   // values
        );

        write_kvs(
            &*ctx.memtable,
            12, // sequence
            ValueType::Put,
            &[(1000, 1), (1000, 2)], // keys
            &[Some(21), Some(22)],   // values
        );

        {
            let iter_ctx = IterContext {
                batch_size: 1,
                visible_sequence: 9,
                for_flush: false,
            };

            let mut iter = ctx.memtable.iter(iter_ctx).unwrap();
            check_iter_content(
                &mut *iter,
                &[], // keys
                &[], // sequences
                &[], // value types
                &[], // values
            );
        }

        {
            let iter_ctx = IterContext {
                batch_size: 1,
                visible_sequence: 10,
                for_flush: false,
            };

            let mut iter = ctx.memtable.iter(iter_ctx).unwrap();
            check_iter_content(
                &mut *iter,
                &[(1000, 1), (1000, 2)],           // keys
                &[10, 10],                         // sequences
                &[ValueType::Put, ValueType::Put], // value types
                &[Some(1), Some(2)],               // values
            );
        }

        {
            let iter_ctx = IterContext {
                batch_size: 1,
                visible_sequence: 11,
                for_flush: false,
            };

            let mut iter = ctx.memtable.iter(iter_ctx).unwrap();
            check_iter_content(
                &mut *iter,
                &[(1000, 1), (1000, 2)],           // keys
                &[11, 11],                         // sequences
                &[ValueType::Put, ValueType::Put], // value types
                &[Some(11), Some(12)],             // values
            );
        }
    });
}

// TODO(yingwen): Test key overwrite in same batch.

#[tokio::test]
async fn test_flush() {
    let tester = MemtableTester::default();

    let memtable = tester.new_memtables().get(0).unwrap().clone();

    write_kvs(
        &*memtable,
        10, // sequence
        ValueType::Put,
        &[
            (1000, 1),
            (1000, 2),
            (2002, 1),
            (2003, 1),
            (2003, 5),
            (1001, 1),
        ], // keys
        &[Some(1), Some(2), Some(7), Some(8), Some(9), Some(3)], // values
    );

    let config = FlushConfig::default();
    let sst_dir = match &config.backend {
        Backend::Fs { dir } => dir.clone(),
    };

    let flusher = FlushTask::try_new(config).await.unwrap();
    let sst_file_name = "test-flush.parquet";

    flusher
        .write_rows(&memtable, sst_file_name, None)
        .await
        .unwrap();

    // verify parquet file

    let reader = std::fs::File::open(PathBuf::from(sst_dir).join(sst_file_name)).unwrap();
    let mut file_reader = FileReader::try_new(reader, None, Some(128), None, None).unwrap();

    // chunk schema: timestamp, __version, __sequence, __value_type, v1
    let chunk = file_reader.next().unwrap().unwrap();
    assert_eq!(5, chunk.arrays().len());

    assert_eq!(
        Arc::new(Int64Array::from_slice(&[
            1000, 1000, 1001, 2002, 2003, 2003
        ])) as Arc<dyn Array>,
        chunk.arrays()[0]
    );

    assert_eq!(
        Arc::new(UInt64Array::from_slice(&[1, 2, 1, 1, 1, 5])) as Arc<dyn Array>,
        chunk.arrays()[1]
    );

    assert_eq!(
        Arc::new(UInt64Array::from_slice(&[10, 10, 10, 10, 10, 10])) as Arc<dyn Array>,
        chunk.arrays()[2]
    );

    assert_eq!(
        Arc::new(UInt8Array::from_slice(&[0, 0, 0, 0, 0, 0])) as Arc<dyn Array>,
        chunk.arrays()[3]
    );

    assert_eq!(
        Arc::new(UInt64Array::from_slice(&[1, 2, 3, 7, 8, 9])) as Arc<dyn Array>,
        chunk.arrays()[4]
    );
}
