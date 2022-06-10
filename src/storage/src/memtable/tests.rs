use datatypes::prelude::*;
use datatypes::type_id::LogicalTypeId;
use datatypes::vectors::{Int64VectorBuilder, UInt64VectorBuilder};

use super::*;
use crate::metadata::RegionMetadata;
use crate::test_util::descriptor_util::RegionDescBuilder;

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
    let keys = vec![
        Arc::new(key_builders.0.finish()) as _,
        Arc::new(key_builders.1.finish()) as _,
    ];

    let mut value_builder = UInt64VectorBuilder::with_capacity(values.len());
    for value in values {
        value_builder.push(*value);
    }
    let values = vec![Arc::new(value_builder.finish()) as _];

    KeyValues {
        sequence,
        value_type,
        start_index_in_batch,
        keys,
        values,
    }
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

fn check_iter_content(
    iter: &mut dyn BatchIterator,
    keys: &[(i64, u64)],
    sequences: &[u64],
    value_types: &[ValueType],
    values: &[Option<u64>],
) {
    let mut index = 0;
    while let Some(batch) = iter.next().unwrap() {
        assert_eq!(2, batch.keys.len());
        assert_eq!(1, batch.values.len());
        let row_num = batch.keys[0].len();
        assert_eq!(row_num, batch.keys[1].len());
        assert_eq!(row_num, batch.sequences.len());
        assert_eq!(row_num, batch.value_types.len());
        assert_eq!(row_num, batch.values[0].len());

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

#[test]
fn test_write_iter_memtable() {
    let builder = DefaultMemtableBuilder {};
    let schema = schema_for_test();
    let mem = builder.build(schema.clone());

    write_kvs(
        &*mem,
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
        &[Some(1), Some(2), Some(3), Some(7), Some(8), Some(9)], // values
    );
    write_kvs(
        &*mem,
        11, // sequence
        ValueType::Put,
        &[(1002, 1), (1003, 1), (1004, 1)], // keys
        &[None, Some(5), None],             // values
    );

    let mut iter = mem.iter(IterContext { batch_size: 4 }).unwrap();
    assert_eq!(schema, *iter.schema());
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
