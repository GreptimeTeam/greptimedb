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

//! Projection helpers shared by flat projection code.

use std::cmp::Ordering;

use common_recordbatch::error::DataTypesSnafu;
use datatypes::prelude::{ConcreteDataType, DataType};
use datatypes::value::Value;
use datatypes::vectors::VectorRef;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;

use crate::cache::CacheStrategy;
use crate::error::{InvalidRequestSnafu, Result};

/// Only cache vector when its length `<=` this value.
pub(crate) const MAX_VECTOR_LENGTH_TO_CACHE: usize = 16384;

pub(crate) fn read_column_ids_from_projection(
    metadata: &RegionMetadataRef,
    projection: &[usize],
) -> Result<Vec<ColumnId>> {
    let mut column_ids = Vec::with_capacity(projection.len().max(1));
    if projection.is_empty() {
        column_ids.push(metadata.time_index_column().column_id);
        return Ok(column_ids);
    }

    for idx in projection {
        let column = metadata
            .column_metadatas
            .get(*idx)
            .with_context(|| InvalidRequestSnafu {
                region_id: metadata.region_id,
                reason: format!("projection index {} is out of bound", idx),
            })?;
        column_ids.push(column.column_id);
    }
    Ok(column_ids)
}

/// Gets a vector with repeated values from specific cache or creates a new one.
pub(crate) fn repeated_vector_with_cache(
    data_type: &ConcreteDataType,
    value: &Value,
    num_rows: usize,
    cache_strategy: &CacheStrategy,
) -> common_recordbatch::error::Result<VectorRef> {
    if let Some(vector) = cache_strategy.get_repeated_vector(data_type, value) {
        match vector.len().cmp(&num_rows) {
            Ordering::Less => (),
            Ordering::Equal => return Ok(vector),
            Ordering::Greater => return Ok(vector.slice(0, num_rows)),
        }
    }

    let vector = new_repeated_vector(data_type, value, num_rows)?;
    if vector.len() <= MAX_VECTOR_LENGTH_TO_CACHE {
        cache_strategy.put_repeated_vector(value.clone(), vector.clone());
    }

    Ok(vector)
}

/// Returns a vector with repeated values.
pub(crate) fn new_repeated_vector(
    data_type: &ConcreteDataType,
    value: &Value,
    num_rows: usize,
) -> common_recordbatch::error::Result<VectorRef> {
    let mut mutable_vector = data_type.create_mutable_vector(1);
    mutable_vector
        .try_push_value_ref(&value.as_value_ref())
        .context(DataTypesSnafu)?;
    let base_vector = mutable_vector.to_vector();
    Ok(base_vector.replicate(&[num_rows]))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::OpType;
    use common_recordbatch::RecordBatch;
    use datatypes::arrow::array::{Int64Array, TimestampMillisecondArray, UInt8Array, UInt64Array};
    use datatypes::arrow::datatypes::Field;
    use datatypes::arrow::util::pretty;
    use datatypes::value::ValueRef;
    use mito_codec::row_converter::{DensePrimaryKeyCodec, PrimaryKeyCodecExt, SortField};
    use mito_codec::test_util::TestRegionMetadataBuilder;
    use store_api::storage::consts::{
        OP_TYPE_COLUMN_NAME, PRIMARY_KEY_COLUMN_NAME, SEQUENCE_COLUMN_NAME,
    };

    use super::*;
    use crate::read::flat_projection::FlatProjectionMapper;

    fn print_record_batch(record_batch: RecordBatch) -> String {
        pretty::pretty_format_batches(&[record_batch.into_df_record_batch()])
            .unwrap()
            .to_string()
    }

    fn new_flat_batch(
        ts_start: Option<i64>,
        idx_tags: &[(usize, i64)],
        idx_fields: &[(usize, i64)],
        num_rows: usize,
    ) -> datatypes::arrow::record_batch::RecordBatch {
        let mut columns = Vec::with_capacity(1 + idx_tags.len() + idx_fields.len() + 3);
        let mut fields = Vec::with_capacity(1 + idx_tags.len() + idx_fields.len() + 3);

        for (i, tag) in idx_tags {
            let array = Arc::new(Int64Array::from_iter_values(std::iter::repeat_n(
                *tag, num_rows,
            ))) as _;
            columns.push(array);
            fields.push(Field::new(
                format!("k{i}"),
                datatypes::arrow::datatypes::DataType::Int64,
                true,
            ));
        }

        for (i, field) in idx_fields {
            let array = Arc::new(Int64Array::from_iter_values(std::iter::repeat_n(
                *field, num_rows,
            ))) as _;
            columns.push(array);
            fields.push(Field::new(
                format!("v{i}"),
                datatypes::arrow::datatypes::DataType::Int64,
                true,
            ));
        }

        if let Some(ts_start) = ts_start {
            let timestamps = Arc::new(TimestampMillisecondArray::from_iter_values(
                (0..num_rows).map(|i| ts_start + i as i64 * 1000),
            )) as _;
            columns.push(timestamps);
            fields.push(Field::new(
                "ts",
                datatypes::arrow::datatypes::DataType::Timestamp(
                    datatypes::arrow::datatypes::TimeUnit::Millisecond,
                    None,
                ),
                true,
            ));
        }

        let converter = DensePrimaryKeyCodec::with_fields(
            (0..idx_tags.len())
                .map(|idx| {
                    (
                        idx as u32,
                        SortField::new(ConcreteDataType::int64_datatype()),
                    )
                })
                .collect(),
        );
        let encoded_pk = converter
            .encode(idx_tags.iter().map(|(_, v)| ValueRef::Int64(*v)))
            .unwrap();

        let pk_values: Vec<&[u8]> = std::iter::repeat_n(encoded_pk.as_slice(), num_rows).collect();
        let keys = datatypes::arrow::array::UInt32Array::from_iter(0..num_rows as u32);
        let values = Arc::new(datatypes::arrow::array::BinaryArray::from_vec(pk_values));
        let pk_array =
            Arc::new(datatypes::arrow::array::DictionaryArray::try_new(keys, values).unwrap()) as _;
        columns.push(pk_array);
        fields.push(Field::new_dictionary(
            PRIMARY_KEY_COLUMN_NAME,
            datatypes::arrow::datatypes::DataType::UInt32,
            datatypes::arrow::datatypes::DataType::Binary,
            false,
        ));

        columns.push(Arc::new(UInt64Array::from_iter_values(0..num_rows as u64)) as _);
        fields.push(Field::new(
            SEQUENCE_COLUMN_NAME,
            datatypes::arrow::datatypes::DataType::UInt64,
            false,
        ));

        columns.push(Arc::new(UInt8Array::from_iter_values(
            (0..num_rows).map(|_| OpType::Put as u8),
        )) as _);
        fields.push(Field::new(
            OP_TYPE_COLUMN_NAME,
            datatypes::arrow::datatypes::DataType::UInt8,
            false,
        ));

        let schema = Arc::new(datatypes::arrow::datatypes::Schema::new(fields));

        datatypes::arrow::record_batch::RecordBatch::try_new(schema, columns).unwrap()
    }

    #[test]
    fn test_flat_projection_mapper_all() {
        let metadata = Arc::new(
            TestRegionMetadataBuilder::default()
                .num_tags(2)
                .num_fields(2)
                .build(),
        );
        let cache = CacheStrategy::Disabled;
        let mapper = FlatProjectionMapper::all(&metadata).unwrap();
        assert_eq!(
            &[0, 1, 2, 3, 4],
            mapper.read_columns().column_ids().as_slice()
        );
        assert_eq!(
            [
                (1, ConcreteDataType::int64_datatype()),
                (2, ConcreteDataType::int64_datatype()),
                (3, ConcreteDataType::int64_datatype()),
                (4, ConcreteDataType::int64_datatype()),
                (0, ConcreteDataType::timestamp_millisecond_datatype())
            ],
            mapper.batch_schema()
        );

        let batch = new_flat_batch(Some(0), &[(1, 1), (2, 2)], &[(3, 3), (4, 4)], 3);
        let record_batch = mapper.convert(&batch, &cache).unwrap();
        let expect = "\
+---------------------+----+----+----+----+
| ts                  | k0 | k1 | v0 | v1 |
+---------------------+----+----+----+----+
| 1970-01-01T00:00:00 | 1  | 2  | 3  | 4  |
| 1970-01-01T00:00:01 | 1  | 2  | 3  | 4  |
| 1970-01-01T00:00:02 | 1  | 2  | 3  | 4  |
+---------------------+----+----+----+----+";
        assert_eq!(expect, print_record_batch(record_batch));
    }

    #[test]
    fn test_flat_projection_mapper_with_projection() {
        let metadata = Arc::new(
            TestRegionMetadataBuilder::default()
                .num_tags(2)
                .num_fields(2)
                .build(),
        );
        let cache = CacheStrategy::Disabled;
        let mapper = FlatProjectionMapper::new(&metadata, [4, 1]).unwrap();
        assert_eq!(&[4, 1], mapper.read_columns().column_ids().as_slice());
        assert_eq!(
            [
                (1, ConcreteDataType::int64_datatype()),
                (4, ConcreteDataType::int64_datatype()),
                (0, ConcreteDataType::timestamp_millisecond_datatype())
            ],
            mapper.batch_schema()
        );

        let batch = new_flat_batch(None, &[(1, 1)], &[(4, 4)], 3);
        let record_batch = mapper.convert(&batch, &cache).unwrap();
        let expect = "\
+----+----+
| v1 | k0 |
+----+----+
| 4  | 1  |
| 4  | 1  |
| 4  | 1  |
+----+----+";
        assert_eq!(expect, print_record_batch(record_batch));
    }

    #[test]
    fn test_flat_projection_mapper_read_superset() {
        let metadata = Arc::new(
            TestRegionMetadataBuilder::default()
                .num_tags(2)
                .num_fields(2)
                .build(),
        );
        let cache = CacheStrategy::Disabled;
        let mapper =
            FlatProjectionMapper::new_with_read_columns(&metadata, vec![4, 1], vec![4, 1, 3])
                .unwrap();
        assert_eq!(&[4, 1, 3], mapper.read_columns().column_ids().as_slice());

        let batch = new_flat_batch(None, &[(1, 1)], &[(3, 3), (4, 4)], 3);
        let record_batch = mapper.convert(&batch, &cache).unwrap();
        let expect = "\
+----+----+
| v1 | k0 |
+----+----+
| 4  | 1  |
| 4  | 1  |
| 4  | 1  |
+----+----+";
        assert_eq!(expect, print_record_batch(record_batch));
    }

    #[test]
    fn test_flat_projection_mapper_empty_projection() {
        let metadata = Arc::new(
            TestRegionMetadataBuilder::default()
                .num_tags(2)
                .num_fields(2)
                .build(),
        );
        let cache = CacheStrategy::Disabled;
        let mapper = FlatProjectionMapper::new(&metadata, []).unwrap();
        assert_eq!(&[0], mapper.read_columns().column_ids().as_slice());
        assert!(mapper.output_schema().is_empty());
        assert_eq!(
            [(0, ConcreteDataType::timestamp_millisecond_datatype())],
            mapper.batch_schema()
        );

        let batch = new_flat_batch(Some(0), &[], &[], 3);
        let record_batch = mapper.convert(&batch, &cache).unwrap();
        assert_eq!(3, record_batch.num_rows());
        assert_eq!(0, record_batch.num_columns());
        assert!(record_batch.schema.is_empty());
    }
}
