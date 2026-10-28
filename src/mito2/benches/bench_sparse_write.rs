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

//! Measures the sparse row and bulk conversion paths used by ingestion and WAL replay.
//! Input preparation, TSID calculation, protocol decoding, and I/O are excluded.

use std::hint::black_box;
use std::sync::Arc;

use api::helper::{ColumnDataTypeWrapper, to_grpc_value};
use api::v1::value::ValueData;
use api::v1::{ColumnDataType, Mutation, OpType, Row, Rows, SemanticType, WriteHint};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datatypes::arrow::array::{
    ArrayRef, BinaryArray, BinaryDictionaryBuilder, Float64Array, TimestampMillisecondArray,
};
use datatypes::arrow::compute;
use datatypes::arrow::datatypes::{DataType, Field, Schema, UInt32Type};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use mito_codec::key_values::KeyValues;
use mito_codec::row_converter::{self, CompositeValues, PrimaryKeyCodec, SparsePrimaryKeyCodec};
use mito2::memtable::bulk::part::{self, BulkPart, BulkPartConverter};
use mito2::sst::{self, FlatSchemaOptions};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder, RegionMetadataRef};
use store_api::metric_engine_consts::{
    DATA_SCHEMA_TABLE_ID_COLUMN_NAME, DATA_SCHEMA_TSID_COLUMN_NAME,
};
use store_api::storage::RegionId;
use store_api::storage::consts::{PRIMARY_KEY_COLUMN_NAME, ReservedColumnId};

const ROWS_PER_BATCH: usize = 4096;
const TABLE_ID: u32 = 1024;

fn build_metadata(num_tags: u32) -> RegionMetadataRef {
    let mut builder = RegionMetadataBuilder::new(RegionId::new(TABLE_ID, 0));
    let mut primary_key = vec![ReservedColumnId::table_id(), ReservedColumnId::tsid()];
    for (name, data_type, column_id) in [
        (
            DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
            ConcreteDataType::uint32_datatype(),
            ReservedColumnId::table_id(),
        ),
        (
            DATA_SCHEMA_TSID_COLUMN_NAME,
            ConcreteDataType::uint64_datatype(),
            ReservedColumnId::tsid(),
        ),
    ] {
        builder.push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(name, data_type, false),
            semantic_type: SemanticType::Tag,
            column_id,
        });
    }
    for index in 0..num_tags {
        let column_id = index * 17 % num_tags;
        builder.push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                format!("tag_{column_id}"),
                ConcreteDataType::string_datatype(),
                true,
            ),
            semantic_type: SemanticType::Tag,
            column_id,
        });
        primary_key.push(column_id);
    }
    builder
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: num_tags,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("value", ConcreteDataType::float64_datatype(), true),
            semantic_type: SemanticType::Field,
            column_id: num_tags + 1,
        })
        .primary_key(primary_key)
        .primary_key_encoding(PrimaryKeyEncoding::Sparse);
    Arc::new(builder.build().unwrap())
}

fn build_keys(num_series: usize, num_tags: u32, value_bytes: usize, unicode: bool) -> Vec<Vec<u8>> {
    let codec = SparsePrimaryKeyCodec::schemaless();
    (0..num_series)
        .map(|series| {
            let tags: Vec<_> = (0..num_tags)
                .map(|index| {
                    // Label name order need not match physical column ID order.
                    let column_id = index * 17 % num_tags;
                    let mut value = format!("{series:08x}{column_id:08x}").into_bytes();
                    assert!(value.len() <= value_bytes);
                    value.resize(value_bytes, b'x');
                    if unicode {
                        // The code point crosses an eight-byte encoding chunk boundary.
                        let start = value_bytes - 9;
                        value[start..start + 4].copy_from_slice("🦀".as_bytes());
                    }
                    (column_id, value)
                })
                .collect();
            let mut key = Vec::new();
            // Permute distinct TSIDs so even one-row-per-series input needs sorting.
            let tsid = (series * 17 % num_series) as u64;
            codec.encode_internal(TABLE_ID, tsid, &mut key).unwrap();
            codec
                .encode_raw_tag_value(
                    tags.iter().map(|(id, value)| (*id, value.as_slice())),
                    &mut key,
                )
                .unwrap();
            assert_eq!(codec.decode_ids(&key).unwrap(), (TABLE_ID, tsid));
            key
        })
        .collect()
}

fn build_rows(metadata: &RegionMetadataRef, keys: &[Vec<u8>]) -> KeyValues {
    let schema = [
        (
            PRIMARY_KEY_COLUMN_NAME,
            ColumnDataType::Binary,
            SemanticType::Tag,
        ),
        (
            "ts",
            ColumnDataType::TimestampMillisecond,
            SemanticType::Timestamp,
        ),
        ("value", ColumnDataType::Float64, SemanticType::Field),
    ]
    .into_iter()
    .map(|(name, data_type, semantic_type)| api::v1::ColumnSchema {
        column_name: name.to_string(),
        datatype: data_type as i32,
        semantic_type: semantic_type as i32,
        ..Default::default()
    })
    .collect();
    let rows = (0..ROWS_PER_BATCH)
        .map(|row| Row {
            values: vec![
                ValueData::BinaryValue(keys[row % keys.len()].clone()).into(),
                ValueData::TimestampMillisecondValue((row / keys.len()) as i64 * 1000).into(),
                ValueData::F64Value(row as f64).into(),
            ],
        })
        .collect();
    KeyValues::new(
        metadata,
        Mutation {
            op_type: OpType::Put as i32,
            sequence: 1,
            rows: Some(Rows { schema, rows }),
            write_hint: Some(WriteHint {
                primary_key_encoding: api::v1::PrimaryKeyEncoding::Sparse.into(),
            }),
        },
    )
    .unwrap()
}

fn build_bulk(keys: &[Vec<u8>], dictionary: bool) -> BulkPart {
    let key_column: ArrayRef = if dictionary {
        let mut builder = BinaryDictionaryBuilder::<UInt32Type>::new();
        for row in 0..ROWS_PER_BATCH {
            builder.append(&keys[row % keys.len()]).unwrap();
        }
        let array = builder.finish();
        assert_eq!(array.values().len(), keys.len());
        Arc::new(array)
    } else {
        Arc::new(BinaryArray::from_iter_values(
            (0..ROWS_PER_BATCH).map(|row| &keys[row % keys.len()]),
        ))
    };
    let columns: Vec<ArrayRef> = vec![
        key_column,
        Arc::new(TimestampMillisecondArray::from_iter_values(
            (0..ROWS_PER_BATCH).map(|row| (row / keys.len()) as i64 * 1000),
        )),
        Arc::new(Float64Array::from_iter_values(
            (0..ROWS_PER_BATCH).map(|row| row as f64),
        )),
    ];
    let schema = Arc::new(Schema::new(
        [PRIMARY_KEY_COLUMN_NAME, "ts", "value"]
            .into_iter()
            .zip(&columns)
            .map(|(name, column)| Field::new(name, column.data_type().clone(), false))
            .collect::<Vec<_>>(),
    ));
    BulkPart {
        batch: RecordBatch::try_new(schema, columns).unwrap(),
        min_timestamp: 0,
        max_timestamp: (ROWS_PER_BATCH / keys.len() - 1) as i64 * 1000,
        sequence: 1,
        min_sequence: 1,
        timestamp_index: 1,
        raw_data: None,
    }
}

fn build_raw_rows(metadata: &RegionMetadataRef, keys: &[Vec<u8>]) -> KeyValues {
    let codec = SparsePrimaryKeyCodec::schemaless();
    let tags: Vec<Vec<_>> = keys
        .iter()
        .map(|key| {
            let CompositeValues::Sparse(values) = codec.decode(key).unwrap() else {
                unreachable!()
            };
            metadata
                .primary_key
                .iter()
                .map(|id| to_grpc_value(values.get(id).unwrap().clone()))
                .collect()
        })
        .collect();
    let schema = metadata
        .column_metadatas
        .iter()
        .map(|column| api::v1::ColumnSchema {
            column_name: column.column_schema.name.clone(),
            datatype: ColumnDataTypeWrapper::try_from(column.column_schema.data_type.clone())
                .unwrap()
                .datatype() as i32,
            semantic_type: column.semantic_type as i32,
            ..Default::default()
        })
        .collect();
    let rows = (0..ROWS_PER_BATCH)
        .map(|row| {
            let mut values = tags[row % tags.len()].clone();
            values.push(
                ValueData::TimestampMillisecondValue((row / tags.len()) as i64 * 1000).into(),
            );
            values.push(ValueData::F64Value(row as f64).into());
            Row { values }
        })
        .collect();
    KeyValues::new(
        metadata,
        Mutation {
            op_type: OpType::Put as i32,
            sequence: 1,
            rows: Some(Rows { schema, rows }),
            write_hint: None,
        },
    )
    .unwrap()
}

// Check every output row outside timing, including repeated series and the sort order.
fn check_output(part: &BulkPart, keys: &[Vec<u8>]) {
    assert_eq!(part.batch.num_rows(), ROWS_PER_BATCH);
    let encoded = compute::cast(
        part.batch.column_by_name(PRIMARY_KEY_COLUMN_NAME).unwrap(),
        &DataType::Binary,
    )
    .unwrap();
    let encoded = encoded.as_any().downcast_ref::<BinaryArray>().unwrap();
    let timestamps = part
        .batch
        .column_by_name("ts")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    let values = part
        .batch
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let rows_per_series = ROWS_PER_BATCH / keys.len();
    let mut series_order: Vec<_> = (0..keys.len()).collect();
    series_order.sort_unstable_by_key(|&series| &keys[series]);
    for row in 0..ROWS_PER_BATCH {
        let series = series_order[row / rows_per_series];
        let sample = row % rows_per_series;
        assert_eq!(encoded.value(row), keys[series]);
        assert_eq!(timestamps.value(row), sample as i64 * 1000);
        assert_eq!(values.value(row), (sample * keys.len() + series) as f64);
    }
}

fn bench_sparse_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("sparse_write");
    group.throughput(Throughput::Elements(ROWS_PER_BATCH as u64));

    for (num_tags, value_bytes, unicode) in [
        (8, 16, false),
        (32, 16, false),
        (32, 128, false),
        (32, 128, true),
        (512, 16, true),
    ] {
        let metadata = build_metadata(num_tags);
        let codec = row_converter::build_primary_key_codec(&metadata);
        let schema = sst::to_flat_sst_arrow_schema(
            &metadata,
            &FlatSchemaOptions::from_encoding(PrimaryKeyEncoding::Sparse),
        );
        for num_series in [64, ROWS_PER_BATCH] {
            let suffix = if unicode { "_unicode" } else { "" };
            let name = format!("{num_tags}tags_{value_bytes}bytes_{num_series}series{suffix}");
            let keys = build_keys(num_series, num_tags, value_bytes, unicode);
            let rows = build_rows(&metadata, &keys);
            let new_converter = || {
                BulkPartConverter::new(
                    &metadata,
                    schema.clone(),
                    ROWS_PER_BATCH,
                    codec.clone(),
                    true,
                )
            };

            let mut converter = new_converter();
            converter.append_key_values(&rows).unwrap();
            check_output(&converter.convert().unwrap(), &keys);
            group.bench_function(BenchmarkId::new("rows", &name), |b| {
                b.iter_batched(
                    new_converter,
                    |mut converter| {
                        converter.append_key_values(black_box(&rows)).unwrap();
                        black_box(converter.convert().unwrap())
                    },
                    // A wide-label batch can own tens of MiB; do not retain many outputs.
                    BatchSize::PerIteration,
                );
            });

            for (input, dictionary) in [("bulk_binary", false), ("bulk_dictionary", true)] {
                let part = build_bulk(&keys, dictionary);
                let convert = |part| {
                    part::convert_bulk_part(part, &metadata, codec.clone(), schema.clone(), true)
                        .unwrap()
                        .unwrap()
                };
                check_output(&convert(part.clone()), &keys);
                group.bench_function(BenchmarkId::new(input, &name), |b| {
                    b.iter_batched(
                        || part.clone(),
                        |part| black_box(convert(part)),
                        BatchSize::PerIteration,
                    );
                });
            }

            let raw_rows = build_raw_rows(&metadata, &keys);
            let mut converter = new_converter();
            converter.append_key_values(&raw_rows).unwrap();
            check_output(&converter.convert().unwrap(), &keys);
            group.bench_function(BenchmarkId::new("raw_rows", &name), |b| {
                b.iter_batched(
                    new_converter,
                    |mut converter| {
                        converter.append_key_values(black_box(&raw_rows)).unwrap();
                        black_box(converter.convert().unwrap())
                    },
                    BatchSize::PerIteration,
                );
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_sparse_write);
criterion_main!(benches);
