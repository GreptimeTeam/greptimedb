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

use std::hint::black_box;
use std::sync::Arc;

use api::v1::SemanticType;
use common_query::prelude::{greptime_timestamp, greptime_value};
use common_recordbatch::filter::SimpleFilterEvaluator;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::Column;
use datafusion_expr::{BinaryExpr, Expr, Literal, Operator};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use datatypes::value::{Value, ValueRef};
use mito_codec::row_converter::{
    DensePrimaryKeyCodec, PrimaryKeyCodec, PrimaryKeyCodecExt, SparsePrimaryKeyCodec,
};
use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder, RegionMetadataRef};
use store_api::storage::{ColumnId, RegionId};

fn setup_metadata() -> RegionMetadataRef {
    let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
    builder
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("pod", ConcreteDataType::string_datatype(), true),
            semantic_type: SemanticType::Tag,
            column_id: 1,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "namespace",
                ConcreteDataType::string_datatype(),
                true,
            ),
            semantic_type: SemanticType::Tag,
            column_id: 2,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "container",
                ConcreteDataType::string_datatype(),
                true,
            ),
            semantic_type: SemanticType::Tag,
            column_id: 3,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                greptime_value(),
                ConcreteDataType::float64_datatype(),
                false,
            ),
            semantic_type: SemanticType::Field,
            column_id: 4,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                greptime_timestamp(),
                ConcreteDataType::timestamp_nanosecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: 5,
        })
        .primary_key(vec![1, 2, 3]);
    Arc::new(builder.build().unwrap())
}

fn create_test_row() -> Vec<(ColumnId, ValueRef<'static>)> {
    vec![
        (1, ValueRef::String("greptime-frontend-6989d9899-22222")),
        (2, ValueRef::String("greptime-cluster")),
        (3, ValueRef::String("greptime-frontend-6989d9899-22222")),
    ]
}

fn create_eq_filter(column_name: &str, value: &str) -> SimpleFilterEvaluator {
    let expr = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(Expr::Column(Column::from_name(column_name))),
        op: Operator::Eq,
        right: Box::new(value.lit()),
    });
    SimpleFilterEvaluator::try_new(&expr).unwrap()
}

fn create_or_eq_filter(column_name: &str, values: &[&str]) -> SimpleFilterEvaluator {
    assert!(!values.is_empty());

    let mut expr = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(Expr::Column(Column::from_name(column_name))),
        op: Operator::Eq,
        right: Box::new(values[0].lit()),
    });
    for value in &values[1..] {
        let rhs = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name(column_name))),
            op: Operator::Eq,
            right: Box::new(value.lit()),
        });
        expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(expr),
            op: Operator::Or,
            right: Box::new(rhs),
        });
    }
    SimpleFilterEvaluator::try_new(&expr).unwrap()
}

fn encode_dense_pk(metadata: &RegionMetadataRef, row: &[(ColumnId, ValueRef<'static>)]) -> Vec<u8> {
    let codec = DensePrimaryKeyCodec::new(metadata);
    let mut pk = Vec::new();
    codec
        .encode_to_vec(row.iter().map(|(_, v)| v.clone()), &mut pk)
        .unwrap();
    pk
}

fn encode_sparse_pk(
    metadata: &RegionMetadataRef,
    row: &[(ColumnId, ValueRef<'static>)],
) -> Vec<u8> {
    let codec = SparsePrimaryKeyCodec::new(metadata);
    let mut pk = Vec::new();
    codec.encode_to_vec(row.iter().cloned(), &mut pk).unwrap();
    pk
}

fn matches_dense_scalar(
    metadata: &RegionMetadataRef,
    codec: &DensePrimaryKeyCodec,
    filters: &[SimpleFilterEvaluator],
    pk: &[u8],
    offsets_buf: &mut Vec<usize>,
) -> bool {
    offsets_buf.clear();
    if filters.is_empty() || metadata.primary_key.is_empty() {
        return true;
    }

    let mut result = true;
    for filter in filters {
        let Some(column) = metadata.column_by_name(filter.column_name()) else {
            continue;
        };
        if column.semantic_type != SemanticType::Tag {
            continue;
        }

        let index = metadata.primary_key_index(column.column_id).unwrap();
        let value = codec.decode_value_at(pk, index, offsets_buf).unwrap();
        let scalar_value = value
            .try_to_scalar_value(&column.column_schema.data_type)
            .unwrap();
        result &= filter.evaluate_scalar(&scalar_value).unwrap_or(true);
    }

    result
}

fn matches_sparse_scalar(
    metadata: &RegionMetadataRef,
    codec: &SparsePrimaryKeyCodec,
    filters: &[SimpleFilterEvaluator],
    pk: &[u8],
    offsets_map: &mut std::collections::HashMap<ColumnId, usize>,
) -> bool {
    offsets_map.clear();
    if filters.is_empty() || metadata.primary_key.is_empty() {
        return true;
    }

    let mut result = true;
    for filter in filters {
        let Some(column) = metadata.column_by_name(filter.column_name()) else {
            continue;
        };
        if column.semantic_type != SemanticType::Tag {
            continue;
        }

        let value = if let Some(offset) = codec.has_column(pk, offsets_map, column.column_id) {
            codec.decode_value_at(pk, offset, column.column_id).unwrap()
        } else {
            Value::Null
        };
        let scalar_value = value
            .try_to_scalar_value(&column.column_schema.data_type)
            .unwrap();
        result &= filter.evaluate_scalar(&scalar_value).unwrap_or(true);
    }

    result
}

fn bench_primary_key_filter(c: &mut Criterion) {
    let metadata = setup_metadata();
    let row = create_test_row();

    let eq_filter = create_eq_filter("pod", "greptime-frontend-6989d9899-22222");
    let or_filter = create_or_eq_filter(
        "pod",
        &[
            "a",
            "b",
            "c",
            "d",
            "e",
            "f",
            "g",
            "greptime-frontend-6989d9899-22222",
        ],
    );

    let cases = [("eq", eq_filter), ("or_eq", or_filter)];

    for (case_name, filter) in cases {
        let filters = Arc::new(vec![filter]);

        let dense_pk = encode_dense_pk(&metadata, &row);
        let dense_codec = DensePrimaryKeyCodec::new(&metadata);
        let mut dense_fast = dense_codec.primary_key_filter(&metadata, filters.clone());
        let mut dense_offsets = Vec::new();

        let sparse_pk = encode_sparse_pk(&metadata, &row);
        let sparse_codec = SparsePrimaryKeyCodec::new(&metadata);
        let mut sparse_fast = sparse_codec.primary_key_filter(&metadata, filters.clone());
        let mut sparse_offsets = std::collections::HashMap::new();

        let mut group = c.benchmark_group(format!("primary_key_filter/{case_name}"));

        group.bench_function("dense/fast", |b| {
            b.iter(|| black_box(dense_fast.matches(black_box(&dense_pk))))
        });
        group.bench_function("dense/scalar", |b| {
            b.iter(|| {
                black_box(matches_dense_scalar(
                    &metadata,
                    &dense_codec,
                    filters.as_ref(),
                    black_box(&dense_pk),
                    &mut dense_offsets,
                ))
            })
        });

        group.bench_function("sparse/fast", |b| {
            b.iter(|| black_box(sparse_fast.matches(black_box(&sparse_pk))))
        });
        group.bench_function("sparse/scalar", |b| {
            b.iter(|| {
                black_box(matches_sparse_scalar(
                    &metadata,
                    &sparse_codec,
                    filters.as_ref(),
                    black_box(&sparse_pk),
                    &mut sparse_offsets,
                ))
            })
        });

        group.finish();
    }
}

criterion_group!(benches, bench_primary_key_filter);
criterion_main!(benches);
