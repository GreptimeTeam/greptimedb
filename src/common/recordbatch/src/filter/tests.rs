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

use std::sync::Arc;

use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{col, lit};
use datafusion::physical_expr::create_physical_expr;
use datafusion_common::DFSchema;
use datatypes::arrow::datatypes::{DataType, Field, Schema};

use datatypes::arrow::array::{BooleanArray, RecordBatch};

use crate::filter::{batch_filter, boolean_array_to_scan_mask};

#[test]
fn batch_filter_test() {
    let expr = col("ts").gt(lit(123456u64));
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("ts", DataType::UInt64, false),
    ]);
    let df_schema = DFSchema::try_from(schema.clone()).unwrap();
    let props = ExecutionProps::new();
    let physical_expr = create_physical_expr(
        &expr,
        &df_schema,
        &props,
        &PhysicalPlanningContext::default(),
    )
    .unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(datatypes::arrow::array::Int32Array::from(vec![4, 5, 6])),
            Arc::new(datatypes::arrow::array::UInt64Array::from(vec![
                123456, 123457, 123458,
            ])),
        ],
    )
    .unwrap();
    let new_batch = batch_filter(&batch, &physical_expr).unwrap();
    assert_eq!(new_batch.num_rows(), 2);
    let first_column_values = new_batch
        .column(0)
        .as_any()
        .downcast_ref::<datatypes::arrow::array::Int32Array>()
        .unwrap();
    let expected = datatypes::arrow::array::Int32Array::from(vec![5, 6]);
    assert_eq!(first_column_values, &expected);
}

#[test]
fn test_nullable_boolean_predicate_becomes_scan_mask() {
    let predicate = BooleanArray::from(vec![Some(true), None, Some(false)]);
    assert_eq!(
        BooleanArray::from(vec![true, false, false]),
        boolean_array_to_scan_mask(&predicate)
    );
}

#[test]
fn test_partial_metadata() {
    use datafusion_common::ScalarValue;

    use crate::filter::MetadataFilter;

    let node = col("node_id").eq(lit(7u64));
    let path = col("file_path").eq(lit("table/region/file.parquet"));
    let filters = MetadataFilter::new(&[node.clone().and(path.clone())]);
    assert!(!filters.matches(|name| (name == "node_id").then_some(ScalarValue::UInt64(Some(8)))));
    assert!(filters.matches(|name| (name == "node_id").then_some(ScalarValue::UInt64(Some(7)))));
    assert!(!filters.matches(|name| {
        (name == "file_path").then(|| ScalarValue::Utf8(Some("other/file.parquet".into())))
    }));
    assert!(filters.matches(|name| {
        (name == "file_path").then(|| ScalarValue::Utf8(Some("table/region/file.parquet".into())))
    }));
    // Unknown alternatives cannot be discarded from OR, nor extracted from NOT.
    let disjunction = MetadataFilter::new(&[node.clone().or(path.clone())]);
    assert!(
        disjunction.matches(|name| (name == "node_id").then_some(ScalarValue::UInt64(Some(8))))
    );
    let negation = MetadataFilter::new(&[datafusion::logical_expr::Expr::Not(Box::new(
        node.and(path),
    ))]);
    assert!(negation.matches(|_| None::<ScalarValue>));
    // Unsupported LIKE remains a residual even when its column is available.
    let residual = MetadataFilter::new(&[col("file_path").like(lit("table/%"))]);
    assert!(residual.matches(|_| Some(ScalarValue::Utf8(Some("other/file".into())))));
    // Missing metadata differs from known NULL; type errors conservatively pass.
    let null_filter = MetadataFilter::new(&[col("node_id").is_not_null()]);
    assert!(null_filter.matches(|_| None::<ScalarValue>));
    assert!(!null_filter.matches(|_| Some(ScalarValue::UInt64(None))));
    let typed = MetadataFilter::new(&[col("node_id").eq(lit(7u64))]);
    assert!(typed.matches(|_| Some(ScalarValue::Utf8(Some("invalid".into())))));
}

#[test]
fn test_borrowed_and_owned_values() {
    use datafusion_common::ScalarValue;

    use crate::filter::MetadataFilter;

    let pruner = MetadataFilter::new(&[col("path").gt_eq(lit("a")).and(col("path").lt(lit("z")))]);
    for (value, expected) in [
        (ScalarValue::Utf8(Some("a".into())), true),
        (ScalarValue::Utf8(Some("z".into())), false),
        (ScalarValue::Utf8(None), false),
        (ScalarValue::UInt64(Some(1)), true),
    ] {
        assert_eq!(pruner.matches(|_| Some(&value)), expected);
        assert_eq!(pruner.matches(|_| Some(value.clone())), expected);
    }
}

#[test]
fn test_borrowed_lazy_metadata() {
    use std::cell::OnceCell;

    use datafusion_common::ScalarValue;

    use crate::filter::MetadataFilter;

    let pruner = MetadataFilter::new(&[col("path").gt(lit("a")).and(col("path").lt(lit("z")))]);
    let path = OnceCell::new();
    let mut builds = 0;
    assert!(pruner.matches(|name| {
        (name == "path").then(|| {
            path.get_or_init(|| {
                builds += 1;
                ScalarValue::Utf8(Some("region/file.parquet".into()))
            })
        })
    }));
    assert_eq!(builds, 1);
    assert_eq!(
        path.into_inner(),
        Some(ScalarValue::Utf8(Some("region/file.parquet".into())))
    );

    let path = OnceCell::new();
    assert!(
        MetadataFilter::default()
            .matches(|_| { Some(path.get_or_init(|| ScalarValue::Utf8(Some("unused".into())))) })
    );
    assert!(path.get().is_none());
}
