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

use std::cell::OnceCell;
use std::sync::Arc;

use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator, cast, col, lit};
use datafusion::physical_expr::create_physical_expr;
use datafusion::prelude::SessionContext;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchema, ScalarValue};
use datatypes::arrow::datatypes::{DataType, Field, Schema};

use datatypes::arrow::array::{BooleanArray, Int32Array, RecordBatch, UInt64Array};

use crate::filter::{
    MetadataFilter, SimpleFilterEvaluator, batch_filter, boolean_array_to_scan_mask,
};

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
fn test_metadata_filter_dictionary_cast() {
    let data_type = DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "label",
        data_type.clone(),
        true,
    )]));
    let expressions = [cast(col("label"), DataType::Utf8).eq(lit("matched"))];
    let typed = MetadataFilter::new_with_column_type(&expressions, &|name| {
        (name == "label").then(|| data_type.clone())
    });
    let from_schema = MetadataFilter::new_with_schema(&expressions, &schema);
    let untyped = MetadataFilter::new(&expressions);
    for (value, expected) in [
        (Some("matched"), true),
        (Some("other"), false),
        (None, false),
    ] {
        let value = ScalarValue::Dictionary(
            Box::new(DataType::UInt32),
            Box::new(ScalarValue::Utf8(value.map(String::from))),
        );
        assert_eq!(typed.matches(|_| Some(&value)), expected);
        assert_eq!(from_schema.matches(|_| Some(&value)), expected);
        // Without source types the cast remains residual, so no candidate is pruned.
        assert!(untyped.matches(|_| Some(&value)));
    }
    assert!(typed.matches(|_| None::<ScalarValue>));
    assert!(from_schema.matches(|_| None::<ScalarValue>));
}

#[test]
fn test_borrowed_and_owned_values() {
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

#[test]
fn test_typed_metadata_numeric_cast_boundaries() {
    let decimal = DataType::Decimal128(20, 0);
    let column = cast(col("node_id"), decimal.clone());
    let column_type = |name: &str| (name == "node_id").then_some(DataType::UInt64);
    let matches = |expression: Expr, value| {
        MetadataFilter::new_with_column_type(&[expression], &column_type)
            .matches(|_| Some(ScalarValue::UInt64(value)))
    };
    for value in [0, 7, u64::MAX] {
        let expression =
            column
                .clone()
                .eq(lit(ScalarValue::Decimal128(Some(value as i128), 20, 0)));
        assert!(matches(expression.clone(), Some(value)));
        assert!(!matches(expression.clone(), Some(value.wrapping_add(1))));
        assert!(!matches(expression.clone(), None));
        // Neither the default metadata constructor nor the existing typed evaluator changes.
        assert!(
            MetadataFilter::new(std::slice::from_ref(&expression))
                .matches(|_| Some(ScalarValue::UInt64(Some(value.wrapping_add(1)))))
        );
        assert!(
            SimpleFilterEvaluator::try_new_with_column_type(&expression, &column_type).is_none()
        );
        assert!(
            MetadataFilter::new_with_column_type(&[expression], &|_| None)
                .matches(|_| Some(ScalarValue::UInt64(Some(value.wrapping_add(1)))))
        );
    }
    let nested = cast(cast(lit(7i32), DataType::Int64), decimal.clone());
    assert!(matches(column.clone().eq(nested.clone()), Some(7)));
    assert!(!matches(nested.eq(column.clone()), Some(8)));
    for literal in [
        ScalarValue::Decimal128(Some(-1), 20, 0),
        ScalarValue::Decimal128(Some(u64::MAX as i128 + 1), 20, 0),
        ScalarValue::Decimal128(Some(15), 20, 1),
    ] {
        for op in [
            Operator::Eq,
            Operator::NotEq,
            Operator::Lt,
            Operator::LtEq,
            Operator::Gt,
            Operator::GtEq,
        ] {
            let expression = Expr::BinaryExpr(BinaryExpr::new(
                Box::new(column.clone()),
                op,
                Box::new(lit(literal.clone())),
            ));
            for value in [None, Some(0), Some(u64::MAX)] {
                assert!(matches(expression.clone(), value));
            }
        }
    }
    // Unsupported decimal parameters must never reach the exact-cast helper,
    // including at an inner step of a nested constant cast.
    for (precision, scale) in [(20, -1), (0, 0), (39, 0), (20, 21)] {
        let source = lit(ScalarValue::Decimal128(Some(1), precision, scale));
        let target = DataType::Decimal128(precision, scale);
        for literal in [
            source.clone(),
            cast(source, decimal.clone()),
            cast(cast(lit(1i64), target), decimal.clone()),
        ] {
            assert!(matches(column.clone().eq(literal), Some(0)));
        }
    }
    // Lossy literal casts and lossy/unknown column casts stay residual.
    let lossy_literal = cast(
        lit(ScalarValue::Decimal128(Some(15), 20, 1)),
        decimal.clone(),
    );
    assert!(matches(column.clone().eq(lossy_literal), Some(0)));
    assert!(matches(
        cast(col("node_id"), DataType::Int64).eq(lit(7i64)),
        Some(0)
    ));
    assert!(matches(column.clone().is_null(), None));
    assert!(!matches(column.clone().is_null(), Some(0)));
    assert!(!matches(column.clone().is_not_null(), None));
    assert!(matches(column.clone().is_not_null(), Some(0)));
    let null_comparison = column.clone().eq(lit(ScalarValue::Decimal128(None, 20, 0)));
    assert!(!matches(null_comparison.clone(), None));
    assert!(!matches(null_comparison, Some(0)));
    let equals_seven = column.eq(cast(lit(7i64), decimal));
    // Cross-column and unsupported same-column OR alternatives must not be discarded.
    for residual in [
        col("other").eq(lit(1i64)),
        cast(col("node_id"), DataType::Utf8).like(lit("8%")),
    ] {
        assert!(matches(equals_seven.clone().or(residual.clone()), Some(8)));
        assert!(!matches(equals_seven.clone().and(residual), Some(8)));
    }
}

#[tokio::test]
async fn test_typed_metadata_casts_from_sql_analyzer() {
    let values = [None, Some(0), Some(1), Some(7), Some(u64::MAX)];
    let context = SessionContext::new();
    context
        .register_batch(
            "metadata",
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("node_id", DataType::UInt64, true),
                    Field::new("id", DataType::Int32, false),
                ])),
                vec![
                    Arc::new(UInt64Array::from(values.to_vec())),
                    Arc::new(Int32Array::from_iter_values(0..values.len() as i32)),
                ],
            )
            .unwrap(),
        )
        .unwrap();
    let mut predicates = Vec::new();
    for op in ["=", "!=", "<", "<=", ">", ">="] {
        predicates.push(format!("node_id {op} 7"));
        predicates.push(format!("7 {op} node_id"));
    }
    predicates.extend(
        [
            "CAST(node_id AS DECIMAL(20,0)) IS NULL",
            "CAST(node_id AS DECIMAL(20,0)) IS NOT NULL",
            "node_id = 7 OR node_id = 1",
            "node_id = 7 OR CAST(node_id AS DECIMAL(20,0)) IS NULL",
            "node_id >= 1 AND node_id < 7",
            "CAST(node_id AS DECIMAL(20,0)) = CAST(CAST(7 AS BIGINT) AS DECIMAL(20,0))",
        ]
        .map(str::to_string),
    );
    for predicate in predicates {
        let sql = format!("SELECT id FROM metadata WHERE {predicate}");
        let state = context.state();
        let plan = state.create_logical_plan(&sql).await.unwrap();
        let analyzed = state
            .analyzer()
            .execute_and_check(plan, state.config_options(), |_, _| {})
            .unwrap();
        let mut filters = Vec::new();
        analyzed
            .apply(|plan| {
                if let LogicalPlan::Filter(filter) = plan {
                    filters.push(filter.predicate.clone());
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .unwrap();
        assert_eq!(filters.len(), 1, "{sql}");
        let mut has_decimal_cast = false;
        filters[0]
            .apply(|expr| {
                if let Expr::Cast(cast) = expr {
                    has_decimal_cast |= matches!(cast.expr.as_ref(), Expr::Column(_))
                        && cast.field.data_type() == &DataType::Decimal128(20, 0);
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .unwrap();
        assert!(has_decimal_cast, "{sql}: {}", filters[0]);
        let filter = MetadataFilter::new_with_column_type(&filters, &|name| {
            (name == "node_id").then_some(DataType::UInt64)
        });
        let expected = context
            .sql(&sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>();
        let default_filter = MetadataFilter::new(&filters);
        for (id, value) in values.into_iter().enumerate() {
            // Same analyzed expression and candidate; only typed normalization changes.
            assert!(
                default_filter.matches(|_| Some(ScalarValue::UInt64(value))),
                "{sql}, value {value:?}"
            );
            assert_eq!(
                filter.matches(|_| Some(ScalarValue::UInt64(value))),
                expected.contains(&(id as i32)),
                "{sql}, value {value:?}, analyzed {}",
                filters[0]
            );
        }
    }
}
