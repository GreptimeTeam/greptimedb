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
use datafusion::logical_expr::{BinaryExpr, Expr, Literal, Operator, col, lit};
use datafusion::physical_expr::create_physical_expr;
use datafusion_common::arrow::array::ArrayRef;
use datafusion_common::arrow::buffer::BooleanBuffer;
use datafusion_common::cast::as_boolean_array;
use datafusion_common::{Column, DFSchema, ScalarValue};
use datatypes::arrow::array::{Array, DictionaryArray, RecordBatch};
use datatypes::arrow::datatypes::{DataType, Field, Schema, TimeUnit, UInt32Type};
use datatypes::value::Value;
use regex::Regex;

use crate::filter::predicate::{SimplePredicate, regexp_is_match_dictionary};
use crate::filter::{SimpleFilterEvaluator, batch_filter, boolean_array_to_scan_mask};

#[test]
fn unsupported_filter_op() {
    // `+` is not supported
    let expr = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(Expr::Column(Column::from_name("foo"))),
        op: Operator::Plus,
        right: Box::new(1.lit()),
    });
    assert!(SimpleFilterEvaluator::try_new(&expr).is_none());

    // two literal is not supported
    let expr = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(1.lit()),
        op: Operator::Eq,
        right: Box::new(1.lit()),
    });
    assert!(SimpleFilterEvaluator::try_new(&expr).is_none());

    // two column is not supported
    let expr = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(Expr::Column(Column::from_name("foo"))),
        op: Operator::Eq,
        right: Box::new(Expr::Column(Column::from_name("bar"))),
    });
    assert!(SimpleFilterEvaluator::try_new(&expr).is_none());

    // compound expr is not supported
    let expr = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("foo"))),
            op: Operator::Eq,
            right: Box::new(1.lit()),
        })),
        op: Operator::Eq,
        right: Box::new(1.lit()),
    });
    assert!(SimpleFilterEvaluator::try_new(&expr).is_none());
    for expr in [
        col("other").is_null().or(col("label").not_eq(lit("lo"))),
        col("a.label")
            .is_null()
            .or(col("b.label").not_eq(lit("lo"))),
        col("label").is_null().or(col("label").eq(col("other"))),
        col("label")
            .is_null()
            .or(Expr::Cast(datafusion::logical_expr::Cast::new(
                Box::new(col("label")),
                DataType::Int64,
            ))
            .eq(lit("lo"))),
    ] {
        assert!(SimpleFilterEvaluator::try_new(&expr).is_none(), "{expr}");
    }
}

#[test]
fn string_cast_requires_matching_column_type() {
    let cast = Expr::Cast(datafusion::logical_expr::Cast::new(
        Box::new(col("label")),
        DataType::Utf8,
    ));
    let expr = col("label").is_null().or(cast.not_eq(lit("lo")));
    for data_type in [
        DataType::Int64,
        DataType::Timestamp(TimeUnit::Millisecond, None),
        DataType::Binary,
    ] {
        assert!(
            SimpleFilterEvaluator::try_new_with_column_type(&expr, &|_| Some(data_type.clone()))
                .is_none()
        );
    }
    assert!(SimpleFilterEvaluator::try_new(&expr).is_none());
}

#[tokio::test]
async fn single_column_predicates_match_datafusion() {
    use datafusion::prelude::SessionContext;
    use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
    use datatypes::arrow::array::{Int32Array, StringArray, UInt32Array};

    let values = vec![
        None,
        Some(""),
        Some("lo"),
        Some("eth0"),
        Some("a\nb"),
        Some("服务"),
        None,
    ];
    let strings: ArrayRef = Arc::new(StringArray::from(values));
    let dictionary: ArrayRef = Arc::new(DictionaryArray::<UInt32Type>::new(
        UInt32Array::from(vec![
            None,
            Some(0),
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
        ]),
        Arc::new(StringArray::from(vec![
            Some(""),
            Some("lo"),
            Some("eth0"),
            Some("a\nb"),
            Some("服务"),
            None,
        ])),
    ));
    let null = || lit(ScalarValue::Utf8(None));
    let mut predicates = vec![
        col("label").is_null(),
        col("label").is_not_null(),
        col("label")
            .is_null()
            .or(col("label").eq(lit("")))
            .and(col("label").is_not_null()),
        col("label")
            .not_eq(lit("lo"))
            .and(col("label").not_eq(lit("eth0"))),
        col("label")
            .eq(lit("lo"))
            .or(col("label").eq(lit("eth0")))
            .or(col("label").eq(lit(""))),
        col("label").in_list(vec![lit(""), lit("lo"), null()], false),
        col("label").in_list(vec![lit(""), lit("lo")], true),
        col("label").in_list(vec![lit("lo"), null()], true),
        col("label")
            .is_null()
            .or(col("label").in_list(vec![lit("lo"), null()], true)),
        col("label").eq(null()).or(col("label").eq(lit("eth0"))),
        col("label").gt(lit("a")).and(col("label").lt(lit("z"))),
        lit("lo").not_eq(col("label")),
    ];
    for (op, literal) in [
        (Operator::Eq, ""),
        (Operator::NotEq, "lo"),
        (Operator::RegexNotMatch, "^(?:lo|vfat)$"),
        (Operator::RegexMatch, "^(?:|eth0)$"),
    ] {
        let comparison = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("label")),
            op,
            Box::new(lit(literal)),
        ));
        predicates.extend([
            col("label").is_null().or(comparison.clone()),
            comparison.or(col("label").is_null()),
        ]);
    }
    for input in [strings, dictionary] {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("label", input.data_type().clone(), true),
            Field::new("id", DataType::Int32, false),
        ]));
        ctx.register_batch(
            "labels",
            RecordBatch::try_new(
                schema,
                vec![
                    input.clone(),
                    Arc::new(Int32Array::from_iter_values(0..input.len() as i32)),
                ],
            )
            .unwrap(),
        )
        .unwrap();
        for expr in &predicates {
            let df = ctx
                .table("labels")
                .await
                .unwrap()
                .filter(expr.clone())
                .unwrap();
            let plan = df.clone().into_optimized_plan().unwrap();
            let expected = df
                .collect()
                .await
                .unwrap()
                .iter()
                .flat_map(|batch| {
                    batch
                        .column_by_name("id")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect::<Vec<_>>();
            let evaluate = |expr: &Expr| {
                let evaluator = SimpleFilterEvaluator::try_new_with_column_type(expr, &|name| {
                    (name == "label").then(|| input.data_type().clone())
                })
                .unwrap_or_else(|| panic!("unsupported: {expr}"));
                let mask = evaluator.evaluate_array(&input).unwrap();
                for (i, matched) in mask.iter().enumerate() {
                    let scalar = ScalarValue::try_from_array(&input, i).unwrap();
                    assert_eq!(
                        evaluator.evaluate_scalar(&scalar).unwrap(),
                        matched,
                        "{expr}, row={i}"
                    );
                }
                mask
            };
            let selected = |mask: BooleanBuffer| {
                mask.iter()
                    .enumerate()
                    .filter_map(|(i, matched)| matched.then_some(i as i32))
                    .collect::<Vec<_>>()
            };
            assert_eq!(selected(evaluate(expr)), expected, "{expr}");
            let mut filters = Vec::new();
            plan.apply(|plan| {
                if let datafusion::logical_expr::LogicalPlan::Filter(filter) = plan {
                    filters.push(filter.predicate.clone());
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .unwrap();
            if !filters.is_empty() {
                let mut mask = BooleanBuffer::new_set(input.len());
                for filter in filters {
                    mask = &mask & &evaluate(&filter);
                }
                assert_eq!(selected(mask), expected, "optimized: {plan}");
            }
        }
    }
}

#[test]
fn float_membership_matches_datafusion() {
    use datatypes::arrow::array::Float64Array;

    let input: ArrayRef = Arc::new(Float64Array::from(vec![
        None,
        Some(-0.0),
        Some(0.0),
        Some(f64::NAN),
        Some(-f64::NAN),
        Some(f64::NEG_INFINITY),
        Some(f64::INFINITY),
        Some(1.0),
    ]));
    let schema = Schema::new(vec![Field::new("value", DataType::Float64, true)]);
    let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![input.clone()]).unwrap();
    for literals in [
        vec![lit(0.0), lit(f64::NAN)],
        vec![lit(-0.0)],
        vec![lit(f64::NAN), lit(ScalarValue::Float64(None))],
    ] {
        for negated in [false, true] {
            let membership = col("value").in_list(literals.clone(), negated);
            for expr in [membership.clone(), col("value").is_null().or(membership)] {
                let physical = create_physical_expr(
                    &expr,
                    &DFSchema::try_from(schema.clone()).unwrap(),
                    &ExecutionProps::new(),
                    &PhysicalPlanningContext::default(),
                )
                .unwrap();
                let result = physical
                    .evaluate(&batch)
                    .unwrap()
                    .into_array(input.len())
                    .unwrap();
                let expected = boolean_array_to_scan_mask(as_boolean_array(&result).unwrap());
                let evaluator = SimpleFilterEvaluator::try_new(&expr).unwrap();
                assert_eq!(
                    evaluator.evaluate_array(&input).unwrap(),
                    *expected.values(),
                    "{expr}"
                );
                for i in 0..input.len() {
                    let scalar = ScalarValue::try_from_array(&input, i).unwrap();
                    assert_eq!(
                        evaluator.evaluate_scalar(&scalar).unwrap(),
                        expected.value(i),
                        "{expr}, row={i}"
                    );
                }
            }
        }
    }
}

#[test]
fn supported_filter_op() {
    // equal
    let expr = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(Expr::Column(Column::from_name("foo"))),
        op: Operator::Eq,
        right: Box::new(1.lit()),
    });
    let _ = SimpleFilterEvaluator::try_new(&expr).unwrap();

    // swap operands
    let expr = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(1.lit()),
        op: Operator::Lt,
        right: Box::new(Expr::Column(Column::from_name("foo"))),
    });
    let evaluator = SimpleFilterEvaluator::try_new(&expr).unwrap();
    assert!(evaluator.is_gt());
    assert_eq!(evaluator.column_name, "foo".to_string());
}

#[test]
fn run_on_array() {
    let expr = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(Expr::Column(Column::from_name("foo"))),
        op: Operator::Eq,
        right: Box::new(1i64.lit()),
    });
    let evaluator = SimpleFilterEvaluator::try_new(&expr).unwrap();

    let input_1 = Arc::new(datatypes::arrow::array::Int64Array::from(vec![1, 2, 3])) as _;
    let result = evaluator.evaluate_array(&input_1).unwrap();
    assert_eq!(result, BooleanBuffer::from(vec![true, false, false]));

    let input_2 = Arc::new(datatypes::arrow::array::Int64Array::from(vec![1, 1, 1])) as _;
    let result = evaluator.evaluate_array(&input_2).unwrap();
    assert_eq!(result, BooleanBuffer::from(vec![true, true, true]));

    let input_3 = Arc::new(datatypes::arrow::array::Int64Array::new_null(0)) as _;
    let result = evaluator.evaluate_array(&input_3).unwrap();
    assert_eq!(result, BooleanBuffer::from(vec![]));
}

#[test]
fn run_on_scalar() {
    let expr = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(Expr::Column(Column::from_name("foo"))),
        op: Operator::Lt,
        right: Box::new(1i64.lit()),
    });
    let evaluator = SimpleFilterEvaluator::try_new(&expr).unwrap();

    let input_1 = ScalarValue::Int64(Some(1));
    let result = evaluator.evaluate_scalar(&input_1).unwrap();
    assert!(!result);

    let input_2 = ScalarValue::Int64(Some(0));
    let result = evaluator.evaluate_scalar(&input_2).unwrap();
    assert!(result);

    let input_3 = ScalarValue::Int64(None);
    let result = evaluator.evaluate_scalar(&input_3).unwrap();
    assert!(!result);
}

#[test]
fn test_complex_filter_expression() {
    // Create an expression tree for: col = 'B' OR col = 'C' OR col = 'D'
    let col_eq_b = col("col").eq(lit("B"));
    let col_eq_c = col("col").eq(lit("C"));
    let col_eq_d = col("col").eq(lit("D"));

    // Build the OR chain
    let col_or_expr = col_eq_b.or(col_eq_c).or(col_eq_d);

    // Check that SimpleFilterEvaluator can handle OR chain
    let or_evaluator = SimpleFilterEvaluator::try_new(&col_or_expr).unwrap();
    assert_eq!(or_evaluator.column_name, "col");
    assert!(or_evaluator.is_or_eq_chain());
    assert_eq!(
        or_evaluator.literal_list_values().unwrap(),
        vec![Value::from("B"), Value::from("C"), Value::from("D")]
    );

    // Create a schema and batch for testing
    let schema = Schema::new(vec![Field::new("col", DataType::Utf8, false)]);
    let df_schema = DFSchema::try_from(schema.clone()).unwrap();
    let props = ExecutionProps::new();
    let physical_expr = create_physical_expr(
        &col_or_expr,
        &df_schema,
        &props,
        &PhysicalPlanningContext::default(),
    )
    .unwrap();

    // Create test data
    let col_data = Arc::new(datatypes::arrow::array::StringArray::from(vec![
        "B", "C", "E", "B", "C", "D", "F",
    ]));
    let batch = RecordBatch::try_new(Arc::new(schema), vec![col_data]).unwrap();
    let expected = datatypes::arrow::array::StringArray::from(vec!["B", "C", "B", "C", "D"]);

    // Filter the batch
    let filtered_batch = batch_filter(&batch, &physical_expr).unwrap();

    // Expected: rows with col in ("B", "C", "D")
    // That would be rows 0, 1, 3, 4, 5
    assert_eq!(filtered_batch.num_rows(), 5);

    let col_filtered = filtered_batch
        .column(0)
        .as_any()
        .downcast_ref::<datatypes::arrow::array::StringArray>()
        .unwrap();
    assert_eq!(col_filtered, &expected);
}

#[test]
fn test_maybe_build_regex() {
    // Test case for RegexMatch (case sensitive, non-negative)
    let (regex, negative) = SimplePredicate::maybe_build_regex(
        Operator::RegexMatch,
        &ScalarValue::Utf8(Some("a.*b".to_string())),
    )
    .unwrap();
    assert!(regex.is_some());
    assert!(!negative);
    assert!(regex.unwrap().is_match("axxb"));

    // Test case for RegexIMatch (case insensitive, non-negative)
    let (regex, negative) = SimplePredicate::maybe_build_regex(
        Operator::RegexIMatch,
        &ScalarValue::Utf8(Some("a.*b".to_string())),
    )
    .unwrap();
    assert!(regex.is_some());
    assert!(!negative);
    assert!(regex.unwrap().is_match("AxxB"));

    // Test case for RegexNotMatch (case sensitive, negative)
    let (regex, negative) = SimplePredicate::maybe_build_regex(
        Operator::RegexNotMatch,
        &ScalarValue::Utf8(Some("a.*b".to_string())),
    )
    .unwrap();
    assert!(regex.is_some());
    assert!(negative);

    // Test case for RegexNotIMatch (case insensitive, negative)
    let (regex, negative) = SimplePredicate::maybe_build_regex(
        Operator::RegexNotIMatch,
        &ScalarValue::Utf8(Some("a.*b".to_string())),
    )
    .unwrap();
    assert!(regex.is_some());
    assert!(negative);

    // Test with empty regex pattern
    let (regex, negative) = SimplePredicate::maybe_build_regex(
        Operator::RegexMatch,
        &ScalarValue::Utf8(Some("".to_string())),
    )
    .unwrap();
    assert!(regex.is_none());
    assert!(!negative);

    // Test with non-regex operator
    let (regex, negative) = SimplePredicate::maybe_build_regex(
        Operator::Eq,
        &ScalarValue::Utf8(Some("a.*b".to_string())),
    )
    .unwrap();
    assert!(regex.is_none());
    assert!(!negative);

    // Test with invalid regex pattern
    let result = SimplePredicate::maybe_build_regex(
        Operator::RegexMatch,
        &ScalarValue::Utf8(Some("a(b".to_string())),
    );
    assert!(result.is_err());

    // Test with non-string value
    let result =
        SimplePredicate::maybe_build_regex(Operator::RegexMatch, &ScalarValue::Int64(Some(123)));
    assert!(result.is_err());

    // Test with null value
    let result = SimplePredicate::maybe_build_regex(Operator::RegexMatch, &ScalarValue::Utf8(None));
    assert!(result.is_err());
}

#[test]
fn test_regex_match_dictionary_array() {
    use datatypes::arrow::array::StringDictionaryBuilder;

    // Create a StringDictionaryArray
    let mut builder = StringDictionaryBuilder::<UInt32Type>::new();
    builder.append("apple").unwrap();
    builder.append("banana").unwrap();
    builder.append("apple").unwrap();
    builder.append("cherry").unwrap();
    let dict_array = builder.finish();

    // Test regex that matches "apple"
    let regex = regex::Regex::new(r"app.*").unwrap();
    let result = regexp_is_match_dictionary(&dict_array, Some(&regex)).unwrap();

    // Should match indices 0 and 2 (both "apple")
    assert_eq!(result.len(), 4);
    assert!(result.value(0)); // "apple"
    assert!(!result.value(1)); // "banana"
    assert!(result.value(2)); // "apple"
    assert!(!result.value(3)); // "cherry"

    // Test regex that matches "banana"
    let regex2 = regex::Regex::new(r"ban.*").unwrap();
    let result2 = regexp_is_match_dictionary(&dict_array, Some(&regex2)).unwrap();

    assert!(!result2.value(0)); // "apple"
    assert!(result2.value(1)); // "banana"
    assert!(!result2.value(2)); // "apple"
    assert!(!result2.value(3)); // "cherry"

    // Test with no regex (should match all)
    let result3 = regexp_is_match_dictionary(&dict_array, None).unwrap();
    assert!(result3.value(0));
    assert!(result3.value(1));
    assert!(result3.value(2));
    assert!(result3.value(3));
}

#[test]
fn test_regex_scan_masks_preserve_sql_null_semantics() {
    let plain = Arc::new(datatypes::arrow::array::StringArray::from(vec![
        Some("api"),
        Some("API"),
        Some("db"),
        None,
    ])) as ArrayRef;
    assert_regex_scan_masks(
        &plain,
        [
            vec![true, false, false, false],
            vec![true, true, false, false],
            vec![false, true, true, false],
            vec![false, false, true, false],
        ],
    );

    let dictionary = DictionaryArray::new(
        datatypes::arrow::array::UInt32Array::from(vec![Some(0), Some(1), Some(2), None, Some(3)]),
        Arc::new(datatypes::arrow::array::StringArray::from(vec![
            Some("api"),
            Some("API"),
            Some("db"),
            None,
        ])),
    );
    let raw = regexp_is_match_dictionary(&dictionary, Some(&Regex::new("^api$").unwrap())).unwrap();
    assert!(raw.is_null(3)); // null dictionary key
    assert!(raw.is_null(4)); // non-null key referencing a null dictionary value

    let dictionary = Arc::new(dictionary) as ArrayRef;
    assert_regex_scan_masks(
        &dictionary,
        [
            vec![true, false, false, false, false],
            vec![true, true, false, false, false],
            vec![false, true, true, false, false],
            vec![false, false, true, false, false],
        ],
    );

    let negative = regex_evaluator(Operator::RegexNotMatch);
    assert!(!negative.evaluate_scalar(&ScalarValue::Utf8(None)).unwrap());
}

fn assert_regex_scan_masks(input: &ArrayRef, expected: [Vec<bool>; 4]) {
    for (op, expected) in [
        Operator::RegexMatch,
        Operator::RegexIMatch,
        Operator::RegexNotMatch,
        Operator::RegexNotIMatch,
    ]
    .into_iter()
    .zip(expected)
    {
        assert_eq!(
            BooleanBuffer::from(expected),
            regex_evaluator(op).evaluate_array(input).unwrap(),
            "{op:?}"
        );
    }
}

fn regex_evaluator(op: Operator) -> SimpleFilterEvaluator {
    let expr = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(Expr::Column(Column::from_name("host"))),
        op,
        right: Box::new("^api$".lit()),
    });
    SimpleFilterEvaluator::try_new(&expr).unwrap()
}
