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

//! Timestamp-unit adaptation for predicates evaluated against historical SSTs.

use common_time::timestamp::div_mod_units;
use datafusion::logical_expr::Operator;
use datafusion_common::ScalarValue;
use datafusion_common::arrow::array::{ArrayRef, Datum, Scalar};
use datatypes::arrow::datatypes::TimeUnit;

use crate::filter::predicate::SimplePredicate;

/// Builds a tz-naive timestamp literal for `value` in `unit`.
pub fn timestamp_scalar_value(value: i64, unit: TimeUnit) -> ScalarValue {
    match unit {
        TimeUnit::Second => ScalarValue::TimestampSecond(Some(value), None),
        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(value), None),
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(value), None),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(value), None),
    }
}

impl SimplePredicate {
    pub(crate) fn cast_timestamp_unit(&self, target_unit: TimeUnit) -> Option<Self> {
        // Constants represent scan outcomes here. As in the public Matched/Pruned
        // result, UNKNOWN can be collapsed to false under these AND/OR-only trees.
        match self {
            Self::And(left, right) | Self::Or(left, right) => {
                let left = left.cast_timestamp_unit(target_unit)?;
                let right = right.cast_timestamp_unit(target_unit)?;
                let and = matches!(self, Self::And(..));
                Some(match (left, right) {
                    (Self::Constant(Some(false)), _) | (_, Self::Constant(Some(false))) if and => {
                        Self::Constant(Some(false))
                    }
                    (Self::Constant(Some(true)), _) | (_, Self::Constant(Some(true))) if !and => {
                        Self::Constant(Some(true))
                    }
                    (Self::Constant(Some(true)), other) | (other, Self::Constant(Some(true)))
                        if and =>
                    {
                        other
                    }
                    (Self::Constant(Some(false)), other) | (other, Self::Constant(Some(false)))
                        if !and =>
                    {
                        other
                    }
                    (left, right) => {
                        if and {
                            Self::And(Box::new(left), Box::new(right))
                        } else {
                            Self::Or(Box::new(left), Box::new(right))
                        }
                    }
                })
            }
            Self::IsNull { .. } => Some(self.clone()),
            Self::Constant(value) => Some(Self::Constant(Some(value.unwrap_or(false)))),
            Self::InList { literals, negated } => {
                let mut converted = Vec::with_capacity(literals.len());
                for literal in literals {
                    let scalar = ScalarValue::try_from_array(literal.get().0, 0).ok()?;
                    let (value, unit) = timestamp_scalar_parts(&scalar)?;
                    let Some(value) = value else {
                        if *negated {
                            return Some(Self::Constant(Some(false)));
                        }
                        continue;
                    };
                    let cast = div_mod_units(value, unit.into(), target_unit.into())?;
                    if cast.remainder == 0 {
                        converted.push(timestamp_scalar(cast.quotient, target_unit)?);
                    }
                }
                if converted.is_empty() {
                    return Some(Self::Constant(Some(*negated)));
                }
                Some(Self::InList {
                    literals: converted,
                    negated: *negated,
                })
            }
            Self::Comparison { literal, op, .. } => {
                let scalar = ScalarValue::try_from_array(literal.get().0, 0).ok()?;
                let (value, unit) = timestamp_scalar_parts(&scalar)?;
                let Some(value) = value else {
                    return Some(Self::Constant(Some(false)));
                };
                if unit == target_unit {
                    return Some(self.clone());
                }
                let cast = div_mod_units(value, unit.into(), target_unit.into())?;
                let divisible = cast.remainder == 0;
                let literal = timestamp_scalar(cast.quotient, target_unit)?;
                let filter = |op| Self::Comparison {
                    literal: literal.clone(),
                    op,
                    regex: None,
                    regex_negative: false,
                };
                Some(match op {
                    Operator::Eq if divisible => filter(Operator::Eq),
                    Operator::Eq => Self::Constant(Some(false)),
                    Operator::NotEq if divisible => filter(Operator::NotEq),
                    Operator::NotEq => Self::Constant(Some(true)),
                    // The quotient is floor(L), including negative timestamps.
                    // v > L is v > quotient whether or not L is representable.
                    Operator::Gt => filter(Operator::Gt),
                    Operator::GtEq if divisible => filter(Operator::GtEq),
                    // Between target values, v >= L is v > quotient.
                    Operator::GtEq => filter(Operator::Gt),
                    Operator::Lt if divisible => filter(Operator::Lt),
                    // Between target values, v < L is v <= quotient.
                    Operator::Lt => filter(Operator::LtEq),
                    Operator::LtEq => filter(Operator::LtEq),
                    _ => return None,
                })
            }
        }
    }
}

/// Extracts the value and unit from a tz-naive timestamp scalar.
fn timestamp_scalar_parts(scalar: &ScalarValue) -> Option<(Option<i64>, TimeUnit)> {
    let (value, unit, timezone) = match scalar {
        ScalarValue::TimestampSecond(v, tz) => (*v, TimeUnit::Second, tz),
        ScalarValue::TimestampMillisecond(v, tz) => (*v, TimeUnit::Millisecond, tz),
        ScalarValue::TimestampMicrosecond(v, tz) => (*v, TimeUnit::Microsecond, tz),
        ScalarValue::TimestampNanosecond(v, tz) => (*v, TimeUnit::Nanosecond, tz),
        _ => return None,
    };
    // A timezone-aware literal doesn't compare against a tz-naive column;
    // leave it to the caller instead of guessing the intended semantics.
    (timezone.is_none()).then_some((value, unit))
}

/// Builds a one-element scalar array holding `value` in `unit`.
fn timestamp_scalar(value: i64, unit: TimeUnit) -> Option<Scalar<ArrayRef>> {
    timestamp_scalar_value(value, unit).to_scalar().ok()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::execution::context::ExecutionProps;
    use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
    use datafusion::logical_expr::{Expr, col, lit};
    use datafusion::physical_expr::create_physical_expr;
    use datafusion_common::arrow::array::ArrayRef;
    use datafusion_common::arrow::buffer::BooleanBuffer;
    use datafusion_common::cast::as_boolean_array;
    use datafusion_common::{DFSchema, ScalarValue};
    use datatypes::arrow::array::{
        RecordBatch, TimestampMillisecondArray, TimestampNanosecondArray,
    };
    use datatypes::arrow::datatypes::{Field, Schema};
    use datatypes::data_type::ConcreteDataType;

    use crate::filter::{SimpleFilterEvaluator, TimestampUnitCast, boolean_array_to_scan_mask};

    #[test]
    fn compound_timestamp_filters_preserve_unit_conversion() {
        use datatypes::arrow::array::TimestampMicrosecondArray;

        let null = || lit(ScalarValue::TimestampMicrosecond(None, None));
        let predicates = [
            col("ts")
                .gt(lit(ts_us(-1500)))
                .and(col("ts").lt(lit(ts_us(500)))),
            col("ts")
                .eq(lit(ts_us(500)))
                .or(col("ts").gt_eq(lit(ts_us(1500)))),
            col("ts").in_list(vec![lit(ts_us(500)), lit(ts_us(1000)), null()], false),
            col("ts").in_list(vec![lit(ts_us(500)), lit(ts_us(1000))], true),
            col("ts").in_list(vec![lit(ts_us(500)), null()], true),
            col("ts")
                .is_null()
                .or(col("ts").in_list(vec![lit(ts_us(500)), null()], true)),
            col("ts")
                .is_not_null()
                .and(col("ts").not_eq(lit(ts_us(500)))),
            col("ts").eq(null()).or(col("ts").eq(lit(ts_us(-1000)))),
            col("ts")
                .eq(lit(ts_us(500)))
                .and(col("ts").gt(lit(ts_us(0)))),
            col("ts")
                .not_eq(lit(ts_us(500)))
                .or(col("ts").lt(lit(ts_us(0)))),
        ];
        let native: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
            -2000, -1000, 0, 1000, 2000,
        ]));
        let old: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![-2, -1, 0, 1, 2]));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            native.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![native]).unwrap();
        for expr in predicates {
            let physical = create_physical_expr(
                &expr,
                &DFSchema::try_from(schema.as_ref().clone()).unwrap(),
                &ExecutionProps::new(),
                &PhysicalPlanningContext::default(),
            )
            .unwrap();
            let result = physical
                .evaluate(&batch)
                .unwrap()
                .into_array(batch.num_rows())
                .unwrap();
            let expected = boolean_array_to_scan_mask(as_boolean_array(&result).unwrap())
                .values()
                .clone();
            let actual = match cast_to_ms(&expr).unwrap() {
                TimestampUnitCast::Matched => BooleanBuffer::new_set(old.len()),
                TimestampUnitCast::Pruned => BooleanBuffer::new_unset(old.len()),
                TimestampUnitCast::Filter(filter) => filter.evaluate_array(&old).unwrap(),
            };
            assert_eq!(actual, expected, "{expr}");
        }
    }

    fn ts_us(v: i64) -> ScalarValue {
        ScalarValue::TimestampMicrosecond(Some(v), None)
    }

    fn cast_to_ms(expr: &Expr) -> Option<TimestampUnitCast> {
        SimpleFilterEvaluator::try_new(expr)
            .unwrap()
            .cast_timestamp_unit(&ConcreteDataType::timestamp_millisecond_datatype())
    }

    /// Evaluates a cast `Filter` outcome against millisecond values and
    /// returns the mask.
    fn eval_ms_mask(cast: Option<TimestampUnitCast>, values: &[i64]) -> Vec<bool> {
        let filter = match cast.expect("cast must apply") {
            TimestampUnitCast::Filter(filter) => filter,
            other => panic!("expected Filter outcome, got {other:?}"),
        };
        let array = Arc::new(TimestampMillisecondArray::from(values.to_vec())) as ArrayRef;
        filter.evaluate_array(&array).unwrap().iter().collect()
    }

    #[test]
    fn cast_timestamp_unit_converts_representable_literal() {
        // ts = 7_000_000us evaluated against a ms column becomes ts = 7000ms.
        assert_eq!(
            vec![false, true, false],
            eval_ms_mask(
                cast_to_ms(&col("ts").eq(lit(ts_us(7_000_000)))),
                &[6_999, 7_000, 7_001]
            )
        );
        // != complements =.
        assert_eq!(
            vec![true, false, true],
            eval_ms_mask(
                cast_to_ms(&col("ts").not_eq(lit(ts_us(7_000_000)))),
                &[6_999, 7_000, 7_001]
            )
        );
    }

    #[test]
    fn cast_timestamp_unit_prunes_non_representable_equality() {
        // 7_000_500us is not a whole millisecond: no ms row can equal it,
        // and every ms row satisfies `!=`.
        assert!(matches!(
            cast_to_ms(&col("ts").eq(lit(ts_us(7_000_500)))),
            Some(TimestampUnitCast::Pruned)
        ));
        assert!(matches!(
            cast_to_ms(&col("ts").not_eq(lit(ts_us(7_000_500)))),
            Some(TimestampUnitCast::Matched)
        ));
    }

    #[test]
    fn cast_timestamp_unit_strengthens_inequalities() {
        // 2_500_500us is strictly between 2500ms and 2501ms: the cast must
        // not round the literal to a boundary the original predicate
        // excludes (>= 2500ms would wrongly match 2500ms).
        let values = vec![2_499, 2_500, 2_501];
        assert_eq!(
            vec![false, false, true],
            eval_ms_mask(cast_to_ms(&col("ts").gt(lit(ts_us(2_500_500)))), &values)
        );
        assert_eq!(
            vec![false, false, true],
            eval_ms_mask(cast_to_ms(&col("ts").gt_eq(lit(ts_us(2_500_500)))), &values)
        );
        assert_eq!(
            vec![true, true, false],
            eval_ms_mask(cast_to_ms(&col("ts").lt(lit(ts_us(2_500_500)))), &values)
        );
        assert_eq!(
            vec![true, true, false],
            eval_ms_mask(cast_to_ms(&col("ts").lt_eq(lit(ts_us(2_500_500)))), &values)
        );

        // Representable boundary keeps the original operator.
        let values = vec![2_499, 2_500, 2_501];
        assert_eq!(
            vec![false, false, true],
            eval_ms_mask(cast_to_ms(&col("ts").gt(lit(ts_us(2_500_000)))), &values)
        );
        assert_eq!(
            vec![false, true, true],
            eval_ms_mask(cast_to_ms(&col("ts").gt_eq(lit(ts_us(2_500_000)))), &values)
        );
        assert_eq!(
            vec![true, false, false],
            eval_ms_mask(cast_to_ms(&col("ts").lt(lit(ts_us(2_500_000)))), &values)
        );
        assert_eq!(
            vec![true, true, false],
            eval_ms_mask(cast_to_ms(&col("ts").lt_eq(lit(ts_us(2_500_000)))), &values)
        );
    }

    #[test]
    fn cast_timestamp_unit_handles_negative_instants() {
        // -2_500_500us floors to -2501ms with remainder 500us: only rows
        // with instant >= -2.5005ms may match >= / >.
        let values = vec![-2_502, -2_501, -2_500];
        assert_eq!(
            vec![false, false, true],
            eval_ms_mask(
                cast_to_ms(&col("ts").gt_eq(lit(ts_us(-2_500_500)))),
                &values
            )
        );
        assert_eq!(
            vec![false, false, true],
            eval_ms_mask(cast_to_ms(&col("ts").gt(lit(ts_us(-2_500_500)))), &values)
        );
        assert_eq!(
            vec![true, true, false],
            eval_ms_mask(
                cast_to_ms(&col("ts").lt_eq(lit(ts_us(-2_500_500)))),
                &values
            )
        );
        // Exactly representable negative literal: -2_500_000us == -2500ms.
        assert_eq!(
            vec![false, false, true],
            eval_ms_mask(cast_to_ms(&col("ts").eq(lit(ts_us(-2_500_000)))), &values)
        );
    }

    #[test]
    fn cast_timestamp_unit_or_chain_drops_unrepresentable_literals() {
        // Only 7_000_000us is a whole millisecond; 7_000_500us cannot match.
        let expr = col("ts")
            .eq(lit(ts_us(7_000_500)))
            .or(col("ts").eq(lit(ts_us(7_000_000))));
        assert_eq!(
            vec![false, true],
            eval_ms_mask(cast_to_ms(&expr), &[6_999, 7_000])
        );

        // A chain where no literal is representable matches nothing.
        let expr = col("ts")
            .eq(lit(ts_us(7_000_500)))
            .or(col("ts").eq(lit(ts_us(6_000_500))));
        assert!(matches!(cast_to_ms(&expr), Some(TimestampUnitCast::Pruned)));
    }

    #[test]
    fn cast_timestamp_unit_null_literal_prunes() {
        // A NULL literal never compares true.
        assert!(matches!(
            cast_to_ms(&col("ts").eq(lit(ScalarValue::TimestampMicrosecond(None, None)))),
            Some(TimestampUnitCast::Pruned)
        ));
    }

    #[test]
    fn cast_timestamp_unit_same_unit_returns_filter_directly() {
        // A literal already in the target unit is returned unchanged.
        let expr = col("ts").eq(lit(ts_us(7_000_000)));
        let filter = SimpleFilterEvaluator::try_new(&expr).unwrap();
        let cast = filter
            .cast_timestamp_unit(&ConcreteDataType::timestamp_microsecond_datatype())
            .unwrap();
        let TimestampUnitCast::Filter(f) = cast else {
            panic!("expected Filter, got {cast:?}")
        };
        assert!(filter.is_eq() && f.is_eq());
        assert_eq!(filter.column_name(), f.column_name());
        assert_eq!(filter.literal_value(), f.literal_value());
    }

    #[test]
    fn cast_timestamp_unit_rejects_non_timestamps() {
        // Non-timestamp target type.
        assert!(
            SimpleFilterEvaluator::try_new(&col("ts").gt(lit(ts_us(1))))
                .unwrap()
                .cast_timestamp_unit(&ConcreteDataType::int64_datatype())
                .is_none()
        );
        // Non-timestamp literal against a timestamp target.
        assert!(cast_to_ms(&col("ts").gt(lit(42_i64))).is_none());
    }

    #[test]
    fn cast_timestamp_unit_to_finer_unit() {
        // 5 seconds against a nanosecond column becomes 5_000_000_000ns.
        let filter = SimpleFilterEvaluator::try_new(
            &col("ts").eq(lit(ScalarValue::TimestampSecond(Some(5), None))),
        )
        .unwrap()
        .cast_timestamp_unit(&ConcreteDataType::timestamp_nanosecond_datatype())
        .and_then(|cast| match cast {
            TimestampUnitCast::Filter(filter) => Some(filter),
            _ => None,
        })
        .unwrap();
        let array = Arc::new(TimestampNanosecondArray::from(vec![
            4_999_999_999,
            5_000_000_000,
        ])) as ArrayRef;
        assert_eq!(
            vec![false, true],
            filter
                .evaluate_array(&array)
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
        );
    }
}
