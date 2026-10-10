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

//! Preflight validation of persisted partition expressions against an ALTER TABLE schema change.
//!
//! Persisted partition bounds keep the types they were written with. When a column
//! referenced by a partition expression changes its type, the region rewrites the bound
//! literal to the new column type while building the partition predicate on scan. If a
//! bound cannot be represented with the new type (a classic example is a millisecond
//! timestamp bound that overflows when the time index column is widened to nanoseconds),
//! every scan of that region fails at that point. Such an ALTER must be rejected before
//! it is submitted to any region, otherwise already-altered regions keep failing while
//! the rest of the table is untouched.
//!
//! The `partition` crate depends on `common-meta`, so this crate cannot depend on
//! `partition` back and cannot reuse [`partition::expr::PartitionExpr`]. The `Persisted*`
//! types below mirror the JSON representation of `partition::partition::PartitionBound` and
//! `partition::expr::PartitionExpr` and must be kept in sync with `partition/src/partition.rs`
//! and `partition/src/expr.rs`. The compatibility check itself deliberately repeats the
//! same conversion (value -> [`ScalarValue`]) and the same checked cast
//! ([`ScalarValue::cast_to`]) that the region performs when it coerces the persisted bound
//! to the new column type, so a bound that would fail on scan is rejected here as well.

use std::collections::HashMap;

use common_telemetry::warn;
use datafusion_common::ScalarValue;
use datatypes::data_type::{ConcreteDataType, DataType};
use datatypes::schema::Schema;
use datatypes::value::{
    Value, duration_to_scalar_value, time_to_scalar_value, timestamp_to_scalar_value,
};
use serde::Deserialize;
use store_api::storage::RegionId;

use crate::error::{PartitionExprIncompatibleSnafu, Result};
use crate::rpc::router::RegionRoute;

/// Mirrors `partition::partition::PartitionBound`.
#[derive(Debug, Deserialize)]
enum PersistedPartitionBound {
    /// Deprecated since 0.9.0. Only deserialized to keep unknown old formats parseable.
    #[allow(dead_code)]
    Value(Value),
    /// Deprecated since 0.15.0.
    MaxValue,
    Expr(PersistedPartitionExpr),
}

/// Mirrors `partition::expr::PartitionExpr`.
#[derive(Debug, Deserialize)]
struct PersistedPartitionExpr {
    lhs: Box<PersistedOperand>,
    op: PersistedOp,
    rhs: Box<PersistedOperand>,
}

/// Mirrors `partition::expr::Operand`.
#[derive(Debug, Deserialize)]
enum PersistedOperand {
    Column(String),
    Value(Value),
    Expr(PersistedPartitionExpr),
}

/// Mirrors `partition::expr::RestrictedOp`.
#[derive(Debug, Deserialize)]
enum PersistedOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
}

/// Returns the columns whose data type differs between `old_schema` and `new_schema`,
/// mapped to the new type. Columns only present in one of the schemas (added or dropped)
/// are not included; dropping a column referenced by a partition expression is already
/// rejected by the table metadata validation.
pub(crate) fn changed_column_types(
    old_schema: &Schema,
    new_schema: &Schema,
) -> HashMap<String, ConcreteDataType> {
    let mut changed = HashMap::new();
    for new_column in new_schema.column_schemas() {
        let Some(old_column) = old_schema.column_schema_by_name(&new_column.name) else {
            continue;
        };
        if old_column.data_type != new_column.data_type {
            changed.insert(new_column.name.clone(), new_column.data_type.clone());
        }
    }

    changed
}

/// Validates that the persisted partition bounds of every region are still representable
/// with the types of `changed_columns`. Returns an error naming the first offending
/// region without modifying anything.
pub(crate) fn validate_region_partition_bounds(
    changed_columns: &HashMap<String, ConcreteDataType>,
    region_routes: &[RegionRoute],
) -> Result<()> {
    if changed_columns.is_empty() {
        return Ok(());
    }

    for route in region_routes {
        let expr_json = route.region.partition_expr();
        if expr_json.is_empty() {
            continue;
        }

        let bound: PersistedPartitionBound = match serde_json::from_str(&expr_json) {
            Ok(bound) => bound,
            Err(err) => {
                // A malformed persisted expression already fails every scan of this
                // region. It is unrelated to the schema change, so don't block the ALTER.
                warn!(
                    "Skipping partition expression validation for region {}, \
                     the persisted expression is not parseable: {}, expr: {}",
                    route.region.id, err, expr_json
                );
                continue;
            }
        };
        let PersistedPartitionBound::Expr(expr) = bound else {
            // Deprecated bound formats don't carry expressions and are not used
            // when the region builds its partition predicate.
            continue;
        };

        validate_expr(&expr, changed_columns, route.region.id)?;
    }

    Ok(())
}

fn validate_expr(
    expr: &PersistedPartitionExpr,
    changed_columns: &HashMap<String, ConcreteDataType>,
    region_id: RegionId,
) -> Result<()> {
    match expr.op {
        PersistedOp::And | PersistedOp::Or => {
            validate_operand(&expr.lhs, changed_columns, region_id)?;
            validate_operand(&expr.rhs, changed_columns, region_id)?;
        }
        PersistedOp::Eq
        | PersistedOp::NotEq
        | PersistedOp::Lt
        | PersistedOp::LtEq
        | PersistedOp::Gt
        | PersistedOp::GtEq => {
            // Comparison between a column and a bound literal in either order.
            validate_comparison(&expr.lhs, &expr.rhs, changed_columns, region_id)?;
            validate_comparison(&expr.rhs, &expr.lhs, changed_columns, region_id)?;
        }
    }

    Ok(())
}

fn validate_operand(
    operand: &PersistedOperand,
    changed_columns: &HashMap<String, ConcreteDataType>,
    region_id: RegionId,
) -> Result<()> {
    match operand {
        PersistedOperand::Expr(expr) => validate_expr(expr, changed_columns, region_id),
        PersistedOperand::Column(_) | PersistedOperand::Value(_) => Ok(()),
    }
}

fn validate_comparison(
    operand: &PersistedOperand,
    other: &PersistedOperand,
    changed_columns: &HashMap<String, ConcreteDataType>,
    region_id: RegionId,
) -> Result<()> {
    let (PersistedOperand::Column(column), PersistedOperand::Value(value)) = (operand, other)
    else {
        return Ok(());
    };
    let Some(target_type) = changed_columns.get(column) else {
        return Ok(());
    };
    // Values not representable as a partition bound (lists, structs, json) are
    // rejected by the partition parser and cannot appear in a persisted expression.
    let Some(scalar) = value_to_scalar_value(value) else {
        return Ok(());
    };

    let target_arrow_type = target_type.as_arrow_type();
    if scalar.data_type() == target_arrow_type {
        return Ok(());
    }
    if let Err(error) = scalar.cast_to(&target_arrow_type) {
        warn!(
            error;
            "Persisted partition bound is not representable with the altered column type, \
             region_id: {}, column: '{}', target_type: {}",
            region_id,
            column,
            target_type
        );
        return PartitionExprIncompatibleSnafu {
            region_id,
            column: column.clone(),
            target_type: target_type.to_string(),
        }
        .fail();
    }

    Ok(())
}

/// Converts a persisted bound value to a [`ScalarValue`], mirroring
/// `partition::expr::Operand::try_as_logical_expr`. Returns `None` for values that
/// cannot appear in a persisted partition expression.
fn value_to_scalar_value(value: &Value) -> Option<ScalarValue> {
    Some(match value {
        Value::Boolean(v) => ScalarValue::Boolean(Some(*v)),
        Value::UInt8(v) => ScalarValue::UInt8(Some(*v)),
        Value::UInt16(v) => ScalarValue::UInt16(Some(*v)),
        Value::UInt32(v) => ScalarValue::UInt32(Some(*v)),
        Value::UInt64(v) => ScalarValue::UInt64(Some(*v)),
        Value::Int8(v) => ScalarValue::Int8(Some(*v)),
        Value::Int16(v) => ScalarValue::Int16(Some(*v)),
        Value::Int32(v) => ScalarValue::Int32(Some(*v)),
        Value::Int64(v) => ScalarValue::Int64(Some(*v)),
        Value::Float32(v) => ScalarValue::Float32(Some(v.0)),
        Value::Float64(v) => ScalarValue::Float64(Some(v.0)),
        Value::String(v) => ScalarValue::Utf8(Some(v.as_utf8().to_string())),
        Value::Binary(v) => ScalarValue::Binary(Some(v.to_vec())),
        Value::Date(v) => ScalarValue::Date32(Some(v.val())),
        Value::Null => ScalarValue::Null,
        Value::Timestamp(t) => timestamp_to_scalar_value(t.unit(), Some(t.value())),
        Value::Time(t) => time_to_scalar_value(*t.unit(), Some(t.value())).ok()?,
        Value::IntervalYearMonth(v) => ScalarValue::IntervalYearMonth(Some(v.to_i32())),
        Value::IntervalDayTime(v) => ScalarValue::IntervalDayTime(Some((*v).into())),
        Value::IntervalMonthDayNano(v) => ScalarValue::IntervalMonthDayNano(Some((*v).into())),
        Value::Duration(d) => duration_to_scalar_value(d.unit(), Some(d.value())),
        Value::Decimal128(d) => {
            let (v, p, s) = d.to_scalar_value();
            ScalarValue::Decimal128(v, p, s)
        }
        Value::List(_) | Value::Struct(_) | Value::Json(_) => return None,
    })
}

#[cfg(test)]
mod tests {
    use datatypes::schema::ColumnSchema;
    use store_api::storage::RegionId;

    use super::*;
    use crate::rpc::router::Region;

    fn schema(ts_type: ConcreteDataType) -> Schema {
        Schema::new(vec![
            ColumnSchema::new("ts", ts_type, false),
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
        ])
    }

    fn ts_bound(millis: i64) -> String {
        format!(
            r#"{{"Expr":{{"lhs":{{"Column":"ts"}},"op":"Lt","rhs":{{"Value":{{"Timestamp":{{"value":{millis},"unit":"Millisecond"}}}}}}}}}}"#
        )
    }

    fn routes(exprs: &[&str]) -> Vec<RegionRoute> {
        exprs
            .iter()
            .enumerate()
            .map(|(index, expr)| RegionRoute {
                region: Region {
                    id: RegionId::new(1024, index as u32 + 1),
                    partition_expr: expr.to_string(),
                    ..Default::default()
                },
                ..Default::default()
            })
            .collect()
    }

    fn changed(column: &str, data_type: ConcreteDataType) -> HashMap<String, ConcreteDataType> {
        HashMap::from([(column.to_string(), data_type)])
    }

    #[test]
    fn test_changed_column_types() {
        let old = schema(ConcreteDataType::timestamp_millisecond_datatype());
        let new = schema(ConcreteDataType::timestamp_nanosecond_datatype());
        let changed = changed_column_types(&old, &new);
        assert_eq!(changed.len(), 1);
        assert_eq!(
            changed["ts"],
            ConcreteDataType::timestamp_nanosecond_datatype()
        );

        // Added and dropped columns are not type changes.
        let mut new_columns = old.column_schemas().to_vec();
        new_columns.push(ColumnSchema::new(
            "extra",
            ConcreteDataType::int64_datatype(),
            true,
        ));
        let new = Schema::new(new_columns);
        assert!(changed_column_types(&old, &new).is_empty());
    }

    #[test]
    fn test_overflowing_bound_is_rejected() {
        // Year 3000 fits milliseconds but overflows nanoseconds.
        let routes = routes(&[&ts_bound(32_503_680_000_000)]);
        let err = validate_region_partition_bounds(
            &changed("ts", ConcreteDataType::timestamp_nanosecond_datatype()),
            &routes,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("region 4398046511105"),
            "unexpected error: {err}"
        );
        assert!(err.to_string().contains("'ts'"), "unexpected error: {err}");
    }

    #[test]
    fn test_representable_bound_is_accepted() {
        // 2024-01-01T00:00:00Z fits both microseconds and nanoseconds.
        let routes = routes(&[&ts_bound(1_704_067_200_000)]);
        validate_region_partition_bounds(
            &changed("ts", ConcreteDataType::timestamp_nanosecond_datatype()),
            &routes,
        )
        .unwrap();
        validate_region_partition_bounds(
            &changed("ts", ConcreteDataType::timestamp_microsecond_datatype()),
            &routes,
        )
        .unwrap();
    }

    #[test]
    fn test_later_region_rejection_names_the_region() {
        let routes = routes(&[&ts_bound(1_704_067_200_000), &ts_bound(32_503_680_000_000)]);
        let err = validate_region_partition_bounds(
            &changed("ts", ConcreteDataType::timestamp_nanosecond_datatype()),
            &routes,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("region 4398046511106"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_unrelated_and_unchanged_columns_are_skipped() {
        let other_column =
            r#"{"Expr":{"lhs":{"Column":"host"},"op":"Eq","rhs":{"Value":{"String":"a"}}}}"#;
        // `host` changed, but this bound is a string that any host type accepts.
        validate_region_partition_bounds(
            &changed("host", ConcreteDataType::string_datatype()),
            &routes(&[other_column]),
        )
        .unwrap();
        // `ts` bound with an unrelated column changed.
        validate_region_partition_bounds(
            &changed("host", ConcreteDataType::string_datatype()),
            &routes(&[&ts_bound(32_503_680_000_000)]),
        )
        .unwrap();
    }

    #[test]
    fn test_nested_expression_is_validated() {
        let expr = r#"{"Expr":{"lhs":{"Expr":{"lhs":{"Column":"ts"},"op":"GtEq","rhs":{"Value":{"Timestamp":{"value":0,"unit":"Millisecond"}}}}},"op":"And","rhs":{"Expr":{"lhs":{"Column":"ts"},"op":"Lt","rhs":{"Value":{"Timestamp":{"value":32503680000000,"unit":"Millisecond"}}}}}}}"#.to_string();
        let err = validate_region_partition_bounds(
            &changed("ts", ConcreteDataType::timestamp_nanosecond_datatype()),
            &routes(&[&expr]),
        )
        .unwrap_err();
        assert!(err.to_string().contains("'ts'"), "unexpected error: {err}");
    }

    #[test]
    fn test_deprecated_and_empty_bounds_are_skipped() {
        validate_region_partition_bounds(
            &changed("ts", ConcreteDataType::timestamp_nanosecond_datatype()),
            &routes(&["", r#""MaxValue""#, r#"{"Value":{"UInt32":1}}"#, "not-json"]),
        )
        .unwrap();
    }
}
