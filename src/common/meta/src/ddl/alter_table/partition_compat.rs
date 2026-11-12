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

//! Rejects an ALTER TABLE whose column type changes would break the persisted
//! partition expressions of a region, before any region is altered.
//!
//! Bounds keep the type they were written with. Row routing compares them with
//! row values as `Value`s, which only stays valid across a timestamp unit
//! widening; scans coerce the bound to the common comparison type, which can
//! overflow (a millisecond bound in year 3000 cast to nanoseconds).
//!
//! `partition` depends on this crate, so the `Persisted*` types mirror the JSON of
//! `partition::partition::PartitionBound` and `partition::expr::PartitionExpr`.
//! A test in `partition` runs real serialized expressions through this validator.

use std::collections::HashMap;

use common_telemetry::warn;
use datafusion_common::ScalarValue;
use datafusion_expr::type_coercion::binary::comparison_coercion;
use datatypes::data_type::{ConcreteDataType, DataType};
use datatypes::schema::Schema;
use datatypes::value::{
    Value, duration_to_scalar_value, time_to_scalar_value, timestamp_to_scalar_value,
};
use serde::Deserialize;
use store_api::storage::RegionId;

use crate::error::{PartitionExprIncompatibleSnafu, Result};
use crate::rpc::router::RegionRoute;

#[derive(Debug, Deserialize)]
enum PersistedPartitionBound {
    /// Deprecated since 0.9.0.
    #[allow(dead_code)]
    Value(Value),
    /// Deprecated since 0.15.0.
    MaxValue,
    Expr(PersistedPartitionExpr),
}

#[derive(Debug, Deserialize)]
struct PersistedPartitionExpr {
    lhs: Box<PersistedOperand>,
    op: PersistedOp,
    rhs: Box<PersistedOperand>,
}

#[derive(Debug, Deserialize)]
enum PersistedOperand {
    Column(String),
    Value(Value),
    Expr(PersistedPartitionExpr),
    /// `iceberg_hash(column)`.
    IcebergHash(String),
}

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

/// Returns the columns present in both schemas whose type differs, as (old, new).
pub(crate) fn changed_column_types(
    old_schema: &Schema,
    new_schema: &Schema,
) -> HashMap<String, (ConcreteDataType, ConcreteDataType)> {
    let mut changed = HashMap::new();
    for new_column in new_schema.column_schemas() {
        let Some(old_column) = old_schema.column_schema_by_name(&new_column.name) else {
            continue;
        };
        if old_column.data_type != new_column.data_type {
            changed.insert(
                new_column.name.clone(),
                (old_column.data_type.clone(), new_column.data_type.clone()),
            );
        }
    }

    changed
}

/// Returns an error naming the first region whose partition expression can't take
/// the type changes in `changed_columns`.
///
/// Public so that tests in `partition` can run real serialized expressions through it.
pub fn validate_region_partition_bounds(
    changed_columns: &HashMap<String, (ConcreteDataType, ConcreteDataType)>,
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
                // Such a region already fails every scan, regardless of this ALTER.
                warn!(
                    "Skipping partition expression validation for region {}, \
                     the persisted expression is not parseable: {}, expr: {}",
                    route.region.id, err, expr_json
                );
                continue;
            }
        };
        // Deprecated bound formats are not used to build partition predicates.
        let PersistedPartitionBound::Expr(expr) = bound else {
            continue;
        };

        validate_expr(&expr, changed_columns, route.region.id)?;
    }

    Ok(())
}

fn validate_expr(
    expr: &PersistedPartitionExpr,
    changed_columns: &HashMap<String, (ConcreteDataType, ConcreteDataType)>,
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
            validate_comparison(&expr.lhs, &expr.rhs, changed_columns, region_id)?;
            validate_comparison(&expr.rhs, &expr.lhs, changed_columns, region_id)?;
        }
    }

    Ok(())
}

fn validate_operand(
    operand: &PersistedOperand,
    changed_columns: &HashMap<String, (ConcreteDataType, ConcreteDataType)>,
    region_id: RegionId,
) -> Result<()> {
    match operand {
        PersistedOperand::Expr(expr) => validate_expr(expr, changed_columns, region_id),
        PersistedOperand::Column(_)
        | PersistedOperand::Value(_)
        | PersistedOperand::IcebergHash(_) => Ok(()),
    }
}

fn validate_comparison(
    operand: &PersistedOperand,
    other: &PersistedOperand,
    changed_columns: &HashMap<String, (ConcreteDataType, ConcreteDataType)>,
    region_id: RegionId,
) -> Result<()> {
    let (column, value) = match (operand, other) {
        (PersistedOperand::Column(column), PersistedOperand::Value(value)) => (column, value),
        // The hash depends on the column type, so any change reroutes rows. Table
        // metadata rejects such changes first; this covers stale partition columns.
        (PersistedOperand::IcebergHash(column), PersistedOperand::Value(_)) => {
            return match changed_columns.get(column) {
                Some((_, target_type)) => incompatible(region_id, column, target_type),
                None => Ok(()),
            };
        }
        _ => return Ok(()),
    };
    let Some((old_type, target_type)) = changed_columns.get(column) else {
        return Ok(());
    };
    let Some(scalar) = value_to_scalar_value(value) else {
        return Ok(());
    };

    if !old_type.is_timestamp_unit_widening_to(target_type) {
        return incompatible(region_id, column, target_type);
    }

    // Mirrors the coercion `PredicateGroup::new` applies on scan: the bound must
    // cast to the common comparison type.
    let Some(common_type) = comparison_coercion(&target_type.as_arrow_type(), &scalar.data_type())
    else {
        return incompatible(region_id, column, target_type);
    };
    if scalar.data_type() != common_type
        && let Err(error) = scalar.cast_to(&common_type)
    {
        warn!(error; "Partition bound of region {} can't cast to {}", region_id, common_type);
        return incompatible(region_id, column, target_type);
    }

    Ok(())
}

fn incompatible(region_id: RegionId, column: &str, target_type: &ConcreteDataType) -> Result<()> {
    PartitionExprIncompatibleSnafu {
        region_id,
        column,
        target_type: target_type.to_string(),
    }
    .fail()
}

/// Converts a bound the same way as `partition::expr::Operand::try_as_logical_expr`.
/// Returns `None` for values that can't be partition bounds.
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

    fn changed(
        column: &str,
        old_type: ConcreteDataType,
        new_type: ConcreteDataType,
    ) -> HashMap<String, (ConcreteDataType, ConcreteDataType)> {
        HashMap::from([(column.to_string(), (old_type, new_type))])
    }

    #[test]
    fn test_changed_column_types() {
        let old = schema(ConcreteDataType::timestamp_millisecond_datatype());
        let new = schema(ConcreteDataType::timestamp_nanosecond_datatype());
        let changed = changed_column_types(&old, &new);
        assert_eq!(changed.len(), 1);
        assert_eq!(
            changed["ts"],
            (
                ConcreteDataType::timestamp_millisecond_datatype(),
                ConcreteDataType::timestamp_nanosecond_datatype()
            )
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
            &changed(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                ConcreteDataType::timestamp_nanosecond_datatype(),
            ),
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
            &changed(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                ConcreteDataType::timestamp_nanosecond_datatype(),
            ),
            &routes,
        )
        .unwrap();
        validate_region_partition_bounds(
            &changed(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                ConcreteDataType::timestamp_microsecond_datatype(),
            ),
            &routes,
        )
        .unwrap();
    }

    #[test]
    fn test_later_region_rejection_names_the_region() {
        let routes = routes(&[&ts_bound(1_704_067_200_000), &ts_bound(32_503_680_000_000)]);
        let err = validate_region_partition_bounds(
            &changed(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                ConcreteDataType::timestamp_nanosecond_datatype(),
            ),
            &routes,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("region 4398046511106"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_unrelated_columns_are_skipped() {
        let host_bound =
            r#"{"Expr":{"lhs":{"Column":"host"},"op":"Eq","rhs":{"Value":{"String":"a"}}}}"#;
        // The changed column is not referenced by this bound.
        validate_region_partition_bounds(
            &changed(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                ConcreteDataType::timestamp_nanosecond_datatype(),
            ),
            &routes(&[host_bound]),
        )
        .unwrap();
        // The referenced column did not change.
        validate_region_partition_bounds(
            &changed(
                "host",
                ConcreteDataType::int32_datatype(),
                ConcreteDataType::string_datatype(),
            ),
            &routes(&[&ts_bound(32_503_680_000_000)]),
        )
        .unwrap();
    }

    #[test]
    fn test_nested_expression_is_validated() {
        let expr = r#"{"Expr":{"lhs":{"Expr":{"lhs":{"Column":"ts"},"op":"GtEq","rhs":{"Value":{"Timestamp":{"value":0,"unit":"Millisecond"}}}}},"op":"And","rhs":{"Expr":{"lhs":{"Column":"ts"},"op":"Lt","rhs":{"Value":{"Timestamp":{"value":32503680000000,"unit":"Millisecond"}}}}}}}"#.to_string();
        let err = validate_region_partition_bounds(
            &changed(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                ConcreteDataType::timestamp_nanosecond_datatype(),
            ),
            &routes(&[&expr]),
        )
        .unwrap_err();
        assert!(err.to_string().contains("'ts'"), "unexpected error: {err}");
    }

    #[test]
    fn test_deprecated_and_empty_bounds_are_skipped() {
        validate_region_partition_bounds(
            &changed(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                ConcreteDataType::timestamp_nanosecond_datatype(),
            ),
            &routes(&["", r#""MaxValue""#, r#"{"Value":{"UInt32":1}}"#, "not-json"]),
        )
        .unwrap();
    }

    #[test]
    fn test_field_narrowing_is_rejected_for_persisted_bounds() {
        // Scans would coerce both sides, but row routing compares Int64 bounds with
        // Int32 rows.
        let bound =
            r#"{"Expr":{"lhs":{"Column":"v"},"op":"Lt","rhs":{"Value":{"Int64":3000000000}}}}"#;
        let err = validate_region_partition_bounds(
            &changed(
                "v",
                ConcreteDataType::int64_datatype(),
                ConcreteDataType::int32_datatype(),
            ),
            &routes(&[bound]),
        )
        .unwrap_err();
        assert!(err.to_string().contains("'v'"), "unexpected error: {err}");
    }

    #[test]
    fn test_non_timestamp_widening_is_rejected_for_persisted_bounds() {
        // Row routing would compare Int32 bounds with Int64 rows.
        let bound = r#"{"Expr":{"lhs":{"Column":"v"},"op":"Lt","rhs":{"Value":{"Int32":10}}}}"#;
        let err = validate_region_partition_bounds(
            &changed(
                "v",
                ConcreteDataType::int32_datatype(),
                ConcreteDataType::int64_datatype(),
            ),
            &routes(&[bound]),
        )
        .unwrap_err();
        assert!(err.to_string().contains("'v'"), "unexpected error: {err}");
    }

    #[test]
    fn test_bound_without_common_comparison_type_is_rejected() {
        // No common comparison type between a boolean and a timestamp.
        let bound =
            r#"{"Expr":{"lhs":{"Column":"ts"},"op":"Lt","rhs":{"Value":{"Boolean":true}}}}"#;
        let err = validate_region_partition_bounds(
            &changed(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                ConcreteDataType::timestamp_nanosecond_datatype(),
            ),
            &routes(&[bound]),
        )
        .unwrap_err();
        assert!(err.to_string().contains("'ts'"), "unexpected error: {err}");
    }
}
