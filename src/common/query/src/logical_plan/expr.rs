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

use common_time::range::TimestampRange;
use common_time::timestamp::TimeUnit;
use common_time::Timestamp;
use datafusion_common::{Column, ScalarValue};
use datafusion_expr::expr::Expr;
use datafusion_expr::{and, binary_expr, Operator};
use datatypes::data_type::DataType;
use datatypes::schema::ColumnSchema;
use datatypes::value::Value;

/// Builds a filter for a timestamp column with the same type as the timestamp column.
/// Returns [None] if time range is [None] or full time range.
pub fn build_same_type_ts_filter(
    ts_schema: &ColumnSchema,
    time_range: Option<TimestampRange>,
) -> Option<Expr> {
    let time_range = time_range?;
    let start = time_range
        .start()
        .and_then(|start| ts_schema.data_type.try_cast(Value::Timestamp(start)));
    let end = time_range
        .end()
        .and_then(|end| ts_schema.data_type.try_cast(Value::Timestamp(end)));

    let time_range = match (start, end) {
        (Some(Value::Timestamp(start)), Some(Value::Timestamp(end))) => {
            TimestampRange::new(start, end)
        }
        (Some(Value::Timestamp(start)), None) => Some(TimestampRange::from_start(start)),
        (None, Some(Value::Timestamp(end))) => Some(TimestampRange::until_end(end, false)),
        _ => return None,
    };
    build_filter_from_timestamp(&ts_schema.name, time_range.as_ref())
}

/// Builds an `Expr` that filters timestamp column from given timestamp range.
/// Returns [None] if time range is [None] or full time range.
pub fn build_filter_from_timestamp(
    ts_col_name: &str,
    time_range: Option<&TimestampRange>,
) -> Option<Expr> {
    let time_range = time_range?;
    let ts_col_expr = Expr::Column(Column::from_name(ts_col_name));

    match (time_range.start(), time_range.end()) {
        (None, None) => None,
        (Some(start), None) => Some(binary_expr(
            ts_col_expr,
            Operator::GtEq,
            timestamp_to_literal(start),
        )),
        (None, Some(end)) => Some(binary_expr(
            ts_col_expr,
            Operator::Lt,
            timestamp_to_literal(end),
        )),
        (Some(start), Some(end)) => Some(and(
            binary_expr(
                ts_col_expr.clone(),
                Operator::GtEq,
                timestamp_to_literal(start),
            ),
            binary_expr(ts_col_expr, Operator::Lt, timestamp_to_literal(end)),
        )),
    }
}

/// Converts a [Timestamp] to datafusion literal value.
fn timestamp_to_literal(timestamp: &Timestamp) -> Expr {
    let scalar_value = match timestamp.unit() {
        TimeUnit::Second => ScalarValue::TimestampSecond(Some(timestamp.value()), None),
        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(timestamp.value()), None),
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(timestamp.value()), None),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(timestamp.value()), None),
    };
    Expr::Literal(scalar_value, None)
}

#[cfg(test)]
mod tests {
    use datafusion_expr::Literal;

    use super::*;

    #[test]
    fn test_timestamp_to_literal() {
        let timestamp = Timestamp::new(123456789, TimeUnit::Second);
        let expected = ScalarValue::TimestampSecond(Some(123456789), None).lit();
        assert_eq!(timestamp_to_literal(&timestamp), expected);

        let timestamp = Timestamp::new(123456789, TimeUnit::Millisecond);
        let expected = ScalarValue::TimestampMillisecond(Some(123456789), None).lit();
        assert_eq!(timestamp_to_literal(&timestamp), expected);

        let timestamp = Timestamp::new(123456789, TimeUnit::Microsecond);
        let expected = ScalarValue::TimestampMicrosecond(Some(123456789), None).lit();
        assert_eq!(timestamp_to_literal(&timestamp), expected);

        let timestamp = Timestamp::new(123456789, TimeUnit::Nanosecond);
        let expected = ScalarValue::TimestampNanosecond(Some(123456789), None).lit();
        assert_eq!(timestamp_to_literal(&timestamp), expected);
    }
}
