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
pub use datafusion_expr::expr::Expr as DfExpr;
use datafusion_expr::{and, binary_expr, Operator};

/// Central struct of query API.
/// Represent logical expressions such as `A + 1`, or `CAST(c1 AS int)`.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Expr {
    df_expr: DfExpr,
}

impl Expr {
    pub fn df_expr(&self) -> &DfExpr {
        &self.df_expr
    }
}

impl From<DfExpr> for Expr {
    fn from(df_expr: DfExpr) -> Self {
        Self { df_expr }
    }
}

/// Builds an `Expr` that filters timestamp column from given timestamp range.
/// Returns [None] if time range is [None] or full time range.
pub fn build_filter_from_timestamp(
    ts_col_name: &str,
    time_range: Option<&TimestampRange>,
) -> Option<Expr> {
    let Some(time_range) = time_range else {
        return None;
    };
    let ts_col_expr = DfExpr::Column(Column {
        relation: None,
        name: ts_col_name.to_string(),
    });

    let df_expr = match (time_range.start(), time_range.end()) {
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
    };

    df_expr.map(Expr::from)
}

/// Converts a [Timestamp] to datafusion literal value.
fn timestamp_to_literal(timestamp: &Timestamp) -> DfExpr {
    let scalar_value = match timestamp.unit() {
        TimeUnit::Second => ScalarValue::TimestampSecond(Some(timestamp.value()), None),
        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(timestamp.value()), None),
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(timestamp.value()), None),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(timestamp.value()), None),
    };
    DfExpr::Literal(scalar_value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_df_expr() {
        let df_expr = DfExpr::Wildcard;

        let expr: Expr = df_expr.into();

        assert_eq!(DfExpr::Wildcard, *expr.df_expr());
    }
}
