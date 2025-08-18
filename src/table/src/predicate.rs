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

use common_telemetry::{error, warn};
use common_time::range::TimestampRange;
use common_time::timestamp::TimeUnit;
use common_time::Timestamp;
use datafusion::common::ScalarValue;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion_common::pruning::PruningStatistics;
use datafusion_common::ToDFSchema;
use datafusion_expr::expr::{Expr, InList};
use datafusion_expr::{Between, BinaryExpr, Operator};
use datafusion_physical_expr::execution_props::ExecutionProps;
use datafusion_physical_expr::{create_physical_expr, PhysicalExpr};
use datatypes::arrow;
use datatypes::value::scalar_value_to_timestamp;
use snafu::ResultExt;

use crate::error;

#[cfg(test)]
mod stats;

/// Assert the scalar value is not utf8. Returns `None` if it's utf8.
/// In theory, it should be converted to a timestamp scalar value by `TypeConversionRule`.
macro_rules! return_none_if_utf8 {
    ($lit: ident) => {
        if matches!($lit, ScalarValue::Utf8(_)) {
            warn!(
                "Unexpected ScalarValue::Utf8 in time range predicate: {:?}. Maybe it's an implicit bug, please report it to https://github.com/GreptimeTeam/greptimedb/issues",
                $lit
            );

            // Make the predicate ineffective.
            return None;
        }
    };
}

/// Reference-counted pointer to a list of logical exprs.
#[derive(Debug, Clone)]
pub struct Predicate {
    /// logical exprs
    exprs: Arc<Vec<Expr>>,
}

impl Predicate {
    /// Creates a new `Predicate` by converting logical exprs to physical exprs that can be
    /// evaluated against record batches.
    /// Returns error when failed to convert exprs.
    pub fn new(exprs: Vec<Expr>) -> Self {
        Self {
            exprs: Arc::new(exprs),
        }
    }

    /// Returns the logical exprs.
    pub fn exprs(&self) -> &[Expr] {
        &self.exprs
    }

    /// Builds physical exprs according to provided schema.
    pub fn to_physical_exprs(
        &self,
        schema: &arrow::datatypes::SchemaRef,
    ) -> error::Result<Vec<Arc<dyn PhysicalExpr>>> {
        let df_schema = schema
            .clone()
            .to_dfschema_ref()
            .context(error::DatafusionSnafu)?;

        // TODO(hl): `execution_props` provides variables required by evaluation.
        // we may reuse the `execution_props` from `SessionState` once we support
        // registering variables.
        let execution_props = &ExecutionProps::new();

        Ok(self
            .exprs
            .iter()
            .filter_map(|expr| create_physical_expr(expr, df_schema.as_ref(), execution_props).ok())
            .collect::<Vec<_>>())
    }

    /// Evaluates the predicate against the `stats`.
    /// Returns a vector of boolean values, among which `false` means the row group can be skipped.
    pub fn prune_with_stats<S: PruningStatistics>(
        &self,
        stats: &S,
        schema: &arrow::datatypes::SchemaRef,
    ) -> Vec<bool> {
        let mut res = vec![true; stats.num_containers()];
        let physical_exprs = match self.to_physical_exprs(schema) {
            Ok(expr) => expr,
            Err(e) => {
                warn!(e; "Failed to build physical expr from predicates: {:?}", &self.exprs);
                return res;
            }
        };

        for expr in &physical_exprs {
            match PruningPredicate::try_new(expr.clone(), schema.clone()) {
                Ok(p) => match p.prune(stats) {
                    Ok(r) => {
                        for (curr_val, res) in r.into_iter().zip(res.iter_mut()) {
                            *res &= curr_val
                        }
                    }
                    Err(e) => {
                        warn!(e; "Failed to prune row groups");
                    }
                },
                Err(e) => {
                    error!(e; "Failed to create predicate for expr");
                }
            }
        }
        res
    }
}

// tests for `build_time_range_predicate` locates in src/query/tests/time_range_filter_test.rs
// since it requires query engine to convert sql to filters.
/// `build_time_range_predicate` extracts time range from logical exprs to facilitate fast
/// time range pruning.
pub fn build_time_range_predicate(
    ts_col_name: &str,
    ts_col_unit: TimeUnit,
    filters: &[Expr],
) -> TimestampRange {
    let mut res = TimestampRange::min_to_max();
    for expr in filters {
        if let Some(range) = extract_time_range_from_expr(ts_col_name, ts_col_unit, expr) {
            res = res.and(&range);
        }
    }
    res
}

/// Extract time range filter from `WHERE`/`IN (...)`/`BETWEEN` clauses.
/// Return None if no time range can be found in expr.
fn extract_time_range_from_expr(
    ts_col_name: &str,
    ts_col_unit: TimeUnit,
    expr: &Expr,
) -> Option<TimestampRange> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            extract_from_binary_expr(ts_col_name, ts_col_unit, left, op, right)
        }
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) => extract_from_between_expr(ts_col_name, ts_col_unit, expr, negated, low, high),
        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => extract_from_in_list_expr(ts_col_name, expr, *negated, list),
        _ => None,
    }
}

fn extract_from_binary_expr(
    ts_col_name: &str,
    ts_col_unit: TimeUnit,
    left: &Expr,
    op: &Operator,
    right: &Expr,
) -> Option<TimestampRange> {
    match op {
        Operator::Eq => get_timestamp_filter(ts_col_name, left, right)
            .and_then(|(ts, _)| ts.convert_to(ts_col_unit))
            .map(TimestampRange::single),
        Operator::Lt => {
            let (ts, reverse) = get_timestamp_filter(ts_col_name, left, right)?;
            if reverse {
                // [lit] < ts_col
                let ts_val = ts.convert_to(ts_col_unit)?.value();
                Some(TimestampRange::from_start(Timestamp::new(
                    ts_val + 1,
                    ts_col_unit,
                )))
            } else {
                // ts_col < [lit]
                ts.convert_to_ceil(ts_col_unit)
                    .map(|ts| TimestampRange::until_end(ts, false))
            }
        }
        Operator::LtEq => {
            let (ts, reverse) = get_timestamp_filter(ts_col_name, left, right)?;
            if reverse {
                // [lit] <= ts_col
                ts.convert_to_ceil(ts_col_unit)
                    .map(TimestampRange::from_start)
            } else {
                // ts_col <= [lit]
                ts.convert_to(ts_col_unit)
                    .map(|ts| TimestampRange::until_end(ts, true))
            }
        }
        Operator::Gt => {
            let (ts, reverse) = get_timestamp_filter(ts_col_name, left, right)?;
            if reverse {
                // [lit] > ts_col
                ts.convert_to_ceil(ts_col_unit)
                    .map(|t| TimestampRange::until_end(t, false))
            } else {
                // ts_col > [lit]
                let ts_val = ts.convert_to(ts_col_unit)?.value();
                Some(TimestampRange::from_start(Timestamp::new(
                    ts_val + 1,
                    ts_col_unit,
                )))
            }
        }
        Operator::GtEq => {
            let (ts, reverse) = get_timestamp_filter(ts_col_name, left, right)?;
            if reverse {
                // [lit] >= ts_col
                ts.convert_to(ts_col_unit)
                    .map(|t| TimestampRange::until_end(t, true))
            } else {
                // ts_col >= [lit]
                ts.convert_to_ceil(ts_col_unit)
                    .map(TimestampRange::from_start)
            }
        }
        Operator::And => {
            // instead of return none when failed to extract time range from left/right, we unwrap the none into
            // `TimestampRange::min_to_max`.
            let left = extract_time_range_from_expr(ts_col_name, ts_col_unit, left)
                .unwrap_or_else(TimestampRange::min_to_max);
            let right = extract_time_range_from_expr(ts_col_name, ts_col_unit, right)
                .unwrap_or_else(TimestampRange::min_to_max);
            Some(left.and(&right))
        }
        Operator::Or => {
            let left = extract_time_range_from_expr(ts_col_name, ts_col_unit, left)?;
            let right = extract_time_range_from_expr(ts_col_name, ts_col_unit, right)?;
            Some(left.or(&right))
        }
        _ => None,
    }
}

fn get_timestamp_filter(ts_col_name: &str, left: &Expr, right: &Expr) -> Option<(Timestamp, bool)> {
    let (col, lit, reverse) = match (left, right) {
        (Expr::Column(column), Expr::Literal(scalar, _)) => (column, scalar, false),
        (Expr::Literal(scalar, _), Expr::Column(column)) => (column, scalar, true),
        _ => {
            return None;
        }
    };
    if col.name != ts_col_name {
        return None;
    }

    return_none_if_utf8!(lit);
    scalar_value_to_timestamp(lit, None).map(|t| (t, reverse))
}

fn extract_from_between_expr(
    ts_col_name: &str,
    ts_col_unit: TimeUnit,
    expr: &Expr,
    negated: &bool,
    low: &Expr,
    high: &Expr,
) -> Option<TimestampRange> {
    let Expr::Column(col) = expr else {
        return None;
    };
    if col.name != ts_col_name {
        return None;
    }

    if *negated {
        return None;
    }

    match (low, high) {
        (Expr::Literal(low, _), Expr::Literal(high, _)) => {
            return_none_if_utf8!(low);
            return_none_if_utf8!(high);

            let low_opt =
                scalar_value_to_timestamp(low, None).and_then(|ts| ts.convert_to(ts_col_unit));
            let high_opt = scalar_value_to_timestamp(high, None)
                .and_then(|ts| ts.convert_to_ceil(ts_col_unit));
            Some(TimestampRange::new_inclusive(low_opt, high_opt))
        }
        _ => None,
    }
}

/// Extract time range filter from `IN (...)` expr.
fn extract_from_in_list_expr(
    ts_col_name: &str,
    expr: &Expr,
    negated: bool,
    list: &[Expr],
) -> Option<TimestampRange> {
    if negated {
        return None;
    }
    let Expr::Column(col) = expr else {
        return None;
    };
    if col.name != ts_col_name {
        return None;
    }

    if list.is_empty() {
        return Some(TimestampRange::empty());
    }
    let mut init_range = TimestampRange::empty();
    for expr in list {
        if let Expr::Literal(scalar, _) = expr {
            return_none_if_utf8!(scalar);
            if let Some(timestamp) = scalar_value_to_timestamp(scalar, None) {
                init_range = init_range.or(&TimestampRange::single(timestamp))
            } else {
                // TODO(hl): maybe we should raise an error here since cannot parse
                // timestamp value from in list expr
                return None;
            }
        }
    }
    Some(init_range)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_test_util::temp_dir::{create_temp_dir, TempDir};
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion_common::{Column, ScalarValue};
    use datafusion_expr::{col, lit, BinaryExpr, Literal, Operator};
    use datatypes::arrow::array::Int32Array;
    use datatypes::arrow::datatypes::{DataType, Field, Schema};
    use datatypes::arrow::record_batch::RecordBatch;
    use datatypes::arrow_array::StringArray;
    use parquet::arrow::ParquetRecordBatchStreamBuilder;
    use parquet::file::properties::WriterProperties;

    use super::*;
    use crate::predicate::stats::RowGroupPruningStatistics;

    fn check_build_predicate(expr: Expr, expect: TimestampRange) {
        assert_eq!(
            expect,
            build_time_range_predicate("ts", TimeUnit::Millisecond, &[expr])
        );
    }

    #[test]
    fn test_gt() {
        // ts > 1ms
        check_build_predicate(
            col("ts").gt(lit(ScalarValue::TimestampMillisecond(Some(1), None))),
            TimestampRange::from_start(Timestamp::new_millisecond(2)),
        );

        // 1ms > ts
        check_build_predicate(
            lit(ScalarValue::TimestampMillisecond(Some(1), None)).gt(col("ts")),
            TimestampRange::until_end(Timestamp::new_millisecond(1), false),
        );

        // 1001us > ts
        check_build_predicate(
            lit(ScalarValue::TimestampMicrosecond(Some(1001), None)).gt(col("ts")),
            TimestampRange::until_end(Timestamp::new_millisecond(1), true),
        );

        // ts > 1001us
        check_build_predicate(
            col("ts").gt(lit(ScalarValue::TimestampMicrosecond(Some(1001), None))),
            TimestampRange::from_start(Timestamp::new_millisecond(2)),
        );

        // 1s > ts
        check_build_predicate(
            lit(ScalarValue::TimestampSecond(Some(1), None)).gt(col("ts")),
            TimestampRange::until_end(Timestamp::new_millisecond(1000), false),
        );

        // ts > 1s
        check_build_predicate(
            col("ts").gt(lit(ScalarValue::TimestampSecond(Some(1), None))),
            TimestampRange::from_start(Timestamp::new_millisecond(1001)),
        );
    }

    #[test]
    fn test_gt_eq() {
        // ts >= 1ms
        check_build_predicate(
            col("ts").gt_eq(lit(ScalarValue::TimestampMillisecond(Some(1), None))),
            TimestampRange::from_start(Timestamp::new_millisecond(1)),
        );

        // 1ms >= ts
        check_build_predicate(
            lit(ScalarValue::TimestampMillisecond(Some(1), None)).gt_eq(col("ts")),
            TimestampRange::until_end(Timestamp::new_millisecond(1), true),
        );

        // 1001us >= ts
        check_build_predicate(
            lit(ScalarValue::TimestampMicrosecond(Some(1001), None)).gt_eq(col("ts")),
            TimestampRange::until_end(Timestamp::new_millisecond(1), true),
        );

        // ts >= 1001us
        check_build_predicate(
            col("ts").gt_eq(lit(ScalarValue::TimestampMicrosecond(Some(1001), None))),
            TimestampRange::from_start(Timestamp::new_millisecond(2)),
        );

        // 1s >= ts
        check_build_predicate(
            lit(ScalarValue::TimestampSecond(Some(1), None)).gt_eq(col("ts")),
            TimestampRange::until_end(Timestamp::new_millisecond(1000), true),
        );

        // ts >= 1s
        check_build_predicate(
            col("ts").gt_eq(lit(ScalarValue::TimestampSecond(Some(1), None))),
            TimestampRange::from_start(Timestamp::new_millisecond(1000)),
        );
    }

    #[test]
    fn test_lt() {
        // ts < 1ms
        check_build_predicate(
            col("ts").lt(lit(ScalarValue::TimestampMillisecond(Some(1), None))),
            TimestampRange::until_end(Timestamp::new_millisecond(1), false),
        );

        // 1ms < ts
        check_build_predicate(
            lit(ScalarValue::TimestampMillisecond(Some(1), None)).lt(col("ts")),
            TimestampRange::from_start(Timestamp::new_millisecond(2)),
        );

        // 1001us < ts
        check_build_predicate(
            lit(ScalarValue::TimestampMicrosecond(Some(1001), None)).lt(col("ts")),
            TimestampRange::from_start(Timestamp::new_millisecond(2)),
        );

        // ts < 1001us
        check_build_predicate(
            col("ts").lt(lit(ScalarValue::TimestampMicrosecond(Some(1001), None))),
            TimestampRange::until_end(Timestamp::new_millisecond(1), true),
        );

        // 1s < ts
        check_build_predicate(
            lit(ScalarValue::TimestampSecond(Some(1), None)).lt(col("ts")),
            TimestampRange::from_start(Timestamp::new_millisecond(1001)),
        );

        // ts < 1s
        check_build_predicate(
            col("ts").lt(lit(ScalarValue::TimestampSecond(Some(1), None))),
            TimestampRange::until_end(Timestamp::new_millisecond(1000), false),
        );
    }

    #[test]
    fn test_lt_eq() {
        // ts <= 1ms
        check_build_predicate(
            col("ts").lt_eq(lit(ScalarValue::TimestampMillisecond(Some(1), None))),
            TimestampRange::until_end(Timestamp::new_millisecond(1), true),
        );

        // 1ms <= ts
        check_build_predicate(
            lit(ScalarValue::TimestampMillisecond(Some(1), None)).lt_eq(col("ts")),
            TimestampRange::from_start(Timestamp::new_millisecond(1)),
        );

        // 1001us <= ts
        check_build_predicate(
            lit(ScalarValue::TimestampMicrosecond(Some(1001), None)).lt_eq(col("ts")),
            TimestampRange::from_start(Timestamp::new_millisecond(2)),
        );

        // ts <= 1001us
        check_build_predicate(
            col("ts").lt_eq(lit(ScalarValue::TimestampMicrosecond(Some(1001), None))),
            TimestampRange::until_end(Timestamp::new_millisecond(1), true),
        );

        // 1s <= ts
        check_build_predicate(
            lit(ScalarValue::TimestampSecond(Some(1), None)).lt_eq(col("ts")),
            TimestampRange::from_start(Timestamp::new_millisecond(1000)),
        );

        // ts <= 1s
        check_build_predicate(
            col("ts").lt_eq(lit(ScalarValue::TimestampSecond(Some(1), None))),
            TimestampRange::until_end(Timestamp::new_millisecond(1000), true),
        );
    }

    async fn gen_test_parquet_file(dir: &TempDir, cnt: usize) -> (String, Arc<Schema>) {
        let path = dir
            .path()
            .join("test-prune.parquet")
            .to_string_lossy()
            .to_string();

        let name_field = Field::new("name", DataType::Utf8, true);
        let count_field = Field::new("cnt", DataType::Int32, true);
        let schema = Arc::new(Schema::new(vec![name_field, count_field]));

        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path.clone())
            .unwrap();

        let write_props = WriterProperties::builder()
            .set_max_row_group_size(10)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(write_props)).unwrap();

        for i in (0..cnt).step_by(10) {
            let name_array = Arc::new(StringArray::from(
                (i..(i + 10).min(cnt))
                    .map(|i| i.to_string())
                    .collect::<Vec<_>>(),
            )) as Arc<_>;
            let count_array = Arc::new(Int32Array::from(
                (i..(i + 10).min(cnt)).map(|i| i as i32).collect::<Vec<_>>(),
            )) as Arc<_>;
            let rb = RecordBatch::try_new(schema.clone(), vec![name_array, count_array]).unwrap();
            writer.write(&rb).unwrap();
        }
        let _ = writer.close().unwrap();
        (path, schema)
    }

    async fn assert_prune(array_cnt: usize, filters: Vec<Expr>, expect: Vec<bool>) {
        let dir = create_temp_dir("prune_parquet");
        let (path, arrow_schema) = gen_test_parquet_file(&dir, array_cnt).await;
        let schema = Arc::new(datatypes::schema::Schema::try_from(arrow_schema.clone()).unwrap());
        let arrow_predicate = Predicate::new(filters);
        let builder = ParquetRecordBatchStreamBuilder::new(
            tokio::fs::OpenOptions::new()
                .read(true)
                .open(path)
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        let metadata = builder.metadata().clone();
        let row_groups = metadata.row_groups();

        let stats = RowGroupPruningStatistics::new(row_groups, &schema);
        let res = arrow_predicate.prune_with_stats(&stats, &arrow_schema);
        assert_eq!(expect, res);
    }

    fn gen_predicate(max_val: i32, op: Operator) -> Vec<Expr> {
        vec![datafusion_expr::Expr::BinaryExpr(BinaryExpr {
            left: Box::new(datafusion_expr::Expr::Column(Column::from_name("cnt"))),
            op,
            right: Box::new(max_val.lit()),
        })]
    }

    #[tokio::test]
    async fn test_prune_empty() {
        assert_prune(3, vec![], vec![true]).await;
    }

    #[tokio::test]
    async fn test_prune_all_match() {
        let p = gen_predicate(3, Operator::Gt);
        assert_prune(2, p, vec![false]).await;
    }

    #[tokio::test]
    async fn test_prune_gt() {
        let p = gen_predicate(29, Operator::Gt);
        assert_prune(
            100,
            p,
            vec![
                false, false, false, true, true, true, true, true, true, true,
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_prune_eq_expr() {
        let p = gen_predicate(30, Operator::Eq);
        assert_prune(40, p, vec![false, false, false, true]).await;
    }

    #[tokio::test]
    async fn test_prune_neq_expr() {
        let p = gen_predicate(30, Operator::NotEq);
        assert_prune(40, p, vec![true, true, true, true]).await;
    }

    #[tokio::test]
    async fn test_prune_gteq_expr() {
        let p = gen_predicate(29, Operator::GtEq);
        assert_prune(40, p, vec![false, false, true, true]).await;
    }

    #[tokio::test]
    async fn test_prune_lt_expr() {
        let p = gen_predicate(30, Operator::Lt);
        assert_prune(40, p, vec![true, true, true, false]).await;
    }

    #[tokio::test]
    async fn test_prune_lteq_expr() {
        let p = gen_predicate(30, Operator::LtEq);
        assert_prune(40, p, vec![true, true, true, true]).await;
    }

    #[tokio::test]
    async fn test_prune_between_expr() {
        let p = gen_predicate(30, Operator::LtEq);
        assert_prune(40, p, vec![true, true, true, true]).await;
    }

    #[tokio::test]
    async fn test_or() {
        // cnt > 30 or cnt < 20
        let e = datafusion_expr::Expr::Column(Column::from_name("cnt"))
            .gt(30.lit())
            .or(datafusion_expr::Expr::Column(Column::from_name("cnt")).lt(20.lit()));
        assert_prune(40, vec![e], vec![true, true, false, true]).await;
    }

    #[tokio::test]
    async fn test_to_physical_expr() {
        let predicate = Predicate::new(vec![
            col("host").eq(lit("host_a")),
            col("ts").gt(lit(ScalarValue::TimestampMicrosecond(Some(123), None))),
        ]);

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "host",
            arrow::datatypes::DataType::Utf8,
            false,
        )]));

        let predicates = predicate.to_physical_exprs(&schema).unwrap();
        assert!(!predicates.is_empty());
    }
}
