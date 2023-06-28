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

use common_query::logical_plan::{DfExpr, Expr};
use common_telemetry::{error, warn};
use common_time::range::TimestampRange;
use common_time::timestamp::TimeUnit;
use common_time::Timestamp;
use datafusion::parquet::file::metadata::RowGroupMetaData;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion_common::ToDFSchema;
use datafusion_expr::expr::InList;
use datafusion_expr::{Between, BinaryExpr, Operator};
use datafusion_physical_expr::execution_props::ExecutionProps;
use datafusion_physical_expr::{create_physical_expr, PhysicalExpr};
use datatypes::schema::SchemaRef;
use datatypes::value::scalar_value_to_timestamp;
use snafu::ResultExt;

use crate::error;
use crate::predicate::stats::RowGroupPruningStatistics;

mod stats;

#[derive(Clone)]
pub struct Predicate {
    /// The schema of underlying storage.
    schema: SchemaRef,
    /// Physical expressions of this predicate.
    exprs: Vec<Arc<dyn PhysicalExpr>>,
}

impl Predicate {
    /// Creates a new `Predicate` by converting logical exprs to physical exprs that can be
    /// evaluated against record batches.
    /// Returns error when failed to convert exprs.
    pub fn try_new(exprs: Vec<Expr>, schema: SchemaRef) -> error::Result<Self> {
        let arrow_schema = schema.arrow_schema();
        let df_schema = arrow_schema
            .clone()
            .to_dfschema_ref()
            .context(error::DatafusionSnafu)?;

        // TODO(hl): `execution_props` provides variables required by evaluation.
        // we may reuse the `execution_props` from `SessionState` once we support
        // registering variables.
        let execution_props = &ExecutionProps::new();

        let physical_exprs = exprs
            .iter()
            .map(|expr| {
                create_physical_expr(
                    expr.df_expr(),
                    df_schema.as_ref(),
                    arrow_schema.as_ref(),
                    execution_props,
                )
            })
            .collect::<Result<_, _>>()
            .context(error::DatafusionSnafu)?;

        Ok(Self {
            schema,
            exprs: physical_exprs,
        })
    }

    #[inline]
    pub fn exprs(&self) -> &[Arc<dyn PhysicalExpr>] {
        &self.exprs
    }

    /// Builds an empty predicate from given schema.
    pub fn empty(schema: SchemaRef) -> Self {
        Self {
            schema,
            exprs: vec![],
        }
    }

    /// Evaluates the predicate against row group metadata.
    /// Returns a vector of boolean values, among which `false` means the row group can be skipped.
    pub fn prune_row_groups(&self, row_groups: &[RowGroupMetaData]) -> Vec<bool> {
        let mut res = vec![true; row_groups.len()];
        let arrow_schema = self.schema.arrow_schema();
        for expr in &self.exprs {
            match PruningPredicate::try_new(expr.clone(), arrow_schema.clone()) {
                Ok(p) => {
                    let stat = RowGroupPruningStatistics::new(row_groups, &self.schema);
                    match p.prune(&stat) {
                        Ok(r) => {
                            for (curr_val, res) in r.into_iter().zip(res.iter_mut()) {
                                *res &= curr_val
                            }
                        }
                        Err(e) => {
                            warn!("Failed to prune row groups, error: {:?}", e);
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to create predicate for expr, error: {:?}", e);
                }
            }
        }
        res
    }
}

// tests for `TimeRangePredicateBuilder` locates in src/query/tests/time_range_filter_test.rs
// since it requires query engine to convert sql to filters.
/// `TimeRangePredicateBuilder` extracts time range from logical exprs to facilitate fast
/// time range pruning.
pub struct TimeRangePredicateBuilder<'a> {
    ts_col_name: &'a str,
    ts_col_unit: TimeUnit,
    filters: &'a [Expr],
}

impl<'a> TimeRangePredicateBuilder<'a> {
    pub fn new(ts_col_name: &'a str, ts_col_unit: TimeUnit, filters: &'a [Expr]) -> Self {
        Self {
            ts_col_name,
            ts_col_unit,
            filters,
        }
    }

    pub fn build(&self) -> TimestampRange {
        let mut res = TimestampRange::min_to_max();
        for expr in self.filters {
            let range = self
                .extract_time_range_from_expr(expr.df_expr())
                .unwrap_or_else(TimestampRange::min_to_max);
            res = res.and(&range);
        }
        res
    }

    /// Extract time range filter from `WHERE`/`IN (...)`/`BETWEEN` clauses.
    /// Return None if no time range can be found in expr.
    fn extract_time_range_from_expr(&self, expr: &DfExpr) -> Option<TimestampRange> {
        match expr {
            DfExpr::BinaryExpr(BinaryExpr { left, op, right }) => {
                self.extract_from_binary_expr(left, op, right)
            }
            DfExpr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => self.extract_from_between_expr(expr, negated, low, high),
            DfExpr::InList(InList {
                expr,
                list,
                negated,
            }) => self.extract_from_in_list_expr(expr, *negated, list),
            _ => None,
        }
    }

    fn extract_from_binary_expr(
        &self,
        left: &DfExpr,
        op: &Operator,
        right: &DfExpr,
    ) -> Option<TimestampRange> {
        match op {
            Operator::Eq => self
                .get_timestamp_filter(left, right)
                .and_then(|ts| ts.convert_to(self.ts_col_unit))
                .map(TimestampRange::single),
            Operator::Lt => self
                .get_timestamp_filter(left, right)
                .and_then(|ts| ts.convert_to_ceil(self.ts_col_unit))
                .map(|ts| TimestampRange::until_end(ts, false)),
            Operator::LtEq => self
                .get_timestamp_filter(left, right)
                .and_then(|ts| ts.convert_to_ceil(self.ts_col_unit))
                .map(|ts| TimestampRange::until_end(ts, true)),
            Operator::Gt => self
                .get_timestamp_filter(left, right)
                .and_then(|ts| ts.convert_to(self.ts_col_unit))
                .map(TimestampRange::from_start),
            Operator::GtEq => self
                .get_timestamp_filter(left, right)
                .and_then(|ts| ts.convert_to(self.ts_col_unit))
                .map(TimestampRange::from_start),
            Operator::And => {
                // instead of return none when failed to extract time range from left/right, we unwrap the none into
                // `TimestampRange::min_to_max`.
                let left = self
                    .extract_time_range_from_expr(left)
                    .unwrap_or_else(TimestampRange::min_to_max);
                let right = self
                    .extract_time_range_from_expr(right)
                    .unwrap_or_else(TimestampRange::min_to_max);
                Some(left.and(&right))
            }
            Operator::Or => {
                let left = self.extract_time_range_from_expr(left)?;
                let right = self.extract_time_range_from_expr(right)?;
                Some(left.or(&right))
            }
            Operator::NotEq
            | Operator::Plus
            | Operator::Minus
            | Operator::Multiply
            | Operator::Divide
            | Operator::Modulo
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
            | Operator::RegexMatch
            | Operator::RegexIMatch
            | Operator::RegexNotMatch
            | Operator::RegexNotIMatch
            | Operator::BitwiseAnd
            | Operator::BitwiseOr
            | Operator::BitwiseXor
            | Operator::BitwiseShiftRight
            | Operator::BitwiseShiftLeft
            | Operator::StringConcat => None,
        }
    }

    fn get_timestamp_filter(&self, left: &DfExpr, right: &DfExpr) -> Option<Timestamp> {
        let (col, lit) = match (left, right) {
            (DfExpr::Column(column), DfExpr::Literal(scalar)) => (column, scalar),
            (DfExpr::Literal(scalar), DfExpr::Column(column)) => (column, scalar),
            _ => {
                return None;
            }
        };
        if col.name != self.ts_col_name {
            return None;
        }
        scalar_value_to_timestamp(lit)
    }

    fn extract_from_between_expr(
        &self,
        expr: &DfExpr,
        negated: &bool,
        low: &DfExpr,
        high: &DfExpr,
    ) -> Option<TimestampRange> {
        let DfExpr::Column(col) = expr else { return None; };
        if col.name != self.ts_col_name {
            return None;
        }

        if *negated {
            return None;
        }

        match (low, high) {
            (DfExpr::Literal(low), DfExpr::Literal(high)) => {
                let low_opt =
                    scalar_value_to_timestamp(low).and_then(|ts| ts.convert_to(self.ts_col_unit));
                let high_opt = scalar_value_to_timestamp(high)
                    .and_then(|ts| ts.convert_to_ceil(self.ts_col_unit));
                Some(TimestampRange::new_inclusive(low_opt, high_opt))
            }
            _ => None,
        }
    }

    /// Extract time range filter from `IN (...)` expr.
    fn extract_from_in_list_expr(
        &self,
        expr: &DfExpr,
        negated: bool,
        list: &[DfExpr],
    ) -> Option<TimestampRange> {
        if negated {
            return None;
        }
        let DfExpr::Column(col) = expr else { return None; };
        if col.name != self.ts_col_name {
            return None;
        }

        if list.is_empty() {
            return Some(TimestampRange::empty());
        }
        let mut init_range = TimestampRange::empty();
        for expr in list {
            if let DfExpr::Literal(scalar) = expr {
                if let Some(timestamp) = scalar_value_to_timestamp(scalar) {
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
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_test_util::temp_dir::{create_temp_dir, TempDir};
    use datafusion::parquet::arrow::ArrowWriter;
    pub use datafusion::parquet::schema::types::BasicTypeInfo;
    use datafusion_common::{Column, ScalarValue};
    use datafusion_expr::{BinaryExpr, Expr, Literal, Operator};
    use datatypes::arrow::array::Int32Array;
    use datatypes::arrow::datatypes::{DataType, Field, Schema};
    use datatypes::arrow::record_batch::RecordBatch;
    use datatypes::arrow_array::StringArray;
    use parquet::arrow::ParquetRecordBatchStreamBuilder;
    use parquet::file::properties::WriterProperties;

    use super::*;

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

    async fn assert_prune(
        array_cnt: usize,
        filters: Vec<common_query::logical_plan::Expr>,
        expect: Vec<bool>,
    ) {
        let dir = create_temp_dir("prune_parquet");
        let (path, schema) = gen_test_parquet_file(&dir, array_cnt).await;
        let schema = Arc::new(datatypes::schema::Schema::try_from(schema).unwrap());
        let arrow_predicate = Predicate::try_new(filters, schema.clone()).unwrap();
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
        let res = arrow_predicate.prune_row_groups(row_groups);
        assert_eq!(expect, res);
    }

    fn gen_predicate(max_val: i32, op: Operator) -> Vec<common_query::logical_plan::Expr> {
        vec![common_query::logical_plan::Expr::from(Expr::BinaryExpr(
            BinaryExpr {
                left: Box::new(Expr::Column(Column::from_name("cnt"))),
                op,
                right: Box::new(Expr::Literal(ScalarValue::Int32(Some(max_val)))),
            },
        ))]
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
        let e = Expr::Column(Column::from_name("cnt"))
            .gt(30.lit())
            .or(Expr::Column(Column::from_name("cnt")).lt(20.lit()));
        assert_prune(40, vec![e.into()], vec![true, true, false, true]).await;
    }
}
