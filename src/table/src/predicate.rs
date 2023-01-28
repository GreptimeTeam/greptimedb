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

use common_query::logical_plan::{DfExpr, Expr};
use common_telemetry::{error, warn};
use common_time::range::TimestampRange;
use common_time::Timestamp;
use datafusion::parquet::file::metadata::RowGroupMetaData;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion_expr::{Between, BinaryExpr, Operator};
use datatypes::schema::SchemaRef;
use datatypes::value::scalar_value_to_timestamp;

use crate::predicate::stats::RowGroupPruningStatistics;

mod stats;

#[derive(Default, Clone)]
pub struct Predicate {
    exprs: Vec<Expr>,
}

impl Predicate {
    pub fn new(exprs: Vec<Expr>) -> Self {
        Self { exprs }
    }

    pub fn empty() -> Self {
        Self { exprs: vec![] }
    }

    pub fn prune_row_groups(
        &self,
        schema: SchemaRef,
        row_groups: &[RowGroupMetaData],
    ) -> Vec<bool> {
        let mut res = vec![true; row_groups.len()];
        for expr in &self.exprs {
            match PruningPredicate::try_new(expr.df_expr().clone(), schema.arrow_schema().clone()) {
                Ok(p) => {
                    let stat = RowGroupPruningStatistics::new(row_groups, &schema);
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
pub struct TimeRangePredicateBuilder<'a> {
    ts_col_name: &'a str,
    filters: &'a [Expr],
}

impl<'a> TimeRangePredicateBuilder<'a> {
    pub fn new(ts_col_name: &'a str, filters: &'a [Expr]) -> Self {
        Self {
            ts_col_name,
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
            DfExpr::InList {
                expr,
                list,
                negated,
            } => self.extract_from_in_list_expr(expr, *negated, list),
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
                .map(TimestampRange::single),
            Operator::Lt => self
                .get_timestamp_filter(left, right)
                .map(|ts| TimestampRange::until_end(ts, false)),
            Operator::LtEq => self
                .get_timestamp_filter(left, right)
                .map(|ts| TimestampRange::until_end(ts, true)),
            Operator::Gt => self
                .get_timestamp_filter(left, right)
                .map(TimestampRange::from_start),
            Operator::GtEq => self
                .get_timestamp_filter(left, right)
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
            | Operator::Like
            | Operator::NotLike
            | Operator::ILike
            | Operator::NotILike
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
                let low_opt = scalar_value_to_timestamp(low);
                let high_opt = scalar_value_to_timestamp(high);
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
    use tempdir::TempDir;

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
        writer.close().unwrap();
        (path, schema)
    }

    async fn assert_prune(array_cnt: usize, predicate: Predicate, expect: Vec<bool>) {
        let dir = TempDir::new("prune_parquet").unwrap();
        let (path, schema) = gen_test_parquet_file(&dir, array_cnt).await;
        let schema = Arc::new(datatypes::schema::Schema::try_from(schema).unwrap());
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
        let res = predicate.prune_row_groups(schema, row_groups);
        assert_eq!(expect, res);
    }

    fn gen_predicate(max_val: i32, op: Operator) -> Predicate {
        Predicate::new(vec![common_query::logical_plan::Expr::from(
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(Column::from_name("cnt"))),
                op,
                right: Box::new(Expr::Literal(ScalarValue::Int32(Some(max_val)))),
            }),
        )])
    }

    #[tokio::test]
    async fn test_prune_empty() {
        assert_prune(3, Predicate::empty(), vec![true]).await;
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
        let p = Predicate::new(vec![e.into()]);
        assert_prune(40, p, vec![true, true, false, true]).await;
    }
}
