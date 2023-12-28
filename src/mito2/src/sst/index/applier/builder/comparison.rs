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

use datafusion_expr::{Expr as DfExpr, Operator};
use index::inverted_index::search::predicate::{Bound, Predicate, Range, RangePredicate};
use index::inverted_index::Bytes;

use crate::error::Result;
use crate::sst::index::applier::builder::SstIndexApplierBuilder;

impl<'a> SstIndexApplierBuilder<'a> {
    /// Collects a comparison expression in the form of
    /// `column < lit`, `column > lit`, `column <= lit`, `column >= lit`,
    /// `lit < column`, `lit > column`, `lit <= column`, `lit >= column`.
    pub(crate) fn collect_comparison_expr(
        &mut self,
        left: &DfExpr,
        op: &Operator,
        right: &DfExpr,
    ) -> Result<()> {
        match op {
            Operator::Lt => {
                if matches!(right, DfExpr::Column(_)) {
                    self.collect_gt(right, left)
                } else {
                    self.collect_lt(left, right)
                }
            }
            Operator::LtEq => {
                if matches!(right, DfExpr::Column(_)) {
                    self.collect_ge(right, left)
                } else {
                    self.collect_le(left, right)
                }
            }
            Operator::Gt => {
                if matches!(right, DfExpr::Column(_)) {
                    self.collect_lt(right, left)
                } else {
                    self.collect_gt(left, right)
                }
            }
            Operator::GtEq => {
                if matches!(right, DfExpr::Column(_)) {
                    self.collect_le(right, left)
                } else {
                    self.collect_ge(left, right)
                }
            }
            _ => Ok(()),
        }
    }

    fn collect_lt(&mut self, left: &DfExpr, right: &DfExpr) -> Result<()> {
        self.collect_cmp(left, right, |value| Range {
            lower: None,
            upper: Some(Bound {
                inclusive: false,
                value,
            }),
        })
    }

    fn collect_gt(&mut self, left: &DfExpr, right: &DfExpr) -> Result<()> {
        self.collect_cmp(left, right, |value| Range {
            lower: Some(Bound {
                inclusive: false,
                value,
            }),
            upper: None,
        })
    }

    fn collect_le(&mut self, left: &DfExpr, right: &DfExpr) -> Result<()> {
        self.collect_cmp(left, right, |value| Range {
            lower: None,
            upper: Some(Bound {
                inclusive: true,
                value,
            }),
        })
    }

    fn collect_ge(&mut self, left: &DfExpr, right: &DfExpr) -> Result<()> {
        self.collect_cmp(left, right, |value| Range {
            lower: Some(Bound {
                inclusive: true,
                value,
            }),
            upper: None,
        })
    }

    fn collect_cmp(
        &mut self,
        column: &DfExpr,
        literal: &DfExpr,
        range_builder: impl FnOnce(Bytes) -> Range,
    ) -> Result<()> {
        let Some(column_name) = Self::column_name(column) else {
            return Ok(());
        };
        let Some(lit) = Self::nonnull_lit(literal) else {
            return Ok(());
        };
        let Some(data_type) = self.tag_column_type(column_name)? else {
            return Ok(());
        };

        let predicate = Predicate::Range(RangePredicate {
            range: range_builder(Self::encode_lit(lit, data_type)?),
        });

        self.add_predicate(column_name, predicate);
        Ok(())
    }
}
