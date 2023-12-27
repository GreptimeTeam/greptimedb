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

use std::iter;

use datafusion_expr::{Expr as DfExpr, Operator};
use index::inverted_index::search::predicate::{
    Bound, InListPredicate, Predicate, Range, RangePredicate,
};
use index::inverted_index::Bytes;

use crate::error::Result;
use crate::sst::index::applier::builder::SstIndexApplierBuilder;

impl<'a> SstIndexApplierBuilder<'a> {
    pub(crate) fn collect_comparison_expr(
        &mut self,
        left: &DfExpr,
        op: &Operator,
        right: &DfExpr,
    ) -> Result<()> {
        match op {
            Operator::Eq => self.collect_eq(left, right),
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

    /// ```sql
    /// column_name <> literal
    /// # or
    /// literal <> column_name
    /// ```
    fn collect_eq(&mut self, left: &DfExpr, right: &DfExpr) -> Result<()> {
        let (column, lit) = match (left, right) {
            (DfExpr::Column(c), DfExpr::Literal(lit)) if !lit.is_null() => (c, lit),
            (DfExpr::Literal(lit), DfExpr::Column(c)) if !lit.is_null() => (c, lit),
            _ => return Ok(()),
        };

        let Some(data_type) = self.tag_column_type(&column.name)? else {
            return Ok(());
        };
        let bytes = Self::encode_lit(lit, data_type)?;

        let predicate = Predicate::InList(InListPredicate {
            list: iter::once(bytes).collect(),
        });
        self.add_predicate(&column.name, predicate);
        Ok(())
    }

    /// ```sql
    /// column_name < literal
    /// ```
    fn collect_lt(&mut self, left: &DfExpr, right: &DfExpr) -> Result<()> {
        self.collect_cmp(left, right, |value| Range {
            lower: None,
            upper: Some(Bound {
                inclusive: false,
                value,
            }),
        })
    }

    /// ```sql
    /// column_name > literal
    /// ```
    fn collect_gt(&mut self, left: &DfExpr, right: &DfExpr) -> Result<()> {
        self.collect_cmp(left, right, |value| Range {
            lower: Some(Bound {
                inclusive: false,
                value,
            }),
            upper: None,
        })
    }

    /// ```sql
    /// column_name <= literal
    /// ```
    fn collect_le(&mut self, left: &DfExpr, right: &DfExpr) -> Result<()> {
        self.collect_cmp(left, right, |value| Range {
            lower: None,
            upper: Some(Bound {
                inclusive: true,
                value,
            }),
        })
    }

    /// ```sql
    /// column_name >= literal
    /// ```
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
        left: &DfExpr,
        right: &DfExpr,
        range: impl FnOnce(Bytes) -> Range,
    ) -> Result<()> {
        let DfExpr::Column(c) = left else {
            return Ok(());
        };
        let lit = match right {
            DfExpr::Literal(lit) if !lit.is_null() => lit,
            _ => return Ok(()),
        };

        let Some(data_type) = self.tag_column_type(&c.name)? else {
            return Ok(());
        };
        let bytes = Self::encode_lit(lit, data_type)?;

        let predicate = Predicate::Range(RangePredicate {
            range: range(bytes),
        });
        self.add_predicate(&c.name, predicate);
        Ok(())
    }
}
