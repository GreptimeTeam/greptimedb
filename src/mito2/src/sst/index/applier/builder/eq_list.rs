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

use std::collections::HashSet;

use datafusion_expr::{BinaryExpr, Expr as DfExpr, Operator};
use datatypes::data_type::ConcreteDataType;
use index::inverted_index::search::predicate::{InListPredicate, Predicate};
use index::inverted_index::Bytes;

use crate::error::Result;
use crate::sst::index::applier::builder::SstIndexApplierBuilder;

impl<'a> SstIndexApplierBuilder<'a> {
    pub(crate) fn collect_eq(&mut self, left: &DfExpr, right: &DfExpr) -> Result<()> {
        let Some(column_name) = Self::column_name(left).or_else(|| Self::column_name(right)) else {
            return Ok(());
        };
        let Some(lit) = Self::nonnull_lit(right).or_else(|| Self::nonnull_lit(left)) else {
            return Ok(());
        };
        let Some(data_type) = self.tag_column_type(column_name)? else {
            return Ok(());
        };

        let predicate = Predicate::InList(InListPredicate {
            list: HashSet::from_iter([Self::encode_lit(lit, data_type)?]),
        });
        self.add_predicate(column_name, predicate);
        Ok(())
    }

    /// ```sql
    /// column_name <> literal1 OR column_name <> literal2 OR ...
    /// ```
    pub(crate) fn collect_or_eq_list(&mut self, eq_expr: &DfExpr, or_list: &DfExpr) -> Result<()> {
        let DfExpr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Eq,
            right,
        }) = eq_expr
        else {
            return Ok(());
        };

        let Some(column_name) = Self::column_name(left).or_else(|| Self::column_name(right)) else {
            return Ok(());
        };
        let Some(lit) = Self::nonnull_lit(right).or_else(|| Self::nonnull_lit(left)) else {
            return Ok(());
        };
        let Some(data_type) = self.tag_column_type(column_name)? else {
            return Ok(());
        };

        let bytes = Self::encode_lit(lit, data_type.clone())?;
        let mut inlist = HashSet::from_iter([bytes]);

        if Self::collect_eq_list_inner(column_name, &data_type, or_list, &mut inlist)? {
            let predicate = Predicate::InList(InListPredicate { list: inlist });
            self.add_predicate(column_name, predicate);
        }

        Ok(())
    }

    fn collect_eq_list_inner(
        column_name: &str,
        data_type: &ConcreteDataType,
        expr: &DfExpr,
        inlist: &mut HashSet<Bytes>,
    ) -> Result<bool> {
        let DfExpr::BinaryExpr(BinaryExpr {
            left,
            op: op @ (Operator::Eq | Operator::Or),
            right,
        }) = expr
        else {
            return Ok(false);
        };

        if op == &Operator::Or {
            let r = Self::collect_eq_list_inner(column_name, data_type, left, inlist)?
                .then(|| Self::collect_eq_list_inner(column_name, data_type, right, inlist))
                .transpose()?
                .unwrap_or(false);
            return Ok(r);
        }

        if op == &Operator::Eq {
            let Some(name) = Self::column_name(left).or_else(|| Self::column_name(right)) else {
                return Ok(false);
            };
            if column_name != name {
                return Ok(false);
            }
            let Some(lit) = Self::nonnull_lit(right).or_else(|| Self::nonnull_lit(left)) else {
                return Ok(false);
            };

            inlist.insert(Self::encode_lit(lit, data_type.clone())?);
            return Ok(true);
        }

        Ok(false)
    }
}
