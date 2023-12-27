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
    /// ```sql
    /// column_name <> literal1 OR column_name <> literal2 OR ...
    /// ```
    pub(crate) fn collect_or_eq_list(&mut self, eq_expr: &DfExpr, or_list: &DfExpr) -> Result<()> {
        let (column_name, lit) = match eq_expr {
            DfExpr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::Eq,
                right,
            }) => match (left.as_ref(), right.as_ref()) {
                // column_name <> literal
                (DfExpr::Column(c), DfExpr::Literal(lit)) if !lit.is_null() => (&c.name, lit),
                // literal <> column_name
                (DfExpr::Literal(lit), DfExpr::Column(c)) if !lit.is_null() => (&c.name, lit),
                _ => return Ok(()),
            },
            _ => return Ok(()),
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
        let DfExpr::BinaryExpr(BinaryExpr { left, op, right }) = expr else {
            return Ok(false);
        };
        match op {
            Operator::Eq => {
                let lit = match (left.as_ref(), right.as_ref()) {
                    (DfExpr::Column(c), DfExpr::Literal(lit))
                        if c.name == column_name && !lit.is_null() =>
                    {
                        lit
                    }
                    (DfExpr::Literal(lit), DfExpr::Column(c))
                        if c.name == column_name && !lit.is_null() =>
                    {
                        lit
                    }
                    _ => return Ok(false),
                };
                let bytes = Self::encode_lit(lit, data_type.clone())?;
                inlist.insert(bytes);
                Ok(true)
            }

            Operator::Or => {
                let (left, right) = (left.as_ref(), right.as_ref());
                if Self::collect_eq_list_inner(column_name, data_type, left, inlist)? {
                    Self::collect_eq_list_inner(column_name, data_type, right, inlist)
                } else {
                    Ok(false)
                }
            }

            _ => return Ok(false),
        }
    }
}
