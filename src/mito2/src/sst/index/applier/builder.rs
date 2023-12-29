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

mod between;
mod comparison;
mod eq_list;
mod in_list;
mod regex_match;

use std::collections::HashMap;
use std::sync::Arc;

use common_query::logical_plan::Expr;
use datafusion_common::ScalarValue;
use datafusion_expr::{BinaryExpr, Expr as DfExpr, Operator};
use datatypes::data_type::ConcreteDataType;
use datatypes::value::Value;
use index::inverted_index::search::index_apply::PredicatesIndexApplier;
use index::inverted_index::search::predicate::Predicate;
use object_store::ObjectStore;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadata;

use crate::error::{BuildIndexApplierSnafu, ColumnNotFoundSnafu, Result};
use crate::row_converter::SortField;
use crate::sst::index::applier::SstIndexApplier;
use crate::sst::index::codec::IndexValueCodec;
use crate::sst::index::store::InstrumentedStore;

type ColumnName = String;

pub struct SstIndexApplierBuilder<'a> {
    region_dir: String,
    object_store: ObjectStore,
    metadata: &'a RegionMetadata,
    output: HashMap<ColumnName, Vec<Predicate>>,
}

impl<'a> SstIndexApplierBuilder<'a> {
    pub fn new(
        region_dir: String,
        object_store: ObjectStore,
        metadata: &'a RegionMetadata,
    ) -> Self {
        Self {
            region_dir,
            object_store,
            metadata,
            output: HashMap::default(),
        }
    }

    pub fn build(mut self, exprs: &[Expr]) -> Result<Option<SstIndexApplier>> {
        for expr in exprs {
            self.traverse_and_collect(expr.df_expr())?;
        }

        if self.output.is_empty() {
            return Ok(None);
        }

        let predicates = self.output.into_iter().collect();
        let applier = PredicatesIndexApplier::try_from(predicates);
        Ok(Some(SstIndexApplier::new(
            self.region_dir,
            InstrumentedStore::new(self.object_store),
            Arc::new(applier.context(BuildIndexApplierSnafu)?),
        )))
    }

    fn traverse_and_collect(&mut self, expr: &DfExpr) -> Result<()> {
        match expr {
            DfExpr::Between(between) => self.collect_between(between),
            DfExpr::InList(in_list) => self.collect_inlist(in_list),
            DfExpr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
                Operator::And => {
                    self.traverse_and_collect(left)?;
                    self.traverse_and_collect(right)
                }
                Operator::Or => self.collect_or_eq_list(left, right),
                Operator::Eq => self.collect_eq(left, right),
                Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => {
                    self.collect_comparison_expr(left, op, right)
                }
                Operator::RegexMatch => self.collect_regex_match(left, right),
                _ => Ok(()),
            },

            // TODO(zhongzc): support more expressions, e.g. IsNull, IsNotNull, ...
            _ => Ok(()),
        }
    }

    fn add_predicate(&mut self, column_name: &str, predicate: Predicate) {
        match self.output.get_mut(column_name) {
            Some(predicates) => predicates.push(predicate),
            None => {
                self.output.insert(column_name.to_string(), vec![predicate]);
            }
        }
    }

    fn tag_column_type(&self, column_name: &str) -> Result<Option<ConcreteDataType>> {
        let column = self
            .metadata
            .column_by_name(column_name)
            .context(ColumnNotFoundSnafu {
                column: column_name,
            })?;

        Ok(column
            .is_tag()
            .then(|| column.column_schema.data_type.clone()))
    }

    fn lit_not_null(expr: &DfExpr) -> Option<&ScalarValue> {
        match expr {
            DfExpr::Literal(lit) if !lit.is_null() => Some(lit),
            _ => None,
        }
    }

    fn column_name(expr: &DfExpr) -> Option<&str> {
        match expr {
            DfExpr::Column(column) => Some(&column.name),
            _ => None,
        }
    }

    fn encode_lit(lit: &ScalarValue, data_type: ConcreteDataType) -> Result<Vec<u8>> {
        let value = Value::try_from(lit.clone()).unwrap();
        let mut bytes = vec![];
        let field = SortField::new(data_type);
        IndexValueCodec::encode_value(value.as_value_ref(), &field, &mut bytes)?;
        Ok(bytes)
    }
}
