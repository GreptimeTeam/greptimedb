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

use datafusion_expr::expr::InList;
use index::inverted_index::search::predicate::{InListPredicate, Predicate};

use crate::error::Result;
use crate::sst::index::applier::builder::SstIndexApplierBuilder;

impl<'a> SstIndexApplierBuilder<'a> {
    /// Collects an in list expression in the form of `column IN (lit, lit, ...)`.
    pub(crate) fn collect_inlist(&mut self, inlist: &InList) -> Result<()> {
        if inlist.negated {
            return Ok(());
        }
        let Some(column_name) = Self::column_name(&inlist.expr) else {
            return Ok(());
        };
        let Some(data_type) = self.tag_column_type(column_name)? else {
            return Ok(());
        };

        let mut predicate = InListPredicate {
            list: HashSet::with_capacity(inlist.list.len()),
        };
        for lit in &inlist.list {
            let Some(lit) = Self::nonnull_lit(lit) else {
                return Ok(());
            };

            predicate
                .list
                .insert(Self::encode_lit(lit, data_type.clone())?);
        }

        self.add_predicate(column_name, Predicate::InList(predicate));
        Ok(())
    }
}
