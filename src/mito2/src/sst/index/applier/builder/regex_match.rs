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

use datafusion_common::ScalarValue;
use datafusion_expr::Expr as DfExpr;
use index::inverted_index::search::predicate::{Predicate, RegexMatchPredicate};

use crate::error::Result;
use crate::sst::index::applier::builder::SstIndexApplierBuilder;

impl<'a> SstIndexApplierBuilder<'a> {
    pub(crate) fn collect_regex_match(&mut self, column: &DfExpr, pattern: &DfExpr) -> Result<()> {
        let Some(column_name) = Self::column_name(column) else {
            return Ok(());
        };
        let DfExpr::Literal(ScalarValue::Utf8(Some(pattern))) = pattern else {
            return Ok(());
        };
        let Some(data_type) = self.tag_column_type(column_name)? else {
            return Ok(());
        };
        if !data_type.is_string() {
            return Ok(());
        }

        let predicate = Predicate::RegexMatch(RegexMatchPredicate {
            pattern: pattern.clone(),
        });
        self.add_predicate(column_name, predicate);
        Ok(())
    }
}
