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
    /// ```sql
    /// column_name REGEXP literal
    /// ```
    pub(crate) fn collect_regex_match(&mut self, left: &DfExpr, right: &DfExpr) -> Result<()> {
        let (column, pattern) = match (left, right) {
            (DfExpr::Column(c), DfExpr::Literal(ScalarValue::Utf8(Some(pattern)))) => (c, pattern),
            _ => return Ok(()),
        };

        let Some(data_type) = self.tag_column_type(&column.name)? else {
            return Ok(());
        };
        if !data_type.is_string() {
            return Ok(());
        }

        let predicate = Predicate::RegexMatch(RegexMatchPredicate {
            pattern: pattern.clone(),
        });
        self.add_predicate(&column.name, predicate);
        Ok(())
    }
}
