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

use datafusion_expr::{Between, Expr as DfExpr};
use index::inverted_index::search::predicate::{Bound, Predicate, Range, RangePredicate};

use crate::error::Result;
use crate::sst::index::applier::builder::SstIndexApplierBuilder;

impl<'a> SstIndexApplierBuilder<'a> {
    /// ```sql
    /// column_name BETWEEN literal1 AND literal2
    /// ```
    pub(crate) fn collect_between(&mut self, between: &Between) -> Result<()> {
        if between.negated {
            return Ok(());
        }

        let DfExpr::Column(c) = between.expr.as_ref() else {
            return Ok(());
        };
        let low = match between.low.as_ref() {
            DfExpr::Literal(lit) if !lit.is_null() => lit,
            _ => return Ok(()),
        };
        let high = match between.high.as_ref() {
            DfExpr::Literal(lit) if !lit.is_null() => lit,
            _ => return Ok(()),
        };

        let Some(data_type) = self.tag_column_type(&c.name)? else {
            return Ok(());
        };
        let low = Self::encode_lit(low, data_type.clone())?;
        let high = Self::encode_lit(high, data_type)?;

        let predicate = Predicate::Range(RangePredicate {
            range: Range {
                lower: Some(Bound {
                    inclusive: true,
                    value: low,
                }),
                upper: Some(Bound {
                    inclusive: true,
                    value: high,
                }),
            },
        });

        self.add_predicate(&c.name, predicate);
        Ok(())
    }
}
