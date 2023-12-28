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

use datafusion_expr::Between;
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

        let Some(column_name) = Self::column_name(&between.expr) else {
            return Ok(());
        };
        let Some(data_type) = self.tag_column_type(column_name)? else {
            return Ok(());
        };
        let Some(low) = Self::lit_not_null(&between.low) else {
            return Ok(());
        };
        let Some(high) = Self::lit_not_null(&between.high) else {
            return Ok(());
        };

        let predicate = Predicate::Range(RangePredicate {
            range: Range {
                lower: Some(Bound {
                    inclusive: true,
                    value: Self::encode_lit(low, data_type.clone())?,
                }),
                upper: Some(Bound {
                    inclusive: true,
                    value: Self::encode_lit(high, data_type)?,
                }),
            },
        });

        self.add_predicate(column_name, predicate);
        Ok(())
    }
}
