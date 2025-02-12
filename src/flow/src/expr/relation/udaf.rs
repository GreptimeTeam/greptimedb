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

use std::fmt::Debug;

use datafusion_common::DFSchema;
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};

use crate::expr::ScalarExpr;
use crate::Result;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct OrderingReq {
    pub exprs: Vec<SortExpr>,
}

impl OrderingReq {
    pub fn to_lex_ordering(&self, schema: &DFSchema) -> Result<LexOrdering> {
        Ok(LexOrdering::new(
            self.exprs
                .iter()
                .map(|e| e.to_sort_phy_expr(schema))
                .collect::<Result<_>>()?,
        ))
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct SortExpr {
    /// expression representing the column to sort
    pub expr: ScalarExpr,
    /// Whether to sort in descending order
    pub descending: bool,
    /// Whether to sort nulls first
    pub nulls_first: bool,
}

impl SortExpr {
    pub fn to_sort_phy_expr(&self, schema: &DFSchema) -> Result<PhysicalSortExpr> {
        let phy = self.expr.as_physical_expr(schema)?;
        let sort_options = datafusion_common::arrow::compute::SortOptions {
            descending: self.descending,
            nulls_first: self.nulls_first,
        };
        Ok(PhysicalSortExpr {
            expr: phy,
            options: sort_options,
        })
    }
}
