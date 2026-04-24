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

use datafusion_expr::utils::COUNT_STAR_EXPANSION;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::aggregate::AggregateFunctionExpr;
use datafusion_physical_expr::expressions::{Column as PhysicalColumn, Literal};
pub use store_api::region_engine::SupportStatAggr;

pub fn support_stat_aggr_from_aggr_expr(
    aggr: &AggregateFunctionExpr,
    time_index_column: &str,
) -> Option<SupportStatAggr> {
    match (aggr.fun().name(), aggr.expressions().as_slice()) {
        ("count", []) => Some(SupportStatAggr::CountRows),
        ("count", [arg])
            if arg
                .as_any()
                .downcast_ref::<Literal>()
                .is_some_and(|lit| lit.value() == &COUNT_STAR_EXPANSION) =>
        {
            Some(SupportStatAggr::CountRows)
        }
        ("count", [arg]) if let Some(col) = arg.as_any().downcast_ref::<PhysicalColumn>() => {
            if col.name() == time_index_column {
                Some(SupportStatAggr::CountRows)
            } else {
                Some(SupportStatAggr::CountNonNull {
                    column_name: col.name().to_string(),
                })
            }
        }
        ("min", [arg]) if let Some(col) = arg.as_any().downcast_ref::<PhysicalColumn>() => {
            Some(SupportStatAggr::MinValue {
                column_name: col.name().to_string(),
            })
        }
        ("max", [arg]) if let Some(col) = arg.as_any().downcast_ref::<PhysicalColumn>() => {
            Some(SupportStatAggr::MaxValue {
                column_name: col.name().to_string(),
            })
        }
        _ => None,
    }
}
