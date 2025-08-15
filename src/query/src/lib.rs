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

#![feature(let_chains)]
#![feature(int_roundings)]
#![feature(try_blocks)]
#![feature(stmt_expr_attributes)]
#![feature(iterator_try_collect)]
#![feature(box_patterns)]

mod analyze;
pub mod dataframe;
pub mod datafusion;
pub mod dist_plan;
pub mod dummy_catalog;
pub mod error;
pub mod executor;
pub mod log_query;
pub mod metrics;
pub mod optimizer;
pub mod options;
pub mod parser;
mod part_sort;
pub mod physical_wrapper;
pub mod plan;
pub mod planner;
pub mod promql;
pub mod query_engine;
mod range_select;
pub mod region_query;
pub mod sql;
pub(crate) mod window_sort;

#[cfg(test)]
pub(crate) mod test_util;
#[cfg(test)]
mod tests;

pub use crate::datafusion::DfContextProviderAdapter;
pub use crate::query_engine::{
    QueryEngine, QueryEngineContext, QueryEngineFactory, QueryEngineRef,
};
