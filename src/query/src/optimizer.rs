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

pub mod constant_term;
pub mod count_wildcard;
pub mod parallelize_scan;
pub mod pass_distribution;
pub mod remove_duplicate;
pub mod scan_hint;
pub mod string_normalization;
#[cfg(test)]
pub(crate) mod test_util;
pub mod transcribe_atat;
pub mod type_conversion;
#[cfg(feature = "vector_index")]
pub mod vector_search;
pub mod windowed_sort;

use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::LogicalPlan;

use crate::QueryEngineContext;

/// [`ExtensionAnalyzerRule`]s transform [`LogicalPlan`]s in some way to make
/// the plan valid prior to the rest of the DataFusion optimization process.
/// It's an extension of datafusion [`AnalyzerRule`]s but accepts [`QueryEngineContext`] as the second parameter.
pub trait ExtensionAnalyzerRule {
    /// Rewrite `plan`
    fn analyze(
        &self,
        plan: LogicalPlan,
        ctx: &QueryEngineContext,
        config: &ConfigOptions,
    ) -> Result<LogicalPlan>;
}
