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

mod block_eval;
mod decoded_block;
pub(crate) mod dense_budget;
mod direct_binding;
mod evaluate;
mod execution;
mod grid;
mod online_execution;
mod online_frontier;
mod online_input;
mod online_plan;
mod online_rewrite;
mod online_scan;
mod plan;
pub(crate) mod plan_cache;
mod plan_template;
pub(crate) mod query_program;
mod range;
mod scan_max;
mod scan_max_plan;
mod scan_range;
mod scan_range_aggregate;
mod scan_range_plan;
mod series_fragment;
mod series_instant_max;
mod series_pack;
mod series_range;
mod source;
mod vector;
mod vm_codec;

pub(crate) use direct_binding::try_bind as try_bind_fragment;
pub(crate) use online_frontier::{
    build as plan_frontier, exceeds_join_grid, needs_series_execution, split as split_frontier,
    split_selectors as split_selector_frontier,
};
pub(crate) use online_input::{raw_series_input, selector_requests};
pub(crate) use online_plan::{OnlineNode, build as plan_online, unwrap_explain};
pub(crate) use online_rewrite::RewriteCollector;
pub(crate) use plan::{GridPlanner, plan};
pub(crate) use scan_max_plan::ScanMaxRule;
pub(crate) use series_fragment::SeriesFragment;
pub(crate) use series_instant_max::SeriesInstantMax;
pub(crate) use series_pack::SeriesPack;
pub(crate) use series_range::SeriesRange;
