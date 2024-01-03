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

mod empty_metric;
mod histogram_fold;
mod instant_manipulate;
mod normalize;
mod planner;
mod range_manipulate;
mod series_divide;
#[cfg(test)]
mod test_util;
mod union_distinct_on;

use datafusion::arrow::datatypes::{ArrowPrimitiveType, TimestampMillisecondType};
pub use empty_metric::{build_special_time_expr, EmptyMetric, EmptyMetricExec, EmptyMetricStream};
pub use histogram_fold::{HistogramFold, HistogramFoldExec, HistogramFoldStream};
pub use instant_manipulate::{InstantManipulate, InstantManipulateExec, InstantManipulateStream};
pub use normalize::{SeriesNormalize, SeriesNormalizeExec, SeriesNormalizeStream};
pub use planner::PromExtensionPlanner;
pub use range_manipulate::{RangeManipulate, RangeManipulateExec, RangeManipulateStream};
pub use series_divide::{SeriesDivide, SeriesDivideExec, SeriesDivideStream};
pub use union_distinct_on::{UnionDistinctOn, UnionDistinctOnExec, UnionDistinctOnStream};

pub(crate) type Millisecond = <TimestampMillisecondType as ArrowPrimitiveType>::Native;
