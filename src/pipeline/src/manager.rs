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

use std::sync::Arc;

use common_time::Timestamp;
use datatypes::timestamp::TimestampNanosecond;

use crate::table::PipelineTable;
use crate::{GreptimeTransformer, Pipeline};

pub mod error;
pub mod pipeline_operator;
pub mod table;
pub mod util;

/// Pipeline version. An optional timestamp with nanosecond precision.
///
/// If the version is None, it means the latest version of the pipeline.
/// User can specify the version by providing a timestamp string formatted as iso8601.
/// When it used in cache key, it will be converted to i64 meaning the number of nanoseconds since the epoch.
pub type PipelineVersion = Option<TimestampNanosecond>;

/// Pipeline info. A tuple of timestamp and pipeline reference.
pub type PipelineInfo = (Timestamp, PipelineRef);

pub type PipelineTableRef = Arc<PipelineTable>;
pub type PipelineRef = Arc<Pipeline<GreptimeTransformer>>;
