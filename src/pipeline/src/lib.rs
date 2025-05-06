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

mod dispatcher;
pub mod error;
mod etl;
mod manager;
mod metrics;
mod tablesuffix;

pub use etl::processor::Processor;
pub use etl::transform::transformer::greptime::{GreptimePipelineParams, SchemaInfo};
pub use etl::transform::transformer::identity_pipeline;
pub use etl::transform::GreptimeTransformer;
pub use etl::value::{Array, Map, Value};
pub use etl::{
    json_array_to_map, json_to_map, parse, simd_json_array_to_map, simd_json_to_map, Content,
    DispatchedTo, Pipeline, PipelineExecOutput, PipelineMap,
};
pub use manager::{
    pipeline_operator, table, util, IdentityTimeIndex, PipelineContext, PipelineDefinition,
    PipelineInfo, PipelineRef, PipelineTableRef, PipelineVersion, PipelineWay, SelectInfo,
    GREPTIME_INTERNAL_IDENTITY_PIPELINE_NAME, GREPTIME_INTERNAL_TRACE_PIPELINE_V1_NAME,
};
