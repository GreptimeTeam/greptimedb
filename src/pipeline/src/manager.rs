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
use itertools::Itertools;
use util::to_pipeline_version;

use crate::error::Result;
use crate::table::PipelineTable;
use crate::{GreptimeTransformer, Pipeline};

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

/// SelectInfo is used to store the selected keys from OpenTelemetry record attrs
/// The key is used to uplift value from the attributes and serve as column name in the table
#[derive(Default)]
pub struct SelectInfo {
    pub keys: Vec<String>,
}

/// Try to convert a string to SelectInfo
/// The string should be a comma-separated list of keys
/// example: "key1,key2,key3"
/// The keys will be sorted and deduplicated
impl From<String> for SelectInfo {
    fn from(value: String) -> Self {
        let mut keys: Vec<String> = value.split(',').map(|s| s.to_string()).sorted().collect();
        keys.dedup();

        SelectInfo { keys }
    }
}

impl SelectInfo {
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

pub const GREPTIME_INTERNAL_IDENTITY_PIPELINE_NAME: &str = "greptime_identity";
pub const GREPTIME_INTERNAL_TRACE_PIPELINE_V1_NAME: &str = "greptime_trace_v1";

/// Enum for holding information of a pipeline, which is either pipeline itself,
/// or information that be used to retrieve a pipeline from `PipelineHandler`
pub enum PipelineDefinition {
    Resolved(Arc<Pipeline<GreptimeTransformer>>),
    ByNameAndValue((String, PipelineVersion)),
    GreptimeIdentityPipeline,
}

impl PipelineDefinition {
    pub fn from_name(name: &str, version: PipelineVersion) -> Self {
        if name == GREPTIME_INTERNAL_IDENTITY_PIPELINE_NAME {
            Self::GreptimeIdentityPipeline
        } else {
            Self::ByNameAndValue((name.to_owned(), version))
        }
    }
}

pub enum PipelineWay {
    OtlpLogDirect(Box<SelectInfo>),
    Pipeline(PipelineDefinition),
    OtlpTraceDirectV0,
    OtlpTraceDirectV1,
}

impl PipelineWay {
    pub fn from_name_and_default(
        name: Option<&str>,
        version: Option<&str>,
        default_pipeline: PipelineWay,
    ) -> Result<PipelineWay> {
        if let Some(pipeline_name) = name {
            if pipeline_name == GREPTIME_INTERNAL_TRACE_PIPELINE_V1_NAME {
                Ok(PipelineWay::OtlpTraceDirectV1)
            } else {
                Ok(PipelineWay::Pipeline(PipelineDefinition::from_name(
                    pipeline_name,
                    to_pipeline_version(version)?,
                )))
            }
        } else {
            Ok(default_pipeline)
        }
    }
}
