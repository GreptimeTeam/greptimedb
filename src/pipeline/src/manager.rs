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

use api::v1::value::ValueData;
use api::v1::ColumnDataType;
use common_time::timestamp::TimeUnit;
use common_time::Timestamp;
use datatypes::timestamp::TimestampNanosecond;
use itertools::Itertools;
use session::context::Channel;
use snafu::ensure;
use util::to_pipeline_version;

use crate::error::{CastTypeSnafu, InvalidCustomTimeIndexSnafu, PipelineMissingSnafu, Result};
use crate::etl::value::time::{MS_RESOLUTION, NS_RESOLUTION, S_RESOLUTION, US_RESOLUTION};
use crate::table::PipelineTable;
use crate::{GreptimePipelineParams, Pipeline, Value};

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
pub type PipelineRef = Arc<Pipeline>;

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
pub const GREPTIME_INTERNAL_TRACE_PIPELINE_V0_NAME: &str = "greptime_trace_v0";
pub const GREPTIME_INTERNAL_TRACE_PIPELINE_V1_NAME: &str = "greptime_trace_v1";

/// Enum for holding information of a pipeline, which is either pipeline itself,
/// or information that be used to retrieve a pipeline from `PipelineHandler`
#[derive(Debug, Clone)]
pub enum PipelineDefinition {
    Resolved(Arc<Pipeline>),
    ByNameAndValue((String, PipelineVersion)),
    GreptimeIdentityPipeline(Option<IdentityTimeIndex>),
}

impl PipelineDefinition {
    pub fn from_name(
        name: &str,
        version: PipelineVersion,
        custom_time_index: Option<(String, bool)>,
    ) -> Result<Self> {
        if name == GREPTIME_INTERNAL_IDENTITY_PIPELINE_NAME {
            Ok(Self::GreptimeIdentityPipeline(
                custom_time_index
                    .map(|(config, ignore_errors)| {
                        IdentityTimeIndex::from_config(config, ignore_errors)
                    })
                    .transpose()?,
            ))
        } else {
            Ok(Self::ByNameAndValue((name.to_owned(), version)))
        }
    }

    pub fn is_identity(&self) -> bool {
        matches!(self, Self::GreptimeIdentityPipeline(_))
    }

    pub fn get_custom_ts(&self) -> Option<&IdentityTimeIndex> {
        if let Self::GreptimeIdentityPipeline(custom_ts) = self {
            custom_ts.as_ref()
        } else {
            None
        }
    }
}

pub struct PipelineContext<'a> {
    pub pipeline_definition: &'a PipelineDefinition,
    pub pipeline_param: &'a GreptimePipelineParams,
    pub channel: Channel,
}

impl<'a> PipelineContext<'a> {
    pub fn new(
        pipeline_definition: &'a PipelineDefinition,
        pipeline_param: &'a GreptimePipelineParams,
        channel: Channel,
    ) -> Self {
        Self {
            pipeline_definition,
            pipeline_param,
            channel,
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
        default_pipeline: Option<PipelineWay>,
    ) -> Result<PipelineWay> {
        if let Some(pipeline_name) = name {
            if pipeline_name == GREPTIME_INTERNAL_TRACE_PIPELINE_V1_NAME {
                Ok(PipelineWay::OtlpTraceDirectV1)
            } else if pipeline_name == GREPTIME_INTERNAL_TRACE_PIPELINE_V0_NAME {
                Ok(PipelineWay::OtlpTraceDirectV0)
            } else {
                Ok(PipelineWay::Pipeline(PipelineDefinition::from_name(
                    pipeline_name,
                    to_pipeline_version(version)?,
                    None,
                )?))
            }
        } else if let Some(default_pipeline) = default_pipeline {
            Ok(default_pipeline)
        } else {
            PipelineMissingSnafu.fail()
        }
    }
}

const IDENTITY_TS_EPOCH: &str = "epoch";
const IDENTITY_TS_DATESTR: &str = "datestr";

#[derive(Debug, Clone)]
pub enum IdentityTimeIndex {
    Epoch(String, TimeUnit, bool),
    DateStr(String, String, bool),
}

impl IdentityTimeIndex {
    pub fn from_config(config: String, ignore_errors: bool) -> Result<Self> {
        let parts = config.split(';').collect::<Vec<&str>>();
        ensure!(
            parts.len() == 3,
            InvalidCustomTimeIndexSnafu {
                config,
                reason: "config format: '<field>;<type>;<config>'",
            }
        );

        let field = parts[0].to_string();
        match parts[1] {
            IDENTITY_TS_EPOCH => match parts[2] {
                NS_RESOLUTION => Ok(IdentityTimeIndex::Epoch(
                    field,
                    TimeUnit::Nanosecond,
                    ignore_errors,
                )),
                US_RESOLUTION => Ok(IdentityTimeIndex::Epoch(
                    field,
                    TimeUnit::Microsecond,
                    ignore_errors,
                )),
                MS_RESOLUTION => Ok(IdentityTimeIndex::Epoch(
                    field,
                    TimeUnit::Millisecond,
                    ignore_errors,
                )),
                S_RESOLUTION => Ok(IdentityTimeIndex::Epoch(
                    field,
                    TimeUnit::Second,
                    ignore_errors,
                )),
                _ => InvalidCustomTimeIndexSnafu {
                    config,
                    reason: "epoch type must be one of ns, us, ms, s",
                }
                .fail(),
            },
            IDENTITY_TS_DATESTR => Ok(IdentityTimeIndex::DateStr(
                field,
                parts[2].to_string(),
                ignore_errors,
            )),
            _ => InvalidCustomTimeIndexSnafu {
                config,
                reason: "identity time index type must be one of epoch, datestr",
            }
            .fail(),
        }
    }

    pub fn get_column_name(&self) -> &String {
        match self {
            IdentityTimeIndex::Epoch(field, _, _) => field,
            IdentityTimeIndex::DateStr(field, _, _) => field,
        }
    }

    pub fn get_ignore_errors(&self) -> bool {
        match self {
            IdentityTimeIndex::Epoch(_, _, ignore_errors) => *ignore_errors,
            IdentityTimeIndex::DateStr(_, _, ignore_errors) => *ignore_errors,
        }
    }

    pub fn get_datatype(&self) -> ColumnDataType {
        match self {
            IdentityTimeIndex::Epoch(_, unit, _) => match unit {
                TimeUnit::Nanosecond => ColumnDataType::TimestampNanosecond,
                TimeUnit::Microsecond => ColumnDataType::TimestampMicrosecond,
                TimeUnit::Millisecond => ColumnDataType::TimestampMillisecond,
                TimeUnit::Second => ColumnDataType::TimestampSecond,
            },
            IdentityTimeIndex::DateStr(_, _, _) => ColumnDataType::TimestampNanosecond,
        }
    }

    pub fn get_timestamp(&self, value: Option<&Value>) -> Result<ValueData> {
        match self {
            IdentityTimeIndex::Epoch(_, unit, ignore_errors) => {
                let v = match value {
                    Some(Value::Int32(v)) => *v as i64,
                    Some(Value::Int64(v)) => *v,
                    Some(Value::Uint32(v)) => *v as i64,
                    Some(Value::Uint64(v)) => *v as i64,
                    Some(Value::String(s)) => match s.parse::<i64>() {
                        Ok(v) => v,
                        Err(_) => {
                            return if_ignore_errors(
                                *ignore_errors,
                                *unit,
                                format!("failed to convert {} to number", s),
                            )
                        }
                    },
                    Some(Value::Timestamp(timestamp)) => timestamp.to_unit(unit),
                    Some(v) => {
                        return if_ignore_errors(
                            *ignore_errors,
                            *unit,
                            format!("unsupported value type to convert to timestamp: {}", v),
                        )
                    }
                    None => {
                        return if_ignore_errors(*ignore_errors, *unit, "missing field".to_string())
                    }
                };
                Ok(time_unit_to_value_data(*unit, v))
            }
            IdentityTimeIndex::DateStr(_, format, ignore_errors) => {
                let v = match value {
                    Some(Value::String(s)) => s,
                    Some(v) => {
                        return if_ignore_errors(
                            *ignore_errors,
                            TimeUnit::Nanosecond,
                            format!("unsupported value type to convert to date string: {}", v),
                        );
                    }
                    None => {
                        return if_ignore_errors(
                            *ignore_errors,
                            TimeUnit::Nanosecond,
                            "missing field".to_string(),
                        )
                    }
                };

                let timestamp = match chrono::DateTime::parse_from_str(v, format) {
                    Ok(ts) => ts,
                    Err(_) => {
                        return if_ignore_errors(
                            *ignore_errors,
                            TimeUnit::Nanosecond,
                            format!("failed to parse date string: {}, format: {}", v, format),
                        )
                    }
                };

                Ok(ValueData::TimestampNanosecondValue(
                    timestamp.timestamp_nanos_opt().unwrap_or_default(),
                ))
            }
        }
    }
}

fn if_ignore_errors(ignore_errors: bool, unit: TimeUnit, msg: String) -> Result<ValueData> {
    if ignore_errors {
        Ok(time_unit_to_value_data(
            unit,
            Timestamp::current_time(unit).value(),
        ))
    } else {
        CastTypeSnafu { msg }.fail()
    }
}

fn time_unit_to_value_data(unit: TimeUnit, v: i64) -> ValueData {
    match unit {
        TimeUnit::Nanosecond => ValueData::TimestampNanosecondValue(v),
        TimeUnit::Microsecond => ValueData::TimestampMicrosecondValue(v),
        TimeUnit::Millisecond => ValueData::TimestampMillisecondValue(v),
        TimeUnit::Second => ValueData::TimestampSecondValue(v),
    }
}
