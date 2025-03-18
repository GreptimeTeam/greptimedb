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

use std::collections::HashMap;
use std::sync::Arc;

use common_plugins::GREPTIME_EXEC_PREFIX;
use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::ExecutionPlan;
use headers::{Header, HeaderName, HeaderValue};
use hyper::HeaderMap;
use serde_json::Value;

pub mod constants {
    // New HTTP headers would better distinguish use cases among:
    // * GreptimeDB
    // * GreptimeCloud
    // * ...
    //
    // And thus trying to use:
    // * x-greptime-db-xxx
    // * x-greptime-cloud-xxx
    //
    // ... accordingly
    //
    // Most of the headers are for GreptimeDB and thus using `x-greptime-db-` as prefix.
    // Only use `x-greptime-cloud` when it's intentionally used by GreptimeCloud.

    // LEGACY HEADERS - KEEP IT UNMODIFIED
    pub const GREPTIME_DB_HEADER_FORMAT: &str = "x-greptime-format";
    pub const GREPTIME_DB_HEADER_TIMEOUT: &str = "x-greptime-timeout";
    pub const GREPTIME_DB_HEADER_EXECUTION_TIME: &str = "x-greptime-execution-time";
    pub const GREPTIME_DB_HEADER_METRICS: &str = "x-greptime-metrics";
    pub const GREPTIME_DB_HEADER_NAME: &str = "x-greptime-db-name";
    pub const GREPTIME_TIMEZONE_HEADER_NAME: &str = "x-greptime-timezone";
    pub const GREPTIME_DB_HEADER_ERROR_CODE: &str = common_error::GREPTIME_DB_HEADER_ERROR_CODE;

    // Deprecated: pipeline is also used with trace, so we remove log from it.
    pub const GREPTIME_LOG_PIPELINE_NAME_HEADER_NAME: &str = "x-greptime-log-pipeline-name";
    pub const GREPTIME_LOG_PIPELINE_VERSION_HEADER_NAME: &str = "x-greptime-log-pipeline-version";

    // More generic pipeline header name
    pub const GREPTIME_PIPELINE_NAME_HEADER_NAME: &str = "x-greptime-pipeline-name";
    pub const GREPTIME_PIPELINE_VERSION_HEADER_NAME: &str = "x-greptime-pipeline-version";

    pub const GREPTIME_LOG_TABLE_NAME_HEADER_NAME: &str = "x-greptime-log-table-name";
    pub const GREPTIME_LOG_EXTRACT_KEYS_HEADER_NAME: &str = "x-greptime-log-extract-keys";
    pub const GREPTIME_TRACE_TABLE_NAME_HEADER_NAME: &str = "x-greptime-trace-table-name";
    /// The header key that contains the pipeline params.
    pub const GREPTIME_PIPELINE_PARAMS_HEADER: &str = "x-greptime-pipeline-params";
}

pub static GREPTIME_DB_HEADER_FORMAT: HeaderName =
    HeaderName::from_static(constants::GREPTIME_DB_HEADER_FORMAT);
pub static GREPTIME_DB_HEADER_EXECUTION_TIME: HeaderName =
    HeaderName::from_static(constants::GREPTIME_DB_HEADER_EXECUTION_TIME);
pub static GREPTIME_DB_HEADER_METRICS: HeaderName =
    HeaderName::from_static(constants::GREPTIME_DB_HEADER_METRICS);

/// Header key of `db-name`. Example format of the header value is `greptime-public`.
pub static GREPTIME_DB_HEADER_NAME: HeaderName =
    HeaderName::from_static(constants::GREPTIME_DB_HEADER_NAME);

/// Header key of query specific timezone. Example format of the header value is `Asia/Shanghai` or `+08:00`.
pub static GREPTIME_TIMEZONE_HEADER_NAME: HeaderName =
    HeaderName::from_static(constants::GREPTIME_TIMEZONE_HEADER_NAME);

pub static CONTENT_TYPE_PROTOBUF_STR: &str = "application/x-protobuf";
pub static CONTENT_TYPE_PROTOBUF: HeaderValue = HeaderValue::from_static(CONTENT_TYPE_PROTOBUF_STR);
pub static CONTENT_ENCODING_SNAPPY: HeaderValue = HeaderValue::from_static("snappy");

pub static CONTENT_TYPE_NDJSON_STR: &str = "application/x-ndjson";

pub struct GreptimeDbName(Option<String>);

impl Header for GreptimeDbName {
    fn name() -> &'static HeaderName {
        &GREPTIME_DB_HEADER_NAME
    }

    fn decode<'i, I>(values: &mut I) -> Result<Self, headers::Error>
    where
        Self: Sized,
        I: Iterator<Item = &'i HeaderValue>,
    {
        if let Some(value) = values.next() {
            let str_value = value.to_str().map_err(|_| headers::Error::invalid())?;
            Ok(Self(Some(str_value.to_owned())))
        } else {
            Ok(Self(None))
        }
    }

    fn encode<E: Extend<HeaderValue>>(&self, values: &mut E) {
        if let Some(name) = &self.0 {
            if let Ok(value) = HeaderValue::from_str(name) {
                values.extend(std::iter::once(value));
            }
        }
    }
}

impl GreptimeDbName {
    pub fn value(&self) -> Option<&String> {
        self.0.as_ref()
    }
}

// collect write
pub fn write_cost_header_map(cost: usize) -> HeaderMap {
    let mut header_map = HeaderMap::new();
    if cost > 0 {
        let mut map: HashMap<String, Value> = HashMap::new();
        map.insert(
            common_plugins::GREPTIME_EXEC_WRITE_COST.to_string(),
            Value::from(cost),
        );
        let _ = serde_json::to_string(&map)
            .ok()
            .and_then(|s| HeaderValue::from_str(&s).ok())
            .and_then(|v| header_map.insert(&GREPTIME_DB_HEADER_METRICS, v));
    }
    header_map
}

fn collect_into_maps(name: &str, value: u64, maps: &mut [&mut HashMap<String, u64>]) {
    if name.starts_with(GREPTIME_EXEC_PREFIX) && value > 0 {
        maps.iter_mut().for_each(|map| {
            map.entry(name.to_string())
                .and_modify(|v| *v += value)
                .or_insert(value);
        });
    }
}

pub fn collect_plan_metrics(plan: &Arc<dyn ExecutionPlan>, maps: &mut [&mut HashMap<String, u64>]) {
    if let Some(m) = plan.metrics() {
        m.iter().for_each(|m| match m.value() {
            MetricValue::Count { name, count } => {
                collect_into_maps(name, count.value() as u64, maps);
            }
            MetricValue::Gauge { name, gauge } => {
                collect_into_maps(name, gauge.value() as u64, maps);
            }
            MetricValue::Time { name, time } => {
                if name.starts_with(GREPTIME_EXEC_PREFIX) {
                    // override
                    maps.iter_mut().for_each(|map| {
                        map.insert(name.to_string(), time.value() as u64);
                    });
                }
            }
            _ => {}
        });
    }

    for c in plan.children() {
        collect_plan_metrics(c, maps);
    }
}
