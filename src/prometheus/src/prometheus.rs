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

//! Prometheus protocol supportings, handles prometheus remote_write, remote_read logic
use api::prometheus::remote::{Sample, TimeSeries};
use openmetrics_parser::{MetricsExposition, PrometheusType, PrometheusValue};
use snafu::ResultExt;
use snap::raw::{Decoder, Encoder};

use crate::error::{self, Result};

mod read;
mod route;
mod write;

pub use read::*;
pub use route::*;
pub use write::*;

/// The prometheus table columns
pub const TIMESTAMP_COLUMN_NAME: &str = "greptime_timestamp";
pub const FIELD_COLUMN_NAME: &str = "greptime_value";
pub const METRIC_NAME_LABEL: &str = "__name__";

/// Metrics for push gateway protocol
pub struct Metrics {
    pub exposition: MetricsExposition<PrometheusType, PrometheusValue>,
}

#[inline]
pub fn snappy_decompress(buf: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = Decoder::new();
    decoder
        .decompress_vec(buf)
        .context(error::DecompressPromRemoteRequestSnafu)
}

#[inline]
pub fn snappy_compress(buf: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = Encoder::new();
    encoder
        .compress_vec(buf)
        .context(error::DecompressPromRemoteRequestSnafu)
}

/// Mock timeseries for test, it is both used in servers and frontend crate
/// So we present it here
pub fn mock_timeseries() -> Vec<TimeSeries> {
    vec![
        TimeSeries {
            labels: vec![
                new_label(METRIC_NAME_LABEL.to_string(), "metric1".to_string()),
                new_label("job".to_string(), "spark".to_string()),
            ],
            samples: vec![
                Sample {
                    value: 1.0f64,
                    timestamp: 1000,
                },
                Sample {
                    value: 2.0f64,
                    timestamp: 2000,
                },
            ],
            ..Default::default()
        },
        TimeSeries {
            labels: vec![
                new_label(METRIC_NAME_LABEL.to_string(), "metric2".to_string()),
                new_label("instance".to_string(), "test_host1".to_string()),
                new_label("idc".to_string(), "z001".to_string()),
            ],
            samples: vec![
                Sample {
                    value: 3.0f64,
                    timestamp: 1000,
                },
                Sample {
                    value: 4.0f64,
                    timestamp: 2000,
                },
            ],
            ..Default::default()
        },
        TimeSeries {
            labels: vec![
                new_label(METRIC_NAME_LABEL.to_string(), "metric3".to_string()),
                new_label("idc".to_string(), "z002".to_string()),
                new_label("app".to_string(), "biz".to_string()),
            ],
            samples: vec![
                Sample {
                    value: 5.0f64,
                    timestamp: 1000,
                },
                Sample {
                    value: 6.0f64,
                    timestamp: 2000,
                },
                Sample {
                    value: 7.0f64,
                    timestamp: 3000,
                },
            ],
            ..Default::default()
        },
    ]
}
