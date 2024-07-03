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

use lazy_static::lazy_static;
use prometheus::{register_histogram_vec, HistogramVec};

lazy_static! {
    pub static ref METRIC_PIPELINE_CREATE_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "greptime_pipeline_create_duration_seconds",
        "Histogram of the pipeline creation duration",
        &["success"]
    )
    .unwrap();
    pub static ref METRIC_PIPELINE_DELETE_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "greptime_pipeline_delete_duration_seconds",
        "Histogram of the pipeline deletion duration",
        &["success"]
    )
    .unwrap();
    pub static ref METRIC_PIPELINE_RETRIEVE_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "greptime_pipeline_retrieve_duration_seconds",
        "Histogram of the pipeline retrieval duration",
        &["success"]
    )
    .unwrap();
}
