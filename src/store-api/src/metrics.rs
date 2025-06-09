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
    pub static ref CONVERT_REGION_BULK_REQUEST: HistogramVec = register_histogram_vec!(
        "greptime_datanode_convert_region_request",
        "datanode duration to convert region request",
        &["stage"],
        vec![
            0.001, 0.005, 0.01, 0.02, 0.03, 0.04, 0.05, 0.10, 0.15, 0.2, 0.3, 0.4, 0.5, 1.0, 1.5,
            2.0, 2.5, 3.0, 4.0, 5.0
        ]
    )
    .unwrap();
}
