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

use prometheus::{Encoder, TextEncoder};

/// a server that serves metrics
/// only start when datanode starts in distributed mode
#[derive(Copy, Clone)]
pub struct MetricsHandler;

impl MetricsHandler {
    pub fn render(&self) -> String {
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();
        // Gather the metrics.
        let metric_families = prometheus::gather();
        // Encode them to send.
        match encoder.encode(&metric_families, &mut buffer) {
            Ok(_) => match String::from_utf8(buffer) {
                Ok(s) => s,
                Err(e) => e.to_string(),
            },
            Err(e) => e.to_string(),
        }
    }
}
