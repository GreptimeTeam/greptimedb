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

use std::time::Duration;

use common_base::Plugins;
use common_telemetry::metric::{convert_metric_to_write_request, MetricFilter};
use common_telemetry::{error, info};
use common_time::Timestamp;
use prost::Message;
use reqwest::Response;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use tokio::time;

use crate::error::{InvalidRemoteWriteConfigSnafu, Result, SendPromRemoteRequestSnafu};
use crate::prom_store::snappy_compress;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct SystemMetricOption {
    pub enable: bool,
    pub endpoint: String,
    pub db: String,
    #[serde(with = "humantime_serde")]
    pub write_interval: Duration,
}

impl Default for SystemMetricOption {
    fn default() -> Self {
        Self {
            enable: false,
            endpoint: "127.0.0.1:4000".to_string(),
            db: String::new(),
            write_interval: Duration::from_secs(30),
        }
    }
}

#[derive(Default, Clone)]
pub struct SystemMetricTask {
    config: SystemMetricOption,
    filter: Option<MetricFilter>,
}

impl SystemMetricTask {
    pub fn try_new(config: &SystemMetricOption, plugins: Option<&Plugins>) -> Result<Option<Self>> {
        if !config.enable {
            return Ok(None);
        }
        let filter = plugins.map(|p| p.get::<MetricFilter>()).unwrap_or(None);
        ensure!(
            config.write_interval.as_secs() != 0,
            InvalidRemoteWriteConfigSnafu {
                msg: "Expected System metric write_interval greater than zero"
            }
        );
        ensure!(
            !config.db.is_empty(),
            InvalidRemoteWriteConfigSnafu {
                msg: "Expected System metric db not empty"
            }
        );
        Ok(Some(Self {
            config: config.clone(),
            filter,
        }))
    }
    pub fn start(&self) {
        if !self.config.enable {
            return;
        }
        let mut interval = time::interval(self.config.write_interval);
        let sec = self.config.write_interval.as_secs();
        let endpoint = format!(
            "http://{}/v1/prometheus/write?db={}",
            self.config.endpoint, self.config.db
        );
        let filter = self.filter.clone();
        let _handle = common_runtime::spawn_bg(async move {
            info!(
                "Start system metric task to endpoint: {}, interval: {}s",
                endpoint, sec
            );
            // Pass the first tick. Because the first tick completes immediately.
            interval.tick().await;
            loop {
                interval.tick().await;
                match write_system_metric(&endpoint, filter.as_ref()).await {
                    Ok(resp) => {
                        if !resp.status().is_success() {
                            error!("report system metric error, msg: {:#?}", resp);
                        }
                    }
                    Err(e) => error!("report system metric failed, error {}", e),
                };
            }
        });
    }
}

/// Export the collected metrics, encode metrics into [RemoteWrite format](https://prometheus.io/docs/concepts/remote_write_spec/),
/// and send metrics to Prometheus remote-write compatible receiver (e.g. `greptimedb`) specified by `url`.
/// User could use `MetricFilter` to filter metric they don't want collect
pub async fn write_system_metric(url: &str, filter: Option<&MetricFilter>) -> Result<Response> {
    let metric_families = prometheus::gather();
    let request = convert_metric_to_write_request(
        metric_families,
        filter,
        Timestamp::current_millis().value(),
    );
    // RemoteWrite format require compress by snappy
    let content = snappy_compress(&request.encode_to_vec())?;
    let client = reqwest::Client::new();
    client
        .post(url)
        .header("X-Prometheus-Remote-Write-Version", "0.1.0")
        .header("Content-Type", "application/x-protobuf")
        .body(content)
        .send()
        .await
        .context(SendPromRemoteRequestSnafu)
}
