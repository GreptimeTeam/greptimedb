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

use std::env;
use std::time::Duration;

use common_runtime::error::{Error, Result};
use common_runtime::{RepeatedTask, TaskFunction};
use common_telemetry::debug;
use reqwest::{Client, Response};
use serde::{Deserialize, Serialize};

pub const TELEMETRY_URL: &str = "https://api-preview.greptime.cloud/db/otel/statistics";

pub static TELEMETRY_INTERVAL: Duration = Duration::from_secs(30);

pub static TELEMETRY_UUID_KEY: &str = "greptimedb_telemetry_uuid";

pub type GreptimeDBTelemetryTask = RepeatedTask<Error>;

#[derive(Serialize, Deserialize, Debug)]
struct StatisticData {
    pub os: String,
    pub version: String,
    pub arch: String,
    pub mode: Mode,
    pub git_commit: String,
    pub nodes: Option<i32>,
    pub uuid: String,
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Distributed,
    Standalone,
}

#[async_trait::async_trait]
pub trait Collector {
    fn get_version(&self) -> String {
        env!("CARGO_PKG_VERSION").to_string()
    }
    fn get_git_hash(&self) -> String {
        env!("GIT_COMMIT").to_string()
    }
    fn get_os(&self) -> String {
        env::consts::OS.to_string()
    }

    fn get_arch(&self) -> String {
        env::consts::ARCH.to_string()
    }

    fn get_mode(&self) -> Mode;
    async fn get_nodes(&self) -> i32;
    async fn get_uuid(&mut self) -> String;
}

pub struct GreptimeDBTelemetry {
    statistics: Box<dyn Collector + Send + Sync>,
    client: Option<Client>,
    telemetry_url: &'static str,
}

#[async_trait::async_trait]
impl TaskFunction<Error> for GreptimeDBTelemetry {
    fn name(&self) -> &str {
        "Greptimedb-telemetry-task"
    }

    async fn call(&mut self) -> Result<()> {
        self.report_telemetry_info().await;
        Ok(())
    }
}

impl GreptimeDBTelemetry {
    pub fn new(statistics: Box<dyn Collector + Send + Sync>) -> Self {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(3))
            .build();
        Self {
            statistics,
            client: client.ok(),
            telemetry_url: TELEMETRY_URL,
        }
    }
    pub async fn report_telemetry_info(&mut self) -> Option<Response> {
        let data = StatisticData {
            os: self.statistics.get_os(),
            version: self.statistics.get_version(),
            git_commit: self.statistics.get_git_hash(),
            arch: self.statistics.get_arch(),
            mode: self.statistics.get_mode(),
            nodes: Some(self.statistics.get_nodes().await),
            uuid: self.statistics.get_uuid().await,
        };

        if let Some(client) = self.client.as_ref() {
            debug!("report version: {:?}", data);
            client
                .post(self.telemetry_url)
                .json(&data)
                .send()
                .await
                .ok()
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::env;

    use hyper::service::{make_service_fn, service_fn};
    use hyper::Server;
    use tokio::spawn;

    use crate::{Collector, GreptimeDBTelemetry, Mode, StatisticData};

    async fn echo(req: hyper::Request<hyper::Body>) -> hyper::Result<hyper::Response<hyper::Body>> {
        Ok(hyper::Response::new(req.into_body()))
    }

    #[tokio::test]
    async fn test_gretimedb_telemetry() {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        spawn(async move {
            let make_svc = make_service_fn(|_conn| {
                // This is the `Service` that will handle the connection.
                // `service_fn` is a helper to convert a function that
                // returns a Response into a `Service`.
                async { Ok::<_, Infallible>(service_fn(echo)) }
            });

            let addr = ([127, 0, 0, 1], 9527).into();

            let server = Server::bind(&addr).serve(make_svc);
            let graceful = server.with_graceful_shutdown(async {
                rx.await.ok();
            });
            let _ = graceful.await;
            Ok::<_, Infallible>(())
        });
        struct TestStatistic {}

        #[async_trait::async_trait]
        impl Collector for TestStatistic {
            fn get_mode(&self) -> Mode {
                Mode::Standalone
            }

            async fn get_nodes(&self) -> i32 {
                1
            }

            async fn get_uuid(&mut self) -> String {
                "test".to_string()
            }
        }

        let statistic = Box::new(TestStatistic {});
        let mut report = GreptimeDBTelemetry::new(statistic);
        report.telemetry_url = "http://localhost:9527";
        let response = report.report_telemetry_info().await.unwrap();
        let body = response.json::<StatisticData>().await.unwrap();
        assert_eq!(env::consts::ARCH, body.arch);
        assert_eq!(env::consts::OS, body.os);
        assert_eq!(env!("CARGO_PKG_VERSION"), body.version);
        assert_eq!(env!("GIT_COMMIT"), body.git_commit);
        assert_eq!(Mode::Standalone, body.mode);
        assert_eq!(1, body.nodes.unwrap());
        tx.send(()).unwrap();
    }
}
