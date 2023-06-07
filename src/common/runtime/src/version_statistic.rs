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
use std::result::Result as StdResult;
use std::sync::Arc;
use std::time::Duration;
use std::{env, u8};

use once_cell::sync::Lazy;
use reqwest::{Client, Response};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::repeated_task::{RepeatedTask, TaskFunction};

pub const VERSION_REPORT_URL: &str =
    "https://api.greptime.cloud/opentelemetry/greptimedb-statistic";

pub static VERSION_REPORT_INTERVAL: Lazy<Duration> = Lazy::new(|| Duration::from_secs(60 * 60));

pub type VersionReportTask = RepeatedTask<Error>;

#[derive(Serialize, Deserialize)]
struct ReportData {
    pub os: String,
    pub version: String,
    pub arch: String,
    pub mode: String,
    pub git_commit: String,
    pub nodes: Option<u8>,
}

#[async_trait::async_trait]
pub trait Statistic {
    async fn get_version(&self) -> String {
        env!("CARGO_PKG_VERSION").to_string()
    }
    async fn get_git_hash(&self) -> String {
        env!("GIT_COMMIT").to_string()
    }
    async fn get_os(&self) -> String {
        env::consts::OS.to_string()
    }

    async fn get_arch(&self) -> String {
        env::consts::ARCH.to_string()
    }
    async fn get_mode(&self) -> String;
    async fn get_nodes(&self) -> u8;
}

pub struct GreptimeVersionReport {
    statistic: Arc<dyn Statistic + Send + Sync>,
    client: Client,
    report_url: &'static str,
}

#[async_trait::async_trait]
impl TaskFunction<Error> for GreptimeVersionReport {
    fn name(&self) -> &str {
        "Greptime-vresion-report-task"
    }

    async fn call(&mut self) -> Result<()> {
        //ignore result
        let _ = self.report_version().await;
        Ok(())
    }
}

impl GreptimeVersionReport {
    pub fn new(statistic: Arc<dyn Statistic + Send + Sync>) -> Self {
        Self {
            statistic,
            client: Client::new(),
            report_url: VERSION_REPORT_URL,
        }
    }
    pub async fn report_version(&self) -> StdResult<Response, reqwest::Error> {
        let data = ReportData {
            os: self.statistic.get_os().await,
            version: self.statistic.get_version().await,
            git_commit: self.statistic.get_git_hash().await,
            arch: self.statistic.get_arch().await,
            mode: self.statistic.get_mode().await,
            nodes: Some(self.statistic.get_nodes().await),
        };

        self.client.post(self.report_url).json(&data).send().await
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::Arc;
    use std::{env, u8};

    use hyper::service::{make_service_fn, service_fn};
    use hyper::Server;
    use tokio::spawn;

    use crate::version_statistic::{GreptimeVersionReport, ReportData, Statistic};

    async fn echo(req: hyper::Request<hyper::Body>) -> hyper::Result<hyper::Response<hyper::Body>> {
        Ok(hyper::Response::new(req.into_body()))
    }

    #[tokio::test]
    async fn test_version_report() {
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
        impl Statistic for TestStatistic {
            async fn get_mode(&self) -> String {
                "test".to_string()
            }

            async fn get_nodes(&self) -> u8 {
                1
            }
        }

        let statistic = Arc::new(TestStatistic {});
        let mut report = GreptimeVersionReport::new(statistic);
        report.report_url = "http://localhost:9527";
        let response = report.report_version().await.unwrap();
        let body = response.json::<ReportData>().await.unwrap();
        assert_eq!(env::consts::ARCH, body.arch);
        assert_eq!(env::consts::OS, body.os);
        assert_eq!(env!("CARGO_PKG_VERSION"), body.version);
        assert_eq!(env!("GIT_COMMIT"), body.git_commit);
        assert_eq!("test", body.mode);
        assert_eq!(1, body.nodes.unwrap());
        tx.send(()).unwrap();
    }
}
