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
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common_runtime::error::{Error, Result};
use common_runtime::{BoxedTaskFunction, RepeatedTask, TaskFunction};
use common_telemetry::{debug, info};
use common_version::build_info;
use reqwest::{Client, Response};
use serde::{Deserialize, Serialize};

/// The URL to report telemetry data.
pub const TELEMETRY_URL: &str = "https://telemetry.greptimestats.com/db/otel/statistics";
/// The local installation uuid cache file
const UUID_FILE_NAME: &str = ".greptimedb-telemetry-uuid";

/// The default interval of reporting telemetry data to greptime cloud
pub static TELEMETRY_INTERVAL: Duration = Duration::from_secs(60 * 30);
/// The default connect timeout to greptime cloud.
const GREPTIMEDB_TELEMETRY_CLIENT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// The default request timeout to greptime cloud.
const GREPTIMEDB_TELEMETRY_CLIENT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

pub enum GreptimeDBTelemetryTask {
    Enable((RepeatedTask<Error>, Arc<AtomicBool>)),
    Disable,
}

impl GreptimeDBTelemetryTask {
    pub fn should_report(&self, value: bool) {
        match self {
            GreptimeDBTelemetryTask::Enable((_, should_report)) => {
                should_report.store(value, Ordering::Relaxed);
            }
            GreptimeDBTelemetryTask::Disable => {}
        }
    }

    pub fn enable(
        interval: Duration,
        task_fn: BoxedTaskFunction<Error>,
        should_report: Arc<AtomicBool>,
    ) -> Self {
        GreptimeDBTelemetryTask::Enable((
            RepeatedTask::new(interval, task_fn).with_initial_delay(Some(Duration::ZERO)),
            should_report,
        ))
    }

    pub fn disable() -> Self {
        GreptimeDBTelemetryTask::Disable
    }

    pub fn start(&self) -> Result<()> {
        match self {
            GreptimeDBTelemetryTask::Enable((task, _)) => {
                print_anonymous_usage_data_disclaimer();
                task.start(common_runtime::global_runtime())
            }
            GreptimeDBTelemetryTask::Disable => Ok(()),
        }
    }

    pub async fn stop(&self) -> Result<()> {
        match self {
            GreptimeDBTelemetryTask::Enable((task, _)) => task.stop().await,
            GreptimeDBTelemetryTask::Disable => Ok(()),
        }
    }
}

/// Telemetry data to report
#[derive(Serialize, Deserialize, Debug)]
struct StatisticData {
    /// Operating system name, such as `linux`, `windows` etc.
    pub os: String,
    /// The greptimedb version
    pub version: String,
    /// The architecture of the CPU, such as `x86`, `x86_64` etc.
    pub arch: String,
    /// The running mode, `standalone` or `distributed`.
    pub mode: Mode,
    /// The git commit revision of greptimedb
    pub git_commit: String,
    /// The node number
    pub nodes: Option<i32>,
    /// The local installation uuid
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
        build_info().version.to_string()
    }

    fn get_git_hash(&self) -> String {
        build_info().commit.to_string()
    }

    fn get_os(&self) -> String {
        env::consts::OS.to_string()
    }

    fn get_arch(&self) -> String {
        env::consts::ARCH.to_string()
    }

    fn get_mode(&self) -> Mode;

    fn get_retry(&self) -> i32;

    fn inc_retry(&mut self);

    fn set_uuid_cache(&mut self, uuid: String);

    fn get_uuid_cache(&self) -> Option<String>;

    async fn get_nodes(&self) -> Option<i32>;

    fn get_uuid(&mut self, working_home: &Option<String>) -> Option<String> {
        match self.get_uuid_cache() {
            Some(uuid) => Some(uuid),
            None => {
                if self.get_retry() > 3 {
                    return None;
                }
                match default_get_uuid(working_home) {
                    Some(uuid) => {
                        self.set_uuid_cache(uuid.clone());
                        Some(uuid)
                    }
                    None => {
                        self.inc_retry();
                        None
                    }
                }
            }
        }
    }
}

fn print_anonymous_usage_data_disclaimer() {
    info!("Attention: GreptimeDB now collects anonymous usage data to help improve its roadmap and prioritize features.");
    info!(
        "To learn more about this anonymous program and how to deactivate it if you don't want to participate, please visit the following URL: ");
    info!("https://docs.greptime.com/reference/telemetry");
}

pub fn default_get_uuid(working_home: &Option<String>) -> Option<String> {
    let temp_dir = env::temp_dir();

    let mut path = PathBuf::new();
    path.push(
        working_home
            .as_ref()
            .map(Path::new)
            .unwrap_or_else(|| temp_dir.as_path()),
    );
    path.push(UUID_FILE_NAME);

    let path = path.as_path();
    match std::fs::read(path) {
        Ok(bytes) => Some(String::from_utf8_lossy(&bytes).to_string()),
        Err(e) => {
            if e.kind() == ErrorKind::NotFound {
                let uuid = uuid::Uuid::new_v4().to_string();
                let _ = std::fs::write(path, uuid.as_bytes());
                Some(uuid)
            } else {
                None
            }
        }
    }
}

/// Report version info to GreptimeDB.
/// We do not collect any identity-sensitive information.
/// This task is scheduled to run every 30 minutes.
/// The task will be disabled default. It can be enabled by setting the build feature `greptimedb-telemetry`
/// Collector is used to collect the version info. It can be implemented by different components.
/// client is used to send the HTTP request to GreptimeDB.
/// telemetry_url is the GreptimeDB url.
pub struct GreptimeDBTelemetry {
    statistics: Box<dyn Collector + Send + Sync>,
    client: Option<Client>,
    working_home: Option<String>,
    telemetry_url: &'static str,
    should_report: Arc<AtomicBool>,
    report_times: usize,
}

#[async_trait::async_trait]
impl TaskFunction<Error> for GreptimeDBTelemetry {
    fn name(&self) -> &str {
        "Greptimedb-telemetry-task"
    }

    async fn call(&mut self) -> Result<()> {
        if self.should_report.load(Ordering::Relaxed) {
            self.report_telemetry_info().await;
        }
        Ok(())
    }
}

impl GreptimeDBTelemetry {
    pub fn new(
        working_home: Option<String>,
        statistics: Box<dyn Collector + Send + Sync>,
        should_report: Arc<AtomicBool>,
    ) -> Self {
        let client = Client::builder()
            .connect_timeout(GREPTIMEDB_TELEMETRY_CLIENT_CONNECT_TIMEOUT)
            .timeout(GREPTIMEDB_TELEMETRY_CLIENT_REQUEST_TIMEOUT)
            .build();
        Self {
            working_home,
            statistics,
            client: client.ok(),
            telemetry_url: TELEMETRY_URL,
            should_report,
            report_times: 0,
        }
    }

    pub async fn report_telemetry_info(&mut self) -> Option<Response> {
        match self.statistics.get_uuid(&self.working_home) {
            Some(uuid) => {
                let data = StatisticData {
                    os: self.statistics.get_os(),
                    version: self.statistics.get_version(),
                    git_commit: self.statistics.get_git_hash(),
                    arch: self.statistics.get_arch(),
                    mode: self.statistics.get_mode(),
                    nodes: self.statistics.get_nodes().await,
                    uuid,
                };

                if let Some(client) = self.client.as_ref() {
                    if self.report_times == 0 {
                        info!("reporting greptimedb version: {:?}", data);
                    }
                    let result = client.post(self.telemetry_url).json(&data).send().await;
                    self.report_times += 1;
                    debug!("report version result: {:?}", result);
                    result.ok()
                } else {
                    None
                }
            }
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::env;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::Arc;
    use std::time::Duration;

    use common_test_util::ports;
    use common_version::build_info;
    use hyper::service::{make_service_fn, service_fn};
    use hyper::Server;
    use reqwest::{Client, Response};
    use tokio::spawn;

    use crate::{default_get_uuid, Collector, GreptimeDBTelemetry, Mode, StatisticData};

    static COUNT: AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

    async fn echo(req: hyper::Request<hyper::Body>) -> hyper::Result<hyper::Response<hyper::Body>> {
        let path = req.uri().path();
        if path == "/req-cnt" {
            let body = hyper::Body::from(format!(
                "{}",
                COUNT.load(std::sync::atomic::Ordering::SeqCst)
            ));
            Ok(hyper::Response::new(body))
        } else {
            COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(hyper::Response::new(req.into_body()))
        }
    }

    #[tokio::test]
    async fn test_gretimedb_telemetry() {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let port: u16 = ports::get_port() as u16;
        spawn(async move {
            let make_svc = make_service_fn(|_conn| {
                // This is the `Service` that will handle the connection.
                // `service_fn` is a helper to convert a function that
                // returns a Response into a `Service`.
                async { Ok::<_, Infallible>(service_fn(echo)) }
            });
            let addr = ([127, 0, 0, 1], port).into();

            let server = Server::bind(&addr).serve(make_svc);
            let graceful = server.with_graceful_shutdown(async {
                rx.await.ok();
            });
            let _ = graceful.await;
            Ok::<_, Infallible>(())
        });
        struct TestStatistic;

        struct FailedStatistic;

        #[async_trait::async_trait]
        impl Collector for TestStatistic {
            fn get_mode(&self) -> Mode {
                Mode::Standalone
            }

            async fn get_nodes(&self) -> Option<i32> {
                Some(1)
            }

            fn get_retry(&self) -> i32 {
                unimplemented!()
            }

            fn inc_retry(&mut self) {
                unimplemented!()
            }

            fn set_uuid_cache(&mut self, _: String) {
                unimplemented!()
            }

            fn get_uuid_cache(&self) -> Option<String> {
                unimplemented!()
            }

            fn get_uuid(&mut self, _working_home: &Option<String>) -> Option<String> {
                Some("test".to_string())
            }
        }

        #[async_trait::async_trait]
        impl Collector for FailedStatistic {
            fn get_mode(&self) -> Mode {
                Mode::Standalone
            }

            async fn get_nodes(&self) -> Option<i32> {
                None
            }

            fn get_retry(&self) -> i32 {
                unimplemented!()
            }

            fn inc_retry(&mut self) {
                unimplemented!()
            }

            fn set_uuid_cache(&mut self, _: String) {
                unimplemented!()
            }

            fn get_uuid_cache(&self) -> Option<String> {
                unimplemented!()
            }

            fn get_uuid(&mut self, _working_home: &Option<String>) -> Option<String> {
                None
            }
        }

        async fn get_telemetry_report(
            mut report: GreptimeDBTelemetry,
            url: &'static str,
        ) -> Option<Response> {
            report.telemetry_url = url;
            report.report_telemetry_info().await
        }

        fn contravariance<'a>(x: &'a str) -> &'static str
        where
            'static: 'a,
        {
            unsafe { std::mem::transmute(x) }
        }

        let working_home_temp = tempfile::Builder::new()
            .prefix("greptimedb_telemetry")
            .tempdir()
            .unwrap();
        let working_home = working_home_temp.path().to_str().unwrap().to_string();

        let test_statistic = Box::new(TestStatistic);
        let test_report = GreptimeDBTelemetry::new(
            Some(working_home.clone()),
            test_statistic,
            Arc::new(AtomicBool::new(true)),
        );
        let url = format!("http://localhost:{}", port);
        let response = {
            let url = contravariance(url.as_str());
            get_telemetry_report(test_report, url).await.unwrap()
        };

        let body = response.json::<StatisticData>().await.unwrap();
        assert_eq!(env::consts::ARCH, body.arch);
        assert_eq!(env::consts::OS, body.os);
        assert_eq!(build_info().version, body.version);
        assert_eq!(build_info().commit, body.git_commit);
        assert_eq!(Mode::Standalone, body.mode);
        assert_eq!(1, body.nodes.unwrap());

        let failed_statistic = Box::new(FailedStatistic);
        let failed_report = GreptimeDBTelemetry::new(
            Some(working_home),
            failed_statistic,
            Arc::new(AtomicBool::new(true)),
        );
        let response = {
            let url = contravariance(url.as_str());
            get_telemetry_report(failed_report, url).await
        };
        assert!(response.is_none());

        let client = Client::builder()
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap();

        let cnt_url = format!("{}/req-cnt", url);
        let response = client.get(cnt_url).send().await.unwrap();
        let body = response.text().await.unwrap();
        assert_eq!("1", body);
        tx.send(()).unwrap();
    }

    #[test]
    fn test_get_uuid() {
        let working_home_temp = tempfile::Builder::new()
            .prefix("greptimedb_telemetry")
            .tempdir()
            .unwrap();
        let working_home = working_home_temp.path().to_str().unwrap().to_string();

        let uuid = default_get_uuid(&Some(working_home.clone()));
        assert!(uuid.is_some());
        assert_eq!(uuid, default_get_uuid(&Some(working_home.clone())));
        assert_eq!(uuid, default_get_uuid(&Some(working_home)));
    }
}
