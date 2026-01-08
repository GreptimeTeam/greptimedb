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

#![feature(assert_matches, let_chains)]

use async_trait::async_trait;
use common_telemetry::{error, info};
use stat::{get_cpu_limit, get_memory_limit};

use crate::error::Result;

pub mod cli;
pub mod datanode;
pub mod error;
pub mod flownode;
pub mod frontend;
pub mod metasrv;
pub mod options;
pub mod standalone;

lazy_static::lazy_static! {
    static ref APP_VERSION: prometheus::IntGaugeVec =
        prometheus::register_int_gauge_vec!("greptime_app_version", "app version", &["version", "short_version", "app"]).unwrap();

    static ref CPU_LIMIT: prometheus::IntGaugeVec =
        prometheus::register_int_gauge_vec!("greptime_cpu_limit_in_millicores", "cpu limit in millicores", &["app"]).unwrap();

    static ref MEMORY_LIMIT: prometheus::IntGaugeVec =
        prometheus::register_int_gauge_vec!("greptime_memory_limit_in_bytes", "memory limit in bytes", &["app"]).unwrap();
}

/// wait for the close signal, for unix platform it's SIGINT or SIGTERM
#[cfg(unix)]
async fn start_wait_for_close_signal() -> std::io::Result<()> {
    use tokio::signal::unix::{signal, SignalKind};
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;

    tokio::select! {
        _ = sigint.recv() => {
            info!("Received SIGINT, shutting down");
        }
        _ = sigterm.recv() => {
            info!("Received SIGTERM, shutting down");
        }
    }

    Ok(())
}

/// wait for the close signal, for non-unix platform it's ctrl-c
#[cfg(not(unix))]
async fn start_wait_for_close_signal() -> std::io::Result<()> {
    tokio::signal::ctrl_c().await
}

#[async_trait]
pub trait App: Send {
    fn name(&self) -> &str;

    /// A hook for implementor to make something happened before actual startup. Defaults to no-op.
    async fn pre_start(&mut self) -> Result<()> {
        Ok(())
    }

    async fn start(&mut self) -> Result<()>;

    /// Waits the quit signal by default.
    fn wait_signal(&self) -> bool {
        true
    }

    async fn stop(&mut self) -> Result<()>;

    async fn run(&mut self) -> Result<()> {
        info!("Starting app: {}", self.name());

        self.pre_start().await?;

        self.start().await?;

        if self.wait_signal() {
            if let Err(e) = start_wait_for_close_signal().await {
                error!(e; "Failed to listen for close signal");
                // It's unusual to fail to listen for close signal, maybe there's something unexpected in
                // the underlying system. So we stop the app instead of running nonetheless to let people
                // investigate the issue.
            }
        }

        self.stop().await?;
        info!("Goodbye!");
        Ok(())
    }
}

/// Log the versions of the application.
///
/// `version` should be the same as the output of cli "--version";
/// and the `short_version` is the short version of the codes, often consist of git branch and commit.
pub fn log_versions(version: &str, short_version: &str, app: &str) {
    // Report app version as gauge.
    APP_VERSION
        .with_label_values(&[common_version::version(), short_version, app])
        .inc();

    info!("GreptimeDB version: {}", version);
}

pub fn create_resource_limit_metrics(app: &str) {
    if let Some(cpu_limit) = get_cpu_limit() {
        info!(
            "GreptimeDB start with cpu limit in millicores: {}",
            cpu_limit
        );
        CPU_LIMIT.with_label_values(&[app]).set(cpu_limit);
    }

    if let Some(memory_limit) = get_memory_limit() {
        info!(
            "GreptimeDB start with memory limit in bytes: {}",
            memory_limit
        );
        MEMORY_LIMIT.with_label_values(&[app]).set(memory_limit);
    }
}
