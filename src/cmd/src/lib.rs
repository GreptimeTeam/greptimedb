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
use catalog::information_schema::InformationExtension;
use client::api::v1::meta::ProcedureStatus;
use common_error::ext::BoxedError;
use common_meta::cluster::{ClusterInfo, NodeInfo};
use common_meta::datanode::RegionStat;
use common_meta::ddl::{ExecutorContext, ProcedureExecutor};
use common_meta::rpc::procedure;
use common_procedure::{ProcedureInfo, ProcedureState};
use common_telemetry::{error, info};
use meta_client::MetaClientRef;
use snafu::ResultExt;

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
}

/// wait for the close signal, for unix platform it's SIGINT or SIGTERM
#[cfg(unix)]
async fn start_wait_for_close_signal() -> std::result::Result<(), Box<dyn std::error::Error>> {
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
async fn start_wait_for_close_signal() -> std::result::Result<(), Box<dyn std::error::Error>> {
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

    async fn stop(&self) -> Result<()>;

    async fn run(&mut self) -> Result<()> {
        info!("Starting app: {}", self.name());

        self.pre_start().await?;

        self.start().await?;

        if self.wait_signal() {
            if let Err(e) = start_wait_for_close_signal().await {
                error!(e; "Failed to listen for ctrl-c signal");
                // It's unusual to fail to listen for ctrl-c signal, maybe there's something unexpected in
                // the underlying system. So we stop the app instead of running nonetheless to let people
                // investigate the issue.
            }
        }

        self.stop().await?;
        info!("Goodbye!");
        Ok(())
    }
}

/// Log the versions of the application, and the arguments passed to the cli.
///
/// `version` should be the same as the output of cli "--version";
/// and the `short_version` is the short version of the codes, often consist of git branch and commit.
pub fn log_versions(version: &str, short_version: &str, app: &str) {
    // Report app version as gauge.
    APP_VERSION
        .with_label_values(&[env!("CARGO_PKG_VERSION"), short_version, app])
        .inc();

    // Log version and argument flags.
    info!("GreptimeDB version: {}", version);

    log_env_flags();
}

fn log_env_flags() {
    info!("command line arguments");
    for argument in std::env::args() {
        info!("argument: {}", argument);
    }
}

pub struct DistributedInformationExtension {
    meta_client: MetaClientRef,
}

impl DistributedInformationExtension {
    pub fn new(meta_client: MetaClientRef) -> Self {
        Self { meta_client }
    }
}

#[async_trait::async_trait]
impl InformationExtension for DistributedInformationExtension {
    type Error = catalog::error::Error;

    async fn nodes(&self) -> std::result::Result<Vec<NodeInfo>, Self::Error> {
        self.meta_client
            .list_nodes(None)
            .await
            .map_err(BoxedError::new)
            .context(catalog::error::ListNodesSnafu)
    }

    async fn procedures(&self) -> std::result::Result<Vec<(String, ProcedureInfo)>, Self::Error> {
        let procedures = self
            .meta_client
            .list_procedures(&ExecutorContext::default())
            .await
            .map_err(BoxedError::new)
            .context(catalog::error::ListProceduresSnafu)?
            .procedures;
        let mut result = Vec::with_capacity(procedures.len());
        for procedure in procedures {
            let pid = match procedure.id {
                Some(pid) => pid,
                None => return catalog::error::ProcedureIdNotFoundSnafu {}.fail(),
            };
            let pid = procedure::pb_pid_to_pid(&pid)
                .map_err(BoxedError::new)
                .context(catalog::error::ConvertProtoDataSnafu)?;
            let status = ProcedureStatus::try_from(procedure.status)
                .map(|v| v.as_str_name())
                .unwrap_or("Unknown")
                .to_string();
            let procedure_info = ProcedureInfo {
                id: pid,
                type_name: procedure.type_name,
                start_time_ms: procedure.start_time_ms,
                end_time_ms: procedure.end_time_ms,
                state: ProcedureState::Running,
                lock_keys: procedure.lock_keys,
            };
            result.push((status, procedure_info));
        }

        Ok(result)
    }

    async fn region_stats(&self) -> std::result::Result<Vec<RegionStat>, Self::Error> {
        self.meta_client
            .list_region_stats()
            .await
            .map_err(BoxedError::new)
            .context(catalog::error::ListRegionStatsSnafu)
    }
}
