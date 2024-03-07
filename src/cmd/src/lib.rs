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
use clap::arg;
use common_telemetry::{error, info};

pub mod cli;
pub mod datanode;
pub mod error;
pub mod frontend;
pub mod metasrv;
pub mod options;
pub mod standalone;

lazy_static::lazy_static! {
    static ref APP_VERSION: prometheus::IntGaugeVec =
        prometheus::register_int_gauge_vec!("greptime_app_version", "app version", &["short_version", "version"]).unwrap();
}

#[async_trait]
pub trait App: Send {
    fn name(&self) -> &str;

    /// A hook for implementor to make something happened before actual startup. Defaults to no-op.
    async fn pre_start(&mut self) -> error::Result<()> {
        Ok(())
    }

    async fn start(&mut self) -> error::Result<()>;

    async fn stop(&self) -> error::Result<()>;
}

pub async fn start_app(mut app: Box<dyn App>) -> error::Result<()> {
    info!("Starting app: {}", app.name());

    app.pre_start().await?;

    app.start().await?;

    if let Err(e) = tokio::signal::ctrl_c().await {
        error!("Failed to listen for ctrl-c signal: {}", e);
        // It's unusual to fail to listen for ctrl-c signal, maybe there's something unexpected in
        // the underlying system. So we stop the app instead of running nonetheless to let people
        // investigate the issue.
    }

    app.stop().await?;
    info!("Goodbye!");
    Ok(())
}

pub fn log_versions() {
    // Report app version as gauge.
    APP_VERSION
        .with_label_values(&[short_version(), full_version()])
        .inc();

    // Log version and argument flags.
    info!(
        "short_version: {}, full_version: {}",
        short_version(),
        full_version()
    );

    log_env_flags();
}

pub fn greptimedb_cli() -> clap::Command {
    let cmd = clap::Command::new("greptimedb")
        .version(print_version())
        .subcommand_required(true);

    #[cfg(feature = "tokio-console")]
    let cmd = cmd.arg(arg!(--"tokio-console-addr"[TOKIO_CONSOLE_ADDR]));

    cmd.args([arg!(--"log-dir"[LOG_DIR]), arg!(--"log-level"[LOG_LEVEL])])
}

fn print_version() -> &'static str {
    concat!(
        "\nbranch: ",
        env!("GIT_BRANCH"),
        "\ncommit: ",
        env!("GIT_COMMIT"),
        "\ndirty: ",
        env!("GIT_DIRTY"),
        "\nversion: ",
        env!("CARGO_PKG_VERSION")
    )
}

fn short_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

// {app_name}-{branch_name}-{commit_short}
// The branch name (tag) of a release build should already contain the short
// version so the full version doesn't concat the short version explicitly.
fn full_version() -> &'static str {
    concat!(
        "greptimedb-",
        env!("GIT_BRANCH"),
        "-",
        env!("GIT_COMMIT_SHORT")
    )
}

fn log_env_flags() {
    info!("command line arguments");
    for argument in std::env::args() {
        info!("argument: {}", argument);
    }
}
