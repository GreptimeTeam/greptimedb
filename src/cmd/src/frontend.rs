// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use clap::Parser;
use frontend::frontend::{Frontend, FrontendOptions, Mode};
use frontend::grpc::GrpcOptions;
use frontend::influxdb::InfluxdbOptions;
use frontend::instance::Instance;
use frontend::mysql::MysqlOptions;
use frontend::opentsdb::OpentsdbOptions;
use frontend::postgres::PostgresOptions;
use meta_client::MetaClientOpts;
use snafu::ResultExt;

use crate::error::{self, Result};
use crate::toml_loader;

#[derive(Parser)]
pub struct Command {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

impl Command {
    pub async fn run(self) -> Result<()> {
        self.subcmd.run().await
    }
}

#[derive(Parser)]
enum SubCommand {
    Start(StartCommand),
}

impl SubCommand {
    async fn run(self) -> Result<()> {
        match self {
            SubCommand::Start(cmd) => cmd.run().await,
        }
    }
}

#[derive(Debug, Parser)]
pub struct StartCommand {
    #[clap(long)]
    http_addr: Option<String>,
    #[clap(long)]
    grpc_addr: Option<String>,
    #[clap(long)]
    mysql_addr: Option<String>,
    #[clap(long)]
    postgres_addr: Option<String>,
    #[clap(long)]
    opentsdb_addr: Option<String>,
    #[clap(short, long)]
    config_file: Option<String>,
    #[clap(short, long)]
    influxdb_enable: Option<bool>,
    #[clap(long)]
    metasrv_addr: Option<String>,
}

impl StartCommand {
    async fn run(self) -> Result<()> {
        let opts: FrontendOptions = self.try_into()?;
        let mut frontend = Frontend::new(
            opts.clone(),
            Instance::try_new(&opts)
                .await
                .context(error::StartFrontendSnafu)?,
        );
        frontend.start().await.context(error::StartFrontendSnafu)
    }
}

impl TryFrom<StartCommand> for FrontendOptions {
    type Error = error::Error;

    fn try_from(cmd: StartCommand) -> Result<Self> {
        let mut opts: FrontendOptions = if let Some(path) = cmd.config_file {
            toml_loader::from_file!(&path)?
        } else {
            FrontendOptions::default()
        };

        if let Some(addr) = cmd.http_addr {
            opts.http_addr = Some(addr);
        }
        if let Some(addr) = cmd.grpc_addr {
            opts.grpc_options = Some(GrpcOptions {
                addr,
                ..Default::default()
            });
        }
        if let Some(addr) = cmd.mysql_addr {
            opts.mysql_options = Some(MysqlOptions {
                addr,
                ..Default::default()
            });
        }
        if let Some(addr) = cmd.postgres_addr {
            opts.postgres_options = Some(PostgresOptions {
                addr,
                ..Default::default()
            });
        }
        if let Some(addr) = cmd.opentsdb_addr {
            opts.opentsdb_options = Some(OpentsdbOptions {
                addr,
                ..Default::default()
            });
        }
        if let Some(enable) = cmd.influxdb_enable {
            opts.influxdb_options = Some(InfluxdbOptions { enable });
        }
        if let Some(metasrv_addr) = cmd.metasrv_addr {
            opts.meta_client_opts
                .get_or_insert_with(|| MetaClientOpts::default())
                .metasrv_addr = metasrv_addr
                .split(",")
                .map(&str::trim)
                .map(&str::to_string)
                .collect::<Vec<_>>();
            opts.mode = Mode::Distributed;
        }
        Ok(opts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_try_from_start_command() {
        let command = StartCommand {
            http_addr: Some("127.0.0.1:1234".to_string()),
            grpc_addr: None,
            mysql_addr: Some("127.0.0.1:5678".to_string()),
            postgres_addr: Some("127.0.0.1:5432".to_string()),
            opentsdb_addr: Some("127.0.0.1:4321".to_string()),
            influxdb_enable: Some(false),
            config_file: None,
            metasrv_addr: None,
        };

        let opts: FrontendOptions = command.try_into().unwrap();
        assert_eq!(opts.http_addr, Some("127.0.0.1:1234".to_string()));
        assert_eq!(opts.mysql_options.as_ref().unwrap().addr, "127.0.0.1:5678");
        assert_eq!(
            opts.postgres_options.as_ref().unwrap().addr,
            "127.0.0.1:5432"
        );
        assert_eq!(
            opts.opentsdb_options.as_ref().unwrap().addr,
            "127.0.0.1:4321"
        );

        let default_opts = FrontendOptions::default();
        assert_eq!(
            opts.grpc_options.unwrap().addr,
            default_opts.grpc_options.unwrap().addr
        );
        assert_eq!(
            opts.mysql_options.as_ref().unwrap().runtime_size,
            default_opts.mysql_options.as_ref().unwrap().runtime_size
        );
        assert_eq!(
            opts.postgres_options.as_ref().unwrap().runtime_size,
            default_opts.postgres_options.as_ref().unwrap().runtime_size
        );
        assert_eq!(
            opts.opentsdb_options.as_ref().unwrap().runtime_size,
            default_opts.opentsdb_options.as_ref().unwrap().runtime_size
        );

        assert!(!opts.influxdb_options.unwrap().enable);
    }
}
