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
use common_telemetry::logging;
use meta_srv::bootstrap;
use meta_srv::metasrv::MetaSrvOptions;
use snafu::ResultExt;

use crate::error::{Error, Result};
use crate::{error, toml_loader};

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
struct StartCommand {
    #[clap(long)]
    bind_addr: Option<String>,
    #[clap(long)]
    server_addr: Option<String>,
    #[clap(long)]
    store_addr: Option<String>,
    #[clap(short, long)]
    config_file: Option<String>,
}

impl StartCommand {
    async fn run(self) -> Result<()> {
        logging::info!("MetaSrv start command: {:#?}", self);

        let opts: MetaSrvOptions = self.try_into()?;

        logging::info!("MetaSrv options: {:#?}", opts);

        bootstrap::bootstrap_meta_srv(opts)
            .await
            .context(error::StartMetaServerSnafu)
    }
}

impl TryFrom<StartCommand> for MetaSrvOptions {
    type Error = Error;

    fn try_from(cmd: StartCommand) -> Result<Self> {
        let mut opts: MetaSrvOptions = if let Some(path) = cmd.config_file {
            toml_loader::from_file!(&path)?
        } else {
            MetaSrvOptions::default()
        };

        if let Some(addr) = cmd.bind_addr {
            opts.bind_addr = addr;
        }
        if let Some(addr) = cmd.server_addr {
            opts.server_addr = addr;
        }
        if let Some(addr) = cmd.store_addr {
            opts.store_addr = addr;
        }

        Ok(opts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_from_cmd() {
        let cmd = StartCommand {
            bind_addr: Some("127.0.0.1:3002".to_string()),
            server_addr: Some("127.0.0.01:3002".to_string()),
            store_addr: Some("127.0.0.1:2380".to_string()),
            config_file: None,
        };
        let options: MetaSrvOptions = cmd.try_into().unwrap();
        assert_eq!("127.0.0.1:3002".to_string(), options.bind_addr);
        assert_eq!("127.0.0.01:3002".to_string(), options.server_addr);
        assert_eq!("127.0.0.1:2380".to_string(), options.store_addr);
    }

    #[test]
    fn test_read_from_config_file() {
        let cmd = StartCommand {
            bind_addr: None,
            server_addr: None,
            store_addr: None,
            config_file: Some(format!(
                "{}/../../config/metasrv.example.toml",
                std::env::current_dir().unwrap().as_path().to_str().unwrap()
            )),
        };
        let options: MetaSrvOptions = cmd.try_into().unwrap();
        assert_eq!("127.0.0.01:3002".to_string(), options.bind_addr);
        assert_eq!("127.0.0.01:3002".to_string(), options.server_addr);
        assert_eq!("127.0.0.1:2379".to_string(), options.store_addr);
        assert_eq!(15, options.datanode_lease_secs);
    }
}
