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
use datanode::datanode::{Datanode, DatanodeOptions};
use frontend::frontend::Mode;
use snafu::ResultExt;

use crate::error::{Error, MissingConfigSnafu, Result, StartDatanodeSnafu};
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
struct StartCommand {
    #[clap(long)]
    node_id: Option<u64>,
    #[clap(long)]
    rpc_addr: Option<String>,
    #[clap(long)]
    mysql_addr: Option<String>,
    #[clap(long)]
    metasrv_addr: Option<String>,
    #[clap(short, long)]
    config_file: Option<String>,
}

impl StartCommand {
    async fn run(self) -> Result<()> {
        logging::info!("Datanode start command: {:#?}", self);

        let opts: DatanodeOptions = self.try_into()?;

        logging::info!("Datanode options: {:#?}", opts);

        Datanode::new(opts)
            .await
            .context(StartDatanodeSnafu)?
            .start()
            .await
            .context(StartDatanodeSnafu)
    }
}

impl TryFrom<StartCommand> for DatanodeOptions {
    type Error = Error;
    fn try_from(cmd: StartCommand) -> Result<Self> {
        let mut opts: DatanodeOptions = if let Some(path) = cmd.config_file {
            toml_loader::from_file!(&path)?
        } else {
            DatanodeOptions::default()
        };

        if let Some(addr) = cmd.rpc_addr {
            opts.rpc_addr = addr;
        }
        if let Some(addr) = cmd.mysql_addr {
            opts.mysql_addr = addr;
        }

        if let Some(node_id) = cmd.node_id {
            opts.node_id = Some(node_id);
        }

        if let Some(meta_addr) = cmd.metasrv_addr {
            opts.meta_client_opts.metasrv_addr = meta_addr;
            opts.mode = Mode::Distributed;
        }

        if let (Mode::Distributed, None) = (&opts.mode, &opts.node_id) {
            return MissingConfigSnafu {
                msg: "Missing node id option",
            }
            .fail();
        }
        Ok(opts)
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use datanode::datanode::ObjectStoreConfig;
    use frontend::frontend::Mode;

    use super::*;

    #[test]
    fn test_read_from_config_file() {
        let cmd = StartCommand {
            node_id: None,
            rpc_addr: None,
            mysql_addr: None,
            metasrv_addr: None,
            config_file: Some(format!(
                "{}/../../config/datanode.example.toml",
                std::env::current_dir().unwrap().as_path().to_str().unwrap()
            )),
        };
        let options: DatanodeOptions = cmd.try_into().unwrap();
        assert_eq!("127.0.0.1:3001".to_string(), options.rpc_addr);
        assert_eq!("/tmp/greptimedb/wal".to_string(), options.wal_dir);
        assert_eq!("127.0.0.1:3306".to_string(), options.mysql_addr);
        assert_eq!(4, options.mysql_runtime_size);
        assert_eq!(
            "1.1.1.1:3002".to_string(),
            options.meta_client_opts.metasrv_addr
        );
        assert_eq!(5000, options.meta_client_opts.connect_timeout_millis);
        assert_eq!(3000, options.meta_client_opts.timeout_millis);
        assert!(!options.meta_client_opts.tcp_nodelay);

        match options.storage {
            ObjectStoreConfig::File { data_dir } => {
                assert_eq!("/tmp/greptimedb/data/".to_string(), data_dir)
            }
        };
    }

    #[test]
    fn test_try_from_cmd() {
        assert_eq!(
            Mode::Standalone,
            DatanodeOptions::try_from(StartCommand {
                node_id: None,
                rpc_addr: None,
                mysql_addr: None,
                metasrv_addr: None,
                config_file: None
            })
            .unwrap()
            .mode
        );

        let mode = DatanodeOptions::try_from(StartCommand {
            node_id: Some(42),
            rpc_addr: None,
            mysql_addr: None,
            metasrv_addr: Some("127.0.0.1:3002".to_string()),
            config_file: None,
        })
        .unwrap()
        .mode;
        assert_matches!(mode, Mode::Distributed);

        assert!(DatanodeOptions::try_from(StartCommand {
            node_id: None,
            rpc_addr: None,
            mysql_addr: None,
            metasrv_addr: Some("127.0.0.1:3002".to_string()),
            config_file: None,
        })
        .is_err());

        // Providing node_id but leave metasrv_addr absent is ok since metasrv_addr has default value
        DatanodeOptions::try_from(StartCommand {
            node_id: Some(42),
            rpc_addr: None,
            mysql_addr: None,
            metasrv_addr: None,
            config_file: None,
        })
        .unwrap();
    }

    #[test]
    fn test_merge_config() {
        let dn_opts = DatanodeOptions::try_from(StartCommand {
            node_id: None,
            rpc_addr: None,
            mysql_addr: None,
            metasrv_addr: None,
            config_file: Some(format!(
                "{}/../../config/datanode.example.toml",
                std::env::current_dir().unwrap().as_path().to_str().unwrap()
            )),
        })
        .unwrap();
        assert_eq!(Some(42), dn_opts.node_id);
        assert_eq!("1.1.1.1:3002", dn_opts.meta_client_opts.metasrv_addr);
    }
}
