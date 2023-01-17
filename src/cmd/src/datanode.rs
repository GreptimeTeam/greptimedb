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

use clap::Parser;
use common_telemetry::logging;
use datanode::datanode::{Datanode, DatanodeOptions, ObjectStoreConfig};
use meta_client::MetaClientOpts;
use servers::Mode;
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

#[derive(Debug, Parser, Default)]
struct StartCommand {
    #[clap(long)]
    node_id: Option<u64>,
    #[clap(long)]
    rpc_addr: Option<String>,
    #[clap(long)]
    rpc_hostname: Option<String>,
    #[clap(long)]
    mysql_addr: Option<String>,
    #[clap(long)]
    metasrv_addr: Option<String>,
    #[clap(short, long)]
    config_file: Option<String>,
    #[clap(long)]
    data_dir: Option<String>,
    #[clap(long)]
    wal_dir: Option<String>,
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

        if cmd.rpc_hostname.is_some() {
            opts.rpc_hostname = cmd.rpc_hostname;
        }

        if let Some(addr) = cmd.mysql_addr {
            opts.mysql_addr = addr;
        }

        if let Some(node_id) = cmd.node_id {
            opts.node_id = Some(node_id);
        }

        if let Some(meta_addr) = cmd.metasrv_addr {
            opts.meta_client_opts
                .get_or_insert_with(MetaClientOpts::default)
                .metasrv_addrs = meta_addr
                .split(',')
                .map(&str::trim)
                .map(&str::to_string)
                .collect::<_>();
            opts.mode = Mode::Distributed;
        }

        if let (Mode::Distributed, None) = (&opts.mode, &opts.node_id) {
            return MissingConfigSnafu {
                msg: "Missing node id option",
            }
            .fail();
        }

        if let Some(data_dir) = cmd.data_dir {
            opts.storage = ObjectStoreConfig::File { data_dir };
        }

        if let Some(wal_dir) = cmd.wal_dir {
            opts.wal.dir = wal_dir;
        }
        Ok(opts)
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::time::Duration;

    use datanode::datanode::ObjectStoreConfig;
    use servers::Mode;

    use super::*;

    #[test]
    fn test_read_from_config_file() {
        let cmd = StartCommand {
            config_file: Some(format!(
                "{}/../../config/datanode.example.toml",
                std::env::current_dir().unwrap().as_path().to_str().unwrap()
            )),
            ..Default::default()
        };
        let options: DatanodeOptions = cmd.try_into().unwrap();
        assert_eq!("127.0.0.1:3001".to_string(), options.rpc_addr);
        assert_eq!("/tmp/greptimedb/wal".to_string(), options.wal.dir);
        assert_eq!("127.0.0.1:4406".to_string(), options.mysql_addr);
        assert_eq!(4, options.mysql_runtime_size);
        let MetaClientOpts {
            metasrv_addrs: metasrv_addr,
            timeout_millis,
            connect_timeout_millis,
            tcp_nodelay,
        } = options.meta_client_opts.unwrap();

        assert_eq!(vec!["127.0.0.1:3002".to_string()], metasrv_addr);
        assert_eq!(5000, connect_timeout_millis);
        assert_eq!(3000, timeout_millis);
        assert!(!tcp_nodelay);

        match options.storage {
            ObjectStoreConfig::File { data_dir } => {
                assert_eq!("/tmp/greptimedb/data/".to_string(), data_dir)
            }
            ObjectStoreConfig::S3 { .. } => unreachable!(),
        };
    }

    #[test]
    fn test_try_from_cmd() {
        assert_eq!(
            Mode::Standalone,
            DatanodeOptions::try_from(StartCommand::default())
                .unwrap()
                .mode
        );

        let mode = DatanodeOptions::try_from(StartCommand {
            node_id: Some(42),
            metasrv_addr: Some("127.0.0.1:3002".to_string()),
            ..Default::default()
        })
        .unwrap()
        .mode;
        assert_matches!(mode, Mode::Distributed);

        assert!(DatanodeOptions::try_from(StartCommand {
            metasrv_addr: Some("127.0.0.1:3002".to_string()),
            ..Default::default()
        })
        .is_err());

        // Providing node_id but leave metasrv_addr absent is ok since metasrv_addr has default value
        DatanodeOptions::try_from(StartCommand {
            node_id: Some(42),
            ..Default::default()
        })
        .unwrap();
    }

    #[test]
    fn test_merge_config() {
        let dn_opts = DatanodeOptions::try_from(StartCommand {
            config_file: Some(format!(
                "{}/../../config/datanode.example.toml",
                std::env::current_dir().unwrap().as_path().to_str().unwrap()
            )),
            ..Default::default()
        })
        .unwrap();
        assert_eq!("/tmp/greptimedb/wal", dn_opts.wal.dir);
        assert_eq!(Duration::from_secs(600), dn_opts.wal.purge_interval);
        assert_eq!(1024 * 1024 * 1024, dn_opts.wal.file_size.0);
        assert_eq!(1024 * 1024 * 1024 * 50, dn_opts.wal.purge_threshold.0);
        assert!(!dn_opts.wal.sync_write);
        assert_eq!(Some(42), dn_opts.node_id);
        let MetaClientOpts {
            metasrv_addrs: metasrv_addr,
            timeout_millis,
            connect_timeout_millis,
            tcp_nodelay,
        } = dn_opts.meta_client_opts.unwrap();
        assert_eq!(vec!["127.0.0.1:3002".to_string()], metasrv_addr);
        assert_eq!(3000, timeout_millis);
        assert_eq!(5000, connect_timeout_millis);
        assert!(!tcp_nodelay);
    }
}
