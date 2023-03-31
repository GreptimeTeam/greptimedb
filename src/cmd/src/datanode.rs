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

use std::time::Duration;

use clap::Parser;
use common_telemetry::logging;
use datanode::datanode::{
    Datanode, DatanodeOptions, FileConfig, ObjectStoreConfig, ProcedureConfig,
};
use meta_client::MetaClientOptions;
use servers::Mode;
use snafu::ResultExt;

use crate::error::{Error, MissingConfigSnafu, Result, ShutdownDatanodeSnafu, StartDatanodeSnafu};
use crate::toml_loader;

pub struct Instance {
    datanode: Datanode,
}

impl Instance {
    pub async fn run(&mut self) -> Result<()> {
        self.datanode.start().await.context(StartDatanodeSnafu)
    }

    pub async fn stop(&self) -> Result<()> {
        self.datanode
            .shutdown()
            .await
            .context(ShutdownDatanodeSnafu)
    }
}

#[derive(Parser)]
pub struct Command {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

impl Command {
    pub async fn build(self) -> Result<Instance> {
        self.subcmd.build().await
    }
}

#[derive(Parser)]
enum SubCommand {
    Start(StartCommand),
}

impl SubCommand {
    async fn build(self) -> Result<Instance> {
        match self {
            SubCommand::Start(cmd) => cmd.build().await,
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
    #[clap(long)]
    procedure_dir: Option<String>,
    #[clap(long)]
    http_addr: Option<String>,
    #[clap(long)]
    http_timeout: Option<u64>,
}

impl StartCommand {
    async fn build(self) -> Result<Instance> {
        logging::info!("Datanode start command: {:#?}", self);

        let opts: DatanodeOptions = self.try_into()?;

        logging::info!("Datanode options: {:#?}", opts);

        let datanode = Datanode::new(opts).await.context(StartDatanodeSnafu)?;

        Ok(Instance { datanode })
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
            opts.meta_client_options
                .get_or_insert_with(MetaClientOptions::default)
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
            opts.storage.store = ObjectStoreConfig::File(FileConfig { data_dir });
        }

        if let Some(wal_dir) = cmd.wal_dir {
            opts.wal.dir = wal_dir;
        }
        if let Some(procedure_dir) = cmd.procedure_dir {
            opts.procedure = Some(ProcedureConfig::from_file_path(procedure_dir));
        }
        if let Some(http_addr) = cmd.http_addr {
            opts.http_opts.addr = http_addr
        }
        if let Some(http_timeout) = cmd.http_timeout {
            opts.http_opts.timeout = Duration::from_secs(http_timeout)
        }

        Ok(opts)
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::io::Write;
    use std::time::Duration;

    use common_base::readable_size::ReadableSize;
    use common_test_util::temp_dir::create_named_temp_file;
    use datanode::datanode::{CompactionConfig, ObjectStoreConfig, RegionManifestConfig};
    use servers::Mode;

    use super::*;

    #[test]
    fn test_read_from_config_file() {
        let mut file = create_named_temp_file();
        let toml_str = r#"
            mode = "distributed"
            enable_memory_catalog = false
            node_id = 42
            rpc_addr = "127.0.0.1:3001"
            rpc_hostname = "127.0.0.1"
            rpc_runtime_size = 8
            mysql_addr = "127.0.0.1:4406"
            mysql_runtime_size = 2

            [meta_client_options]
            metasrv_addrs = ["127.0.0.1:3002"]
            timeout_millis = 3000
            connect_timeout_millis = 5000
            tcp_nodelay = true

            [wal]
            dir = "/tmp/greptimedb/wal"
            file_size = "1GB"
            purge_threshold = "50GB"
            purge_interval = "10m"
            read_batch_size = 128
            sync_write = false

            [storage]
            type = "File"
            data_dir = "/tmp/greptimedb/data/"

            [storage.compaction]
            max_inflight_tasks = 3
            max_files_in_level0 = 7
            max_purge_tasks = 32

            [storage.manifest]
            checkpoint_margin = 9
            gc_duration = '7s'
        "#;
        write!(file, "{}", toml_str).unwrap();

        let cmd = StartCommand {
            config_file: Some(file.path().to_str().unwrap().to_string()),
            ..Default::default()
        };
        let options: DatanodeOptions = cmd.try_into().unwrap();
        assert_eq!("127.0.0.1:3001".to_string(), options.rpc_addr);
        assert_eq!("127.0.0.1:4406".to_string(), options.mysql_addr);
        assert_eq!(2, options.mysql_runtime_size);
        assert_eq!(Some(42), options.node_id);

        assert_eq!(Duration::from_secs(600), options.wal.purge_interval);
        assert_eq!(1024 * 1024 * 1024, options.wal.file_size.0);
        assert_eq!(1024 * 1024 * 1024 * 50, options.wal.purge_threshold.0);
        assert!(!options.wal.sync_write);

        let MetaClientOptions {
            metasrv_addrs: metasrv_addr,
            timeout_millis,
            connect_timeout_millis,
            tcp_nodelay,
        } = options.meta_client_options.unwrap();

        assert_eq!(vec!["127.0.0.1:3002".to_string()], metasrv_addr);
        assert_eq!(5000, connect_timeout_millis);
        assert_eq!(3000, timeout_millis);
        assert!(tcp_nodelay);

        match &options.storage.store {
            ObjectStoreConfig::File(FileConfig { data_dir, .. }) => {
                assert_eq!("/tmp/greptimedb/data/", data_dir)
            }
            ObjectStoreConfig::S3 { .. } => unreachable!(),
            ObjectStoreConfig::Oss { .. } => unreachable!(),
        };

        assert_eq!(
            CompactionConfig {
                max_inflight_tasks: 3,
                max_files_in_level0: 7,
                max_purge_tasks: 32,
                sst_write_buffer_size: ReadableSize::mb(8),
            },
            options.storage.compaction,
        );
        assert_eq!(
            RegionManifestConfig {
                checkpoint_margin: Some(9),
                gc_duration: Some(Duration::from_secs(7)),
            },
            options.storage.manifest,
        );
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
}
