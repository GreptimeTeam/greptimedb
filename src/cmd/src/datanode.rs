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

pub mod builder;

use std::time::Duration;

use async_trait::async_trait;
use clap::Parser;
use common_config::Configurable;
use common_telemetry::logging::TracingOptions;
use common_telemetry::{info, warn};
use common_wal::config::DatanodeWalConfig;
use datanode::datanode::Datanode;
use meta_client::MetaClientOptions;
use snafu::{ensure, ResultExt};
use tracing_appender::non_blocking::WorkerGuard;

use crate::datanode::builder::InstanceBuilder;
use crate::error::{
    LoadLayeredConfigSnafu, MissingConfigSnafu, Result, ShutdownDatanodeSnafu, StartDatanodeSnafu,
};
use crate::options::{GlobalOptions, GreptimeOptions};
use crate::App;

pub const APP_NAME: &str = "greptime-datanode";

type DatanodeOptions = GreptimeOptions<datanode::config::DatanodeOptions>;

pub struct Instance {
    datanode: Datanode,

    // Keep the logging guard to prevent the worker from being dropped.
    _guard: Vec<WorkerGuard>,
}

impl Instance {
    pub fn new(datanode: Datanode, guard: Vec<WorkerGuard>) -> Self {
        Self {
            datanode,
            _guard: guard,
        }
    }

    pub fn datanode(&self) -> &Datanode {
        &self.datanode
    }

    /// allow customizing datanode for downstream projects
    pub fn datanode_mut(&mut self) -> &mut Datanode {
        &mut self.datanode
    }
}

#[async_trait]
impl App for Instance {
    fn name(&self) -> &str {
        APP_NAME
    }

    async fn start(&mut self) -> Result<()> {
        plugins::start_datanode_plugins(self.datanode.plugins())
            .await
            .context(StartDatanodeSnafu)?;

        self.datanode.start().await.context(StartDatanodeSnafu)
    }

    async fn stop(&mut self) -> Result<()> {
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
    pub async fn build_with(&self, builder: InstanceBuilder) -> Result<Instance> {
        self.subcmd.build_with(builder).await
    }

    pub fn load_options(&self, global_options: &GlobalOptions) -> Result<DatanodeOptions> {
        match &self.subcmd {
            SubCommand::Start(cmd) => cmd.load_options(global_options),
        }
    }
}

#[derive(Parser)]
enum SubCommand {
    Start(StartCommand),
}

impl SubCommand {
    async fn build_with(&self, builder: InstanceBuilder) -> Result<Instance> {
        match self {
            SubCommand::Start(cmd) => {
                info!("Building datanode with {:#?}", cmd);
                builder.build().await
            }
        }
    }
}

#[derive(Debug, Parser, Default)]
struct StartCommand {
    #[clap(long)]
    node_id: Option<u64>,
    /// The address to bind the gRPC server.
    #[clap(long, alias = "rpc-addr")]
    rpc_bind_addr: Option<String>,
    /// The address advertised to the metasrv, and used for connections from outside the host.
    /// If left empty or unset, the server will automatically use the IP address of the first network interface
    /// on the host, with the same port number as the one specified in `rpc_bind_addr`.
    #[clap(long, alias = "rpc-hostname")]
    rpc_server_addr: Option<String>,
    #[clap(long, value_delimiter = ',', num_args = 1..)]
    metasrv_addrs: Option<Vec<String>>,
    #[clap(short, long)]
    config_file: Option<String>,
    #[clap(long)]
    data_home: Option<String>,
    #[clap(long)]
    wal_dir: Option<String>,
    #[clap(long)]
    http_addr: Option<String>,
    #[clap(long)]
    http_timeout: Option<u64>,
    #[clap(long, default_value = "GREPTIMEDB_DATANODE")]
    env_prefix: String,
}

impl StartCommand {
    fn load_options(&self, global_options: &GlobalOptions) -> Result<DatanodeOptions> {
        let mut opts = DatanodeOptions::load_layered_options(
            self.config_file.as_deref(),
            self.env_prefix.as_ref(),
        )
        .context(LoadLayeredConfigSnafu)?;

        self.merge_with_cli_options(global_options, &mut opts)?;
        opts.component.sanitize();

        Ok(opts)
    }

    // The precedence order is: cli > config file > environment variables > default values.
    #[allow(deprecated)]
    fn merge_with_cli_options(
        &self,
        global_options: &GlobalOptions,
        opts: &mut DatanodeOptions,
    ) -> Result<()> {
        let opts = &mut opts.component;

        if let Some(dir) = &global_options.log_dir {
            opts.logging.dir.clone_from(dir);
        }

        if global_options.log_level.is_some() {
            opts.logging.level.clone_from(&global_options.log_level);
        }

        opts.tracing = TracingOptions {
            #[cfg(feature = "tokio-console")]
            tokio_console_addr: global_options.tokio_console_addr.clone(),
        };

        if let Some(addr) = &self.rpc_bind_addr {
            opts.grpc.bind_addr.clone_from(addr);
        } else if let Some(addr) = &opts.rpc_addr {
            warn!("Use the deprecated attribute `DatanodeOptions.rpc_addr`, please use `grpc.addr` instead.");
            opts.grpc.bind_addr.clone_from(addr);
        }

        if let Some(server_addr) = &self.rpc_server_addr {
            opts.grpc.server_addr.clone_from(server_addr);
        } else if let Some(server_addr) = &opts.rpc_hostname {
            warn!("Use the deprecated attribute `DatanodeOptions.rpc_hostname`, please use `grpc.hostname` instead.");
            opts.grpc.server_addr.clone_from(server_addr);
        }

        if let Some(runtime_size) = opts.rpc_runtime_size {
            warn!("Use the deprecated attribute `DatanodeOptions.rpc_runtime_size`, please use `grpc.runtime_size` instead.");
            opts.grpc.runtime_size = runtime_size;
        }

        if let Some(max_recv_message_size) = opts.rpc_max_recv_message_size {
            warn!("Use the deprecated attribute `DatanodeOptions.rpc_max_recv_message_size`, please use `grpc.max_recv_message_size` instead.");
            opts.grpc.max_recv_message_size = max_recv_message_size;
        }

        if let Some(max_send_message_size) = opts.rpc_max_send_message_size {
            warn!("Use the deprecated attribute `DatanodeOptions.rpc_max_send_message_size`, please use `grpc.max_send_message_size` instead.");
            opts.grpc.max_send_message_size = max_send_message_size;
        }

        if let Some(node_id) = self.node_id {
            opts.node_id = Some(node_id);
        }

        if let Some(metasrv_addrs) = &self.metasrv_addrs {
            opts.meta_client
                .get_or_insert_with(MetaClientOptions::default)
                .metasrv_addrs
                .clone_from(metasrv_addrs);
        }

        ensure!(
            opts.node_id.is_some(),
            MissingConfigSnafu {
                msg: "Missing node id option"
            }
        );

        if let Some(data_home) = &self.data_home {
            opts.storage.data_home.clone_from(data_home);
        }

        // `wal_dir` only affects raft-engine config.
        if let Some(wal_dir) = &self.wal_dir
            && let DatanodeWalConfig::RaftEngine(raft_engine_config) = &mut opts.wal
        {
            if raft_engine_config
                .dir
                .as_ref()
                .is_some_and(|original_dir| original_dir != wal_dir)
            {
                info!("The wal dir of raft-engine is altered to {wal_dir}");
            }
            raft_engine_config.dir.replace(wal_dir.clone());
        }

        if let Some(http_addr) = &self.http_addr {
            opts.http.addr.clone_from(http_addr);
        }

        if let Some(http_timeout) = self.http_timeout {
            opts.http.timeout = Duration::from_secs(http_timeout)
        }

        // Disable dashboard in datanode.
        opts.http.disable_dashboard = true;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::io::Write;
    use std::time::Duration;

    use common_config::ENV_VAR_SEP;
    use common_test_util::temp_dir::create_named_temp_file;
    use datanode::config::{FileConfig, GcsConfig, ObjectStoreConfig, S3Config};
    use servers::heartbeat_options::HeartbeatOptions;

    use super::*;
    use crate::options::GlobalOptions;

    #[test]
    fn test_deprecated_cli_options() {
        common_telemetry::init_default_ut_logging();
        let mut file = create_named_temp_file();
        let toml_str = r#"
            enable_memory_catalog = false
            node_id = 42

            rpc_addr = "127.0.0.1:4001"
            rpc_hostname = "192.168.0.1"
            [grpc]
            bind_addr = "127.0.0.1:3001"
            server_addr = "127.0.0.1"
            runtime_size = 8
        "#;
        write!(file, "{}", toml_str).unwrap();

        let cmd = StartCommand {
            config_file: Some(file.path().to_str().unwrap().to_string()),
            ..Default::default()
        };

        let options = cmd.load_options(&Default::default()).unwrap().component;
        assert_eq!("127.0.0.1:4001".to_string(), options.grpc.bind_addr);
        assert_eq!("192.168.0.1".to_string(), options.grpc.server_addr);
    }

    #[test]
    fn test_read_from_config_file() {
        let mut file = create_named_temp_file();
        let toml_str = r#"
            enable_memory_catalog = false
            node_id = 42

            [grpc]
            addr = "127.0.0.1:3001"
            hostname = "127.0.0.1"
            runtime_size = 8

            [heartbeat]
            interval = "300ms"

            [meta_client]
            metasrv_addrs = ["127.0.0.1:3002"]
            timeout = "3s"
            connect_timeout = "5s"
            ddl_timeout = "10s"
            tcp_nodelay = true

            [wal]
            provider = "raft_engine"
            dir = "/other/wal"
            file_size = "1GB"
            purge_threshold = "50GB"
            purge_interval = "10m"
            read_batch_size = 128
            sync_write = false

            [storage]
            data_home = "./greptimedb_data/"
            type = "File"

            [[storage.providers]]
            type = "Gcs"
            bucket = "foo"
            endpoint = "bar"

            [[storage.providers]]
            type = "S3"
            bucket = "foo"

            [logging]
            level = "debug"
            dir = "./greptimedb_data/test/logs"
        "#;
        write!(file, "{}", toml_str).unwrap();

        let cmd = StartCommand {
            config_file: Some(file.path().to_str().unwrap().to_string()),
            ..Default::default()
        };

        let options = cmd.load_options(&Default::default()).unwrap().component;

        assert_eq!("127.0.0.1:3001".to_string(), options.grpc.bind_addr);
        assert_eq!(Some(42), options.node_id);

        let DatanodeWalConfig::RaftEngine(raft_engine_config) = options.wal else {
            unreachable!()
        };
        assert_eq!("/other/wal", raft_engine_config.dir.unwrap());
        assert_eq!(Duration::from_secs(600), raft_engine_config.purge_interval);
        assert_eq!(1024 * 1024 * 1024, raft_engine_config.file_size.0);
        assert_eq!(
            1024 * 1024 * 1024 * 50,
            raft_engine_config.purge_threshold.0
        );
        assert!(!raft_engine_config.sync_write);

        let HeartbeatOptions {
            interval: heart_beat_interval,
            ..
        } = options.heartbeat;

        assert_eq!(300, heart_beat_interval.as_millis());

        let MetaClientOptions {
            metasrv_addrs: metasrv_addr,
            timeout,
            connect_timeout,
            ddl_timeout,
            tcp_nodelay,
            ..
        } = options.meta_client.unwrap();

        assert_eq!(vec!["127.0.0.1:3002".to_string()], metasrv_addr);
        assert_eq!(5000, connect_timeout.as_millis());
        assert_eq!(10000, ddl_timeout.as_millis());
        assert_eq!(3000, timeout.as_millis());
        assert!(tcp_nodelay);
        assert_eq!("./greptimedb_data/", options.storage.data_home);
        assert!(matches!(
            &options.storage.store,
            ObjectStoreConfig::File(FileConfig { .. })
        ));
        assert_eq!(options.storage.providers.len(), 2);
        assert!(matches!(
            options.storage.providers[0],
            ObjectStoreConfig::Gcs(GcsConfig { .. })
        ));
        assert!(matches!(
            options.storage.providers[1],
            ObjectStoreConfig::S3(S3Config { .. })
        ));

        assert_eq!("debug", options.logging.level.unwrap());
        assert_eq!(
            "./greptimedb_data/test/logs".to_string(),
            options.logging.dir
        );
    }

    #[test]
    fn test_try_from_cmd() {
        assert!((StartCommand {
            metasrv_addrs: Some(vec!["127.0.0.1:3002".to_string()]),
            ..Default::default()
        })
        .load_options(&GlobalOptions::default())
        .is_err());

        // Providing node_id but leave metasrv_addr absent is ok since metasrv_addr has default value
        assert!((StartCommand {
            node_id: Some(42),
            ..Default::default()
        })
        .load_options(&GlobalOptions::default())
        .is_ok());
    }

    #[test]
    fn test_load_log_options_from_cli() {
        let mut cmd = StartCommand::default();

        let result = cmd.load_options(&GlobalOptions {
            log_dir: Some("./greptimedb_data/test/logs".to_string()),
            log_level: Some("debug".to_string()),

            #[cfg(feature = "tokio-console")]
            tokio_console_addr: None,
        });
        // Missing node_id.
        assert_matches!(result, Err(crate::error::Error::MissingConfig { .. }));

        cmd.node_id = Some(42);

        let options = cmd
            .load_options(&GlobalOptions {
                log_dir: Some("./greptimedb_data/test/logs".to_string()),
                log_level: Some("debug".to_string()),

                #[cfg(feature = "tokio-console")]
                tokio_console_addr: None,
            })
            .unwrap()
            .component;

        let logging_opt = options.logging;
        assert_eq!("./greptimedb_data/test/logs", logging_opt.dir);
        assert_eq!("debug", logging_opt.level.as_ref().unwrap());
    }

    #[test]
    fn test_config_precedence_order() {
        let mut file = create_named_temp_file();
        let toml_str = r#"
            enable_memory_catalog = false
            node_id = 42
            rpc_addr = "127.0.0.1:3001"
            rpc_runtime_size = 8
            rpc_hostname = "10.103.174.219"

            [meta_client]
            timeout = "3s"
            connect_timeout = "5s"
            tcp_nodelay = true

            [wal]
            provider = "raft_engine"
            file_size = "1GB"
            purge_threshold = "50GB"
            purge_interval = "5m"
            sync_write = false

            [storage]
            type = "File"
            data_home = "./greptimedb_data/"

            [logging]
            level = "debug"
            dir = "./greptimedb_data/test/logs"
        "#;
        write!(file, "{}", toml_str).unwrap();

        let env_prefix = "DATANODE_UT";
        temp_env::with_vars(
            [
                (
                    // wal.purge_interval = 1m
                    [
                        env_prefix.to_string(),
                        "wal".to_uppercase(),
                        "purge_interval".to_uppercase(),
                    ]
                    .join(ENV_VAR_SEP),
                    Some("1m"),
                ),
                (
                    // wal.read_batch_size = 100
                    [
                        env_prefix.to_string(),
                        "wal".to_uppercase(),
                        "read_batch_size".to_uppercase(),
                    ]
                    .join(ENV_VAR_SEP),
                    Some("100"),
                ),
                (
                    // meta_client.metasrv_addrs = 127.0.0.1:3001,127.0.0.1:3002,127.0.0.1:3003
                    [
                        env_prefix.to_string(),
                        "meta_client".to_uppercase(),
                        "metasrv_addrs".to_uppercase(),
                    ]
                    .join(ENV_VAR_SEP),
                    Some("127.0.0.1:3001,127.0.0.1:3002,127.0.0.1:3003"),
                ),
            ],
            || {
                let command = StartCommand {
                    config_file: Some(file.path().to_str().unwrap().to_string()),
                    wal_dir: Some("/other/wal/dir".to_string()),
                    env_prefix: env_prefix.to_string(),
                    ..Default::default()
                };

                let opts = command.load_options(&Default::default()).unwrap().component;

                // Should be read from env, env > default values.
                let DatanodeWalConfig::RaftEngine(raft_engine_config) = opts.wal else {
                    unreachable!()
                };
                assert_eq!(raft_engine_config.read_batch_size, 100);
                assert_eq!(
                    opts.meta_client.unwrap().metasrv_addrs,
                    vec![
                        "127.0.0.1:3001".to_string(),
                        "127.0.0.1:3002".to_string(),
                        "127.0.0.1:3003".to_string()
                    ]
                );

                // Should be read from config file, config file > env > default values.
                assert_eq!(
                    raft_engine_config.purge_interval,
                    Duration::from_secs(60 * 5)
                );

                // Should be read from cli, cli > config file > env > default values.
                assert_eq!(raft_engine_config.dir.unwrap(), "/other/wal/dir");

                // Should be default value.
                assert_eq!(
                    opts.http.addr,
                    DatanodeOptions::default().component.http.addr
                );
                assert_eq!(opts.grpc.server_addr, "10.103.174.219");
            },
        );
    }
}
