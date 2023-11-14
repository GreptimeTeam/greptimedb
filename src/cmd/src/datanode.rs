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

use std::sync::Arc;
use std::time::Duration;

use catalog::kvbackend::MetaKvBackend;
use clap::Parser;
use common_telemetry::logging;
use datanode::config::DatanodeOptions;
use datanode::datanode::{Datanode, DatanodeBuilder};
use meta_client::MetaClientOptions;
use servers::Mode;
use snafu::{OptionExt, ResultExt};

use crate::error::{MissingConfigSnafu, Result, ShutdownDatanodeSnafu, StartDatanodeSnafu};
use crate::options::{Options, TopLevelOptions};

pub struct Instance {
    datanode: Datanode,
}

impl Instance {
    pub async fn start(&mut self) -> Result<()> {
        plugins::start_datanode_plugins(self.datanode.plugins())
            .await
            .context(StartDatanodeSnafu)?;

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
    pub async fn build(self, opts: DatanodeOptions) -> Result<Instance> {
        self.subcmd.build(opts).await
    }

    pub fn load_options(&self, top_level_opts: TopLevelOptions) -> Result<Options> {
        self.subcmd.load_options(top_level_opts)
    }
}

#[derive(Parser)]
enum SubCommand {
    Start(StartCommand),
}

impl SubCommand {
    async fn build(self, opts: DatanodeOptions) -> Result<Instance> {
        match self {
            SubCommand::Start(cmd) => cmd.build(opts).await,
        }
    }

    fn load_options(&self, top_level_opts: TopLevelOptions) -> Result<Options> {
        match self {
            SubCommand::Start(cmd) => cmd.load_options(top_level_opts),
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
    #[clap(long, value_delimiter = ',', num_args = 1..)]
    metasrv_addr: Option<Vec<String>>,
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
    fn load_options(&self, top_level_opts: TopLevelOptions) -> Result<Options> {
        let mut opts: DatanodeOptions = Options::load_layered_options(
            self.config_file.as_deref(),
            self.env_prefix.as_ref(),
            DatanodeOptions::env_list_keys(),
        )?;

        if let Some(dir) = top_level_opts.log_dir {
            opts.logging.dir = dir;
        }

        if top_level_opts.log_level.is_some() {
            opts.logging.level = top_level_opts.log_level;
        }

        if let Some(addr) = &self.rpc_addr {
            opts.rpc_addr = addr.clone();
        }

        if self.rpc_hostname.is_some() {
            opts.rpc_hostname = self.rpc_hostname.clone();
        }

        if let Some(node_id) = self.node_id {
            opts.node_id = Some(node_id);
        }

        if let Some(metasrv_addrs) = &self.metasrv_addr {
            opts.meta_client
                .get_or_insert_with(MetaClientOptions::default)
                .metasrv_addrs = metasrv_addrs.clone();
            opts.mode = Mode::Distributed;
        }

        if let (Mode::Distributed, None) = (&opts.mode, &opts.node_id) {
            return MissingConfigSnafu {
                msg: "Missing node id option",
            }
            .fail();
        }

        if let Some(data_home) = &self.data_home {
            opts.storage.data_home = data_home.clone();
        }

        if let Some(wal_dir) = &self.wal_dir {
            opts.wal.dir = Some(wal_dir.clone());
        }

        if let Some(http_addr) = &self.http_addr {
            opts.http.addr = http_addr.clone();
        }

        if let Some(http_timeout) = self.http_timeout {
            opts.http.timeout = Duration::from_secs(http_timeout)
        }

        // Disable dashboard in datanode.
        opts.http.disable_dashboard = true;

        Ok(Options::Datanode(Box::new(opts)))
    }

    async fn build(self, mut opts: DatanodeOptions) -> Result<Instance> {
        let plugins = plugins::setup_datanode_plugins(&mut opts)
            .await
            .context(StartDatanodeSnafu)?;

        logging::info!("Datanode start command: {:#?}", self);
        logging::info!("Datanode options: {:#?}", opts);

        let node_id = opts
            .node_id
            .context(MissingConfigSnafu { msg: "'node_id'" })?;

        let meta_config = opts.meta_client.as_ref().context(MissingConfigSnafu {
            msg: "'meta_client_options'",
        })?;

        let meta_client = datanode::heartbeat::new_metasrv_client(node_id, meta_config)
            .await
            .context(StartDatanodeSnafu)?;

        let meta_backend = Arc::new(MetaKvBackend {
            client: Arc::new(meta_client.clone()),
        });

        let datanode = DatanodeBuilder::new(opts, plugins)
            .with_meta_client(meta_client)
            .with_kv_backend(meta_backend)
            .enable_region_server_service()
            .enable_http_service()
            .build()
            .await
            .context(StartDatanodeSnafu)?;

        Ok(Instance { datanode })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::time::Duration;

    use common_test_util::temp_dir::create_named_temp_file;
    use datanode::config::{
        FileConfig, GcsConfig, ObjectStoreConfig, S3Config,
    };
    use servers::heartbeat_options::HeartbeatOptions;
    use servers::Mode;

    use super::*;
    use crate::options::ENV_VAR_SEP;

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

            [heartbeat]
            interval = "300ms"

            [meta_client]
            metasrv_addrs = ["127.0.0.1:3002"]
            timeout = "3s"
            connect_timeout = "5s"
            ddl_timeout = "10s"
            tcp_nodelay = true

            [wal]
            dir = "/other/wal"
            file_size = "1GB"
            purge_threshold = "50GB"
            purge_interval = "10m"
            read_batch_size = 128
            sync_write = false

            [storage]
            data_home = "/tmp/greptimedb/"
            [storage.default_store]
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
            dir = "/tmp/greptimedb/test/logs"
        "#;
        write!(file, "{}", toml_str).unwrap();

        let cmd = StartCommand {
            config_file: Some(file.path().to_str().unwrap().to_string()),
            ..Default::default()
        };

        let Options::Datanode(options) = cmd.load_options(TopLevelOptions::default()).unwrap()
        else {
            unreachable!()
        };

        assert_eq!("127.0.0.1:3001".to_string(), options.rpc_addr);
        assert_eq!(Some(42), options.node_id);
        assert_eq!("/other/wal", options.wal.dir.unwrap());

        assert_eq!(Duration::from_secs(600), options.wal.purge_interval);
        assert_eq!(1024 * 1024 * 1024, options.wal.file_size.0);
        assert_eq!(1024 * 1024 * 1024 * 50, options.wal.purge_threshold.0);
        assert!(!options.wal.sync_write);

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
        assert_eq!("/tmp/greptimedb/", options.storage.data_home);
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
        assert_eq!("/tmp/greptimedb/test/logs".to_string(), options.logging.dir);
    }

    #[test]
    fn test_try_from_cmd() {
        if let Options::Datanode(opt) = StartCommand::default()
            .load_options(TopLevelOptions::default())
            .unwrap()
        {
            assert_eq!(Mode::Standalone, opt.mode)
        }

        if let Options::Datanode(opt) = (StartCommand {
            node_id: Some(42),
            metasrv_addr: Some(vec!["127.0.0.1:3002".to_string()]),
            ..Default::default()
        })
        .load_options(TopLevelOptions::default())
        .unwrap()
        {
            assert_eq!(Mode::Distributed, opt.mode)
        }

        assert!((StartCommand {
            metasrv_addr: Some(vec!["127.0.0.1:3002".to_string()]),
            ..Default::default()
        })
        .load_options(TopLevelOptions::default())
        .is_err());

        // Providing node_id but leave metasrv_addr absent is ok since metasrv_addr has default value
        assert!((StartCommand {
            node_id: Some(42),
            ..Default::default()
        })
        .load_options(TopLevelOptions::default())
        .is_ok());
    }

    #[test]
    fn test_top_level_options() {
        let cmd = StartCommand::default();

        let options = cmd
            .load_options(TopLevelOptions {
                log_dir: Some("/tmp/greptimedb/test/logs".to_string()),
                log_level: Some("debug".to_string()),
            })
            .unwrap();

        let logging_opt = options.logging_options();
        assert_eq!("/tmp/greptimedb/test/logs", logging_opt.dir);
        assert_eq!("debug", logging_opt.level.as_ref().unwrap());
    }

    #[test]
    fn test_config_precedence_order() {
        let mut file = create_named_temp_file();
        let toml_str = r#"
            mode = "distributed"
            enable_memory_catalog = false
            node_id = 42
            rpc_addr = "127.0.0.1:3001"
            rpc_hostname = "127.0.0.1"
            rpc_runtime_size = 8

            [meta_client]
            timeout = "3s"
            connect_timeout = "5s"
            tcp_nodelay = true

            [wal]
            file_size = "1GB"
            purge_threshold = "50GB"
            purge_interval = "10m"
            sync_write = false

            [storage]
            type = "File"
            data_home = "/tmp/greptimedb/"

            [logging]
            level = "debug"
            dir = "/tmp/greptimedb/test/logs"
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

                let Options::Datanode(opts) =
                    command.load_options(TopLevelOptions::default()).unwrap()
                else {
                    unreachable!()
                };

                // Should be read from env, env > default values.
                assert_eq!(opts.wal.read_batch_size, 100,);
                assert_eq!(
                    opts.meta_client.unwrap().metasrv_addrs,
                    vec![
                        "127.0.0.1:3001".to_string(),
                        "127.0.0.1:3002".to_string(),
                        "127.0.0.1:3003".to_string()
                    ]
                );

                // Should be read from config file, config file > env > default values.
                assert_eq!(opts.wal.purge_interval, Duration::from_secs(60 * 10));

                // Should be read from cli, cli > config file > env > default values.
                assert_eq!(opts.wal.dir.unwrap(), "/other/wal/dir");

                // Should be default value.
                assert_eq!(opts.http.addr, DatanodeOptions::default().http.addr);
            },
        );
    }
}
