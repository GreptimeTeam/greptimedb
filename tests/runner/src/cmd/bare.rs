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

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use clap::{Parser, ValueEnum};
use sqlness::interceptor::Registry;
use sqlness::{ConfigBuilder, Runner};

use crate::cmd::SqlnessConfig;
use crate::env::bare::{Env, ServiceProvider, StoreConfig, WalConfig};
use crate::{protocol_interceptor, util};

#[derive(ValueEnum, Debug, Clone)]
#[clap(rename_all = "snake_case")]
enum Wal {
    RaftEngine,
    Kafka,
}

// add a group to ensure that all server addresses are set together
#[derive(clap::Args, Debug, Clone, Default)]
pub(crate) struct ServerAddr {
    /// Address of the grpc server.
    #[clap(short, long)]
    pub(crate) server_addr: Option<String>,

    /// Address of the postgres server. Must be set if server_addr is set.
    #[clap(short, long, requires = "server_addr")]
    pub(crate) pg_server_addr: Option<String>,

    /// Address of the mysql server. Must be set if server_addr is set.
    #[clap(short, long, requires = "server_addr")]
    pub(crate) mysql_server_addr: Option<String>,
}

#[derive(Debug, Parser)]
/// Run sqlness tests in bare mode.
pub struct BareCommand {
    #[clap(flatten)]
    config: SqlnessConfig,

    /// Addresses of the server.
    #[command(flatten)]
    server_addr: ServerAddr,

    /// The type of Wal.
    #[clap(short, long, default_value = "raft_engine")]
    wal: Wal,

    /// The kafka wal broker endpoints. This config will suppress sqlness runner
    /// from starting a kafka cluster, and use the given endpoint as kafka backend.
    #[clap(short, long)]
    kafka_wal_broker_endpoints: Option<String>,

    /// The path to the directory where GreptimeDB's binaries resides.
    /// If not set, sqlness will build GreptimeDB on the fly.
    #[clap(long)]
    bins_dir: Option<PathBuf>,

    /// Preserve persistent state in the temporary directory.
    /// This may affect future test runs.
    #[clap(long)]
    preserve_state: bool,

    /// Pull Different versions of GreptimeDB on need.
    #[clap(long, default_value = "true")]
    pull_version_on_need: bool,

    /// The store addresses for metadata, if empty, will use memory store.
    #[clap(long)]
    store_addrs: Vec<String>,

    /// Whether to setup etcd, by default it is false.
    #[clap(long, default_value = "false")]
    setup_etcd: bool,

    /// Whether to setup pg, by default it is false.
    #[clap(long, default_missing_value = "", num_args(0..=1))]
    setup_pg: Option<ServiceProvider>,

    /// Whether to setup mysql, by default it is false.
    #[clap(long, default_missing_value = "", num_args(0..=1))]
    setup_mysql: Option<ServiceProvider>,

    /// The number of jobs to run in parallel. Default to half of the cores.
    #[clap(short, long, default_value = "0")]
    jobs: usize,

    /// Extra command line arguments when starting GreptimeDB binaries.
    #[clap(long)]
    extra_args: Vec<String>,

    /// Enable flat format for storage engine (sets default_experimental_flat_format = true).
    #[clap(long, default_value = "false")]
    enable_flat_format: bool,
}

impl BareCommand {
    pub async fn run(mut self) -> Result<()> {
        let temp_dir = tempfile::Builder::new()
            .prefix("sqlness")
            .tempdir()
            .context("Failed to create sqlness temp dir")?;
        let sqlness_home = temp_dir.keep();

        let mut interceptor_registry: Registry = Default::default();
        interceptor_registry.register(
            protocol_interceptor::PREFIX,
            Arc::new(protocol_interceptor::ProtocolInterceptorFactory),
        );

        if let Some(d) = &self.config.case_dir {
            ensure!(d.is_dir(), "{} is not a directory", d.display());
        }
        if self.jobs == 0 {
            self.jobs = num_cpus::get() / 2;
        }

        // normalize parallelism to 1 if any of the following conditions are met:
        // Note: parallelism in pg and mysql is possible, but need configuration.
        if self.server_addr.server_addr.is_some()
            || self.setup_etcd
            || self.setup_pg.is_some()
            || self.setup_mysql.is_some()
            || self.kafka_wal_broker_endpoints.is_some()
            || self.config.test_filter != ".*"
        {
            self.jobs = 1;
            println!(
                "Normalizing parallelism to 1 due to server addresses, etcd/pg/mysql setup, or test filter usage"
            );
        }

        let config = ConfigBuilder::default()
            .case_dir(util::get_case_dir(self.config.case_dir))
            .fail_fast(self.config.fail_fast)
            .test_filter(self.config.test_filter)
            .follow_links(true)
            .env_config_file(self.config.env_config_file)
            .interceptor_registry(interceptor_registry)
            .parallelism(self.jobs)
            .build()
            .context("Failed to build sqlness config")?;

        let wal = match self.wal {
            Wal::RaftEngine => WalConfig::RaftEngine,
            Wal::Kafka => WalConfig::Kafka {
                needs_kafka_cluster: self.kafka_wal_broker_endpoints.is_none(),
                broker_endpoints: self
                    .kafka_wal_broker_endpoints
                    .map(|s| s.split(',').map(|s| s.to_string()).collect())
                    // otherwise default to the same port in `kafka-cluster.yml`
                    .unwrap_or(vec!["127.0.0.1:9092".to_string()]),
            },
        };

        let store = StoreConfig {
            store_addrs: self.store_addrs.clone(),
            setup_etcd: self.setup_etcd,
            setup_pg: self.setup_pg,
            setup_mysql: self.setup_mysql,
            enable_flat_format: self.enable_flat_format,
        };

        let runner = Runner::new(
            config,
            Env::new(
                sqlness_home.clone(),
                self.server_addr,
                wal,
                self.pull_version_on_need,
                self.bins_dir,
                store,
                self.extra_args,
            ),
        );
        let run_result = runner.run().await;

        // clean up and exit
        if !self.preserve_state {
            if self.setup_etcd {
                println!("Stopping etcd");
                util::stop_rm_etcd();
            }
            // TODO(weny): remove postgre and mysql containers
            println!("Removing state in {:?}", sqlness_home);
            if let Err(err) = tokio::fs::remove_dir_all(&sqlness_home).await {
                println!(
                    "Warning: failed to remove sqlness state dir {}: {}",
                    sqlness_home.display(),
                    err
                );
            }
        }

        if let Err(err) = run_result {
            return Err(err).context("Sqlness tests failed");
        }

        println!("\x1b[32mAll sqlness tests passed!\x1b[0m");
        Ok(())
    }
}
