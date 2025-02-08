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

#![allow(clippy::print_stdout)]

use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, ValueEnum};
use env::{Env, WalConfig};
use sqlness::interceptor::Registry;
use sqlness::{ConfigBuilder, Runner};

use crate::env::StoreConfig;

mod env;
mod protocol_interceptor;
mod server_mode;
mod util;

#[derive(ValueEnum, Debug, Clone)]
#[clap(rename_all = "snake_case")]
enum Wal {
    RaftEngine,
    Kafka,
}

// add a group to ensure that all server addresses are set together
#[derive(clap::Args, Debug, Clone, Default)]
#[group(multiple = true, requires_all=["server_addr", "pg_server_addr", "mysql_server_addr"])]
struct ServerAddr {
    /// Address of the grpc server.
    #[clap(short, long)]
    server_addr: Option<String>,

    /// Address of the postgres server. Must be set if server_addr is set.
    #[clap(short, long, requires = "server_addr")]
    pg_server_addr: Option<String>,

    /// Address of the mysql server. Must be set if server_addr is set.
    #[clap(short, long, requires = "server_addr")]
    mysql_server_addr: Option<String>,
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
/// SQL Harness for GrepTimeDB
struct Args {
    /// Directory of test cases
    #[clap(short, long)]
    case_dir: Option<PathBuf>,

    /// Fail this run as soon as one case fails if true
    #[arg(short, long, default_value = "false")]
    fail_fast: bool,

    /// Environment Configuration File
    #[clap(short, long, default_value = "config.toml")]
    env_config_file: String,

    /// Name of test cases to run. Accept as a regexp.
    #[clap(short, long, default_value = ".*")]
    test_filter: String,

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
    #[clap(long, default_value = "false")]
    setup_pg: bool,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let temp_dir = tempfile::Builder::new()
        .prefix("sqlness")
        .tempdir()
        .unwrap();
    let sqlness_home = temp_dir.into_path();

    let mut interceptor_registry: Registry = Default::default();
    interceptor_registry.register(
        protocol_interceptor::PREFIX,
        Arc::new(protocol_interceptor::ProtocolInterceptorFactory),
    );

    if let Some(d) = &args.case_dir {
        if !d.is_dir() {
            panic!("{} is not a directory", d.display());
        }
    }
    let config = ConfigBuilder::default()
        .case_dir(util::get_case_dir(args.case_dir))
        .fail_fast(args.fail_fast)
        .test_filter(args.test_filter)
        .follow_links(true)
        .env_config_file(args.env_config_file)
        .interceptor_registry(interceptor_registry)
        .build()
        .unwrap();

    let wal = match args.wal {
        Wal::RaftEngine => WalConfig::RaftEngine,
        Wal::Kafka => WalConfig::Kafka {
            needs_kafka_cluster: args.kafka_wal_broker_endpoints.is_none(),
            broker_endpoints: args
                .kafka_wal_broker_endpoints
                .map(|s| s.split(',').map(|s| s.to_string()).collect())
                // otherwise default to the same port in `kafka-cluster.yml`
                .unwrap_or(vec!["127.0.0.1:9092".to_string()]),
        },
    };

    let store = StoreConfig {
        store_addrs: args.store_addrs.clone(),
        setup_etcd: args.setup_etcd,
        setup_pg: args.setup_pg,
    };

    let runner = Runner::new(
        config,
        Env::new(
            sqlness_home.clone(),
            args.server_addr.clone(),
            wal,
            args.pull_version_on_need,
            args.bins_dir,
            store,
        ),
    );
    match runner.run().await {
        Ok(_) => println!("\x1b[32mAll sqlness tests passed!\x1b[0m"),
        Err(e) => {
            println!("\x1b[31mTest failed: {}\x1b[0m", e);
            std::process::exit(1);
        }
    }

    // clean up and exit
    if !args.preserve_state {
        if args.setup_etcd {
            println!("Stopping etcd");
            util::stop_rm_etcd();
        }
        println!("Removing state in {:?}", sqlness_home);
        tokio::fs::remove_dir_all(sqlness_home).await.unwrap();
    }
}
