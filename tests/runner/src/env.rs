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

use std::fmt::Display;
use std::fs::OpenOptions;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use async_trait::async_trait;
use client::{Client, Database as DB, Error as ClientError};
use common_error::ext::ErrorExt;
use common_error::snafu::ErrorCompat;
use common_query::Output;
use sqlness::{Database, EnvController};
use tokio::process::{Child, Command};

use crate::util;

const DATANODE_ADDR: &str = "127.0.0.1:4100";
const METASRV_ADDR: &str = "127.0.0.1:3002";
const SERVER_ADDR: &str = "127.0.0.1:4001";
const SERVER_LOG_FILE: &str = "/tmp/greptime-sqlness.log";
const METASRV_LOG_FILE: &str = "/tmp/greptime-sqlness-metasrv.log";
const FRONTEND_LOG_FILE: &str = "/tmp/greptime-sqlness-frontend.log";
const DATANODE_LOG_FILE: &str = "/tmp/greptime-sqlness-datanode.log";

pub struct Env {}

#[async_trait]
impl EnvController for Env {
    type DB = GreptimeDB;

    async fn start(&self, mode: &str, _config: Option<&Path>) -> Self::DB {
        match mode {
            "standalone" => Self::start_standalone().await,
            "distributed" => Self::start_distributed().await,
            _ => panic!("Unexpected mode: {mode}"),
        }
    }

    /// Stop one [`Database`].
    #[allow(clippy::print_stdout)]
    async fn stop(&self, _mode: &str, mut database: Self::DB) {
        let mut server = database.server_process;
        Env::stop_server(&mut server).await;
        if let Some(mut metasrv) = database.metasrv_process.take() {
            Env::stop_server(&mut metasrv).await;
        }
        if let Some(mut datanode) = database.datanode_process.take() {
            Env::stop_server(&mut datanode).await;
        }
        println!("Stopped DB.");
    }
}

impl Env {
    #[allow(clippy::print_stdout)]
    pub async fn start_standalone() -> GreptimeDB {
        // Build the DB with `cargo build --bin greptime`
        println!("Going to build the DB...");
        let cargo_build_result = Command::new("cargo")
            .current_dir(util::get_workspace_root())
            .args(["build", "--bin", "greptime"])
            .stdout(Stdio::null())
            .output()
            .await
            .expect("Failed to start GreptimeDB")
            .status;
        if !cargo_build_result.success() {
            panic!("Failed to build GreptimeDB (`cargo build` fails)");
        }
        println!("Build finished, starting...");

        // Open log file (build logs will be truncated).
        let log_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(SERVER_LOG_FILE)
            .unwrap_or_else(|_| panic!("Cannot open log file at {SERVER_LOG_FILE}"));
        // Start the DB
        let server_process = Command::new("./greptime")
            .current_dir(util::get_binary_dir("debug"))
            .args(["standalone", "start"])
            .stdout(log_file)
            .spawn()
            .expect("Failed to start the DB");

        let is_up = util::check_port(SERVER_ADDR.parse().unwrap(), Duration::from_secs(10)).await;
        if !is_up {
            panic!("Server doesn't up in 10 seconds, quit.")
        }
        println!("Started, going to test. Log will be write to {SERVER_LOG_FILE}");

        let client = Client::with_urls(vec![SERVER_ADDR]);
        let db = DB::new("greptime", client.clone());

        GreptimeDB {
            server_process,
            metasrv_process: None,
            datanode_process: None,
            client,
            db,
        }
    }

    pub async fn start_distributed() -> GreptimeDB {
        let cargo_build_result = Command::new("cargo")
            .current_dir(util::get_workspace_root())
            .args(["build", "--bin", "greptime"])
            .stdout(Stdio::null())
            .output()
            .await
            .expect("Failed to start GreptimeDB")
            .status;
        if !cargo_build_result.success() {
            panic!("Failed to build GreptimeDB (`cargo build` fails)");
        }

        // start a distributed GreptimeDB
        let mut meta_server = Env::start_server("metasrv");
        // wait for election
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let mut frontend = Env::start_server("frontend");
        let mut datanode = Env::start_server("datanode");

        for addr in [DATANODE_ADDR, METASRV_ADDR, SERVER_ADDR].iter() {
            let is_up = util::check_port(addr.parse().unwrap(), Duration::from_secs(10)).await;
            if !is_up {
                Env::stop_server(&mut meta_server).await;
                Env::stop_server(&mut frontend).await;
                Env::stop_server(&mut datanode).await;
                panic!("Server {addr} doesn't up in 10 seconds, quit.")
            }
        }

        let client = Client::with_urls(vec![SERVER_ADDR]);
        let db = DB::new("greptime", client.clone());

        GreptimeDB {
            server_process: frontend,
            metasrv_process: Some(meta_server),
            datanode_process: Some(datanode),
            client,
            db,
        }
    }

    async fn stop_server(process: &mut Child) {
        process.kill().await.unwrap();
        let _ = process.wait().await;
    }

    fn start_server(subcommand: &str) -> Child {
        let log_file_name = match subcommand {
            "datanode" => DATANODE_LOG_FILE,
            "frontend" => FRONTEND_LOG_FILE,
            "metasrv" => METASRV_LOG_FILE,
            _ => panic!("Unexpected subcommand: {subcommand}"),
        };
        let log_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(log_file_name)
            .unwrap_or_else(|_| panic!("Cannot open log file at {log_file_name}"));

        let mut args = vec![subcommand, "start"];
        if subcommand == "frontend" {
            args.push("--metasrv-addr=0.0.0.0:3002");
        } else if subcommand == "datanode" {
            args.push("--rpc-addr=0.0.0.0:4100");
            args.push("--metasrv-addr=0.0.0.0:3002");
            args.push("--node-id=1");
            args.push("--data-dir=/tmp/greptimedb_node_1/data");
            args.push("--wal-dir=/tmp/greptimedb_node_1/wal");
        }

        let process = Command::new("./greptime")
            .current_dir(util::get_binary_dir("debug"))
            .args(args)
            .stdout(log_file)
            .spawn()
            .expect("Failed to start the DB");
        process
    }
}

pub struct GreptimeDB {
    server_process: Child,
    metasrv_process: Option<Child>,
    datanode_process: Option<Child>,
    #[allow(dead_code)]
    client: Client,
    db: DB,
}

#[async_trait]
impl Database for GreptimeDB {
    async fn query(&self, query: String) -> Box<dyn Display> {
        let result = self.db.sql(&query).await;
        Box::new(ResultDisplayer { result }) as _
    }
}

struct ResultDisplayer {
    result: Result<Output, ClientError>,
}

impl Display for ResultDisplayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.result {
            Ok(result) => match result {
                Output::AffectedRows(rows) => {
                    write!(f, "Affected Rows: {rows}")
                }
                Output::RecordBatches(recordbatches) => {
                    let pretty = recordbatches.pretty_print().map_err(|e| e.to_string());
                    match pretty {
                        Ok(s) => write!(f, "{s}"),
                        Err(e) => {
                            write!(f, "Failed to pretty format {recordbatches:?}, error: {e}")
                        }
                    }
                }
                Output::Stream(_) => unreachable!(),
            },
            Err(e) => {
                let status_code = e.status_code();
                let root_cause = e.iter_chain().last().unwrap();
                write!(
                    f,
                    "Error: {}({status_code}), {root_cause}",
                    status_code as u32
                )
            }
        }
    }
}
