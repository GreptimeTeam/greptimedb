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
use common_query::Output;
use sqlness::{Database, EnvController};
use tokio::process::{Child, Command};

use crate::util;

const SERVER_ADDR: &str = "127.0.0.1:4001";
const SERVER_LOG_FILE: &str = "/tmp/greptime-sqlness.log";

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
    async fn stop(&self, _mode: &str, mut database: Self::DB) {
        database.server_process.kill().await.unwrap()
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
            .args(["standalone", "start", "-m"])
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
            client,
            db,
        }
    }

    pub async fn start_distributed() -> GreptimeDB {
        todo!()
    }
}

pub struct GreptimeDB {
    server_process: Child,
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
            Err(e) => write!(f, "{e}"),
        }
    }
}
