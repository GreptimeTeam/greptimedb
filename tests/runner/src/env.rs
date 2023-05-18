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
use std::path::{Path, PathBuf};
use std::process::Stdio;
// use tokio::process::{Child, Command};
use std::process::{Child, Command};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use client::{
    Client, Database as DB, Error as ClientError, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME,
};
use common_error::ext::ErrorExt;
use common_error::snafu::ErrorCompat;
use common_query::Output;
use serde::Serialize;
use sqlness::{Database, EnvController, QueryContext};
use tinytemplate::TinyTemplate;
use tokio::sync::Mutex as TokioMutex;

use crate::util;

const DATANODE_ADDR: &str = "127.0.0.1:4100";
const METASRV_ADDR: &str = "127.0.0.1:3002";
const SERVER_ADDR: &str = "127.0.0.1:4001";
const STANDALONE_LOG_FILE: &str = "/tmp/greptime-sqlness-standalone.log";
const METASRV_LOG_FILE: &str = "/tmp/greptime-sqlness-metasrv.log";
const FRONTEND_LOG_FILE: &str = "/tmp/greptime-sqlness-frontend.log";
const DATANODE_LOG_FILE: &str = "/tmp/greptime-sqlness-datanode.log";

pub struct Env {}

#[allow(clippy::print_stdout)]
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
        database.stop();
    }
}

#[allow(clippy::print_stdout)]
impl Env {
    pub async fn start_standalone() -> GreptimeDB {
        Self::build_db().await;

        let db_ctx = GreptimeDBContext::new();

        let server_process = Self::start_server("standalone", &db_ctx, true).await;

        println!("Started, going to test. Log will be write to {STANDALONE_LOG_FILE}");

        let client = Client::with_urls(vec![SERVER_ADDR]);
        let db = DB::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, client);

        GreptimeDB {
            server_process: Arc::new(Mutex::new(server_process)),
            metasrv_process: None,
            frontend_process: None,
            client: TokioMutex::new(db),
            ctx: db_ctx,
            is_standalone: true,
        }
    }

    pub async fn start_distributed() -> GreptimeDB {
        Self::build_db().await;

        let db_ctx = GreptimeDBContext::new();

        // start a distributed GreptimeDB
        let meta_server = Env::start_server("metasrv", &db_ctx, true).await;
        let datanode = Env::start_server("datanode", &db_ctx, true).await;
        let frontend = Env::start_server("frontend", &db_ctx, true).await;

        let client = Client::with_urls(vec![SERVER_ADDR]);
        let db = DB::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, client);

        GreptimeDB {
            server_process: Arc::new(Mutex::new(datanode)),
            metasrv_process: Some(meta_server),
            frontend_process: Some(frontend),
            client: TokioMutex::new(db),
            ctx: db_ctx,
            is_standalone: false,
        }
    }

    fn stop_server(process: &mut Child) {
        let _ = process.kill();
        let _ = process.wait();
    }

    async fn start_server(
        subcommand: &str,
        db_ctx: &GreptimeDBContext,
        truncate_log: bool,
    ) -> Child {
        let log_file_name = match subcommand {
            "datanode" => DATANODE_LOG_FILE,
            "frontend" => FRONTEND_LOG_FILE,
            "metasrv" => METASRV_LOG_FILE,
            "standalone" => STANDALONE_LOG_FILE,
            _ => panic!("Unexpected subcommand: {subcommand}"),
        };
        let log_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(truncate_log)
            .open(log_file_name)
            .unwrap_or_else(|_| panic!("Cannot open log file at {log_file_name}"));

        let mut args = vec![
            "--log-level=debug".to_string(),
            subcommand.to_string(),
            "start".to_string(),
        ];
        match subcommand {
            "datanode" | "standalone" => {
                args.push("-c".to_string());
                args.push(Self::generate_config_file(subcommand, db_ctx));
                args.push("--http-addr=0.0.0.0:5001".to_string());
            }
            "frontend" => {
                args.push("--metasrv-addr=0.0.0.0:3002".to_string());
                args.push("--http-addr=0.0.0.0:5003".to_string());
            }
            "metasrv" => {
                args.push("--use-memory-store".to_string());
                args.push("--http-addr=0.0.0.0:5002".to_string());
            }
            _ => panic!("Unexpected subcommand: {subcommand}"),
        }

        let mut process = Command::new("./greptime")
            .current_dir(util::get_binary_dir("debug"))
            .args(args)
            .stdout(log_file)
            .spawn()
            .unwrap_or_else(|_| panic!("Failed to start the DB with subcommand {subcommand}"));

        // check connection
        let ip_addr = match subcommand {
            "datanode" => DATANODE_ADDR,
            "frontend" => SERVER_ADDR,
            "metasrv" => METASRV_ADDR,
            "standalone" => SERVER_ADDR,
            _ => panic!("Unexpected subcommand: {subcommand}"),
        };
        if !util::check_port(ip_addr.parse().unwrap(), Duration::from_secs(10)).await {
            Env::stop_server(&mut process);
            panic!("{subcommand} doesn't up in 10 seconds, quit.")
        }

        process
    }

    /// stop and restart the server process
    async fn restart_server(db: &GreptimeDB) {
        {
            let mut server_process = db.server_process.lock().unwrap();
            Env::stop_server(&mut server_process);
        }

        // check if the server is distributed or standalone
        let subcommand = if db.is_standalone {
            "standalone"
        } else {
            "datanode"
        };
        let new_server_process = Env::start_server(subcommand, &db.ctx, false).await;

        let mut server_process = db.server_process.lock().unwrap();
        *server_process = new_server_process;
    }

    /// Generate config file to `/tmp/{subcommand}-{current_time}.toml`
    fn generate_config_file(subcommand: &str, db_ctx: &GreptimeDBContext) -> String {
        let mut tt = TinyTemplate::new();

        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push(format!("../conf/{subcommand}-test.toml.template"));
        let template = std::fs::read_to_string(path).unwrap();
        tt.add_template(subcommand, &template).unwrap();

        #[derive(Serialize)]
        struct Context {
            wal_dir: String,
            data_home: String,
            procedure_dir: String,
        }

        let greptimedb_dir = format!("/tmp/greptimedb-{subcommand}-{}", db_ctx.time);
        let ctx = Context {
            wal_dir: format!("{greptimedb_dir}/wal/"),
            data_home: format!("{greptimedb_dir}/data/"),
            procedure_dir: format!("{greptimedb_dir}/procedure/"),
        };
        let rendered = tt.render(subcommand, &ctx).unwrap();

        let conf_file = format!("/tmp/{subcommand}-{}.toml", db_ctx.time);
        println!("Generating {subcommand} config file in {conf_file}, full content:\n{rendered}");
        std::fs::write(&conf_file, rendered).unwrap();

        conf_file
    }

    /// Build the DB with `cargo build --bin greptime`
    async fn build_db() {
        println!("Going to build the DB...");
        let cargo_build_result = Command::new("cargo")
            .current_dir(util::get_workspace_root())
            .args(["build", "--bin", "greptime"])
            .stdout(Stdio::null())
            .output()
            .expect("Failed to start GreptimeDB")
            .status;
        if !cargo_build_result.success() {
            panic!("Failed to build GreptimeDB (`cargo build` fails)");
        }
        println!("Build finished, starting...");
    }
}

pub struct GreptimeDB {
    server_process: Arc<Mutex<Child>>,
    metasrv_process: Option<Child>,
    frontend_process: Option<Child>,
    client: TokioMutex<DB>,
    ctx: GreptimeDBContext,
    is_standalone: bool,
}

#[async_trait]
impl Database for GreptimeDB {
    async fn query(&self, ctx: QueryContext, query: String) -> Box<dyn Display> {
        if ctx.context.contains_key("restart") {
            Env::restart_server(self).await;
        }

        let mut client = self.client.lock().await;
        if query.trim().to_lowercase().starts_with("use ") {
            let database = query
                .split_ascii_whitespace()
                .nth(1)
                .expect("Illegal `USE` statement: expecting a database.")
                .trim_end_matches(';');
            client.set_schema(database);
        }

        let result = client.sql(&query).await;
        Box::new(ResultDisplayer { result }) as _
    }
}

impl GreptimeDB {
    #![allow(clippy::print_stdout)]
    fn stop(&mut self) {
        let mut server = self.server_process.lock().unwrap();
        Env::stop_server(&mut server);
        if let Some(mut metasrv) = self.metasrv_process.take() {
            Env::stop_server(&mut metasrv);
        }
        if let Some(mut datanode) = self.frontend_process.take() {
            Env::stop_server(&mut datanode);
        }
        println!("Stopped DB.");
    }
}

impl Drop for GreptimeDB {
    fn drop(&mut self) {
        self.stop();
    }
}

struct GreptimeDBContext {
    /// Start time in millisecond
    time: i64,
}

impl GreptimeDBContext {
    pub fn new() -> Self {
        Self {
            time: common_time::util::current_time_millis(),
        }
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
