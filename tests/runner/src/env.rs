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
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use client::error::ServerSnafu;
use client::{
    Client, Database as DB, Error as ClientError, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME,
};
use common_error::ext::ErrorExt;
use common_query::Output;
use common_recordbatch::RecordBatches;
use serde::Serialize;
use sqlness::{Database, EnvController, QueryContext};
use tinytemplate::TinyTemplate;
use tokio::sync::Mutex as TokioMutex;

use crate::util;

const METASRV_ADDR: &str = "127.0.0.1:3002";
const SERVER_ADDR: &str = "127.0.0.1:4001";
const DEFAULT_LOG_LEVEL: &str = "--log-level=debug,hyper=warn,tower=warn,datafusion=warn,reqwest=warn,sqlparser=warn,h2=info,opendal=info";

#[derive(Clone)]
pub enum WalConfig {
    RaftEngine,
    Kafka { broker_endpoints: Vec<String> },
}

#[derive(Clone)]
pub struct Env {
    data_home: PathBuf,
    server_addr: Option<String>,
    wal: WalConfig,
}

#[allow(clippy::print_stdout)]
#[async_trait]
impl EnvController for Env {
    type DB = GreptimeDB;

    async fn start(&self, mode: &str, _config: Option<&Path>) -> Self::DB {
        match mode {
            "standalone" => self.start_standalone().await,
            "distributed" => self.start_distributed().await,
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
    pub fn new(data_home: PathBuf, server_addr: Option<String>, wal: WalConfig) -> Self {
        Self {
            data_home,
            server_addr,
            wal,
        }
    }

    async fn start_standalone(&self) -> GreptimeDB {
        if let Some(server_addr) = self.server_addr.clone() {
            self.connect_db(&server_addr)
        } else {
            Self::build_db().await;

            let db_ctx = GreptimeDBContext::new(self.wal.clone());

            let server_process = self.start_server("standalone", &db_ctx, true).await;

            let client = Client::with_urls(vec![SERVER_ADDR]);
            let db = DB::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, client);

            GreptimeDB {
                server_processes: Some(Arc::new(Mutex::new(vec![server_process]))),
                metasrv_process: None,
                frontend_process: None,
                client: TokioMutex::new(db),
                ctx: db_ctx,
                is_standalone: true,
                env: self.clone(),
            }
        }
    }

    async fn start_distributed(&self) -> GreptimeDB {
        if let Some(server_addr) = self.server_addr.clone() {
            self.connect_db(&server_addr)
        } else {
            Self::build_db().await;

            let db_ctx = GreptimeDBContext::new(self.wal.clone());

            // start a distributed GreptimeDB
            let meta_server = self.start_server("metasrv", &db_ctx, true).await;

            let datanode_1 = self.start_server("datanode", &db_ctx, true).await;
            let datanode_2 = self.start_server("datanode", &db_ctx, true).await;
            let datanode_3 = self.start_server("datanode", &db_ctx, true).await;

            let frontend = self.start_server("frontend", &db_ctx, true).await;

            let client = Client::with_urls(vec![SERVER_ADDR]);
            let db = DB::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, client);

            GreptimeDB {
                server_processes: Some(Arc::new(Mutex::new(vec![
                    datanode_1, datanode_2, datanode_3,
                ]))),
                metasrv_process: Some(meta_server),
                frontend_process: Some(frontend),
                client: TokioMutex::new(db),
                ctx: db_ctx,
                is_standalone: false,
                env: self.clone(),
            }
        }
    }

    fn connect_db(&self, server_addr: &str) -> GreptimeDB {
        let client = Client::with_urls(vec![server_addr.to_owned()]);
        let db = DB::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, client);
        GreptimeDB {
            client: TokioMutex::new(db),
            server_processes: None,
            metasrv_process: None,
            frontend_process: None,
            ctx: GreptimeDBContext {
                time: 0,
                datanode_id: Default::default(),
                wal: self.wal.clone(),
            },
            is_standalone: false,
            env: self.clone(),
        }
    }

    fn stop_server(process: &mut Child) {
        let _ = process.kill();
        let _ = process.wait();
    }

    async fn start_server(
        &self,
        subcommand: &str,
        db_ctx: &GreptimeDBContext,
        truncate_log: bool,
    ) -> Child {
        let log_file_name = match subcommand {
            "datanode" => {
                db_ctx.incr_datanode_id();
                format!("greptime-sqlness-datanode-{}.log", db_ctx.datanode_id())
            }
            "frontend" => "greptime-sqlness-frontend.log".to_string(),
            "metasrv" => "greptime-sqlness-metasrv.log".to_string(),
            "standalone" => "greptime-sqlness-standalone.log".to_string(),
            _ => panic!("Unexpected subcommand: {subcommand}"),
        };
        let log_file_name = self.data_home.join(log_file_name).display().to_string();

        let log_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(truncate_log)
            .append(!truncate_log)
            .open(log_file_name)
            .unwrap();

        let (args, check_ip_addr) = match subcommand {
            "datanode" => self.datanode_start_args(db_ctx),
            "standalone" => {
                let args = vec![
                    DEFAULT_LOG_LEVEL.to_string(),
                    subcommand.to_string(),
                    "start".to_string(),
                    "-c".to_string(),
                    self.generate_config_file(subcommand, db_ctx),
                    "--http-addr=127.0.0.1:5002".to_string(),
                ];
                (args, SERVER_ADDR.to_string())
            }
            "frontend" => {
                let args = vec![
                    DEFAULT_LOG_LEVEL.to_string(),
                    subcommand.to_string(),
                    "start".to_string(),
                    "--metasrv-addr=127.0.0.1:3002".to_string(),
                    "--http-addr=127.0.0.1:5003".to_string(),
                ];
                (args, SERVER_ADDR.to_string())
            }
            "metasrv" => {
                let args = vec![
                    DEFAULT_LOG_LEVEL.to_string(),
                    subcommand.to_string(),
                    "start".to_string(),
                    "--use-memory-store".to_string(),
                    "true".to_string(),
                    "--enable-region-failover".to_string(),
                    "false".to_string(),
                    "--http-addr=127.0.0.1:5002".to_string(),
                    "-c".to_string(),
                    self.generate_config_file(subcommand, db_ctx),
                ];
                (args, METASRV_ADDR.to_string())
            }
            _ => panic!("Unexpected subcommand: {subcommand}"),
        };

        if util::check_port(check_ip_addr.parse().unwrap(), Duration::from_secs(1)).await {
            panic!(
                "Port {check_ip_addr} is already in use, please check and retry.",
                check_ip_addr = check_ip_addr
            );
        }

        #[cfg(not(windows))]
        let program = "./greptime";
        #[cfg(windows)]
        let program = "greptime.exe";

        let mut process = Command::new(program)
            .current_dir(util::get_binary_dir("debug"))
            .env("TZ", "UTC")
            .args(args)
            .stdout(log_file)
            .spawn()
            .unwrap_or_else(|error| {
                panic!("Failed to start the DB with subcommand {subcommand},Error: {error}")
            });

        if !util::check_port(check_ip_addr.parse().unwrap(), Duration::from_secs(10)).await {
            Env::stop_server(&mut process);
            panic!("{subcommand} doesn't up in 10 seconds, quit.")
        }

        process
    }

    fn datanode_start_args(&self, db_ctx: &GreptimeDBContext) -> (Vec<String>, String) {
        let id = db_ctx.datanode_id();

        let data_home = self
            .data_home
            .join(format!("greptimedb_datanode_{}_{id}", db_ctx.time));

        let subcommand = "datanode";
        let mut args = vec![
            DEFAULT_LOG_LEVEL.to_string(),
            subcommand.to_string(),
            "start".to_string(),
        ];
        args.push(format!("--rpc-addr=127.0.0.1:410{id}"));
        args.push(format!("--http-addr=127.0.0.1:430{id}"));
        args.push(format!("--data-home={}", data_home.display()));
        args.push(format!("--node-id={id}"));
        args.push("-c".to_string());
        args.push(self.generate_config_file(subcommand, db_ctx));
        args.push("--metasrv-addr=127.0.0.1:3002".to_string());
        (args, format!("127.0.0.1:410{id}"))
    }

    /// stop and restart the server process
    async fn restart_server(&self, db: &GreptimeDB) {
        {
            if let Some(server_process) = db.server_processes.clone() {
                let mut server_processes = server_process.lock().unwrap();
                for server_process in server_processes.iter_mut() {
                    Env::stop_server(server_process);
                }
            }
        }

        // check if the server is distributed or standalone
        let new_server_processes = if db.is_standalone {
            let new_server_process = self.start_server("standalone", &db.ctx, false).await;
            vec![new_server_process]
        } else {
            db.ctx.reset_datanode_id();

            let mut processes = vec![];
            for _ in 0..3 {
                let new_server_process = self.start_server("datanode", &db.ctx, false).await;
                processes.push(new_server_process);
            }
            processes
        };

        if let Some(server_process) = db.server_processes.clone() {
            let mut server_processes = server_process.lock().unwrap();
            *server_processes = new_server_processes;
        }
    }

    /// Generate config file to `/tmp/{subcommand}-{current_time}.toml`
    fn generate_config_file(&self, subcommand: &str, db_ctx: &GreptimeDBContext) -> String {
        let mut tt = TinyTemplate::new();

        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.pop();
        path.push("conf");
        path.push(format!("{subcommand}-test.toml.template"));
        let template = std::fs::read_to_string(path).unwrap();
        tt.add_template(subcommand, &template).unwrap();

        #[derive(Serialize)]
        struct Context {
            wal_dir: String,
            data_home: String,
            procedure_dir: String,
            is_raft_engine: bool,
            kafka_wal_broker_endpoints: String,
        }

        let data_home = self
            .data_home
            .join(format!("greptimedb-{subcommand}-{}", db_ctx.time));
        std::fs::create_dir_all(data_home.as_path()).unwrap();

        let wal_dir = data_home.join("wal").display().to_string();
        let procedure_dir = data_home.join("procedure").display().to_string();
        let ctx = Context {
            wal_dir,
            data_home: data_home.display().to_string(),
            procedure_dir,
            is_raft_engine: db_ctx.is_raft_engine(),
            kafka_wal_broker_endpoints: db_ctx.kafka_wal_broker_endpoints(),
        };
        let rendered = tt.render(subcommand, &ctx).unwrap();

        let conf_file = data_home
            .join(format!("{subcommand}-{}.toml", db_ctx.time))
            .display()
            .to_string();
        println!("Generating {subcommand} config file in {conf_file}, full content:\n{rendered}");
        std::fs::write(&conf_file, rendered).unwrap();

        conf_file
    }

    /// Build the DB with `cargo build --bin greptime`
    async fn build_db() {
        println!("Going to build the DB...");
        let output = Command::new("cargo")
            .current_dir(util::get_workspace_root())
            .args(["build", "--bin", "greptime"])
            .output()
            .expect("Failed to start GreptimeDB");
        if !output.status.success() {
            println!("Failed to build GreptimeDB, {}", output.status);
            println!("Cargo build stdout:");
            io::stdout().write_all(&output.stdout).unwrap();
            println!("Cargo build stderr:");
            io::stderr().write_all(&output.stderr).unwrap();
            panic!();
        }
        println!("Build finished, starting...");
    }
}

pub struct GreptimeDB {
    server_processes: Option<Arc<Mutex<Vec<Child>>>>,
    metasrv_process: Option<Child>,
    frontend_process: Option<Child>,
    client: TokioMutex<DB>,
    ctx: GreptimeDBContext,
    is_standalone: bool,
    env: Env,
}

#[async_trait]
impl Database for GreptimeDB {
    async fn query(&self, ctx: QueryContext, query: String) -> Box<dyn Display> {
        if ctx.context.contains_key("restart") && self.env.server_addr.is_none() {
            self.env.restart_server(self).await;
        }

        let mut client = self.client.lock().await;
        if query.trim().to_lowercase().starts_with("use ") {
            let database = query
                .split_ascii_whitespace()
                .nth(1)
                .expect("Illegal `USE` statement: expecting a database.")
                .trim_end_matches(';');
            client.set_schema(database);
            Box::new(ResultDisplayer {
                result: Ok(Output::AffectedRows(0)),
            }) as _
        } else {
            let mut result = client.sql(&query).await;
            if let Ok(Output::Stream(stream)) = result {
                match RecordBatches::try_collect(stream).await {
                    Ok(recordbatches) => result = Ok(Output::RecordBatches(recordbatches)),
                    Err(e) => {
                        let status_code = e.status_code();
                        let msg = e.output_msg();
                        result = ServerSnafu {
                            code: status_code,
                            msg,
                        }
                        .fail();
                    }
                }
            }
            Box::new(ResultDisplayer { result }) as _
        }
    }
}

impl GreptimeDB {
    #![allow(clippy::print_stdout)]
    fn stop(&mut self) {
        if let Some(server_processes) = self.server_processes.clone() {
            let mut server_processes = server_processes.lock().unwrap();
            for server_process in server_processes.iter_mut() {
                Env::stop_server(server_process);
            }
        }
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
        if self.env.server_addr.is_none() {
            self.stop();
        }
    }
}

struct GreptimeDBContext {
    /// Start time in millisecond
    time: i64,
    datanode_id: AtomicU32,
    wal: WalConfig,
}

impl GreptimeDBContext {
    pub fn new(wal: WalConfig) -> Self {
        Self {
            time: common_time::util::current_time_millis(),
            datanode_id: AtomicU32::new(0),
            wal,
        }
    }

    fn is_raft_engine(&self) -> bool {
        matches!(self.wal, WalConfig::RaftEngine)
    }

    fn kafka_wal_broker_endpoints(&self) -> String {
        match &self.wal {
            WalConfig::RaftEngine => String::new(),
            WalConfig::Kafka { broker_endpoints } => {
                serde_json::to_string(&broker_endpoints).unwrap()
            }
        }
    }

    fn incr_datanode_id(&self) {
        let _ = self.datanode_id.fetch_add(1, Ordering::Relaxed);
    }

    fn datanode_id(&self) -> u32 {
        self.datanode_id.load(Ordering::Relaxed)
    }

    fn reset_datanode_id(&self) {
        self.datanode_id.store(0, Ordering::Relaxed);
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
                let root_cause = e.output_msg();
                write!(
                    f,
                    "Error: {}({status_code}), {root_cause}",
                    status_code as u32
                )
            }
        }
    }
}
