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

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Display;
use std::fs::OpenOptions;
use std::io;
use std::io::Write;
use std::net::SocketAddr;
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
use common_query::{Output, OutputData};
use common_recordbatch::RecordBatches;
use datatypes::data_type::ConcreteDataType;
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::{StringVectorBuilder, VectorRef};
use mysql::prelude::Queryable;
use mysql::{Conn as MySqlClient, Row as MySqlRow};
use sqlness::{Database, EnvController, QueryContext};
use tokio::sync::{Mutex as TokioMutex, OnceCell};
use tokio_postgres::{Client as PgClient, SimpleQueryMessage as PgRow};

use crate::protocol_interceptor::{MYSQL, PROTOCOL_KEY};
use crate::server_mode::ServerMode;
use crate::util::{get_workspace_root, maybe_pull_binary, PROGRAM};
use crate::{util, ServerAddr};

// standalone mode
const SERVER_MODE_STANDALONE_IDX: usize = 0;
// distributed mode
const SERVER_MODE_METASRV_IDX: usize = 0;
const SERVER_MODE_DATANODE_START_IDX: usize = 1;
const SERVER_MODE_FRONTEND_IDX: usize = 4;
const SERVER_MODE_FLOWNODE_IDX: usize = 5;

static INIT: OnceCell<()> = OnceCell::const_new();

#[derive(Clone)]
pub enum WalConfig {
    RaftEngine,
    Kafka {
        /// Indicates whether the runner needs to start a kafka cluster
        /// (it might be available in the external system environment).
        needs_kafka_cluster: bool,
        broker_endpoints: Vec<String>,
    },
}

#[derive(Clone)]
pub struct StoreConfig {
    pub store_addrs: Vec<String>,
    pub setup_etcd: bool,
    pub setup_pg: bool,
    pub setup_mysql: bool,
}

#[derive(Clone)]
pub struct Env {
    sqlness_home: PathBuf,
    server_addrs: ServerAddr,
    wal: WalConfig,

    /// The path to the directory that contains the pre-built GreptimeDB binary.
    /// When running in CI, this is expected to be set.
    /// If not set, this runner will build the GreptimeDB binary itself when needed, and set this field by then.
    bins_dir: Arc<Mutex<Option<PathBuf>>>,
    /// The path to the directory that contains the old pre-built GreptimeDB binaries.
    versioned_bins_dirs: Arc<Mutex<HashMap<String, PathBuf>>>,
    /// Pull different versions of GreptimeDB on need.
    pull_version_on_need: bool,
    /// Store address for metasrv metadata
    store_config: StoreConfig,
}

#[async_trait]
impl EnvController for Env {
    type DB = GreptimeDB;

    async fn start(&self, mode: &str, id: usize, _config: Option<&Path>) -> Self::DB {
        if self.server_addrs.server_addr.is_some() && id > 0 {
            panic!("Parallel test mode is not supported when server address is already set.");
        }

        std::env::set_var("SQLNESS_HOME", self.sqlness_home.display().to_string());
        match mode {
            "standalone" => self.start_standalone(id).await,
            "distributed" => self.start_distributed(id).await,
            _ => panic!("Unexpected mode: {mode}"),
        }
    }

    /// Stop one [`Database`].
    async fn stop(&self, _mode: &str, mut database: Self::DB) {
        database.stop();
    }
}

impl Env {
    pub fn new(
        data_home: PathBuf,
        server_addrs: ServerAddr,
        wal: WalConfig,
        pull_version_on_need: bool,
        bins_dir: Option<PathBuf>,
        store_config: StoreConfig,
    ) -> Self {
        Self {
            sqlness_home: data_home,
            server_addrs,
            wal,
            pull_version_on_need,
            bins_dir: Arc::new(Mutex::new(bins_dir.clone())),
            versioned_bins_dirs: Arc::new(Mutex::new(HashMap::from_iter([(
                "latest".to_string(),
                bins_dir.clone().unwrap_or(util::get_binary_dir("debug")),
            )]))),
            store_config,
        }
    }

    async fn start_standalone(&self, id: usize) -> GreptimeDB {
        println!("Starting standalone instance {id}");

        if self.server_addrs.server_addr.is_some() {
            self.connect_db(&self.server_addrs, id).await
        } else {
            INIT.get_or_init(|| async {
                self.build_db();
                self.setup_wal();
            })
            .await;
            let mut db_ctx = GreptimeDBContext::new(self.wal.clone(), self.store_config.clone());

            let server_mode = ServerMode::random_standalone();
            db_ctx.set_server_mode(server_mode.clone(), SERVER_MODE_STANDALONE_IDX);
            let server_addr = server_mode.server_addr().unwrap();
            let server_process = self.start_server(server_mode, &db_ctx, id, true).await;

            let mut greptimedb = self.connect_db(&server_addr, id).await;
            greptimedb.server_processes = Some(Arc::new(Mutex::new(vec![server_process])));
            greptimedb.is_standalone = true;
            greptimedb.ctx = db_ctx;

            greptimedb
        }
    }

    async fn start_distributed(&self, id: usize) -> GreptimeDB {
        if self.server_addrs.server_addr.is_some() {
            self.connect_db(&self.server_addrs, id).await
        } else {
            INIT.get_or_init(|| async {
                self.build_db();
                self.setup_wal();
                self.setup_etcd();
                self.setup_pg();
                self.setup_mysql().await;
            })
            .await;
            let mut db_ctx = GreptimeDBContext::new(self.wal.clone(), self.store_config.clone());

            // start a distributed GreptimeDB
            let meta_server_mode = ServerMode::random_metasrv();
            let metasrv_port = match &meta_server_mode {
                ServerMode::Metasrv { rpc_server_addr, .. } => rpc_server_addr
                    .split(':')
                    .nth(1)
                    .unwrap()
                    .parse::<u16>()
                    .unwrap(),
                _ => panic!("metasrv mode not set, maybe running in remote mode which doesn't support restart?"),
            };
            db_ctx.set_server_mode(meta_server_mode.clone(), SERVER_MODE_METASRV_IDX);
            let meta_server = self.start_server(meta_server_mode, &db_ctx, id, true).await;

            let datanode_1_mode = ServerMode::random_datanode(metasrv_port, 0);
            db_ctx.set_server_mode(datanode_1_mode.clone(), SERVER_MODE_DATANODE_START_IDX);
            let datanode_1 = self.start_server(datanode_1_mode, &db_ctx, id, true).await;
            let datanode_2_mode = ServerMode::random_datanode(metasrv_port, 1);
            db_ctx.set_server_mode(datanode_2_mode.clone(), SERVER_MODE_DATANODE_START_IDX + 1);
            let datanode_2 = self.start_server(datanode_2_mode, &db_ctx, id, true).await;
            let datanode_3_mode = ServerMode::random_datanode(metasrv_port, 2);
            db_ctx.set_server_mode(datanode_3_mode.clone(), SERVER_MODE_DATANODE_START_IDX + 2);
            let datanode_3 = self.start_server(datanode_3_mode, &db_ctx, id, true).await;

            let frontend_mode = ServerMode::random_frontend(metasrv_port);
            let server_addr = frontend_mode.server_addr().unwrap();
            db_ctx.set_server_mode(frontend_mode.clone(), SERVER_MODE_FRONTEND_IDX);
            let frontend = self.start_server(frontend_mode, &db_ctx, id, true).await;

            let flownode_mode = ServerMode::random_flownode(metasrv_port, 0);
            db_ctx.set_server_mode(flownode_mode.clone(), SERVER_MODE_FLOWNODE_IDX);
            let flownode = self.start_server(flownode_mode, &db_ctx, id, true).await;

            let mut greptimedb = self.connect_db(&server_addr, id).await;

            greptimedb.metasrv_process = Some(meta_server).into();
            greptimedb.server_processes = Some(Arc::new(Mutex::new(vec![
                datanode_1, datanode_2, datanode_3,
            ])));
            greptimedb.frontend_process = Some(frontend).into();
            greptimedb.flownode_process = Some(flownode).into();
            greptimedb.is_standalone = false;
            greptimedb.ctx = db_ctx;

            greptimedb
        }
    }

    async fn create_pg_client(&self, pg_server_addr: &str) -> PgClient {
        let sockaddr: SocketAddr = pg_server_addr.parse().expect(
            "Failed to parse the Postgres server address. Please check if the address is in the format of `ip:port`.",
        );
        let mut config = tokio_postgres::config::Config::new();
        config.host(sockaddr.ip().to_string());
        config.port(sockaddr.port());
        config.dbname(DEFAULT_SCHEMA_NAME);

        // retry to connect to Postgres server until success
        const MAX_RETRY: usize = 3;
        let mut backoff = Duration::from_millis(500);
        for _ in 0..MAX_RETRY {
            if let Ok((pg_client, conn)) = config.connect(tokio_postgres::NoTls).await {
                tokio::spawn(conn);
                return pg_client;
            }
            tokio::time::sleep(backoff).await;
            backoff *= 2;
        }
        panic!("Failed to connect to Postgres server. Please check if the server is running.");
    }

    async fn create_mysql_client(&self, mysql_server_addr: &str) -> MySqlClient {
        let sockaddr: SocketAddr = mysql_server_addr.parse().expect(
            "Failed to parse the MySQL server address. Please check if the address is in the format of `ip:port`.",
        );
        let ops = mysql::OptsBuilder::new()
            .ip_or_hostname(Some(sockaddr.ip().to_string()))
            .tcp_port(sockaddr.port())
            .db_name(Some(DEFAULT_SCHEMA_NAME));
        // retry to connect to MySQL server until success
        const MAX_RETRY: usize = 3;
        let mut backoff = Duration::from_millis(500);

        for _ in 0..MAX_RETRY {
            // exponential backoff
            if let Ok(client) = mysql::Conn::new(ops.clone()) {
                return client;
            }
            tokio::time::sleep(backoff).await;
            backoff *= 2;
        }

        panic!("Failed to connect to MySQL server. Please check if the server is running.")
    }

    async fn connect_db(&self, server_addr: &ServerAddr, id: usize) -> GreptimeDB {
        let grpc_server_addr = server_addr.server_addr.clone().unwrap();
        let pg_server_addr = server_addr.pg_server_addr.clone().unwrap();
        let mysql_server_addr = server_addr.mysql_server_addr.clone().unwrap();

        let grpc_client = Client::with_urls(vec![grpc_server_addr.clone()]);
        let db = DB::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, grpc_client);
        let pg_client = self.create_pg_client(&pg_server_addr).await;
        let mysql_client = self.create_mysql_client(&mysql_server_addr).await;

        GreptimeDB {
            grpc_client: TokioMutex::new(db),
            pg_client: TokioMutex::new(pg_client),
            mysql_client: TokioMutex::new(mysql_client),
            server_processes: None,
            metasrv_process: None.into(),
            frontend_process: None.into(),
            flownode_process: None.into(),
            ctx: GreptimeDBContext {
                time: 0,
                datanode_id: Default::default(),
                wal: self.wal.clone(),
                store_config: self.store_config.clone(),
                server_modes: Vec::new(),
            },
            is_standalone: false,
            env: self.clone(),
            id,
        }
    }

    fn stop_server(process: &mut Child) {
        let _ = process.kill();
        let _ = process.wait();
    }

    async fn start_server(
        &self,
        mode: ServerMode,
        db_ctx: &GreptimeDBContext,
        id: usize,
        truncate_log: bool,
    ) -> Child {
        let log_file_name = match mode {
            ServerMode::Datanode { node_id, .. } => {
                db_ctx.incr_datanode_id();
                format!("greptime-{}-sqlness-datanode-{}.log", id, node_id)
            }
            ServerMode::Flownode { .. } => format!("greptime-{}-sqlness-flownode.log", id),
            ServerMode::Frontend { .. } => format!("greptime-{}-sqlness-frontend.log", id),
            ServerMode::Metasrv { .. } => format!("greptime-{}-sqlness-metasrv.log", id),
            ServerMode::Standalone { .. } => format!("greptime-{}-sqlness-standalone.log", id),
        };
        let stdout_file_name = self.sqlness_home.join(log_file_name).display().to_string();

        println!("DB instance {id} log file at {stdout_file_name}");

        let stdout_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(truncate_log)
            .append(!truncate_log)
            .open(stdout_file_name)
            .unwrap();

        let args = mode.get_args(&self.sqlness_home, self, db_ctx, id);
        let check_ip_addrs = mode.check_addrs();

        for check_ip_addr in &check_ip_addrs {
            if util::check_port(check_ip_addr.parse().unwrap(), Duration::from_secs(1)).await {
                panic!(
                    "Port {check_ip_addr} is already in use, please check and retry.",
                    check_ip_addr = check_ip_addr
                );
            }
        }

        let program = PROGRAM;

        let bins_dir = self.bins_dir.lock().unwrap().clone().expect(
            "GreptimeDB binary is not available. Please pass in the path to the directory that contains the pre-built GreptimeDB binary. Or you may call `self.build_db()` beforehand.",
        );

        let mut process = Command::new(program)
            .current_dir(bins_dir.clone())
            .env("TZ", "UTC")
            .args(args)
            .stdout(stdout_file)
            .spawn()
            .unwrap_or_else(|error| {
                panic!(
                    "Failed to start the DB with subcommand {}, Error: {error}, path: {:?}",
                    mode.name(),
                    bins_dir.join(program)
                );
            });

        for check_ip_addr in &check_ip_addrs {
            if !util::check_port(check_ip_addr.parse().unwrap(), Duration::from_secs(10)).await {
                Env::stop_server(&mut process);
                panic!("{} doesn't up in 10 seconds, quit.", mode.name())
            }
        }

        process
    }

    /// stop and restart the server process
    async fn restart_server(&self, db: &GreptimeDB, is_full_restart: bool) {
        {
            if let Some(server_process) = db.server_processes.clone() {
                let mut server_processes = server_process.lock().unwrap();
                for server_process in server_processes.iter_mut() {
                    Env::stop_server(server_process);
                }
            }

            if is_full_restart {
                if let Some(mut metasrv_process) =
                    db.metasrv_process.lock().expect("poisoned lock").take()
                {
                    Env::stop_server(&mut metasrv_process);
                }
                if let Some(mut frontend_process) =
                    db.frontend_process.lock().expect("poisoned lock").take()
                {
                    Env::stop_server(&mut frontend_process);
                }
            }

            if let Some(mut flownode_process) =
                db.flownode_process.lock().expect("poisoned lock").take()
            {
                Env::stop_server(&mut flownode_process);
            }
        }

        // check if the server is distributed or standalone
        let new_server_processes = if db.is_standalone {
            let server_mode = db
                .ctx
                .get_server_mode(SERVER_MODE_STANDALONE_IDX)
                .cloned()
                .unwrap();
            let server_addr = server_mode.server_addr().unwrap();
            let new_server_process = self.start_server(server_mode, &db.ctx, db.id, false).await;

            *db.pg_client.lock().await = self
                .create_pg_client(&server_addr.pg_server_addr.unwrap())
                .await;
            *db.mysql_client.lock().await = self
                .create_mysql_client(&server_addr.mysql_server_addr.unwrap())
                .await;
            vec![new_server_process]
        } else {
            db.ctx.reset_datanode_id();
            if is_full_restart {
                let metasrv_mode = db
                    .ctx
                    .get_server_mode(SERVER_MODE_METASRV_IDX)
                    .cloned()
                    .unwrap();
                let metasrv = self.start_server(metasrv_mode, &db.ctx, db.id, false).await;
                db.metasrv_process
                    .lock()
                    .expect("lock poisoned")
                    .replace(metasrv);

                // wait for metasrv to start
                // since it seems older version of db might take longer to complete election
                tokio::time::sleep(Duration::from_secs(5)).await;
            }

            let mut processes = vec![];
            for i in 0..3 {
                let datanode_mode = db
                    .ctx
                    .get_server_mode(SERVER_MODE_DATANODE_START_IDX + i)
                    .cloned()
                    .unwrap();
                let new_server_process = self
                    .start_server(datanode_mode, &db.ctx, db.id, false)
                    .await;
                processes.push(new_server_process);
            }

            if is_full_restart {
                let frontend_mode = db
                    .ctx
                    .get_server_mode(SERVER_MODE_FRONTEND_IDX)
                    .cloned()
                    .unwrap();
                let frontend = self
                    .start_server(frontend_mode, &db.ctx, db.id, false)
                    .await;
                db.frontend_process
                    .lock()
                    .expect("lock poisoned")
                    .replace(frontend);
            }

            let flownode_mode = db
                .ctx
                .get_server_mode(SERVER_MODE_FLOWNODE_IDX)
                .cloned()
                .unwrap();
            let flownode = self
                .start_server(flownode_mode, &db.ctx, db.id, false)
                .await;
            db.flownode_process
                .lock()
                .expect("lock poisoned")
                .replace(flownode);

            processes
        };

        if let Some(server_processes) = db.server_processes.clone() {
            let mut server_processes = server_processes.lock().unwrap();
            *server_processes = new_server_processes;
        }
    }

    /// Setup kafka wal cluster if needed. The counterpart is in [GreptimeDB::stop].
    fn setup_wal(&self) {
        if matches!(self.wal, WalConfig::Kafka { needs_kafka_cluster, .. } if needs_kafka_cluster) {
            util::setup_wal();
        }
    }

    /// Setup etcd if needed.
    fn setup_etcd(&self) {
        if self.store_config.setup_etcd {
            let client_ports = self
                .store_config
                .store_addrs
                .iter()
                .map(|s| s.split(':').nth(1).unwrap().parse::<u16>().unwrap())
                .collect::<Vec<_>>();
            util::setup_etcd(client_ports, None, None);
        }
    }

    /// Setup PostgreSql if needed.
    fn setup_pg(&self) {
        if self.store_config.setup_pg {
            let client_ports = self
                .store_config
                .store_addrs
                .iter()
                .map(|s| s.split(':').nth(1).unwrap().parse::<u16>().unwrap())
                .collect::<Vec<_>>();
            let client_port = client_ports.first().unwrap_or(&5432);
            util::setup_pg(*client_port, None);
        }
    }

    /// Setup MySql if needed.
    async fn setup_mysql(&self) {
        if self.store_config.setup_mysql {
            let client_ports = self
                .store_config
                .store_addrs
                .iter()
                .map(|s| s.split(':').nth(1).unwrap().parse::<u16>().unwrap())
                .collect::<Vec<_>>();
            let client_port = client_ports.first().unwrap_or(&3306);
            util::setup_mysql(*client_port, None);

            // Docker of MySQL starts slowly, so we need to wait for a while
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    }

    /// Build the DB with `cargo build --bin greptime`
    fn build_db(&self) {
        if self.bins_dir.lock().unwrap().is_some() {
            return;
        }

        println!("Going to build the DB...");
        let output = Command::new("cargo")
            .current_dir(util::get_workspace_root())
            .args([
                "build",
                "--bin",
                "greptime",
                "--features",
                "pg_kvbackend,mysql_kvbackend",
            ])
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

        let _ = self
            .bins_dir
            .lock()
            .unwrap()
            .insert(util::get_binary_dir("debug"));
    }
}

pub struct GreptimeDB {
    server_processes: Option<Arc<Mutex<Vec<Child>>>>,
    metasrv_process: Mutex<Option<Child>>,
    frontend_process: Mutex<Option<Child>>,
    flownode_process: Mutex<Option<Child>>,
    grpc_client: TokioMutex<DB>,
    pg_client: TokioMutex<PgClient>,
    mysql_client: TokioMutex<MySqlClient>,
    ctx: GreptimeDBContext,
    is_standalone: bool,
    env: Env,
    id: usize,
}

impl GreptimeDB {
    async fn postgres_query(&self, _ctx: QueryContext, query: String) -> Box<dyn Display> {
        let client = self.pg_client.lock().await;
        match client.simple_query(&query).await {
            Ok(rows) => Box::new(PostgresqlFormatter { rows }),
            Err(e) => Box::new(format!("Failed to execute query, encountered: {:?}", e)),
        }
    }

    async fn mysql_query(&self, _ctx: QueryContext, query: String) -> Box<dyn Display> {
        let mut conn = self.mysql_client.lock().await;
        let result = conn.query_iter(query);
        Box::new(match result {
            Ok(result) => {
                let mut rows = vec![];
                let affected_rows = result.affected_rows();
                for row in result {
                    match row {
                        Ok(r) => rows.push(r),
                        Err(e) => {
                            return Box::new(format!("Failed to parse query result, err: {:?}", e))
                        }
                    }
                }

                if rows.is_empty() {
                    format!("affected_rows: {}", affected_rows)
                } else {
                    format!("{}", MysqlFormatter { rows })
                }
            }
            Err(e) => format!("Failed to execute query, err: {:?}", e),
        })
    }

    async fn grpc_query(&self, _ctx: QueryContext, query: String) -> Box<dyn Display> {
        let mut client = self.grpc_client.lock().await;

        let query_str = query.trim().to_lowercase();

        if query_str.starts_with("use ") {
            // use [db]
            let database = query
                .split_ascii_whitespace()
                .nth(1)
                .expect("Illegal `USE` statement: expecting a database.")
                .trim_end_matches(';');
            client.set_schema(database);
            Box::new(ResultDisplayer {
                result: Ok(Output::new_with_affected_rows(0)),
            }) as _
        } else if query_str.starts_with("set time_zone")
            || query_str.starts_with("set session time_zone")
            || query_str.starts_with("set local time_zone")
        {
            // set time_zone='xxx'
            let timezone = query
                .split('=')
                .nth(1)
                .expect("Illegal `SET TIMEZONE` statement: expecting a timezone expr.")
                .trim()
                .strip_prefix('\'')
                .unwrap()
                .strip_suffix("';")
                .unwrap();

            client.set_timezone(timezone);

            Box::new(ResultDisplayer {
                result: Ok(Output::new_with_affected_rows(0)),
            }) as _
        } else {
            let mut result = client.sql(&query).await;
            if let Ok(Output {
                data: OutputData::Stream(stream),
                ..
            }) = result
            {
                match RecordBatches::try_collect(stream).await {
                    Ok(recordbatches) => {
                        result = Ok(Output::new_with_record_batches(recordbatches));
                    }
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

#[async_trait]
impl Database for GreptimeDB {
    async fn query(&self, ctx: QueryContext, query: String) -> Box<dyn Display> {
        if ctx.context.contains_key("restart") && self.env.server_addrs.server_addr.is_none() {
            self.env.restart_server(self, false).await;
        } else if let Some(version) = ctx.context.get("version") {
            let version_bin_dir = self
                .env
                .versioned_bins_dirs
                .lock()
                .expect("lock poison")
                .get(version.as_str())
                .cloned();

            match version_bin_dir {
                Some(path) if path.clone().join(PROGRAM).is_file() => {
                    // use version in versioned_bins_dirs
                    *self.env.bins_dir.lock().unwrap() = Some(path.clone());
                }
                _ => {
                    // use version in dir files
                    maybe_pull_binary(version, self.env.pull_version_on_need).await;
                    let root = get_workspace_root();
                    let new_path = PathBuf::from_iter([&root, version]);
                    *self.env.bins_dir.lock().unwrap() = Some(new_path);
                }
            }

            self.env.restart_server(self, true).await;
            // sleep for a while to wait for the server to fully boot up
            tokio::time::sleep(Duration::from_secs(5)).await;
        }

        if let Some(protocol) = ctx.context.get(PROTOCOL_KEY) {
            // protocol is bound to be either "mysql" or "postgres"
            if protocol == MYSQL {
                self.mysql_query(ctx, query).await
            } else {
                self.postgres_query(ctx, query).await
            }
        } else {
            self.grpc_query(ctx, query).await
        }
    }
}

impl GreptimeDB {
    fn stop(&mut self) {
        if let Some(server_processes) = self.server_processes.clone() {
            let mut server_processes = server_processes.lock().unwrap();
            for mut server_process in server_processes.drain(..) {
                Env::stop_server(&mut server_process);
                println!(
                    "Standalone or Datanode (pid = {}) is stopped",
                    server_process.id()
                );
            }
        }
        if let Some(mut metasrv) = self
            .metasrv_process
            .lock()
            .expect("someone else panic when holding lock")
            .take()
        {
            Env::stop_server(&mut metasrv);
            println!("Metasrv (pid = {}) is stopped", metasrv.id());
        }
        if let Some(mut frontend) = self
            .frontend_process
            .lock()
            .expect("someone else panic when holding lock")
            .take()
        {
            Env::stop_server(&mut frontend);
            println!("Frontend (pid = {}) is stopped", frontend.id());
        }
        if let Some(mut flownode) = self
            .flownode_process
            .lock()
            .expect("someone else panic when holding lock")
            .take()
        {
            Env::stop_server(&mut flownode);
            println!("Flownode (pid = {}) is stopped", flownode.id());
        }
        if matches!(self.ctx.wal, WalConfig::Kafka { needs_kafka_cluster, .. } if needs_kafka_cluster)
        {
            util::teardown_wal();
        }
    }
}

impl Drop for GreptimeDB {
    fn drop(&mut self) {
        if self.env.server_addrs.server_addr.is_none() {
            self.stop();
        }
    }
}

pub struct GreptimeDBContext {
    /// Start time in millisecond
    time: i64,
    datanode_id: AtomicU32,
    wal: WalConfig,
    store_config: StoreConfig,
    server_modes: Vec<ServerMode>,
}

impl GreptimeDBContext {
    pub fn new(wal: WalConfig, store_config: StoreConfig) -> Self {
        Self {
            time: common_time::util::current_time_millis(),
            datanode_id: AtomicU32::new(0),
            wal,
            store_config,
            server_modes: Vec::new(),
        }
    }

    pub(crate) fn time(&self) -> i64 {
        self.time
    }

    pub fn is_raft_engine(&self) -> bool {
        matches!(self.wal, WalConfig::RaftEngine)
    }

    pub fn kafka_wal_broker_endpoints(&self) -> String {
        match &self.wal {
            WalConfig::RaftEngine => String::new(),
            WalConfig::Kafka {
                broker_endpoints, ..
            } => serde_json::to_string(&broker_endpoints).unwrap(),
        }
    }

    fn incr_datanode_id(&self) {
        let _ = self.datanode_id.fetch_add(1, Ordering::Relaxed);
    }

    fn reset_datanode_id(&self) {
        self.datanode_id.store(0, Ordering::Relaxed);
    }

    pub(crate) fn store_config(&self) -> StoreConfig {
        self.store_config.clone()
    }

    fn set_server_mode(&mut self, mode: ServerMode, idx: usize) {
        if idx >= self.server_modes.len() {
            self.server_modes.resize(idx + 1, mode.clone());
        }
        self.server_modes[idx] = mode;
    }

    fn get_server_mode(&self, idx: usize) -> Option<&ServerMode> {
        self.server_modes.get(idx)
    }
}

struct ResultDisplayer {
    result: Result<Output, ClientError>,
}

impl Display for ResultDisplayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.result {
            Ok(result) => match &result.data {
                OutputData::AffectedRows(rows) => {
                    write!(f, "Affected Rows: {rows}")
                }
                OutputData::RecordBatches(recordbatches) => {
                    let pretty = recordbatches.pretty_print().map_err(|e| e.to_string());
                    match pretty {
                        Ok(s) => write!(f, "{s}"),
                        Err(e) => {
                            write!(f, "Failed to pretty format {recordbatches:?}, error: {e}")
                        }
                    }
                }
                OutputData::Stream(_) => unreachable!(),
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

struct PostgresqlFormatter {
    pub rows: Vec<PgRow>,
}

impl Display for PostgresqlFormatter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.rows.is_empty() {
            return f.write_fmt(format_args!("(Empty response)"));
        }

        // create schema
        let schema = match &self.rows[0] {
            PgRow::CommandComplete(affected_rows) => {
                write!(
                    f,
                    "{}",
                    ResultDisplayer {
                        result: Ok(Output::new_with_affected_rows(*affected_rows as usize)),
                    }
                )?;
                return Ok(());
            }
            PgRow::RowDescription(desc) => Arc::new(Schema::new(
                desc.iter()
                    .map(|column| {
                        ColumnSchema::new(column.name(), ConcreteDataType::string_datatype(), false)
                    })
                    .collect(),
            )),
            _ => unreachable!(),
        };
        if schema.num_columns() == 0 {
            return Ok(());
        }

        // convert to string vectors
        let mut columns: Vec<StringVectorBuilder> = (0..schema.num_columns())
            .map(|_| StringVectorBuilder::with_capacity(schema.num_columns()))
            .collect();
        for row in self.rows.iter().skip(1) {
            if let PgRow::Row(row) = row {
                for (i, column) in columns.iter_mut().enumerate().take(schema.num_columns()) {
                    column.push(row.get(i));
                }
            }
        }
        let columns: Vec<VectorRef> = columns
            .into_iter()
            .map(|mut col| Arc::new(col.finish()) as VectorRef)
            .collect();

        // construct recordbatch
        let recordbatches = RecordBatches::try_from_columns(schema, columns)
            .expect("Failed to construct recordbatches from columns. Please check the schema.");
        let result_displayer = ResultDisplayer {
            result: Ok(Output::new_with_record_batches(recordbatches)),
        };
        write!(f, "{}", result_displayer)?;

        Ok(())
    }
}

struct MysqlFormatter {
    pub rows: Vec<MySqlRow>,
}

impl Display for MysqlFormatter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.rows.is_empty() {
            return f.write_fmt(format_args!("(Empty response)"));
        }
        // create schema
        let head_column = &self.rows[0];
        let head_binding = head_column.columns();
        let names = head_binding
            .iter()
            .map(|column| column.name_str())
            .collect::<Vec<Cow<str>>>();
        let schema = Arc::new(Schema::new(
            names
                .iter()
                .map(|name| {
                    ColumnSchema::new(name.to_string(), ConcreteDataType::string_datatype(), false)
                })
                .collect(),
        ));

        // convert to string vectors
        let mut columns: Vec<StringVectorBuilder> = (0..schema.num_columns())
            .map(|_| StringVectorBuilder::with_capacity(schema.num_columns()))
            .collect();
        for row in self.rows.iter() {
            for (i, name) in names.iter().enumerate() {
                columns[i].push(row.get::<String, &str>(name).as_deref());
            }
        }
        let columns: Vec<VectorRef> = columns
            .into_iter()
            .map(|mut col| Arc::new(col.finish()) as VectorRef)
            .collect();

        // construct recordbatch
        let recordbatches = RecordBatches::try_from_columns(schema, columns)
            .expect("Failed to construct recordbatches from columns. Please check the schema.");
        let result_displayer = ResultDisplayer {
            result: Ok(Output::new_with_record_batches(recordbatches)),
        };
        write!(f, "{}", result_displayer)?;

        Ok(())
    }
}
