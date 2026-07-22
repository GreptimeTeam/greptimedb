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

use std::collections::{BTreeSet, HashMap};
use std::fmt::Display;
use std::fs::{self, OpenOptions};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use common_error::ext::ErrorExt;
use sqlness::{Database, EnvController, QueryContext};
use tokio::sync::Mutex as TokioMutex;

use crate::client::MultiProtocolClient;
use crate::cmd::bare::ServerAddr;
use crate::cmd::compat_case::{ExpectedMetricPrimaryKeyEncoding, OldConfig, try_infer_version};
use crate::formatter::{ErrorFormatter, MysqlFormatter, OutputFormatter, PostgresqlFormatter};
use crate::protocol_interceptor::{MYSQL, PROTOCOL_KEY};
use crate::server_mode::{CompatConfigStage, GrpcArgStyle, ServerMode};
use crate::util;
use crate::util::{PROGRAM, get_workspace_root, maybe_pull_binary};

// standalone mode
const SERVER_MODE_STANDALONE_IDX: usize = 0;
// distributed mode
const SERVER_MODE_METASRV_IDX: usize = 0;
const SERVER_MODE_DATANODE_START_IDX: usize = 1;
const SERVER_MODE_FRONTEND_IDX: usize = 4;
const SERVER_MODE_FLOWNODE_IDX: usize = 5;
// Number of datanodes in distributed mode
const DISTRIBUTED_DATANODE_COUNT: usize = 3;
const OLD_ASSERT_POLL_INTERVAL: Duration = Duration::from_millis(50);
const OLD_ASSERT_POLL_DEADLINE: Duration = Duration::from_secs(5);
const OLD_ASSERT_QUIET_WINDOW: Duration = Duration::from_millis(225);

/// Byte offset captured for one old datanode log before a case's setup phase.
#[derive(Debug, Clone)]
pub(crate) struct DatanodeLogSnapshot {
    path: PathBuf,
    offset: u64,
    ends_with_newline: bool,
}

#[derive(Clone, Copy)]
struct AssertionTiming {
    poll_interval: Duration,
    deadline: Duration,
    quiet_window: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MetricEncodingEvent {
    region: String,
    encoding: ExpectedMetricPrimaryKeyEncoding,
}

fn datanode_log_file_name(id: usize, node_id: u32) -> String {
    format!("greptime-{id}-sqlness-datanode-{node_id}.log")
}

/// Parses complete metric-region creation records from a datanode log fragment.
fn parse_metric_encoding_events(records: &str) -> Result<Vec<MetricEncodingEvent>, String> {
    const PREFIX: &str = "Created physical metric region ";
    const MARKER: &str = ", primary key encoding=";

    records
        .lines()
        .map(|line| {
            let line = strip_ansi_csi(line);
            if !line.contains(PREFIX) {
                return Ok(None);
            }
            let (_, remainder) = line.split_once(PREFIX).expect("prefix checked");
            let (region, encoding) = match remainder.split_once(MARKER) {
                Some(parts) => parts,
                None => {
                    return Err(
                        "metric region candidate lacks primary-key encoding marker".to_string()
                    );
                }
            };
            let (encoding, _) = match encoding.split_once(',') {
                Some(parts) => parts,
                None => {
                    return Err("metric region candidate lacks trailing encoding comma".to_string());
                }
            };
            let encoding = match encoding {
                "Dense" => ExpectedMetricPrimaryKeyEncoding::Dense,
                "Sparse" => ExpectedMetricPrimaryKeyEncoding::Sparse,
                _ => {
                    return Err(format!(
                        "metric region candidate has unknown encoding '{encoding}'"
                    ));
                }
            };
            Ok(Some(MetricEncodingEvent {
                region: region.to_string(),
                encoding,
            }))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|events| events.into_iter().flatten().collect())
}

fn matched_metric_regions(
    events: &[MetricEncodingEvent],
    expected: ExpectedMetricPrimaryKeyEncoding,
) -> Result<BTreeSet<String>, String> {
    let mut regions = BTreeSet::new();
    for event in events {
        if event.encoding != expected {
            return Err(format!(
                "region '{}' reported {:?} primary-key encoding, expected {:?}",
                event.region, event.encoding, expected
            ));
        }
        regions.insert(event.region.clone());
    }
    Ok(regions)
}

fn strip_ansi_csi(input: &str) -> String {
    let mut stripped = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' && chars.next_if_eq(&'[').is_some() {
            for csi in chars.by_ref() {
                if ('@'..='~').contains(&csi) {
                    break;
                }
            }
        } else {
            stripped.push(ch);
        }
    }
    stripped
}

fn post_snapshot_records<'a>(snapshot: &DatanodeLogSnapshot, appended: &'a str) -> (&'a str, bool) {
    let appended = if snapshot.ends_with_newline {
        appended
    } else {
        match appended.find('\n') {
            Some(index) => &appended[index + 1..],
            None => "",
        }
    };
    match appended.rfind('\n') {
        Some(index) => (&appended[..=index], index + 1 != appended.len()),
        None => ("", !appended.is_empty()),
    }
}

fn read_appended_log(snapshot: &DatanodeLogSnapshot) -> Result<(String, u64), String> {
    let metadata = fs::metadata(&snapshot.path).map_err(|error| {
        format!(
            "Failed to read old datanode log {} after setup: {error}",
            snapshot.path.display()
        )
    })?;
    if metadata.len() < snapshot.offset {
        return Err(format!(
            "Old datanode log {} was truncated after setup snapshot ({} < {})",
            snapshot.path.display(),
            metadata.len(),
            snapshot.offset
        ));
    }
    let mut file = OpenOptions::new()
        .read(true)
        .open(&snapshot.path)
        .map_err(|error| {
            format!(
                "Failed to read old datanode log {} after setup: {error}",
                snapshot.path.display()
            )
        })?;
    file.seek(SeekFrom::Start(snapshot.offset))
        .map_err(|error| {
            format!(
                "Failed to seek old datanode log {}: {error}",
                snapshot.path.display()
            )
        })?;
    let mut appended = Vec::new();
    file.read_to_end(&mut appended).map_err(|error| {
        format!(
            "Failed to read old datanode log {} after setup: {error}",
            snapshot.path.display()
        )
    })?;
    let appended_len = u64::try_from(appended.len()).map_err(|_| {
        format!(
            "Old datanode log {} appended byte count does not fit in u64",
            snapshot.path.display()
        )
    })?;
    let observed_length = snapshot.offset.checked_add(appended_len).ok_or_else(|| {
        format!(
            "Old datanode log {} observed length overflowed",
            snapshot.path.display()
        )
    })?;
    Ok((
        String::from_utf8_lossy(&appended).into_owned(),
        observed_length,
    ))
}

fn truncate_diagnostic(value: &str) -> &str {
    const MAX_DIAGNOSTIC_BYTES: usize = 4096;
    if value.len() <= MAX_DIAGNOSTIC_BYTES {
        value
    } else {
        value.get(..MAX_DIAGNOSTIC_BYTES).unwrap_or(value)
    }
}

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

#[derive(Debug, Clone)]
pub(crate) enum ServiceProvider {
    Create,
    External(String),
}

impl From<&str> for ServiceProvider {
    fn from(value: &str) -> Self {
        if value.is_empty() {
            Self::Create
        } else {
            Self::External(value.to_string())
        }
    }
}

#[derive(Clone)]
pub struct StoreConfig {
    pub store_addrs: Vec<String>,
    pub setup_etcd: bool,
    pub(crate) setup_pg: Option<ServiceProvider>,
    pub(crate) setup_mysql: Option<ServiceProvider>,
    pub enable_flat_format: bool,
    pub enable_gc: bool,
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
    /// Extra command line arguments when starting GreptimeDB binaries.
    extra_args: Vec<String>,
    /// Cache for the inferred gRPC argument style per `bins_dir`.
    grpc_arg_style_cache: Arc<Mutex<HashMap<PathBuf, GrpcArgStyle>>>,
    /// Active compatibility configuration stage, shared by initial starts and restarts.
    compat_config_stage: Arc<Mutex<CompatConfigStage>>,
}

#[async_trait]
impl EnvController for Env {
    type DB = GreptimeDB;

    async fn start(&self, mode: &str, id: usize, _config: Option<&Path>) -> Self::DB {
        if self.server_addrs.server_addr.is_some() && id > 0 {
            panic!("Parallel test mode is not supported when server address is already set.");
        }

        unsafe {
            std::env::set_var("SQLNESS_HOME", self.sqlness_home.display().to_string());
        }
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
        extra_args: Vec<String>,
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
            extra_args,
            grpc_arg_style_cache: Arc::new(Mutex::new(HashMap::new())),
            compat_config_stage: Arc::new(Mutex::new(CompatConfigStage::Baseline)),
        }
    }

    async fn start_standalone(&self, id: usize) -> GreptimeDB {
        println!("Starting standalone instance id: {id}");

        if self.server_addrs.server_addr.is_some() {
            self.connect_db(&self.server_addrs, id).await
        } else {
            self.build_db();
            self.setup_wal();
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
        self.start_distributed_inner(id).await
    }

    /// Internal: start a distributed cluster with flownode.
    async fn start_distributed_inner(&self, id: usize) -> GreptimeDB {
        if self.server_addrs.server_addr.is_some() {
            self.connect_db(&self.server_addrs, id).await
        } else {
            self.build_db();
            self.setup_wal();
            self.setup_etcd();
            self.setup_pg();
            self.setup_mysql().await;
            let mut db_ctx = GreptimeDBContext::new(self.wal.clone(), self.store_config.clone());

            // start a distributed GreptimeDB
            let meta_server_mode = ServerMode::random_metasrv();
            let metasrv_port = match &meta_server_mode {
                ServerMode::Metasrv {
                    rpc_server_addr, ..
                } => rpc_server_addr
                    .split(':')
                    .nth(1)
                    .unwrap()
                    .parse::<u16>()
                    .unwrap(),
                _ => panic!(
                    "metasrv mode not set, maybe running in remote mode which doesn't support restart?"
                ),
            };
            db_ctx.set_server_mode(meta_server_mode.clone(), SERVER_MODE_METASRV_IDX);
            let meta_server = self.start_server(meta_server_mode, &db_ctx, id, true).await;

            let mut datanodes = Vec::with_capacity(DISTRIBUTED_DATANODE_COUNT);
            for i in 0..DISTRIBUTED_DATANODE_COUNT {
                let datanode_mode = ServerMode::random_datanode(metasrv_port, i as u32);
                db_ctx.set_server_mode(datanode_mode.clone(), SERVER_MODE_DATANODE_START_IDX + i);
                let datanode = self.start_server(datanode_mode, &db_ctx, id, true).await;
                datanodes.push(datanode);
            }

            let frontend_mode = ServerMode::random_frontend(metasrv_port);
            let server_addr = frontend_mode.server_addr().unwrap();
            db_ctx.set_server_mode(frontend_mode.clone(), SERVER_MODE_FRONTEND_IDX);
            let frontend = self.start_server(frontend_mode, &db_ctx, id, true).await;

            let flownode_mode = ServerMode::random_flownode(metasrv_port, 0);
            db_ctx.set_server_mode(flownode_mode.clone(), SERVER_MODE_FLOWNODE_IDX);
            let flownode = self.start_server(flownode_mode, &db_ctx, id, true).await;

            let mut greptimedb = self.connect_db(&server_addr, id).await;

            greptimedb.metasrv_process = Some(meta_server).into();
            greptimedb.server_processes = Some(Arc::new(Mutex::new(datanodes)));
            greptimedb.frontend_process = Some(frontend).into();
            greptimedb.flownode_process = Some(flownode).into();
            greptimedb.is_standalone = false;
            greptimedb.ctx = db_ctx;

            greptimedb
        }
    }

    async fn connect_db(&self, server_addr: &ServerAddr, id: usize) -> GreptimeDB {
        let grpc_server_addr = server_addr.server_addr.as_ref().unwrap();
        let pg_server_addr = server_addr.pg_server_addr.as_ref().unwrap();
        let mysql_server_addr = server_addr.mysql_server_addr.as_ref().unwrap();

        let client =
            MultiProtocolClient::connect(grpc_server_addr, pg_server_addr, mysql_server_addr).await;
        GreptimeDB {
            client: TokioMutex::new(client),
            server_processes: None,
            metasrv_process: None.into(),
            frontend_process: None.into(),
            flownode_process: None.into(),
            active_bins_dir: Mutex::new(self.bins_dir.lock().unwrap().clone()),
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

    /// Infers which gRPC argument style to use for the binary at `bins_dir`.
    fn infer_grpc_arg_style(&self, bins_dir: &Path) -> GrpcArgStyle {
        let cache_key = bins_dir.to_path_buf();

        // Fast path: already cached.
        {
            let cache = self.grpc_arg_style_cache.lock().unwrap();
            if let Some(style) = cache.get(&cache_key) {
                return *style;
            }
        }

        let version = try_infer_version(bins_dir);
        let style = GrpcArgStyle::for_version(version.as_ref());

        // Insert into cache (may race with another thread, but both detect
        // the same value, so it's harmless).
        {
            let mut cache = self.grpc_arg_style_cache.lock().unwrap();
            cache.entry(cache_key).or_insert(style);
        }

        style
    }

    async fn start_server(
        &self,
        mode: ServerMode,
        db_ctx: &GreptimeDBContext,
        id: usize,
        truncate_log: bool,
    ) -> Child {
        let bins_dir = self.bins_dir.lock().unwrap().clone().expect(
            "GreptimeDB binary is not available. Please pass in the path to the directory that contains the pre-built GreptimeDB binary. Or you may call `self.build_db()` beforehand.",
        );

        self.start_server_with_bins_dir(mode, db_ctx, id, truncate_log, bins_dir)
            .await
    }

    async fn start_server_with_bins_dir(
        &self,
        mode: ServerMode,
        db_ctx: &GreptimeDBContext,
        id: usize,
        truncate_log: bool,
        bins_dir: PathBuf,
    ) -> Child {
        let log_file_name = match mode {
            ServerMode::Datanode { node_id, .. } => {
                db_ctx.incr_datanode_id();
                datanode_log_file_name(id, node_id)
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
            .open(&stdout_file_name)
            .unwrap();

        let arg_style = self.infer_grpc_arg_style(&bins_dir);
        let compat_stage = self.compat_stage_for_start();
        let args = mode.get_args(
            &self.sqlness_home,
            self,
            db_ctx,
            id,
            arg_style,
            &compat_stage,
        );
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

        let abs_bins_dir = bins_dir
            .canonicalize()
            .expect("Failed to canonicalize bins_dir");

        let mut process = Command::new(abs_bins_dir.join(program))
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
            if !util::check_port(check_ip_addr.parse().unwrap(), Duration::from_secs(30)).await {
                Env::stop_server(&mut process);
                panic!(
                    "{} doesn't up in 30 seconds, check {} for more details.",
                    mode.name(),
                    stdout_file_name
                )
            }
        }

        process
    }

    /// stop and restart the server process
    pub(crate) async fn restart_server(&self, db: &GreptimeDB, is_full_restart: bool) {
        let bins_dir = db.active_bins_dir.lock().unwrap().clone().expect(
            "GreptimeDB binary is not available. Please pass in the path to the directory that contains the pre-built GreptimeDB binary. Or you may call `self.build_db()` beforehand.",
        );

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

            // Stop flownode if present.
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
            let new_server_process = self
                .start_server_with_bins_dir(server_mode, &db.ctx, db.id, false, bins_dir.clone())
                .await;

            let mut client = db.client.lock().await;
            client
                .reconnect_mysql_client(&server_addr.mysql_server_addr.unwrap())
                .await;
            client
                .reconnect_pg_client(&server_addr.pg_server_addr.unwrap())
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
                let metasrv = self
                    .start_server_with_bins_dir(
                        metasrv_mode,
                        &db.ctx,
                        db.id,
                        false,
                        bins_dir.clone(),
                    )
                    .await;
                db.metasrv_process
                    .lock()
                    .expect("lock poisoned")
                    .replace(metasrv);

                // wait for metasrv to start
                // since it seems older version of db might take longer to complete election
                tokio::time::sleep(Duration::from_secs(5)).await;
            }

            let mut processes = vec![];
            for i in 0..DISTRIBUTED_DATANODE_COUNT {
                let datanode_mode = db
                    .ctx
                    .get_server_mode(SERVER_MODE_DATANODE_START_IDX + i)
                    .cloned()
                    .unwrap();
                let new_server_process = self
                    .start_server_with_bins_dir(
                        datanode_mode,
                        &db.ctx,
                        db.id,
                        false,
                        bins_dir.clone(),
                    )
                    .await;
                processes.push(new_server_process);
            }

            if is_full_restart {
                let frontend_mode = db
                    .ctx
                    .get_server_mode(SERVER_MODE_FRONTEND_IDX)
                    .cloned()
                    .unwrap();
                let server_addr = frontend_mode.server_addr().unwrap();
                let frontend = self
                    .start_server_with_bins_dir(
                        frontend_mode,
                        &db.ctx,
                        db.id,
                        false,
                        bins_dir.clone(),
                    )
                    .await;
                db.frontend_process
                    .lock()
                    .expect("lock poisoned")
                    .replace(frontend);

                // Reconnect protocol clients to the new frontend process
                // so that MySQL/Postgres queries use the restarted frontend,
                // not stale connections to the old (killed) process.
                let mut client = db.client.lock().await;
                client
                    .reconnect_mysql_client(server_addr.mysql_server_addr.as_ref().unwrap())
                    .await;
                client
                    .reconnect_pg_client(server_addr.pg_server_addr.as_ref().unwrap())
                    .await;
            }

            // Restart flownode.
            if let Some(flownode_mode) = db.ctx.get_server_mode(SERVER_MODE_FLOWNODE_IDX).cloned() {
                let flownode = self
                    .start_server_with_bins_dir(
                        flownode_mode,
                        &db.ctx,
                        db.id,
                        false,
                        bins_dir.clone(),
                    )
                    .await;
                db.flownode_process
                    .lock()
                    .expect("lock poisoned")
                    .replace(flownode);
            }

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
        if matches!(self.store_config.setup_pg, Some(ServiceProvider::Create)) {
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
        if matches!(self.store_config.setup_mysql, Some(ServiceProvider::Create)) {
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
        let mut bins_dir = self.bins_dir.lock().unwrap();
        if bins_dir.is_some() {
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
                "pg_kvbackend,mysql_kvbackend,vector_index",
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

        bins_dir.replace(util::get_binary_dir("debug"));
    }

    pub(crate) fn extra_args(&self) -> &Vec<String> {
        &self.extra_args
    }

    /// Start a distributed GreptimeDB cluster. Exposed for compat runner.
    pub(crate) async fn compat_start_distributed(&self, id: usize) -> GreptimeDB {
        self.start_distributed(id).await
    }

    /// Activates a typed old-stage configuration profile for subsequent starts and restarts.
    pub(crate) fn compat_activate_old_profile(&self, old_config: OldConfig) {
        *self.compat_config_stage.lock().unwrap() = CompatConfigStage::Old(old_config);
    }

    /// Atomically clears the old-stage overlay before starting the current binary.
    pub(crate) fn compat_activate_current_profile(&self) {
        *self.compat_config_stage.lock().unwrap() = CompatConfigStage::Current;
    }

    /// Returns the stage that a subsequent server spawn or restart will render.
    pub(crate) fn compat_stage_for_start(&self) -> CompatConfigStage {
        self.compat_config_stage.lock().unwrap().clone()
    }

    /// Captures byte offsets for all distributed datanode logs before one setup case.
    pub(crate) fn compat_snapshot_datanode_logs(
        &self,
        id: usize,
    ) -> Result<Vec<DatanodeLogSnapshot>, String> {
        (0..DISTRIBUTED_DATANODE_COUNT)
            .map(|node_id| {
                let path = self
                    .sqlness_home
                    .join(datanode_log_file_name(id, node_id as u32));
                let mut file = OpenOptions::new().read(true).open(&path).map_err(|error| {
                    format!(
                        "Failed to snapshot old datanode log {}: {error}",
                        path.display()
                    )
                })?;
                let metadata = file.metadata().map_err(|error| {
                    format!(
                        "Failed to snapshot old datanode log {}: {error}",
                        path.display()
                    )
                })?;
                let offset = metadata.len();
                let ends_with_newline = if offset == 0 {
                    true
                } else {
                    file.seek(SeekFrom::Start(offset - 1)).map_err(|error| {
                        format!(
                            "Failed to inspect old datanode log {}: {error}",
                            path.display()
                        )
                    })?;
                    let mut byte = [0];
                    file.read_exact(&mut byte).map_err(|error| {
                        format!(
                            "Failed to inspect old datanode log {}: {error}",
                            path.display()
                        )
                    })?;
                    byte[0] == b'\n'
                };
                Ok(DatanodeLogSnapshot {
                    path,
                    offset,
                    ends_with_newline,
                })
            })
            .collect()
    }

    /// Waits for stable metric primary-key encoding events appended after snapshots.
    pub(crate) async fn compat_assert_metric_primary_key_encoding(
        &self,
        snapshots: &[DatanodeLogSnapshot],
        expected: ExpectedMetricPrimaryKeyEncoding,
    ) -> Result<(), String> {
        self.compat_assert_metric_primary_key_encoding_with_timing(
            snapshots,
            expected,
            AssertionTiming {
                poll_interval: OLD_ASSERT_POLL_INTERVAL,
                deadline: OLD_ASSERT_POLL_DEADLINE,
                quiet_window: OLD_ASSERT_QUIET_WINDOW,
            },
        )
        .await
    }

    async fn compat_assert_metric_primary_key_encoding_with_timing(
        &self,
        snapshots: &[DatanodeLogSnapshot],
        expected: ExpectedMetricPrimaryKeyEncoding,
        timing: AssertionTiming,
    ) -> Result<(), String> {
        let deadline = Instant::now() + timing.deadline;
        let mut previous_lengths = None;
        let mut quiet_since = Instant::now();
        let mut diagnostics = Vec::new();

        loop {
            let mut regions = BTreeSet::new();
            let mut lengths = Vec::with_capacity(snapshots.len());
            let mut has_incomplete = false;
            diagnostics.clear();
            for snapshot in snapshots {
                let (appended, length) = read_appended_log(snapshot)?;
                lengths.push(length);
                diagnostics.push(format!(
                    "{}: {}",
                    snapshot.path.display(),
                    truncate_diagnostic(&appended)
                ));
                let (records, incomplete) = post_snapshot_records(snapshot, &appended);
                has_incomplete |= incomplete;
                let events = parse_metric_encoding_events(records)?;
                let matched = matched_metric_regions(&events, expected).map_err(|error| {
                    format!("Old datanode log {} {error}", snapshot.path.display())
                })?;
                regions.extend(matched);
            }

            if previous_lengths.as_ref() != Some(&lengths) {
                previous_lengths = Some(lengths);
                quiet_since = Instant::now();
            }

            if !regions.is_empty()
                && !has_incomplete
                && quiet_since.elapsed() >= timing.quiet_window
            {
                return Ok(());
            }

            if Instant::now() >= deadline {
                return Err(format!(
                    "No stable old metric primary-key encoding event for expected {:?}; appended log diagnostics: {}",
                    expected,
                    diagnostics.join(" | ")
                ));
            }
            tokio::time::sleep(timing.poll_interval).await;
        }
    }

    /// Full restart of all distributed processes with a new binary directory,
    /// preserving the same context and data.
    /// After restart, waits for the frontend gRPC endpoint to become ready.
    pub(crate) async fn compat_restart_all(&self, db: &GreptimeDB, bins_dir: PathBuf) {
        *db.active_bins_dir.lock().unwrap() = Some(bins_dir);
        self.restart_server(db, true).await;
        self.wait_frontend_ready(db).await;
    }

    /// Wait for frontend gRPC readiness after restart.
    async fn wait_frontend_ready(&self, db: &GreptimeDB) {
        let frontend_mode = db
            .ctx
            .get_server_mode(SERVER_MODE_FRONTEND_IDX)
            .cloned()
            .unwrap();
        if let Some(addr) = frontend_mode.check_addrs().first() {
            println!("Waiting for frontend gRPC readiness at {addr}...");
            crate::util::retry_with_backoff(
                || async {
                    let mut client = db.client.lock().await;
                    match client.grpc_query("SELECT 1").await {
                        Ok(_) => Ok(()),
                        Err(e) => Err(format!("Frontend not ready: {e}")),
                    }
                },
                10,
                std::time::Duration::from_secs(1),
            )
            .await
            .unwrap_or_else(|e| panic!("Frontend failed to become ready: {e}"));
        }
    }
}

pub struct GreptimeDB {
    server_processes: Option<Arc<Mutex<Vec<Child>>>>,
    metasrv_process: Mutex<Option<Child>>,
    frontend_process: Mutex<Option<Child>>,
    flownode_process: Mutex<Option<Child>>,
    client: TokioMutex<MultiProtocolClient>,
    active_bins_dir: Mutex<Option<PathBuf>>,
    ctx: GreptimeDBContext,
    is_standalone: bool,
    env: Env,
    id: usize,
}

impl GreptimeDB {
    async fn postgres_query(&self, _ctx: QueryContext, query: String) -> Box<dyn Display> {
        let mut client = self.client.lock().await;

        match client.postgres_query(&query).await {
            Ok(rows) => Box::new(PostgresqlFormatter::from(rows)),
            Err(e) => Box::new(e),
        }
    }

    async fn mysql_query(&self, _ctx: QueryContext, query: String) -> Box<dyn Display> {
        let mut client = self.client.lock().await;

        match client.mysql_query(&query).await {
            Ok(res) => Box::new(MysqlFormatter::from(res)),
            Err(e) => Box::new(e),
        }
    }

    async fn grpc_query(&self, _ctx: QueryContext, query: String) -> Box<dyn Display> {
        let mut client = self.client.lock().await;

        match client.grpc_query(&query).await {
            Ok(rows) => Box::new(OutputFormatter::from(rows)),
            Err(e) => Box::new(ErrorFormatter::from(e)),
        }
    }

    /// Handle `QueryContext` directives for compat statement execution.
    ///
    /// Inspects `QueryContext` keys set by sqlness interceptors:
    /// - `restart`: restarts the server (datanode-only) if not using external address.
    /// - `version`: switches to the specified binary version and performs a full restart.
    ///
    /// This does **not** execute queries itself; it only prepares the server state.
    /// Used by the compat runner.
    pub(crate) async fn compat_prepare_query_context(&self, ctx: &QueryContext) {
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
                Some(path) if path.join(PROGRAM).is_file() => {
                    *self.active_bins_dir.lock().unwrap() = Some(path);
                }
                _ => {
                    maybe_pull_binary(version, self.env.pull_version_on_need).await;
                    let root = get_workspace_root();
                    let new_path = PathBuf::from_iter([&root, version]);
                    *self.active_bins_dir.lock().unwrap() = Some(new_path);
                }
            }

            self.env.restart_server(self, true).await;
            // sleep for a while to wait for the server to fully boot up
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    pub(crate) async fn compat_query(
        &self,
        query: &str,
        ctx: &QueryContext,
    ) -> Result<String, String> {
        let mut client = self.client.lock().await;

        // Handle protocol switching
        if let Some(protocol) = ctx.context.get(PROTOCOL_KEY) {
            if protocol == MYSQL {
                return match client.mysql_query(query).await {
                    Ok(res) => Ok(crate::formatter::MysqlFormatter::from(res).to_string()),
                    Err(e) => Err(e),
                };
            } else {
                // postgres
                return match client.postgres_query(query).await {
                    Ok(rows) => Ok(crate::formatter::PostgresqlFormatter::from(rows).to_string()),
                    Err(e) => Err(e),
                };
            }
        }

        // Default: gRPC
        match client.grpc_query(query).await {
            Ok(output) => Ok(OutputFormatter::from(output).to_string()),
            Err(e) => {
                let status_code = e.status_code();
                let root_cause = e.output_msg();
                Err(format!(
                    "Error: {}({status_code}), {root_cause}",
                    status_code as u32
                ))
            }
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
                Some(path) if path.join(PROGRAM).is_file() => {
                    // use version in versioned_bins_dirs
                    *self.active_bins_dir.lock().unwrap() = Some(path);
                }
                _ => {
                    // use version in dir files
                    maybe_pull_binary(version, self.env.pull_version_on_need).await;
                    let root = get_workspace_root();
                    let new_path = PathBuf::from_iter([&root, version]);
                    *self.active_bins_dir.lock().unwrap() = Some(new_path);
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

    /// Stop all processes managed by this GreptimeDB. Exposed for compat runner.
    pub(crate) fn compat_stop(&mut self) {
        self.stop();
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn test_env() -> Env {
        Env::new(
            PathBuf::from("."),
            ServerAddr::default(),
            WalConfig::RaftEngine,
            false,
            Some(PathBuf::from(".")),
            StoreConfig {
                store_addrs: vec![],
                setup_etcd: false,
                setup_pg: None,
                setup_mysql: None,
                enable_flat_format: false,
                enable_gc: false,
            },
            vec![],
        )
    }

    fn old_config() -> OldConfig {
        OldConfig {
            datanode: BTreeMap::from([(
                "region_engine.metric.sparse_primary_key_encoding".to_string(),
                false,
            )]),
        }
    }

    #[test]
    fn test_compat_stage_keeps_old_for_setup_restarts_then_stays_current() {
        let env = test_env();
        env.compat_activate_old_profile(old_config());
        assert!(matches!(
            env.compat_stage_for_start(),
            CompatConfigStage::Old(_)
        ));
        // A setup restart consumes the same stage and therefore retains the overlay.
        assert!(matches!(
            env.compat_stage_for_start(),
            CompatConfigStage::Old(_)
        ));

        env.compat_activate_current_profile();
        // The transition happens before a current spawn, and verify restarts stay clean.
        assert!(matches!(
            env.compat_stage_for_start(),
            CompatConfigStage::Current
        ));
        assert!(matches!(
            env.compat_stage_for_start(),
            CompatConfigStage::Current
        ));
    }

    fn event(region: &str, encoding: &str) -> String {
        format!(
            "Created physical metric region {region}, primary key encoding={encoding}, created\n"
        )
    }

    #[test]
    fn test_metric_encoding_parser_accepts_dense_duplicates_and_multiple_regions() {
        let records = format!(
            "{}{}{}",
            event("a,1", "Dense"),
            event("a,1", "Dense"),
            event("b", "Dense")
        );
        let events = parse_metric_encoding_events(&records).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(
            matched_metric_regions(&events, ExpectedMetricPrimaryKeyEncoding::Dense).unwrap(),
            BTreeSet::from(["a,1".to_string(), "b".to_string()])
        );
    }

    #[test]
    fn test_metric_encoding_parser_rejects_mixed_or_opposite_encodings() {
        let sparse = parse_metric_encoding_events(&event("region", "Sparse")).unwrap();
        assert!(matched_metric_regions(&sparse, ExpectedMetricPrimaryKeyEncoding::Dense).is_err());

        let mixed = parse_metric_encoding_events(&format!(
            "{}{}",
            event("dense", "Dense"),
            event("sparse", "Sparse")
        ))
        .unwrap();
        assert!(matched_metric_regions(&mixed, ExpectedMetricPrimaryKeyEncoding::Dense).is_err());
        assert!(
            matched_metric_regions(&[], ExpectedMetricPrimaryKeyEncoding::Dense)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn test_metric_encoding_parser_handles_ansi_prefixes_and_partial_records() {
        let prefixed = format!(
            "{{\"msg\":\"\u{1b}[32m{}\u{1b}[0m\"}}\n",
            event("ansi", "Dense").trim()
        );
        let events = parse_metric_encoding_events(&prefixed).unwrap();
        assert_eq!(events.len(), 1);

        let partial = "Created physical metric region split, primary key encoding=Dense,";
        assert!(parse_metric_encoding_events("").unwrap().is_empty());
        assert_eq!(
            parse_metric_encoding_events(&format!("{partial}\n"))
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn test_metric_encoding_parser_rejects_malformed_candidates() {
        for line in [
            "Created physical metric region r",
            "Created physical metric region r, primary key encoding=Dense",
            "Created physical metric region r, primary key encoding=Unknown,",
        ] {
            assert!(parse_metric_encoding_events(line).is_err(), "{line}");
        }
    }

    #[test]
    fn test_pre_snapshot_partial_line_is_discarded_through_its_boundary() {
        let snapshot = DatanodeLogSnapshot {
            path: PathBuf::from("unused"),
            offset: 1,
            ends_with_newline: false,
        };
        let appended = format!(
            "old suffix, primary key encoding=Dense,\n{}",
            event("new", "Dense")
        );
        let (records, incomplete) = post_snapshot_records(&snapshot, &appended);
        assert!(!incomplete);
        let events = parse_metric_encoding_events(records).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].region, "new");
    }

    #[test]
    fn test_real_snapshot_discards_a_pre_snapshot_partial_line() {
        let temp_dir = tempfile::tempdir().unwrap();
        let env = Env::new(
            temp_dir.path().to_path_buf(),
            ServerAddr::default(),
            WalConfig::RaftEngine,
            false,
            Some(PathBuf::from(".")),
            StoreConfig {
                store_addrs: vec![],
                setup_etcd: false,
                setup_pg: None,
                setup_mysql: None,
                enable_flat_format: false,
                enable_gc: false,
            },
            vec![],
        );
        for node_id in 0..DISTRIBUTED_DATANODE_COUNT {
            let content = if node_id == 0 { "old partial" } else { "" };
            std::fs::write(
                temp_dir
                    .path()
                    .join(datanode_log_file_name(0, node_id as u32)),
                content,
            )
            .unwrap();
        }
        let snapshots = env.compat_snapshot_datanode_logs(0).unwrap();
        assert!(!snapshots[0].ends_with_newline);
        OpenOptions::new()
            .append(true)
            .open(&snapshots[0].path)
            .unwrap()
            .write_all(
                format!(", primary key encoding=Sparse,\n{}", event("new", "Dense")).as_bytes(),
            )
            .unwrap();

        let (appended, _) = read_appended_log(&snapshots[0]).unwrap();
        let (records, incomplete) = post_snapshot_records(&snapshots[0], &appended);
        assert!(!incomplete);
        let events = parse_metric_encoding_events(records).unwrap();
        assert_eq!(
            events,
            vec![MetricEncodingEvent {
                region: "new".to_string(),
                encoding: ExpectedMetricPrimaryKeyEncoding::Dense
            }]
        );
    }

    #[test]
    fn test_completed_dense_then_completed_sparse_fails_after_partial_sparse_waits() {
        let snapshot = DatanodeLogSnapshot {
            path: PathBuf::from("unused"),
            offset: 0,
            ends_with_newline: true,
        };
        let partial = format!(
            "{}Created physical metric region sparse, primary",
            event("dense", "Dense")
        );
        let (records, incomplete) = post_snapshot_records(&snapshot, &partial);
        assert!(incomplete);
        assert!(
            matched_metric_regions(
                &parse_metric_encoding_events(records).unwrap(),
                ExpectedMetricPrimaryKeyEncoding::Dense
            )
            .is_ok()
        );

        let completed = format!("{} key encoding=Sparse,\n", partial);
        let (records, incomplete) = post_snapshot_records(&snapshot, &completed);
        assert!(!incomplete);
        assert!(
            matched_metric_regions(
                &parse_metric_encoding_events(records).unwrap(),
                ExpectedMetricPrimaryKeyEncoding::Dense
            )
            .is_err()
        );
    }

    #[test]
    fn test_datanode_log_snapshots_use_independent_offsets_and_ignore_previous_content() {
        let temp_dir = tempfile::tempdir().unwrap();
        let env = Env::new(
            temp_dir.path().to_path_buf(),
            ServerAddr::default(),
            WalConfig::RaftEngine,
            false,
            Some(PathBuf::from(".")),
            StoreConfig {
                store_addrs: vec![],
                setup_etcd: false,
                setup_pg: None,
                setup_mysql: None,
                enable_flat_format: false,
                enable_gc: false,
            },
            vec![],
        );
        for node_id in 0..DISTRIBUTED_DATANODE_COUNT {
            std::fs::write(
                temp_dir
                    .path()
                    .join(datanode_log_file_name(0, node_id as u32)),
                event(&format!("old-{node_id}"), "Sparse"),
            )
            .unwrap();
        }
        let snapshots = env.compat_snapshot_datanode_logs(0).unwrap();
        for (node_id, snapshot) in snapshots.iter().enumerate() {
            std::fs::write(
                &snapshot.path,
                format!(
                    "{}{}",
                    event(&format!("old-{node_id}"), "Sparse"),
                    event(&format!("new-{node_id}"), "Dense")
                ),
            )
            .unwrap();
        }

        for (node_id, snapshot) in snapshots.iter().enumerate() {
            let (appended, _) = read_appended_log(snapshot).unwrap();
            assert_eq!(appended, event(&format!("new-{node_id}"), "Dense"));
        }
    }

    #[test]
    fn test_snapshot_rejects_missing_datanode_log() {
        let temp_dir = tempfile::tempdir().unwrap();
        let env = Env::new(
            temp_dir.path().to_path_buf(),
            ServerAddr::default(),
            WalConfig::RaftEngine,
            false,
            Some(PathBuf::from(".")),
            StoreConfig {
                store_addrs: vec![],
                setup_etcd: false,
                setup_pg: None,
                setup_mysql: None,
                enable_flat_format: false,
                enable_gc: false,
            },
            vec![],
        );
        std::fs::write(temp_dir.path().join(datanode_log_file_name(0, 0)), "").unwrap();
        std::fs::write(temp_dir.path().join(datanode_log_file_name(0, 1)), "").unwrap();
        assert!(env.compat_snapshot_datanode_logs(0).is_err());
    }

    #[test]
    fn test_datanode_log_reader_rejects_truncation_and_read_errors() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("datanode.log");
        std::fs::write(&path, "before\n").unwrap();
        let snapshot = DatanodeLogSnapshot {
            path: path.clone(),
            offset: 7,
            ends_with_newline: true,
        };
        std::fs::write(&path, "x\n").unwrap();
        assert!(
            read_appended_log(&snapshot)
                .unwrap_err()
                .contains("truncated")
        );

        let missing = DatanodeLogSnapshot {
            path: temp_dir.path().join("missing.log"),
            offset: 0,
            ends_with_newline: true,
        };
        assert!(read_appended_log(&missing).is_err());
    }

    #[tokio::test]
    async fn test_metric_assertion_waits_for_completed_stable_records() {
        let temp_dir = tempfile::tempdir().unwrap();
        let env = Env::new(
            temp_dir.path().to_path_buf(),
            ServerAddr::default(),
            WalConfig::RaftEngine,
            false,
            Some(PathBuf::from(".")),
            StoreConfig {
                store_addrs: vec![],
                setup_etcd: false,
                setup_pg: None,
                setup_mysql: None,
                enable_flat_format: false,
                enable_gc: false,
            },
            vec![],
        );
        for node_id in 0..DISTRIBUTED_DATANODE_COUNT {
            std::fs::write(
                temp_dir
                    .path()
                    .join(datanode_log_file_name(0, node_id as u32)),
                "",
            )
            .unwrap();
        }
        let snapshots = env.compat_snapshot_datanode_logs(0).unwrap();
        let event = event("split", "Dense");
        let split_at = event.len() / 2;
        std::fs::write(&snapshots[0].path, &event[..split_at]).unwrap();
        let path = snapshots[0].path.clone();
        let remainder = event[split_at..].to_string();
        let writer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(75)).await;
            OpenOptions::new()
                .append(true)
                .open(path)
                .unwrap()
                .write_all(remainder.as_bytes())
                .unwrap();
        });

        env.compat_assert_metric_primary_key_encoding(
            &snapshots,
            ExpectedMetricPrimaryKeyEncoding::Dense,
        )
        .await
        .unwrap();
        writer.await.unwrap();
    }

    #[tokio::test]
    async fn test_polling_waits_for_incomplete_sparse_then_rejects_it_when_completed() {
        let temp_dir = tempfile::tempdir().unwrap();
        let env = Env::new(
            temp_dir.path().to_path_buf(),
            ServerAddr::default(),
            WalConfig::RaftEngine,
            false,
            Some(PathBuf::from(".")),
            StoreConfig {
                store_addrs: vec![],
                setup_etcd: false,
                setup_pg: None,
                setup_mysql: None,
                enable_flat_format: false,
                enable_gc: false,
            },
            vec![],
        );
        for node_id in 0..DISTRIBUTED_DATANODE_COUNT {
            std::fs::write(
                temp_dir
                    .path()
                    .join(datanode_log_file_name(0, node_id as u32)),
                "",
            )
            .unwrap();
        }
        let snapshots = env.compat_snapshot_datanode_logs(0).unwrap();
        OpenOptions::new()
            .append(true)
            .open(&snapshots[0].path)
            .unwrap()
            .write_all(
                format!(
                    "{}Created physical metric region sparse, primary",
                    event("dense", "Dense")
                )
                .as_bytes(),
            )
            .unwrap();
        let assertion_env = env.clone();
        let assertion_snapshots = snapshots.clone();
        let assertion = tokio::spawn(async move {
            assertion_env
                .compat_assert_metric_primary_key_encoding_with_timing(
                    &assertion_snapshots,
                    ExpectedMetricPrimaryKeyEncoding::Dense,
                    AssertionTiming {
                        poll_interval: Duration::from_millis(5),
                        deadline: Duration::from_millis(500),
                        quiet_window: Duration::from_millis(20),
                    },
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(!assertion.is_finished());
        OpenOptions::new()
            .append(true)
            .open(&snapshots[0].path)
            .unwrap()
            .write_all(b" key encoding=Sparse,\n")
            .unwrap();
        let error = assertion.await.unwrap().unwrap_err();
        assert!(error.contains("Sparse"));
    }

    #[tokio::test]
    async fn test_metric_assertion_rejects_zero_events() {
        let temp_dir = tempfile::tempdir().unwrap();
        let env = Env::new(
            temp_dir.path().to_path_buf(),
            ServerAddr::default(),
            WalConfig::RaftEngine,
            false,
            Some(PathBuf::from(".")),
            StoreConfig {
                store_addrs: vec![],
                setup_etcd: false,
                setup_pg: None,
                setup_mysql: None,
                enable_flat_format: false,
                enable_gc: false,
            },
            vec![],
        );
        for node_id in 0..DISTRIBUTED_DATANODE_COUNT {
            std::fs::write(
                temp_dir
                    .path()
                    .join(datanode_log_file_name(0, node_id as u32)),
                "unrelated old datanode log\n",
            )
            .unwrap();
        }
        let snapshots = env.compat_snapshot_datanode_logs(0).unwrap();

        let error = env
            .compat_assert_metric_primary_key_encoding_with_timing(
                &snapshots,
                ExpectedMetricPrimaryKeyEncoding::Dense,
                AssertionTiming {
                    poll_interval: Duration::from_millis(1),
                    deadline: Duration::from_millis(20),
                    quiet_window: Duration::from_millis(5),
                },
            )
            .await
            .unwrap_err();
        assert!(error.contains("No stable old metric primary-key encoding event"));
    }
}
