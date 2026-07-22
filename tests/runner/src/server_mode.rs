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

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Mutex, OnceLock};

use serde::Serialize;
use tinytemplate::TinyTemplate;
use toml::Value;

use crate::cmd::bare::ServerAddr;
use crate::cmd::compat_case::{OldConfig, Version, validate_old_datanode_path};
use crate::env::bare::{Env, GreptimeDBContext, ServiceProvider};
use crate::util;

const DEFAULT_LOG_LEVEL: &str = "--log-level=debug,hyper=warn,tower=warn,datafusion=warn,reqwest=warn,sqlparser=warn,h2=info,opendal=info";

/// The explicitly active compatibility configuration stage.
#[derive(Debug, Clone, Default)]
pub(crate) enum CompatConfigStage {
    /// Ordinary rendering without compatibility-specific configuration.
    #[default]
    Baseline,
    /// Old binary configuration with its typed datanode overlay.
    Old(OldConfig),
    /// Newly rendered current-binary configuration with no old overlay.
    Current,
}

impl CompatConfigStage {
    fn file_suffix(&self) -> &'static str {
        match self {
            Self::Baseline => "",
            Self::Old(_) => "-old",
            Self::Current => "-current",
        }
    }

    fn old_config(&self) -> Option<&OldConfig> {
        match self {
            Self::Old(config) => Some(config),
            Self::Baseline | Self::Current => None,
        }
    }
}

/// Which set of gRPC CLI argument names to use when spawning a GreptimeDB binary.
///
/// The CLI rename from `rpc-*` to `grpc-*` landed before v1.1.0.  Older release
/// binaries (e.g. v1.0.0) only recognize `--rpc-bind-addr` and
/// `--rpc-server-addr`; v1.1.0+ and current binaries use `--grpc-*` names while
/// keeping the old names as hidden aliases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GrpcArgStyle {
    /// Current-style: `--grpc-bind-addr` / `--grpc-server-addr`
    Grpc,
    /// Legacy-style: `--rpc-bind-addr` / `--rpc-server-addr`
    Rpc,
}

impl GrpcArgStyle {
    /// Chooses the argument style from an inferred GreptimeDB binary version.
    ///
    /// Unknown versions are treated as current/development binaries and use the
    /// official `grpc-*` names.
    pub(crate) fn for_version(version: Option<&Version>) -> Self {
        const GRPC_ARG_RENAME_VERSION: Version = Version {
            major: 1,
            minor: 1,
            patch: 0,
        };

        if version.is_some_and(|version| version < &GRPC_ARG_RENAME_VERSION) {
            GrpcArgStyle::Rpc
        } else {
            GrpcArgStyle::Grpc
        }
    }

    /// Returns the CLI flag name for the gRPC bind address.
    pub fn bind_addr_arg(self) -> &'static str {
        match self {
            GrpcArgStyle::Grpc => "--grpc-bind-addr",
            GrpcArgStyle::Rpc => "--rpc-bind-addr",
        }
    }

    /// Returns the CLI flag name for the gRPC server (advertised) address.
    pub fn server_addr_arg(self) -> &'static str {
        match self {
            GrpcArgStyle::Grpc => "--grpc-server-addr",
            GrpcArgStyle::Rpc => "--rpc-server-addr",
        }
    }
}

static USED_PORTS: OnceLock<Mutex<HashSet<u16>>> = OnceLock::new();

fn get_used_ports() -> &'static Mutex<HashSet<u16>> {
    USED_PORTS.get_or_init(|| Mutex::new(HashSet::new()))
}

fn get_unique_random_port() -> u16 {
    // Tricky loop 100 times to find an unused port instead of infinite loop.
    const MAX_ATTEMPTS: usize = 100;

    for _ in 0..MAX_ATTEMPTS {
        let p = util::get_random_port();
        let mut used = get_used_ports().lock().unwrap();
        if !used.contains(&p) {
            used.insert(p);
            return p;
        }
    }

    panic!(
        "Failed to find an unused port after {} attempts",
        MAX_ATTEMPTS
    );
}

#[derive(Clone)]
pub enum ServerMode {
    Standalone {
        http_addr: String,
        rpc_bind_addr: String,
        mysql_addr: String,
        postgres_addr: String,
    },
    Frontend {
        http_addr: String,
        rpc_bind_addr: String,
        mysql_addr: String,
        postgres_addr: String,
        metasrv_addr: String,
    },
    Metasrv {
        rpc_bind_addr: String,
        rpc_server_addr: String,
        http_addr: String,
    },
    Datanode {
        rpc_bind_addr: String,
        rpc_server_addr: String,
        http_addr: String,
        metasrv_addr: String,
        node_id: u32,
    },
    Flownode {
        rpc_bind_addr: String,
        rpc_server_addr: String,
        http_addr: String,
        metasrv_addr: String,
        node_id: u32,
    },
}

#[derive(Serialize)]
struct ConfigContext {
    wal_dir: String,
    data_home: String,
    procedure_dir: String,
    is_raft_engine: bool,
    kafka_wal_broker_endpoints: String,
    use_etcd: bool,
    store_addrs: String,
    instance_id: usize,
    addrs: HashMap<String, String>,
    // enable flat format for storage engine
    enable_flat_format: bool,
    // enable garbage collection in metasrv and datanodes
    enable_gc: bool,
}

/// Applies the intentionally narrow compatibility overlay to rendered datanode TOML.
///
/// This is not a generic TOML merge: every logical path selects one named entry in
/// `[[region_engine]]`, then descends only through table values to set a boolean leaf.
pub(crate) fn apply_old_datanode_overlay(
    rendered: &str,
    old_config: &OldConfig,
) -> Result<String, String> {
    let mut document: Value = toml::from_str(rendered)
        .map_err(|e| format!("Failed to parse rendered datanode TOML: {e}"))?;

    for (path, value) in &old_config.datanode {
        validate_old_datanode_path(path)?;
        apply_region_engine_bool(&mut document, path, *value)?;
    }

    toml::to_string(&document).map_err(|e| format!("Failed to render datanode TOML: {e}"))
}

/// Validates an old-stage overlay against the rendered datanode template used by
/// the runner without creating state or starting services.
pub(crate) fn validate_old_config_against_datanode_template(
    old_config: &OldConfig,
) -> Result<(), String> {
    let mut path = util::sqlness_conf_path();
    path.push("datanode-test.toml.template");
    let template = std::fs::read_to_string(&path)
        .map_err(|e| format!("Failed to read datanode template '{}': {e}", path.display()))?;
    let mut tt = TinyTemplate::new();
    tt.add_template("datanode", &template)
        .map_err(|e| format!("Failed to parse datanode template: {e}"))?;
    let rendered = tt
        .render(
            "datanode",
            &ConfigContext {
                wal_dir: "/tmp/wal".to_string(),
                data_home: "/tmp/data".to_string(),
                procedure_dir: "/tmp/procedure".to_string(),
                is_raft_engine: true,
                kafka_wal_broker_endpoints: String::new(),
                use_etcd: true,
                store_addrs: "\"127.0.0.1:2379\"".to_string(),
                instance_id: 0,
                addrs: [("metasrv_addr".to_string(), "127.0.0.1:3002".to_string())].into(),
                enable_flat_format: false,
                enable_gc: false,
            },
        )
        .map_err(|e| format!("Failed to render datanode template: {e}"))?;

    apply_old_datanode_overlay(&rendered, old_config).map(|_| ())
}

fn apply_region_engine_bool(document: &mut Value, path: &str, value: bool) -> Result<(), String> {
    let segments: Vec<_> = path.split('.').collect();
    let engine = segments[1];
    let leaf_path = &segments[2..];
    let root = document
        .as_table_mut()
        .ok_or_else(|| "Rendered datanode TOML root must be a table".to_string())?;
    let entries = root
        .get_mut("region_engine")
        .ok_or_else(|| "Rendered datanode TOML has no region_engine array".to_string())?
        .as_array_mut()
        .ok_or_else(|| "Rendered datanode TOML region_engine must be an array".to_string())?;

    let matching_entries: Vec<_> = entries
        .iter()
        .enumerate()
        .filter_map(|(index, entry)| {
            entry
                .as_table()
                .and_then(|table| table.contains_key(engine).then_some(index))
        })
        .collect();
    if matching_entries.len() > 1 {
        return Err(format!(
            "Rendered datanode TOML has duplicate region_engine entry '{engine}'"
        ));
    }

    let entry_index = if let Some(index) = matching_entries.first() {
        *index
    } else {
        let mut engine_table = toml::map::Map::new();
        engine_table.insert(engine.to_string(), Value::Table(toml::map::Map::new()));
        entries.push(Value::Table(engine_table));
        entries.len() - 1
    };

    let entry = entries[entry_index]
        .as_table_mut()
        .ok_or_else(|| format!("region_engine entry '{engine}' must be a table"))?;
    let engine_value = entry
        .get_mut(engine)
        .ok_or_else(|| format!("region_engine entry '{engine}' is missing"))?;
    let mut current = engine_value
        .as_table_mut()
        .ok_or_else(|| format!("region_engine entry '{engine}' must be a table"))?;

    for segment in &leaf_path[..leaf_path.len() - 1] {
        let node = current
            .entry((*segment).to_string())
            .or_insert_with(|| Value::Table(toml::map::Map::new()));
        current = node.as_table_mut().ok_or_else(|| {
            format!("old_config.datanode key '{path}' has non-table intermediate '{segment}'")
        })?;
    }

    let leaf = leaf_path.last().expect("validated path has a leaf");
    if let Some(existing) = current.get(*leaf)
        && !existing.is_bool()
    {
        return Err(format!(
            "old_config.datanode key '{path}' conflicts with an existing non-bool value"
        ));
    }
    current.insert((*leaf).to_string(), Value::Boolean(value));
    Ok(())
}

impl ServerMode {
    pub fn random_standalone() -> Self {
        let http_port = get_unique_random_port();
        let rpc_port = get_unique_random_port();
        let mysql_port = get_unique_random_port();
        let postgres_port = get_unique_random_port();

        ServerMode::Standalone {
            http_addr: format!("127.0.0.1:{http_port}"),
            rpc_bind_addr: format!("127.0.0.1:{rpc_port}"),
            mysql_addr: format!("127.0.0.1:{mysql_port}"),
            postgres_addr: format!("127.0.0.1:{postgres_port}"),
        }
    }

    pub fn random_frontend(metasrv_port: u16) -> Self {
        let http_port = get_unique_random_port();
        let rpc_port = get_unique_random_port();
        let mysql_port = get_unique_random_port();
        let postgres_port = get_unique_random_port();

        ServerMode::Frontend {
            http_addr: format!("127.0.0.1:{http_port}"),
            rpc_bind_addr: format!("127.0.0.1:{rpc_port}"),
            mysql_addr: format!("127.0.0.1:{mysql_port}"),
            postgres_addr: format!("127.0.0.1:{postgres_port}"),
            metasrv_addr: format!("127.0.0.1:{metasrv_port}"),
        }
    }

    pub fn random_metasrv() -> Self {
        let bind_port = get_unique_random_port();
        let http_port = get_unique_random_port();

        ServerMode::Metasrv {
            rpc_bind_addr: format!("127.0.0.1:{bind_port}"),
            rpc_server_addr: format!("127.0.0.1:{bind_port}"),
            http_addr: format!("127.0.0.1:{http_port}"),
        }
    }

    pub fn random_datanode(metasrv_port: u16, node_id: u32) -> Self {
        let rpc_port = get_unique_random_port();
        let http_port = get_unique_random_port();

        ServerMode::Datanode {
            rpc_bind_addr: format!("127.0.0.1:{rpc_port}"),
            rpc_server_addr: format!("127.0.0.1:{rpc_port}"),
            http_addr: format!("127.0.0.1:{http_port}"),
            metasrv_addr: format!("127.0.0.1:{metasrv_port}"),
            node_id,
        }
    }

    pub fn random_flownode(metasrv_port: u16, node_id: u32) -> Self {
        let rpc_port = get_unique_random_port();
        let http_port = get_unique_random_port();

        ServerMode::Flownode {
            rpc_bind_addr: format!("127.0.0.1:{rpc_port}"),
            rpc_server_addr: format!("127.0.0.1:{rpc_port}"),
            http_addr: format!("127.0.0.1:{http_port}"),
            metasrv_addr: format!("127.0.0.1:{metasrv_port}"),
            node_id,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            ServerMode::Standalone { .. } => "standalone",
            ServerMode::Frontend { .. } => "frontend",
            ServerMode::Metasrv { .. } => "metasrv",
            ServerMode::Datanode { .. } => "datanode",
            ServerMode::Flownode { .. } => "flownode",
        }
    }

    /// Returns the addresses of the server that needed to be checked.
    pub fn check_addrs(&self) -> Vec<String> {
        match self {
            ServerMode::Standalone {
                rpc_bind_addr,
                mysql_addr,
                postgres_addr,
                http_addr,
                ..
            } => {
                vec![
                    rpc_bind_addr.clone(),
                    mysql_addr.clone(),
                    postgres_addr.clone(),
                    http_addr.clone(),
                ]
            }
            ServerMode::Frontend {
                rpc_bind_addr,
                mysql_addr,
                postgres_addr,
                ..
            } => {
                vec![
                    rpc_bind_addr.clone(),
                    mysql_addr.clone(),
                    postgres_addr.clone(),
                ]
            }
            ServerMode::Metasrv { rpc_bind_addr, .. } => {
                vec![rpc_bind_addr.clone()]
            }
            ServerMode::Datanode { rpc_bind_addr, .. } => {
                vec![rpc_bind_addr.clone()]
            }
            ServerMode::Flownode { rpc_bind_addr, .. } => {
                vec![rpc_bind_addr.clone()]
            }
        }
    }

    /// Returns the server addresses to connect. Only standalone and frontend mode have this.
    pub fn server_addr(&self) -> Option<ServerAddr> {
        match self {
            ServerMode::Standalone {
                rpc_bind_addr,
                mysql_addr,
                postgres_addr,
                ..
            } => Some(ServerAddr {
                server_addr: Some(rpc_bind_addr.clone()),
                pg_server_addr: Some(postgres_addr.clone()),
                mysql_server_addr: Some(mysql_addr.clone()),
            }),
            ServerMode::Frontend {
                rpc_bind_addr,
                mysql_addr,
                postgres_addr,
                ..
            } => Some(ServerAddr {
                server_addr: Some(rpc_bind_addr.clone()),
                pg_server_addr: Some(postgres_addr.clone()),
                mysql_server_addr: Some(mysql_addr.clone()),
            }),
            _ => None,
        }
    }

    pub fn generate_config_file(
        &self,
        sqlness_home: &Path,
        db_ctx: &GreptimeDBContext,
        id: usize,
        compat_stage: &CompatConfigStage,
    ) -> String {
        let mut tt = TinyTemplate::new();

        let mut path = util::sqlness_conf_path();
        path.push(format!("{}-test.toml.template", self.name()));
        let template = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read file '{}' error: {e}", path.display()));
        tt.add_template(self.name(), &template).unwrap();

        let data_home = sqlness_home.join(format!("greptimedb-{}-{}", id, self.name()));
        std::fs::create_dir_all(data_home.as_path()).unwrap();

        let wal_dir = data_home.join("wal").display().to_string();
        let procedure_dir = data_home.join("procedure").display().to_string();

        // Get the required addresses based on server mode
        let addrs: HashMap<String, String> = match self {
            ServerMode::Standalone {
                rpc_bind_addr,
                mysql_addr,
                postgres_addr,
                http_addr,
            } => [
                ("http_addr".to_string(), http_addr.clone()),
                ("grpc_addr".to_string(), rpc_bind_addr.clone()),
                ("mysql_addr".to_string(), mysql_addr.clone()),
                ("postgres_addr".to_string(), postgres_addr.clone()),
            ]
            .into(),
            ServerMode::Frontend { rpc_bind_addr, .. } => {
                [("grpc_addr".to_string(), rpc_bind_addr.clone())].into()
            }
            ServerMode::Datanode { metasrv_addr, .. } => {
                [("metasrv_addr".to_string(), metasrv_addr.clone())].into()
            }
            _ => HashMap::new(),
        };

        let ctx = ConfigContext {
            wal_dir,
            data_home: data_home.display().to_string(),
            procedure_dir,
            is_raft_engine: db_ctx.is_raft_engine(),
            kafka_wal_broker_endpoints: db_ctx.kafka_wal_broker_endpoints(),
            use_etcd: !db_ctx.store_config().store_addrs.is_empty(),
            store_addrs: db_ctx
                .store_config()
                .store_addrs
                .iter()
                .map(|p| format!("\"{p}\""))
                .collect::<Vec<_>>()
                .join(","),
            instance_id: id,
            addrs,
            enable_flat_format: db_ctx.store_config().enable_flat_format,
            enable_gc: db_ctx.store_config().enable_gc,
        };

        let rendered = tt.render(self.name(), &ctx).unwrap();
        let rendered = if matches!(self, Self::Datanode { .. }) {
            compat_stage
                .old_config()
                .map(|config| apply_old_datanode_overlay(&rendered, config))
                .transpose()
                .unwrap_or_else(|e| panic!("Failed to apply old datanode config: {e}"))
                .unwrap_or(rendered)
        } else {
            rendered
        };

        let conf_file = data_home
            .join(format!(
                "{}-{}-{}{}.toml",
                self.name(),
                id,
                db_ctx.time(),
                compat_stage.file_suffix()
            ))
            .display()
            .to_string();
        println!(
            "Generating id {}, {} config file in {conf_file}",
            id,
            self.name()
        );
        std::fs::write(&conf_file, rendered).unwrap();

        conf_file
    }

    pub fn get_args(
        &self,
        sqlness_home: &Path,
        env: &Env,
        db_ctx: &GreptimeDBContext,
        id: usize,
        arg_style: GrpcArgStyle,
        compat_stage: &CompatConfigStage,
    ) -> Vec<String> {
        let mut args = env
            .extra_args()
            .iter()
            .map(String::as_str)
            .chain([DEFAULT_LOG_LEVEL, self.name(), "start"])
            .map(ToString::to_string)
            .collect::<Vec<String>>();

        match self {
            ServerMode::Standalone {
                http_addr,
                rpc_bind_addr,
                mysql_addr,
                postgres_addr,
            } => {
                args.extend([
                    format!(
                        "--log-dir={}/greptimedb-{}-standalone/logs",
                        sqlness_home.display(),
                        id
                    ),
                    "-c".to_string(),
                    self.generate_config_file(sqlness_home, db_ctx, id, compat_stage),
                    format!("--http-addr={http_addr}"),
                    format!("{}={rpc_bind_addr}", arg_style.bind_addr_arg()),
                    format!("--mysql-addr={mysql_addr}"),
                    format!("--postgres-addr={postgres_addr}"),
                ]);
            }
            ServerMode::Frontend {
                http_addr,
                rpc_bind_addr,
                mysql_addr,
                postgres_addr,
                metasrv_addr,
            } => {
                args.extend([
                    format!("--metasrv-addrs={metasrv_addr}"),
                    format!("--http-addr={http_addr}"),
                    format!("{}={rpc_bind_addr}", arg_style.bind_addr_arg()),
                    // since sqlness run on local, bind addr is the same as server addr
                    // this is needed so that `cluster_info`'s server addr column can be correct
                    format!("{}={rpc_bind_addr}", arg_style.server_addr_arg()),
                    format!("--mysql-addr={mysql_addr}"),
                    format!("--postgres-addr={postgres_addr}"),
                    format!(
                        "--log-dir={}/greptimedb-{}-frontend/logs",
                        sqlness_home.display(),
                        id
                    ),
                    "-c".to_string(),
                    self.generate_config_file(sqlness_home, db_ctx, id, compat_stage),
                ]);
            }
            ServerMode::Metasrv {
                rpc_bind_addr,
                rpc_server_addr,
                http_addr,
            } => {
                args.extend([
                    arg_style.bind_addr_arg().to_string(),
                    rpc_bind_addr.clone(),
                    arg_style.server_addr_arg().to_string(),
                    rpc_server_addr.clone(),
                    "--enable-region-failover".to_string(),
                    "false".to_string(),
                    format!("--http-addr={http_addr}"),
                    format!(
                        "--log-dir={}/greptimedb-{}-metasrv/logs",
                        sqlness_home.display(),
                        id
                    ),
                    "-c".to_string(),
                    self.generate_config_file(sqlness_home, db_ctx, id, compat_stage),
                ]);

                if matches!(
                    db_ctx.store_config().setup_pg,
                    Some(ServiceProvider::Create)
                ) {
                    let client_ports = db_ctx
                        .store_config()
                        .store_addrs
                        .iter()
                        .map(|s| s.split(':').nth(1).unwrap().parse::<u16>().unwrap())
                        .collect::<Vec<_>>();
                    let client_port = client_ports.first().unwrap_or(&5432);
                    let pg_server_addr = format!(
                        "postgresql://greptimedb:admin@127.0.0.1:{}/postgres",
                        client_port
                    );
                    args.extend(vec!["--backend".to_string(), "postgres-store".to_string()]);
                    args.extend(vec!["--store-addrs".to_string(), pg_server_addr]);
                } else if let Some(ServiceProvider::External(connection_string)) =
                    db_ctx.store_config().setup_pg
                {
                    println!("Using external PostgreSQL '{connection_string}' as Kvbackend");
                    args.extend([
                        "--backend".to_string(),
                        "postgres-store".to_string(),
                        "--store-addrs".to_string(),
                        connection_string,
                    ]);
                } else if matches!(
                    db_ctx.store_config().setup_mysql,
                    Some(ServiceProvider::Create)
                ) {
                    let client_ports = db_ctx
                        .store_config()
                        .store_addrs
                        .iter()
                        .map(|s| s.split(':').nth(1).unwrap().parse::<u16>().unwrap())
                        .collect::<Vec<_>>();
                    let client_port = client_ports.first().unwrap_or(&3306);
                    let mysql_server_addr =
                        format!("mysql://greptimedb:admin@127.0.0.1:{}/mysql", client_port);
                    args.extend(vec!["--backend".to_string(), "mysql-store".to_string()]);
                    args.extend(vec!["--store-addrs".to_string(), mysql_server_addr]);
                } else if let Some(ServiceProvider::External(connection_string)) =
                    db_ctx.store_config().setup_mysql
                {
                    println!("Using external MySQL '{connection_string}' as Kvbackend");
                    args.extend([
                        "--backend".to_string(),
                        "mysql-store".to_string(),
                        "--store-addrs".to_string(),
                        connection_string,
                    ]);
                } else if db_ctx.store_config().store_addrs.is_empty() {
                    args.extend(vec!["--backend".to_string(), "memory-store".to_string()])
                }
            }
            ServerMode::Datanode {
                rpc_bind_addr,
                rpc_server_addr,
                http_addr,
                metasrv_addr,
                node_id,
            } => {
                let data_home = sqlness_home.join(format!(
                    "greptimedb_{}_datanode_{}_{node_id}",
                    id,
                    db_ctx.time()
                ));
                args.extend([
                    format!("{}={rpc_bind_addr}", arg_style.bind_addr_arg()),
                    format!("{}={rpc_server_addr}", arg_style.server_addr_arg()),
                    format!("--http-addr={http_addr}"),
                    format!("--data-home={}", data_home.display()),
                    format!("--log-dir={}/logs", data_home.display()),
                    format!("--node-id={node_id}"),
                    "-c".to_string(),
                    self.generate_config_file(sqlness_home, db_ctx, id, compat_stage),
                    format!("--metasrv-addrs={metasrv_addr}"),
                ]);
            }
            ServerMode::Flownode {
                rpc_bind_addr,
                rpc_server_addr,
                http_addr,
                metasrv_addr,
                node_id,
            } => {
                args.extend([
                    format!("{}={rpc_bind_addr}", arg_style.bind_addr_arg()),
                    format!("{}={rpc_server_addr}", arg_style.server_addr_arg()),
                    format!("--node-id={node_id}"),
                    format!(
                        "--log-dir={}/greptimedb-{}-flownode/logs",
                        sqlness_home.display(),
                        id
                    ),
                    format!("--metasrv-addrs={metasrv_addr}"),
                    format!("--http-addr={http_addr}"),
                ]);
            }
        }

        args
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use super::*;
    use crate::env::bare::{StoreConfig, WalConfig};

    fn test_env(sqlness_home: &Path) -> (Env, GreptimeDBContext) {
        let store_config = StoreConfig {
            store_addrs: vec!["127.0.0.1:2379".to_string()],
            setup_etcd: true,
            setup_pg: None,
            setup_mysql: None,
            enable_flat_format: false,
            enable_gc: false,
        };
        let env = Env::new(
            sqlness_home.to_path_buf(),
            ServerAddr::default(),
            WalConfig::RaftEngine,
            false,
            Some(PathBuf::from(".")),
            store_config.clone(),
            vec![],
        );
        let db_ctx = GreptimeDBContext::new(WalConfig::RaftEngine, store_config);

        (env, db_ctx)
    }

    fn has_arg(args: &[String], name: &str) -> bool {
        let prefix = format!("{name}=");
        args.iter()
            .any(|arg| arg == name || arg.starts_with(&prefix))
    }

    fn assert_uses_style(args: &[String], style: GrpcArgStyle, expect_server_addr: bool) {
        let bind = style.bind_addr_arg();
        let server = style.server_addr_arg();
        let other_bind = match style {
            GrpcArgStyle::Grpc => "--rpc-bind-addr",
            GrpcArgStyle::Rpc => "--grpc-bind-addr",
        };
        let other_server = match style {
            GrpcArgStyle::Grpc => "--rpc-server-addr",
            GrpcArgStyle::Rpc => "--grpc-server-addr",
        };

        assert!(has_arg(args, bind), "missing {bind} in args: {args:?}");
        assert!(
            !args.iter().any(|arg| arg.contains(other_bind)),
            "unexpected {other_bind} in args: {args:?}"
        );

        if expect_server_addr {
            assert!(has_arg(args, server), "missing {server} in args: {args:?}");
            assert!(
                !args.iter().any(|arg| arg.contains(other_server)),
                "unexpected {other_server} in args: {args:?}"
            );
        }
    }

    fn test_all_modes(env: &Env, db_ctx: &GreptimeDBContext, temp_dir: &Path, style: GrpcArgStyle) {
        let standalone = ServerMode::Standalone {
            http_addr: "127.0.0.1:4000".to_string(),
            rpc_bind_addr: "127.0.0.1:4001".to_string(),
            mysql_addr: "127.0.0.1:4002".to_string(),
            postgres_addr: "127.0.0.1:4003".to_string(),
        };
        assert_uses_style(
            &standalone.get_args(
                temp_dir,
                env,
                db_ctx,
                0,
                style,
                &CompatConfigStage::Baseline,
            ),
            style,
            false,
        );

        let frontend = ServerMode::Frontend {
            http_addr: "127.0.0.1:4100".to_string(),
            rpc_bind_addr: "127.0.0.1:4101".to_string(),
            mysql_addr: "127.0.0.1:4102".to_string(),
            postgres_addr: "127.0.0.1:4103".to_string(),
            metasrv_addr: "127.0.0.1:4001".to_string(),
        };
        assert_uses_style(
            &frontend.get_args(
                temp_dir,
                env,
                db_ctx,
                0,
                style,
                &CompatConfigStage::Baseline,
            ),
            style,
            true,
        );

        let metasrv = ServerMode::Metasrv {
            rpc_bind_addr: "127.0.0.1:4201".to_string(),
            rpc_server_addr: "127.0.0.1:4201".to_string(),
            http_addr: "127.0.0.1:4200".to_string(),
        };
        assert_uses_style(
            &metasrv.get_args(
                temp_dir,
                env,
                db_ctx,
                0,
                style,
                &CompatConfigStage::Baseline,
            ),
            style,
            true,
        );

        let datanode = ServerMode::Datanode {
            rpc_bind_addr: "127.0.0.1:4301".to_string(),
            rpc_server_addr: "127.0.0.1:4301".to_string(),
            http_addr: "127.0.0.1:4300".to_string(),
            metasrv_addr: "127.0.0.1:4001".to_string(),
            node_id: 0,
        };
        assert_uses_style(
            &datanode.get_args(
                temp_dir,
                env,
                db_ctx,
                0,
                style,
                &CompatConfigStage::Baseline,
            ),
            style,
            true,
        );

        let flownode = ServerMode::Flownode {
            rpc_bind_addr: "127.0.0.1:4401".to_string(),
            rpc_server_addr: "127.0.0.1:4401".to_string(),
            http_addr: "127.0.0.1:4400".to_string(),
            metasrv_addr: "127.0.0.1:4001".to_string(),
            node_id: 0,
        };
        assert_uses_style(
            &flownode.get_args(
                temp_dir,
                env,
                db_ctx,
                0,
                style,
                &CompatConfigStage::Baseline,
            ),
            style,
            true,
        );
    }

    #[test]
    fn test_get_args_with_grpc_style() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (env, db_ctx) = test_env(temp_dir.path());
        test_all_modes(&env, &db_ctx, temp_dir.path(), GrpcArgStyle::Grpc);
    }

    #[test]
    fn test_get_args_with_rpc_style() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (env, db_ctx) = test_env(temp_dir.path());
        test_all_modes(&env, &db_ctx, temp_dir.path(), GrpcArgStyle::Rpc);
    }

    #[test]
    fn test_arg_style_for_unknown_version_defaults_to_grpc() {
        assert_eq!(GrpcArgStyle::for_version(None), GrpcArgStyle::Grpc);
    }

    #[test]
    fn test_arg_style_for_legacy_versions_uses_rpc() {
        let v1_0_0 = Version::parse("v1.0.0").unwrap();
        let v1_0_9 = Version::parse("v1.0.9").unwrap();

        assert_eq!(GrpcArgStyle::for_version(Some(&v1_0_0)), GrpcArgStyle::Rpc);
        assert_eq!(GrpcArgStyle::for_version(Some(&v1_0_9)), GrpcArgStyle::Rpc);
    }

    #[test]
    fn test_arg_style_for_current_versions_uses_grpc() {
        let v1_1_0 = Version::parse("v1.1.0").unwrap();
        let v1_2_0 = Version::parse("v1.2.0").unwrap();

        assert_eq!(GrpcArgStyle::for_version(Some(&v1_1_0)), GrpcArgStyle::Grpc);
        assert_eq!(GrpcArgStyle::for_version(Some(&v1_2_0)), GrpcArgStyle::Grpc);
    }

    #[test]
    fn test_generate_distributed_gc_config_when_enabled() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store_config = StoreConfig {
            store_addrs: vec![],
            setup_etcd: false,
            setup_pg: None,
            setup_mysql: None,
            enable_flat_format: false,
            enable_gc: true,
        };
        let db_ctx = GreptimeDBContext::new(WalConfig::RaftEngine, store_config);
        let metasrv = ServerMode::Metasrv {
            rpc_bind_addr: "127.0.0.1:4201".to_string(),
            rpc_server_addr: "127.0.0.1:4201".to_string(),
            http_addr: "127.0.0.1:4200".to_string(),
        };
        let datanode = ServerMode::Datanode {
            rpc_bind_addr: "127.0.0.1:4301".to_string(),
            rpc_server_addr: "127.0.0.1:4301".to_string(),
            http_addr: "127.0.0.1:4300".to_string(),
            metasrv_addr: "127.0.0.1:4201".to_string(),
            node_id: 0,
        };

        let metasrv_config = std::fs::read_to_string(metasrv.generate_config_file(
            temp_dir.path(),
            &db_ctx,
            0,
            &CompatConfigStage::Baseline,
        ))
        .unwrap();
        let datanode_config = std::fs::read_to_string(datanode.generate_config_file(
            temp_dir.path(),
            &db_ctx,
            0,
            &CompatConfigStage::Baseline,
        ))
        .unwrap();

        assert!(metasrv_config.contains("[gc]\nenable = true"));
        assert!(metasrv_config.contains("[gc.experimental_soft_drop]\nenable = true"));
        assert!(datanode_config.contains("[region_engine.mito.gc]\nenable = true"));
    }

    fn metric_overlay() -> OldConfig {
        OldConfig {
            datanode: BTreeMap::from([(
                "region_engine.metric.sparse_primary_key_encoding".to_string(),
                false,
            )]),
        }
    }

    #[test]
    fn test_old_overlay_targets_named_region_engine_entry() {
        let rendered = r#"
[[region_engine]]
[region_engine.mito]
enable_write_cache = true
"#;

        let overlaid = apply_old_datanode_overlay(rendered, &metric_overlay()).unwrap();
        let value: Value = toml::from_str(&overlaid).unwrap();
        let entries = value["region_engine"].as_array().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(
            entries[1]["metric"]["sparse_primary_key_encoding"].as_bool(),
            Some(false)
        );
    }

    #[test]
    fn test_old_overlay_rejects_duplicate_engine_non_table_and_type_conflicts() {
        let overlay = metric_overlay();
        for rendered in [
            "[[region_engine]]\n[region_engine.metric]\n\n[[region_engine]]\n[region_engine.metric]\n",
            "[[region_engine]]\nmetric = false\n",
            "[[region_engine]]\n[region_engine.metric]\nsparse_primary_key_encoding = \"false\"\n",
            "[[region_engine]]\n[region_engine.metric]\ncache = false\n",
        ] {
            let actual_overlay = if rendered.contains("cache = false") {
                OldConfig {
                    datanode: BTreeMap::from([(
                        "region_engine.metric.cache.enabled".to_string(),
                        false,
                    )]),
                }
            } else {
                overlay.clone()
            };
            assert!(apply_old_datanode_overlay(rendered, &actual_overlay).is_err());
        }
    }

    #[test]
    fn test_actual_datanode_template_validates_old_profile_without_state() {
        assert!(validate_old_config_against_datanode_template(&metric_overlay()).is_ok());
    }

    #[test]
    fn test_old_and_current_datanode_configs_are_independently_rendered() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (_, db_ctx) = test_env(temp_dir.path());
        let datanode = ServerMode::Datanode {
            rpc_bind_addr: "127.0.0.1:4301".to_string(),
            rpc_server_addr: "127.0.0.1:4301".to_string(),
            http_addr: "127.0.0.1:4300".to_string(),
            metasrv_addr: "127.0.0.1:4001".to_string(),
            node_id: 0,
        };
        let old_path = datanode.generate_config_file(
            temp_dir.path(),
            &db_ctx,
            0,
            &CompatConfigStage::Old(metric_overlay()),
        );
        let baseline_path = datanode.generate_config_file(
            temp_dir.path(),
            &db_ctx,
            0,
            &CompatConfigStage::Baseline,
        );
        let current_path =
            datanode.generate_config_file(temp_dir.path(), &db_ctx, 0, &CompatConfigStage::Current);

        assert_ne!(old_path, current_path);
        assert!(!baseline_path.ends_with("-.toml"));
        assert!(old_path.ends_with("-old.toml"));
        assert!(current_path.ends_with("-current.toml"));
        assert!(!old_path.ends_with("--old.toml"));
        assert!(!current_path.ends_with("--current.toml"));
        assert!(
            std::fs::read_to_string(old_path)
                .unwrap()
                .contains("sparse_primary_key_encoding")
        );
        assert!(
            !std::fs::read_to_string(current_path)
                .unwrap()
                .contains("sparse_primary_key_encoding")
        );
    }

    #[test]
    fn test_all_old_datanode_renders_receive_the_overlay() {
        let rendered = "[[region_engine]]\n[region_engine.mito]\n";
        for _ in 0..3 {
            let overlaid = apply_old_datanode_overlay(rendered, &metric_overlay()).unwrap();
            assert!(overlaid.contains("sparse_primary_key_encoding = false"));
        }
    }
}
