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

use std::collections::HashSet;
use std::path::Path;
use std::sync::{Mutex, OnceLock};

use serde::Serialize;
use tinytemplate::TinyTemplate;

use crate::env::{Env, GreptimeDBContext};
use crate::{util, ServerAddr};

const DEFAULT_LOG_LEVEL: &str = "--log-level=debug,hyper=warn,tower=warn,datafusion=warn,reqwest=warn,sqlparser=warn,h2=info,opendal=info";

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
    // for following addrs, leave it empty if not needed
    // required for datanode
    metasrv_addr: String,
    // for frontend and standalone
    grpc_addr: String,
    // for standalone
    mysql_addr: String,
    // for standalone
    postgres_addr: String,
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
    ) -> String {
        let mut tt = TinyTemplate::new();

        let mut path = util::sqlness_conf_path();
        path.push(format!("{}-test.toml.template", self.name()));
        let template = std::fs::read_to_string(path).unwrap();
        tt.add_template(self.name(), &template).unwrap();

        let data_home = sqlness_home.join(format!("greptimedb-{}-{}", id, self.name()));
        std::fs::create_dir_all(data_home.as_path()).unwrap();

        let wal_dir = data_home.join("wal").display().to_string();
        let procedure_dir = data_home.join("procedure").display().to_string();

        // Get the required addresses based on server mode
        let (metasrv_addr, grpc_addr, mysql_addr, postgres_addr) = match self {
            ServerMode::Standalone {
                rpc_bind_addr,
                mysql_addr,
                postgres_addr,
                ..
            } => (
                String::new(),
                rpc_bind_addr.clone(),
                mysql_addr.clone(),
                postgres_addr.clone(),
            ),
            ServerMode::Frontend {
                rpc_bind_addr,
                mysql_addr,
                postgres_addr,
                ..
            } => (
                String::new(),
                rpc_bind_addr.clone(),
                mysql_addr.clone(),
                postgres_addr.clone(),
            ),
            ServerMode::Datanode {
                rpc_bind_addr,
                metasrv_addr,
                ..
            } => (
                metasrv_addr.clone(),
                rpc_bind_addr.clone(),
                String::new(),
                String::new(),
            ),
            _ => (String::new(), String::new(), String::new(), String::new()),
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
            metasrv_addr,
            grpc_addr,
            mysql_addr,
            postgres_addr,
        };

        let rendered = tt.render(self.name(), &ctx).unwrap();

        let conf_file = data_home
            .join(format!("{}-{}-{}.toml", self.name(), id, db_ctx.time()))
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
        _env: &Env,
        db_ctx: &GreptimeDBContext,
        id: usize,
    ) -> Vec<String> {
        let mut args = vec![
            DEFAULT_LOG_LEVEL.to_string(),
            self.name().to_string(),
            "start".to_string(),
        ];

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
                    self.generate_config_file(sqlness_home, db_ctx, id),
                    format!("--http-addr={http_addr}"),
                    format!("--rpc-addr={rpc_bind_addr}"),
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
                    format!("--rpc-addr={rpc_bind_addr}"),
                    // since sqlness run on local, bind addr is the same as server addr
                    // this is needed so that `cluster_info`'s server addr column can be correct
                    format!("--rpc-server-addr={rpc_bind_addr}"),
                    format!("--mysql-addr={mysql_addr}"),
                    format!("--postgres-addr={postgres_addr}"),
                    format!(
                        "--log-dir={}/greptimedb-{}-frontend/logs",
                        sqlness_home.display(),
                        id
                    ),
                    "-c".to_string(),
                    self.generate_config_file(sqlness_home, db_ctx, id),
                ]);
            }
            ServerMode::Metasrv {
                rpc_bind_addr,
                rpc_server_addr,
                http_addr,
            } => {
                args.extend([
                    "--bind-addr".to_string(),
                    rpc_bind_addr.clone(),
                    "--server-addr".to_string(),
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
                    self.generate_config_file(sqlness_home, db_ctx, id),
                ]);

                if db_ctx.store_config().setup_pg {
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
                } else if db_ctx.store_config().setup_mysql {
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
                    format!("--rpc-addr={rpc_bind_addr}"),
                    format!("--rpc-server-addr={rpc_server_addr}"),
                    format!("--http-addr={http_addr}"),
                    format!("--data-home={}", data_home.display()),
                    format!("--log-dir={}/logs", data_home.display()),
                    format!("--node-id={node_id}"),
                    "-c".to_string(),
                    self.generate_config_file(sqlness_home, db_ctx, id),
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
                    format!("--rpc-addr={rpc_bind_addr}"),
                    format!("--rpc-server-addr={rpc_server_addr}"),
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
