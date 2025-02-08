// Copyright 2025 Greptime Team
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

use std::path::Path;

use crate::env::{Env, GreptimeDBContext};
use crate::{util, ServerAddr};

const DEFAULT_LOG_LEVEL: &str = "--log-level=debug,hyper=warn,tower=warn,datafusion=warn,reqwest=warn,sqlparser=warn,h2=info,opendal=info";

#[derive(Clone)]
pub enum ServerMode {
    Standalone {
        http_addr: String,
        rpc_addr: String,
        mysql_addr: String,
        postgres_addr: String,
    },
    Frontend {
        http_addr: String,
        rpc_addr: String,
        mysql_addr: String,
        postgres_addr: String,
        metasrv_addr: String,
    },
    Metasrv {
        bind_addr: String,
        server_addr: String,
        http_addr: String,
    },
    Datanode {
        rpc_addr: String,
        rpc_hostname: String,
        http_addr: String,
        metasrv_addr: String,
        node_id: u32,
    },
    Flownode {
        rpc_addr: String,
        rpc_hostname: String,
        http_addr: String,
        metasrv_addr: String,
        node_id: u32,
    },
}

impl ServerMode {
    pub fn random_standalone() -> Self {
        let http_port = util::get_random_port();
        let rpc_port = util::get_random_port();
        let mysql_port = util::get_random_port();
        let postgres_port = util::get_random_port();

        ServerMode::Standalone {
            http_addr: format!("127.0.0.1:{http_port}"),
            rpc_addr: format!("127.0.0.1:{rpc_port}"),
            mysql_addr: format!("127.0.0.1:{mysql_port}"),
            postgres_addr: format!("127.0.0.1:{postgres_port}"),
        }
    }

    pub fn random_frontend(metasrv_port: u16) -> Self {
        let http_port = util::get_random_port();
        let rpc_port = util::get_random_port();
        let mysql_port = util::get_random_port();
        let postgres_port = util::get_random_port();

        ServerMode::Frontend {
            http_addr: format!("127.0.0.1:{http_port}"),
            rpc_addr: format!("127.0.0.1:{rpc_port}"),
            mysql_addr: format!("127.0.0.1:{mysql_port}"),
            postgres_addr: format!("127.0.0.1:{postgres_port}"),
            metasrv_addr: format!("127.0.0.1:{metasrv_port}"),
        }
    }

    pub fn random_metasrv() -> Self {
        let bind_port = util::get_random_port();
        let http_port = util::get_random_port();

        ServerMode::Metasrv {
            bind_addr: format!("127.0.0.1:{bind_port}"),
            server_addr: format!("127.0.0.1:{bind_port}"),
            http_addr: format!("127.0.0.1:{http_port}"),
        }
    }

    pub fn random_datanode(metasrv_port: u16, node_id: u32) -> Self {
        let rpc_port = util::get_random_port();
        let http_port = util::get_random_port();

        ServerMode::Datanode {
            rpc_addr: format!("127.0.0.1:{rpc_port}"),
            rpc_hostname: format!("127.0.0.1:{rpc_port}"),
            http_addr: format!("127.0.0.1:{http_port}"),
            metasrv_addr: format!("127.0.0.1:{metasrv_port}"),
            node_id,
        }
    }

    pub fn random_flownode(metasrv_port: u16, node_id: u32) -> Self {
        let rpc_port = util::get_random_port();
        let http_port = util::get_random_port();

        ServerMode::Flownode {
            rpc_addr: format!("127.0.0.1:{rpc_port}"),
            rpc_hostname: format!("127.0.0.1:{rpc_port}"),
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
                rpc_addr,
                mysql_addr,
                postgres_addr,
                http_addr,
                ..
            } => {
                vec![
                    rpc_addr.clone(),
                    mysql_addr.clone(),
                    postgres_addr.clone(),
                    http_addr.clone(),
                ]
            }
            ServerMode::Frontend {
                rpc_addr,
                mysql_addr,
                postgres_addr,
                ..
            } => {
                vec![rpc_addr.clone(), mysql_addr.clone(), postgres_addr.clone()]
            }
            ServerMode::Metasrv { bind_addr, .. } => {
                vec![bind_addr.clone()]
            }
            ServerMode::Datanode { rpc_addr, .. } => {
                vec![rpc_addr.clone()]
            }
            ServerMode::Flownode { rpc_addr, .. } => {
                vec![rpc_addr.clone()]
            }
        }
    }

    /// Returns the server addresses to connect. Only standalone and frontend mode have this.
    pub fn server_addr(&self) -> Option<ServerAddr> {
        match self {
            ServerMode::Standalone {
                rpc_addr,
                mysql_addr,
                postgres_addr,
                ..
            } => Some(ServerAddr {
                server_addr: Some(rpc_addr.clone()),
                pg_server_addr: Some(postgres_addr.clone()),
                mysql_server_addr: Some(mysql_addr.clone()),
            }),
            ServerMode::Frontend {
                rpc_addr,
                mysql_addr,
                postgres_addr,
                ..
            } => Some(ServerAddr {
                server_addr: Some(rpc_addr.clone()),
                pg_server_addr: Some(postgres_addr.clone()),
                mysql_server_addr: Some(mysql_addr.clone()),
            }),
            _ => None,
        }
    }

    pub fn get_args(
        &self,
        sqlness_home: &Path,
        env: &Env,
        db_ctx: &GreptimeDBContext,
    ) -> Vec<String> {
        let mut args = vec![
            DEFAULT_LOG_LEVEL.to_string(),
            self.name().to_string(),
            "start".to_string(),
        ];

        match self {
            ServerMode::Standalone {
                http_addr,
                rpc_addr,
                mysql_addr,
                postgres_addr,
            } => {
                args.extend([
                    format!(
                        "--log-dir={}/greptimedb-flownode/logs",
                        sqlness_home.display()
                    ),
                    "-c".to_string(),
                    env.generate_config_file(
                        self.name(),
                        db_ctx,
                        String::new(),
                        rpc_addr.to_string(),
                        mysql_addr.to_string(),
                        postgres_addr.to_string(),
                    ),
                    format!("--http-addr={http_addr}"),
                    format!("--rpc-addr={rpc_addr}"),
                    format!("--mysql-addr={mysql_addr}"),
                    format!("--postgres-addr={postgres_addr}"),
                ]);
            }
            ServerMode::Frontend {
                http_addr,
                rpc_addr,
                mysql_addr,
                postgres_addr,
                metasrv_addr,
            } => {
                args.extend([
                    format!("--metasrv-addrs={metasrv_addr}"),
                    format!("--http-addr={http_addr}"),
                    format!("--rpc-addr={rpc_addr}"),
                    format!("--mysql-addr={mysql_addr}"),
                    format!("--postgres-addr={postgres_addr}"),
                    format!(
                        "--log-dir={}/greptimedb-frontend/logs",
                        sqlness_home.display()
                    ),
                    "-c".to_string(),
                    env.generate_config_file(
                        self.name(),
                        db_ctx,
                        String::new(),
                        rpc_addr.to_string(),
                        mysql_addr.to_string(),
                        postgres_addr.to_string(),
                    ),
                ]);
            }
            ServerMode::Metasrv {
                bind_addr,
                server_addr,
                http_addr,
            } => {
                args.extend([
                    "--bind-addr".to_string(),
                    bind_addr.clone(),
                    "--server-addr".to_string(),
                    server_addr.clone(),
                    "--enable-region-failover".to_string(),
                    "false".to_string(),
                    format!("--http-addr={http_addr}"),
                    format!(
                        "--log-dir={}/greptimedb-metasrv/logs",
                        sqlness_home.display()
                    ),
                    "-c".to_string(),
                    env.generate_config_file(
                        self.name(),
                        db_ctx,
                        String::new(),
                        String::new(),
                        String::new(),
                        String::new(),
                    ),
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
                } else if db_ctx.store_config().store_addrs.is_empty() {
                    args.extend(vec!["--backend".to_string(), "memory-store".to_string()])
                }
            }
            ServerMode::Datanode {
                rpc_addr,
                rpc_hostname,
                http_addr,
                metasrv_addr,
                node_id,
            } => {
                let data_home =
                    sqlness_home.join(format!("greptimedb_datanode_{}_{node_id}", db_ctx.time()));
                args.extend([
                    format!("--rpc-addr={rpc_addr}"),
                    format!("--rpc-hostname={rpc_hostname}"),
                    format!("--http-addr={http_addr}"),
                    format!("--data-home={}", data_home.display()),
                    format!("--log-dir={}/logs", data_home.display()),
                    format!("--node-id={node_id}"),
                    "-c".to_string(),
                    env.generate_config_file(
                        self.name(),
                        db_ctx,
                        metasrv_addr.to_string(),
                        rpc_addr.to_string(),
                        String::new(),
                        String::new(),
                    ),
                    format!("--metasrv-addrs={metasrv_addr}"),
                ]);
            }
            ServerMode::Flownode {
                rpc_addr,
                rpc_hostname,
                http_addr,
                metasrv_addr,
                node_id,
            } => {
                args.extend([
                    format!("--rpc-addr={rpc_addr}"),
                    format!("--rpc-hostname={rpc_hostname}"),
                    format!("--node-id={node_id}"),
                    format!(
                        "--log-dir={}/greptimedb-flownode/logs",
                        sqlness_home.display()
                    ),
                    format!("--metasrv-addrs={metasrv_addr}"),
                    format!("--http-addr={http_addr}"),
                ]);
            }
        }

        args
    }
}
