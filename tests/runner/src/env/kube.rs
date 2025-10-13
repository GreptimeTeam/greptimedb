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
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use sqlness::{Database, EnvController, QueryContext};
use tokio::process::Command;
use tokio::sync::Mutex;

use crate::client::MultiProtocolClient;
use crate::formatter::{ErrorFormatter, MysqlFormatter, OutputFormatter, PostgresqlFormatter};
use crate::protocol_interceptor::{MYSQL, PROTOCOL_KEY};

#[async_trait]
pub trait DatabaseManager: Send + Sync {
    // Restarts the database.
    async fn restart(&self, database: &GreptimeDB);
}

#[async_trait]
impl DatabaseManager for () {
    async fn restart(&self, _: &GreptimeDB) {
        // Do nothing
    }
}

#[async_trait]
pub trait ResourcesManager: Send + Sync {
    // Delete namespace.
    async fn delete_namespace(&self);

    // Get namespace.
    fn namespace(&self) -> &str;
}

#[derive(Clone)]
pub struct Env {
    /// Whether to delete the namespace on stop.
    pub delete_namespace_on_stop: bool,
    /// Address of the grpc server.
    pub server_addr: String,
    /// Address of the postgres server.
    pub pg_server_addr: String,
    /// Address of the mysql server.
    pub mysql_server_addr: String,
    /// The database manager.
    pub database_manager: Arc<dyn DatabaseManager>,
    /// The resources manager.
    pub resources_manager: Arc<dyn ResourcesManager>,
}

#[async_trait]
impl EnvController for Env {
    type DB = GreptimeDB;

    async fn start(&self, mode: &str, id: usize, _config: Option<&Path>) -> Self::DB {
        if id > 0 {
            panic!("Parallel test mode is not supported in kube env");
        }

        match mode {
            "standalone" | "distributed" => GreptimeDB {
                client: Mutex::new(
                    MultiProtocolClient::connect(
                        &self.server_addr,
                        &self.pg_server_addr,
                        &self.mysql_server_addr,
                    )
                    .await,
                ),
                database_manager: self.database_manager.clone(),
                resources_manager: self.resources_manager.clone(),
                delete_namespace_on_stop: self.delete_namespace_on_stop,
            },

            _ => panic!("Unexpected mode: {mode}"),
        }
    }

    async fn stop(&self, _mode: &str, database: Self::DB) {
        database.stop().await;
    }
}

pub struct GreptimeDB {
    pub client: Mutex<MultiProtocolClient>,
    pub delete_namespace_on_stop: bool,
    pub database_manager: Arc<dyn DatabaseManager>,
    pub resources_manager: Arc<dyn ResourcesManager>,
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
}

#[async_trait]
impl Database for GreptimeDB {
    async fn query(&self, ctx: QueryContext, query: String) -> Box<dyn Display> {
        if ctx.context.contains_key("restart") {
            self.database_manager.restart(self).await
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
    async fn stop(&self) {
        if self.delete_namespace_on_stop {
            self.resources_manager.delete_namespace().await;
            println!("Deleted namespace({})", self.resources_manager.namespace());
        } else {
            println!(
                "Namespace({}) is not deleted",
                self.resources_manager.namespace()
            );
        }
    }
}

pub struct NaiveResourcesManager {
    namespace: String,
}

impl NaiveResourcesManager {
    pub fn new(namespace: String) -> Self {
        Self { namespace }
    }
}

#[async_trait]
impl ResourcesManager for NaiveResourcesManager {
    async fn delete_namespace(&self) {
        let output = Command::new("kubectl")
            .arg("delete")
            .arg("namespace")
            .arg(&self.namespace)
            .output()
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to execute kubectl delete namespace({}): {}",
                    self.namespace, e
                )
            });

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            panic!("Failed to delete namespace({}): {}", self.namespace, stderr);
        }
    }

    fn namespace(&self) -> &str {
        &self.namespace
    }
}
