use std::fmt::Display;

use async_trait::async_trait;
use client::{Client, Database as DB, Error as ClientError, ObjectResult, Select};
use sqlness::{Database, Environment};
use tokio::process::{Child, Command};

pub struct Env {}

#[async_trait]
impl Environment for Env {
    type DB = GreptimeDB;

    async fn start(&self, mode: &str, _config: Option<String>) -> Self::DB {
        match mode {
            "local" => Self::start_local().await,
            "remote" => Self::start_remote().await,
            _ => panic!("Unexpected mode: {}", mode),
        }
    }

    /// Stop one [`Database`].
    async fn stop(&self, _mode: &str, mut database: Self::DB) {
        if let Err(e) = database.server_process.kill().await {
            eprintln!("Cannot kill the server process, error: {:?}", e);
        }
    }
}

impl Env {
    pub async fn start_local() -> GreptimeDB {
        let server_process = Command::new("cargo")
            .current_dir("../")
            .args(["run", "--", "datanode", "start"])
            .spawn()
            .unwrap_or_else(|_| panic!("Failed to start datanode"));

        let client = Client::with_urls(vec!["127.0.0.1:3001"]);
        let db = DB::new("greptime", client.clone());

        GreptimeDB {
            server_process,
            client,
            db,
        }
    }

    pub async fn start_remote() -> GreptimeDB {
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
        let sql = Select::Sql(query);
        let result = self.db.select(sql).await;
        Box::new(ResultDisplayer { result }) as _
    }
}

struct ResultDisplayer {
    result: Result<ObjectResult, ClientError>,
}

impl Display for ResultDisplayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.result {
            Ok(result) => write!(f, "{:?}", result),
            Err(e) => write!(f, "Failed to execute, error: {:?}", e),
        }
    }
}
