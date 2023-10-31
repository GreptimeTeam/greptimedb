use std::fmt::Display;
use std::path::{Path, PathBuf};
use std::process::exit;

use async_trait::async_trait;
use clap::Parser;
use client::{Client, Database as DB, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use sqlness::{ConfigBuilder, Database, EnvController, QueryContext, Runner};
use tokio::sync::Mutex as TokioMutex;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
/// A cli to run sqlness tests.
struct Args {
    /// Directory of test cases
    #[clap(short, long)]
    case_dir: Option<PathBuf>,

    /// Fail this run as soon as one case fails if true
    #[arg(short, long, default_value = "false")]
    fail_fast: bool,

    /// Name of test cases to run. Accept as a regexp.
    #[clap(short, long, default_value = ".*")]
    test_filter: String,

    /// Address of the server
    #[clap(short, long, default_value = "127.0.0.1:4001")]
    server_addr: String,
}

pub struct GreptimeDB {
    client: TokioMutex<DB>,
}

#[async_trait]
impl Database for GreptimeDB {
    async fn query(&self, ctx: QueryContext, query: String) -> Box<dyn Display> {
        sqlness_util::do_query(ctx, self.client.lock().await, query).await
    }
}

pub struct CliController {
    server_addr: String,
}

impl CliController {
    pub fn new(server_addr: String) -> Self {
        Self { server_addr }
    }

    async fn connect(&self) -> GreptimeDB {
        let client = Client::with_urls(vec![self.server_addr.clone()]);
        let db = DB::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, client);

        GreptimeDB {
            client: TokioMutex::new(db),
        }
    }
}

#[async_trait]
impl EnvController for CliController {
    type DB = GreptimeDB;

    async fn start(&self, mode: &str, _config: Option<&Path>) -> Self::DB {
        match mode {
            "standalone" => self.connect().await,
            "distributed" => self.connect().await,
            _ => panic!("Unexpected mode: {mode}"),
        }
    }

    async fn stop(&self, _mode: &str, _database: Self::DB) {}
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let config = ConfigBuilder::default()
        .case_dir(sqlness_util::get_case_dir(args.case_dir))
        .fail_fast(args.fail_fast)
        .test_filter(args.test_filter)
        .follow_links(true)
        .build()
        .unwrap();
    let runner = Runner::new(config, CliController::new(args.server_addr));
    runner.run().await.unwrap();
}
