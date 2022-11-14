use std::fmt::Display;
use std::time::Duration;

use async_trait::async_trait;
use client::api::v1::codec::SelectResult;
use client::api::v1::column::SemanticType;
use client::api::v1::ColumnDataType;
use client::{Client, Database as DB, Error as ClientError, ObjectResult, Select};
use comfy_table::{Cell, Table};
use sqlness::{Database, Environment};
use tokio::process::{Child, Command};
use tokio::time;

use crate::util;

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
        database.server_process.kill().await.unwrap()
    }
}

impl Env {
    pub async fn start_local() -> GreptimeDB {
        let server_process = Command::new("cargo")
            .current_dir("../")
            .args(["run", "--", "datanode", "start"])
            .spawn()
            .unwrap_or_else(|_| panic!("Failed to start datanode"));

        time::sleep(Duration::from_secs(3)).await;

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
            Ok(result) => match result {
                ObjectResult::Select(select_result) => {
                    write!(
                        f,
                        "{}",
                        SelectResultDisplayer {
                            result: select_result
                        }
                        .display()
                    )
                }
                ObjectResult::Mutate(mutate_result) => {
                    write!(f, "{:?}", mutate_result)
                }
            },
            Err(e) => write!(f, "Failed to execute, error: {:?}", e),
        }
    }
}

struct SelectResultDisplayer<'a> {
    result: &'a SelectResult,
}

impl SelectResultDisplayer<'_> {
    fn display(&self) -> impl Display {
        let mut table = Table::new();
        table.load_preset("||--+-++|    ++++++");

        if self.result.row_count == 0 {
            return table;
        }

        let mut headers = vec![];
        for column in &self.result.columns {
            headers.push(Cell::new(format!(
                "{}, #{:?}, #{:?}",
                column.column_name,
                SemanticType::from_i32(column.semantic_type).unwrap(),
                ColumnDataType::from_i32(column.datatype).unwrap()
            )));
        }
        table.set_header(headers);

        let col_count = self.result.columns.len();
        let row_count = self.result.row_count as usize;
        let columns = self
            .result
            .columns
            .iter()
            .map(|col| {
                util::values_to_string(
                    ColumnDataType::from_i32(col.datatype).unwrap(),
                    col.values.clone().unwrap(),
                )
            })
            .collect::<Vec<_>>();

        for row_index in 0..row_count {
            let mut row = Vec::with_capacity(col_count);
            for col in columns.iter() {
                row.push(col[row_index].clone());
            }
            table.add_row(row);
        }

        table
    }
}
