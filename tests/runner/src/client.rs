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

use std::net::SocketAddr;
use std::time::Duration;

use client::error::ServerSnafu;
use client::{
    Client, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, Database, OutputData, RecordBatches,
};
use common_error::ext::ErrorExt;
use common_query::Output;
use mysql::prelude::Queryable;
use mysql::{Conn as MySqlClient, Row as MySqlRow};
use tokio_postgres::{Client as PgClient, SimpleQueryMessage as PgRow};

use crate::util::retry_with_backoff;

/// A client that can connect to GreptimeDB using multiple protocols.
pub struct MultiProtocolClient {
    grpc_client: Database,
    pg_client: PgClient,
    mysql_client: MySqlClient,
}

/// The result of a MySQL query.
pub enum MysqlSqlResult {
    AffectedRows(u64),
    Rows(Vec<MySqlRow>),
}

impl MultiProtocolClient {
    /// Connect to the GreptimeDB server using multiple protocols.
    ///
    /// # Arguments
    ///
    /// * `grpc_server_addr` - The address of the GreptimeDB server.
    /// * `pg_server_addr` - The address of the Postgres server.
    /// * `mysql_server_addr` - The address of the MySQL server.
    ///
    /// # Returns
    ///
    /// A `MultiProtocolClient` instance.
    ///
    /// # Panics
    ///
    /// Panics if the server addresses are invalid or the connection fails.
    pub async fn connect(
        grpc_server_addr: &str,
        pg_server_addr: &str,
        mysql_server_addr: &str,
    ) -> MultiProtocolClient {
        let grpc_client = Database::new(
            DEFAULT_CATALOG_NAME,
            DEFAULT_SCHEMA_NAME,
            Client::with_urls(vec![grpc_server_addr]),
        );
        let pg_client = create_postgres_client(pg_server_addr).await;
        let mysql_client = create_mysql_client(mysql_server_addr).await;
        MultiProtocolClient {
            grpc_client,
            pg_client,
            mysql_client,
        }
    }

    /// Execute a query on the Postgres server.
    pub async fn postgres_query(&mut self, query: &str) -> Result<Vec<PgRow>, String> {
        match self.pg_client.simple_query(query).await {
            Ok(rows) => Ok(rows),
            Err(e) => Err(format!("Failed to execute query, encountered: {:?}", e)),
        }
    }

    /// Execute a query on the MySQL server.
    pub async fn mysql_query(&mut self, query: &str) -> Result<MysqlSqlResult, String> {
        let result = self.mysql_client.query_iter(query);
        match result {
            Ok(result) => {
                let mut rows = vec![];
                let affected_rows = result.affected_rows();
                for row in result {
                    match row {
                        Ok(r) => rows.push(r),
                        Err(e) => {
                            return Err(format!("Failed to parse query result, err: {:?}", e));
                        }
                    }
                }

                if rows.is_empty() {
                    Ok(MysqlSqlResult::AffectedRows(affected_rows))
                } else {
                    Ok(MysqlSqlResult::Rows(rows))
                }
            }
            Err(e) => Err(format!("Failed to execute query, err: {:?}", e)),
        }
    }

    /// Execute a query on the GreptimeDB server.
    pub async fn grpc_query(&mut self, query: &str) -> Result<Output, client::Error> {
        let query_str = query.trim().to_lowercase();
        if query_str.starts_with("use ") {
            // use [db]
            let database = query
                .split_ascii_whitespace()
                .nth(1)
                .expect("Illegal `USE` statement: expecting a database.")
                .trim_end_matches(';');
            self.grpc_client.set_schema(database);
            Ok(Output::new_with_affected_rows(0))
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

            self.grpc_client.set_timezone(timezone);
            Ok(Output::new_with_affected_rows(0))
        } else {
            let mut result = self.grpc_client.sql(&query).await;
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
            result
        }
    }
}

/// Create a Postgres client with retry.
///
/// # Panics
///
/// Panics if the Postgres server address is invalid or the connection fails.
async fn create_postgres_client(pg_server_addr: &str) -> PgClient {
    let sockaddr: SocketAddr = pg_server_addr.parse().unwrap_or_else(|_| {
        panic!("Failed to parse the Postgres server address {pg_server_addr}. Please check if the address is in the format of `ip:port`.")
    });
    let mut config = tokio_postgres::config::Config::new();
    config.host(sockaddr.ip().to_string());
    config.port(sockaddr.port());
    config.dbname(DEFAULT_SCHEMA_NAME);

    retry_with_backoff(
        || async {
            config
                .connect(tokio_postgres::NoTls)
                .await
                .map(|(client, conn)| {
                    tokio::spawn(conn);
                    client
                })
        },
        3,
        Duration::from_millis(500),
    )
    .await
    .unwrap_or_else(|_| {
        panic!("Failed to connect to Postgres server. Please check if the server is running.")
    })
}

/// Create a MySQL client with retry.
///
/// # Panics
///
/// Panics if the MySQL server address is invalid or the connection fails.
async fn create_mysql_client(mysql_server_addr: &str) -> MySqlClient {
    let sockaddr: SocketAddr = mysql_server_addr.parse().unwrap_or_else(|_| {
        panic!("Failed to parse the MySQL server address {mysql_server_addr}. Please check if the address is in the format of `ip:port`.")
    });
    let ops = mysql::OptsBuilder::new()
        .ip_or_hostname(Some(sockaddr.ip().to_string()))
        .tcp_port(sockaddr.port())
        .db_name(Some(DEFAULT_SCHEMA_NAME));

    retry_with_backoff(
        || async { mysql::Conn::new(ops.clone()) },
        3,
        Duration::from_millis(500),
    )
    .await
    .unwrap_or_else(|_| {
        panic!("Failed to connect to MySQL server. Please check if the server is running.")
    })
}
