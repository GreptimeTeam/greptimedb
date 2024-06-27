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

pub mod config;
pub mod crd;
pub mod health;
#[cfg(feature = "unstable")]
pub mod process;

use std::env;

use common_telemetry::info;
use snafu::ResultExt;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{MySql, Pool};

use crate::error::{self, Result};
use crate::ir::Ident;

/// Database connections
pub struct Connections {
    pub mysql: Option<Pool<MySql>>,
}

const GT_MYSQL_ADDR: &str = "GT_MYSQL_ADDR";

/// Connects to GreptimeDB via env variables.
pub async fn init_greptime_connections_via_env() -> Connections {
    let _ = dotenv::dotenv();
    let mysql = if let Ok(addr) = env::var(GT_MYSQL_ADDR) {
        Some(addr)
    } else {
        info!("GT_MYSQL_ADDR is empty, ignores test");
        None
    };

    init_greptime_connections(mysql).await
}

/// Connects to GreptimeDB.
pub async fn init_greptime_connections(mysql: Option<String>) -> Connections {
    let mysql = if let Some(addr) = mysql {
        Some(
            MySqlPoolOptions::new()
                .connect(&format!("mysql://{addr}/public"))
                .await
                .unwrap(),
        )
    } else {
        None
    };

    Connections { mysql }
}

const GT_FUZZ_BINARY_PATH: &str = "GT_FUZZ_BINARY_PATH";
const GT_FUZZ_INSTANCE_ROOT_DIR: &str = "GT_FUZZ_INSTANCE_ROOT_DIR";

/// The variables for unstable test
pub struct UnstableTestVariables {
    pub binary_path: String,
    pub root_dir: Option<String>,
}

/// Loads env variables for unstable test
pub fn load_unstable_test_env_variables() -> UnstableTestVariables {
    let _ = dotenv::dotenv();
    let binary_path = env::var(GT_FUZZ_BINARY_PATH).expect("GT_FUZZ_BINARY_PATH not found");
    let root_dir = if let Ok(root) = env::var(GT_FUZZ_INSTANCE_ROOT_DIR) {
        Some(root)
    } else {
        None
    };

    UnstableTestVariables {
        binary_path,
        root_dir,
    }
}

/// Flushes memtable to SST file.
pub async fn flush_memtable(e: &Pool<MySql>, table_name: &Ident) -> Result<()> {
    let sql = format!("SELECT flush_table(\"{}\")", table_name);
    let result = sqlx::query(&sql)
        .execute(e)
        .await
        .context(error::ExecuteQuerySnafu { sql })?;
    info!("Flush table: {}\n\nResult: {result:?}\n\n", table_name);

    Ok(())
}

/// Triggers a compaction for table
pub async fn compact_table(e: &Pool<MySql>, table_name: &Ident) -> Result<()> {
    let sql = format!("SELECT compact_table(\"{}\")", table_name);
    let result = sqlx::query(&sql)
        .execute(e)
        .await
        .context(error::ExecuteQuerySnafu { sql })?;
    info!("Compact table: {}\n\nResult: {result:?}\n\n", table_name);

    Ok(())
}
