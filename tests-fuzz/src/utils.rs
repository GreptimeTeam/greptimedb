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

pub mod cluster_info;
pub mod config;
pub mod crd;
pub mod health;
pub mod migration;
pub mod partition;
pub mod pod_failure;
pub mod procedure;
#[cfg(feature = "unstable")]
pub mod process;
pub mod wait;

use std::env;

use common_telemetry::info;
use common_telemetry::tracing::log::LevelFilter;
use paste::paste;
use snafu::ResultExt;
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
use sqlx::{ConnectOptions, MySql, Pool};

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
        let opts = format!("mysql://{addr}/public")
            .parse::<MySqlConnectOptions>()
            .unwrap()
            .log_statements(LevelFilter::Off);

        Some(MySqlPoolOptions::new().connect_with(opts).await.unwrap())
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
    let root_dir = env::var(GT_FUZZ_INSTANCE_ROOT_DIR).ok();

    UnstableTestVariables {
        binary_path,
        root_dir,
    }
}

pub const GT_FUZZ_CLUSTER_NAMESPACE: &str = "GT_FUZZ_CLUSTER_NAMESPACE";
pub const GT_FUZZ_CLUSTER_NAME: &str = "GT_FUZZ_CLUSTER_NAME";

/// Flushes memtable to SST file.
pub async fn flush_memtable(e: &Pool<MySql>, table_name: &Ident) -> Result<()> {
    let sql = format!("admin flush_table(\"{}\")", table_name);
    let result = sqlx::query(&sql)
        .execute(e)
        .await
        .context(error::ExecuteQuerySnafu { sql })?;
    info!("Flush table: {}\n\nResult: {result:?}\n\n", table_name);

    Ok(())
}

/// Triggers a compaction for table
pub async fn compact_table(e: &Pool<MySql>, table_name: &Ident) -> Result<()> {
    let sql = format!("admin compact_table(\"{}\")", table_name);
    let result = sqlx::query(&sql)
        .execute(e)
        .await
        .context(error::ExecuteQuerySnafu { sql })?;
    info!("Compact table: {}\n\nResult: {result:?}\n\n", table_name);

    Ok(())
}

pub const GT_FUZZ_INPUT_MAX_ROWS: &str = "GT_FUZZ_INPUT_MAX_ROWS";
pub const GT_FUZZ_INPUT_MAX_TABLES: &str = "GT_FUZZ_INPUT_MAX_TABLES";
pub const GT_FUZZ_INPUT_MAX_COLUMNS: &str = "GT_FUZZ_INPUT_MAX_COLUMNS";
pub const GT_FUZZ_INPUT_MAX_ALTER_ACTIONS: &str = "GT_FUZZ_INPUT_MAX_ALTER_ACTIONS";
pub const GT_FUZZ_INPUT_MAX_INSERT_ACTIONS: &str = "GT_FUZZ_INPUT_MAX_INSERT_ACTIONS";

macro_rules! make_get_from_env_helper {
    ($key:expr, $default: expr) => {
        paste! {
            #[doc = "Retrieves `" $key "` environment variable \
                     or returns a default value (`" $default "`) if the environment variable is not set.
            "]
            pub fn [<get_ $key:lower>]() -> usize {
                get_from_env_or_default_value($key, $default)
            }
        }
    };
}

make_get_from_env_helper!(GT_FUZZ_INPUT_MAX_ALTER_ACTIONS, 256);
make_get_from_env_helper!(GT_FUZZ_INPUT_MAX_INSERT_ACTIONS, 4);
make_get_from_env_helper!(GT_FUZZ_INPUT_MAX_ROWS, 512);
make_get_from_env_helper!(GT_FUZZ_INPUT_MAX_TABLES, 32);
make_get_from_env_helper!(GT_FUZZ_INPUT_MAX_COLUMNS, 16);

/// Retrieves a value from the environment variables
/// or returns a default value if the environment variable is not set.
fn get_from_env_or_default_value(key: &str, default_value: usize) -> usize {
    let _ = dotenv::dotenv();
    if let Ok(value) = env::var(key) {
        value.parse().unwrap()
    } else {
        default_value
    }
}
