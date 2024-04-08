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

use std::env;

use common_telemetry::info;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{MySql, Pool};

pub struct Connections {
    pub mysql: Option<Pool<MySql>>,
}

const GT_MYSQL_ADDR: &str = "GT_MYSQL_ADDR";

pub async fn init_greptime_connections() -> Connections {
    let _ = dotenv::dotenv();
    let mysql = if let Ok(addr) = env::var(GT_MYSQL_ADDR) {
        Some(
            MySqlPoolOptions::new()
                .connect(&format!("mysql://{addr}/public"))
                .await
                .unwrap(),
        )
    } else {
        info!("GT_MYSQL_ADDR is empty, ignores test");
        None
    };

    Connections { mysql }
}
