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

use snafu::ResultExt;
use sqlx::database::HasArguments;
use sqlx::{ColumnIndex, Database, Decode, Encode, Executor, IntoArguments, Type};

use crate::error::{self, Result};

pub const PEER_TYPE_DATANODE: &str = "DATANODE";

#[derive(Debug, sqlx::FromRow)]
pub struct NodeInfo {
    pub peer_id: i64,
    pub peer_addr: String,
    pub peer_type: String,
    pub active_time: Option<String>,
}

/// Returns all [NodeInfo] in the cluster.
pub async fn fetch_nodes<'a, DB, E>(e: E) -> Result<Vec<NodeInfo>>
where
    DB: Database,
    <DB as HasArguments<'a>>::Arguments: IntoArguments<'a, DB>,
    for<'c> E: 'a + Executor<'c, Database = DB>,
    for<'c> i64: Decode<'c, DB> + Type<DB>,
    for<'c> String: Decode<'c, DB> + Type<DB>,
    for<'c> String: Encode<'c, DB> + Type<DB>,
    for<'c> &'c str: ColumnIndex<<DB as Database>::Row>,
{
    let sql = "select * from information_schema.cluster_info;";
    sqlx::query_as::<_, NodeInfo>(sql)
        .fetch_all(e)
        .await
        .context(error::ExecuteQuerySnafu { sql })
}
