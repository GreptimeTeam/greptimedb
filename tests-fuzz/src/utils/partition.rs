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
use crate::ir::Ident;

#[derive(Debug, sqlx::FromRow)]
pub struct Partition {
    pub datanode_id: u64,
    pub region_id: u64,
}

#[derive(Debug, sqlx::FromRow)]
pub struct PartitionCount {
    pub count: i64,
}

pub async fn count_partitions<'a, DB, E>(e: E, datanode_id: u64) -> Result<PartitionCount>
where
    DB: Database,
    <DB as HasArguments<'a>>::Arguments: IntoArguments<'a, DB>,
    for<'c> E: 'a + Executor<'c, Database = DB>,
    for<'c> i64: Decode<'c, DB> + Type<DB>,
    for<'c> String: Decode<'c, DB> + Type<DB>,
    for<'c> u64: Encode<'c, DB> + Type<DB>,
    for<'c> &'c str: ColumnIndex<<DB as Database>::Row>,
{
    let sql = "select count(1) as count from information_schema.region_peers where peer_id == ?";
    Ok(sqlx::query_as::<_, PartitionCount>(sql)
        .bind(datanode_id)
        .fetch_all(e)
        .await
        .context(error::ExecuteQuerySnafu { sql })?
        .remove(0))
}

/// Returns all [Partition] of the specific `table`
pub async fn fetch_partitions<'a, DB, E>(e: E, table_name: Ident) -> Result<Vec<Partition>>
where
    DB: Database,
    <DB as HasArguments<'a>>::Arguments: IntoArguments<'a, DB>,
    for<'c> E: 'a + Executor<'c, Database = DB>,
    for<'c> u64: Decode<'c, DB> + Type<DB>,
    for<'c> String: Decode<'c, DB> + Type<DB>,
    for<'c> String: Encode<'c, DB> + Type<DB>,
    for<'c> &'c str: ColumnIndex<<DB as Database>::Row>,
{
    let sql = "select b.peer_id as datanode_id, a.greptime_partition_id as region_id
from information_schema.partitions a left join information_schema.region_peers b
on a.greptime_partition_id = b.region_id where a.table_name= ? order by datanode_id asc;";
    sqlx::query_as::<_, Partition>(sql)
        .bind(table_name.value.to_string())
        .fetch_all(e)
        .await
        .context(error::ExecuteQuerySnafu { sql })
}
