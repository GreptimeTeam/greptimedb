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

use snafu::{ResultExt, ensure};
use sqlx::MySqlPool;

use crate::error;
use crate::error::Result;
use crate::ir::Ident;
use crate::ir::create_expr::PartitionDef;

const PARTITIONS_INFO_SCHEMA_SQL: &str = "SELECT table_catalog, table_schema, table_name, \
partition_name, partition_expression, partition_description, greptime_partition_id, \
partition_ordinal_position FROM information_schema.partitions WHERE table_name = ? \
ORDER BY partition_ordinal_position;";

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PartitionInfo {
    pub table_catalog: String,
    pub table_schema: String,
    pub table_name: String,
    pub partition_name: String,
    pub partition_expression: String,
    pub partition_description: String,
    pub greptime_partition_id: u64,
    pub partition_ordinal_position: i64,
}

/// Fetches the partitions info from the information_schema.partitions table.
pub async fn fetch_partitions_info_schema(
    db: &MySqlPool,
    _schema_name: Ident,
    table: &Ident,
) -> Result<Vec<PartitionInfo>> {
    sqlx::query_as::<_, PartitionInfo>(PARTITIONS_INFO_SCHEMA_SQL)
        .bind(&table.value)
        .fetch_all(db)
        .await
        .context(error::ExecuteQuerySnafu {
            sql: PARTITIONS_INFO_SCHEMA_SQL,
        })
}

fn normalize(s: &str) -> String {
    s.replace("\\\"", "\"").replace("\\\\", "\\")
}

/// Asserts the partitions are equal to the expected partitions.
pub fn assert_partitions(expected: &PartitionDef, actual: &[PartitionInfo]) -> Result<()> {
    ensure!(
        expected.exprs.len() == actual.len(),
        error::AssertSnafu {
            reason: format!(
                "Expected partitions length: {}, got: {}",
                expected.exprs.len(),
                actual.len()
            ),
        }
    );

    let expected_exprs = expected.exprs.iter().map(|expr| expr.to_string());
    for expr in expected_exprs {
        let actual_expr = actual
            .iter()
            .find(|info| normalize(&info.partition_description) == normalize(&expr));
        ensure!(
            actual_expr.is_some(),
            error::AssertSnafu {
                reason: format!(
                    "Expected partition expression: '{expr:?}' not found, actual: {:?}",
                    actual
                        .iter()
                        .map(|info| format!("'{:?}'", info.partition_description.clone()))
                        .collect::<Vec<_>>()
                        .join("; ")
                ),
            }
        );
    }

    Ok(())
}
