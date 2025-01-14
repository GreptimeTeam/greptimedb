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

use snafu::{ensure, ResultExt};
use sqlx::{MySqlPool, Row};

use crate::error::{self, Result, UnexpectedSnafu};
use crate::ir::alter_expr::AlterTableOption;

/// Parses table options from the result of `SHOW CREATE TABLE`
/// An example of the result of `SHOW CREATE TABLE`:
/// +-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
/// | Table | Create Table                                                                                                                                                                                           |
/// +-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
/// | json  | CREATE TABLE IF NOT EXISTS `json` (`ts` TIMESTAMP(3) NOT NULL, `j` JSON NULL, TIME INDEX (`ts`)) ENGINE=mito WITH(compaction.twcs.max_output_file_size = '1M', compaction.type = 'twcs', ttl = '1day') |
/// +-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
fn parse_show_create(show_create: &str) -> Result<Vec<AlterTableOption>> {
    if let Some(option_start) = show_create.find("WITH(") {
        let option_end = {
            let remain_str = &show_create[option_start..];
            if let Some(end) = remain_str.find(')') {
                end + option_start
            } else {
                return UnexpectedSnafu {
                    violated: format!("Cannot find the end of the options in: {}", show_create),
                }
                .fail();
            }
        };
        let options = &show_create[option_start + 5..option_end];
        Ok(AlterTableOption::parse_kv_pairs(options)?)
    } else {
        Ok(vec![])
    }
}

/// Fetches table options from the context
pub async fn fetch_table_options(db: &MySqlPool, sql: &str) -> Result<Vec<AlterTableOption>> {
    let fetched_rows = sqlx::query(sql)
        .fetch_all(db)
        .await
        .context(error::ExecuteQuerySnafu { sql })?;
    ensure!(
        fetched_rows.len() == 1,
        error::AssertSnafu {
            reason: format!(
                "Expected fetched row length: 1, got: {}",
                fetched_rows.len(),
            )
        }
    );

    let row = fetched_rows.first().unwrap();
    let show_create = row.try_get::<String, usize>(1).unwrap();
    parse_show_create(&show_create)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use common_base::readable_size::ReadableSize;
    use common_time::Duration;

    use super::*;
    use crate::ir::alter_expr::Ttl;
    use crate::ir::AlterTableOption;

    #[test]
    fn test_parse_show_create() {
        let show_create = "CREATE TABLE IF NOT EXISTS `json` (`ts` TIMESTAMP(3) NOT NULL, `j` JSON NULL, TIME INDEX (`ts`)) ENGINE=mito WITH(compaction.twcs.max_output_file_size = '1M', compaction.type = 'twcs', ttl = '1day')";
        let options = parse_show_create(show_create).unwrap();
        assert_eq!(options.len(), 2);
        assert_eq!(
            options[0],
            AlterTableOption::TwcsMaxOutputFileSize(ReadableSize::from_str("1MB").unwrap())
        );
        assert_eq!(
            options[1],
            AlterTableOption::Ttl(Ttl::Duration(Duration::new_second(24 * 60 * 60)))
        );
    }
}
