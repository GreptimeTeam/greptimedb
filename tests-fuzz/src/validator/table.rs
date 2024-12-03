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
use sqlx::database::HasArguments;
use sqlx::{ColumnIndex, Database, Decode, Encode, Executor, IntoArguments, Row, Type};

use crate::error::{self, Result};
use crate::ir::alter_expr::AlterTableOption;

/// Parses table options from the result of `SHOW CREATE TABLE`
/// An example of the result of `SHOW CREATE TABLE`:
/// +-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
/// | Table | Create Table                                                                                                                                                                                           |
/// +-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
/// | json  | CREATE TABLE IF NOT EXISTS `json` (`ts` TIMESTAMP(3) NOT NULL, `j` JSON NULL, TIME INDEX (`ts`)) ENGINE=mito WITH(compaction.twcs.max_output_file_size = '1M', compaction.type = 'twcs', ttl = '1day') |
/// +-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
fn parse_show_create(show_create: &str) -> Result<Vec<AlterTableOption>> {
    let option_start = show_create.find("WITH(");
    if option_start.is_none() {
        Ok(vec![])
    } else {
        let option_start = option_start.unwrap() + 5;
        let option_end = {
            let mut i = 1;
            while show_create.chars().nth(option_start + i).unwrap() != ')' {
                i += 1;
            }
            i + option_start
        };
        let options = &show_create[option_start..option_end];
        Ok(AlterTableOption::parse_kv_pairs(options)?)
    }
}

/// Fetches table options from the context
pub async fn fetch_table_options<'a, DB, E>(e: E, sql: &'a str) -> Result<Vec<AlterTableOption>>
where
    DB: Database,
    <DB as HasArguments<'a>>::Arguments: IntoArguments<'a, DB>,
    for<'c> E: 'a + Executor<'c, Database = DB>,
    for<'c> String: Decode<'c, DB> + Type<DB>,
    for<'c> String: Encode<'c, DB> + Type<DB>,
    usize: ColumnIndex<<DB as Database>::Row>,
{
    let fetched_rows = sqlx::query(sql)
        .fetch_all(e)
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
            AlterTableOption::Ttl(Duration::new_second(24 * 60 * 60))
        );
    }
}
