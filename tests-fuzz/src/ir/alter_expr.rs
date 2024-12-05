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

use std::fmt::Display;
use std::str::FromStr;

use common_base::readable_size::ReadableSize;
use common_query::AddColumnLocation;
use common_time::Duration;
use derive_builder::Builder;
use serde::{Deserialize, Serialize};
use store_api::mito_engine_options::{
    APPEND_MODE_KEY, COMPACTION_TYPE, TTL_KEY, TWCS_MAX_ACTIVE_WINDOW_FILES,
    TWCS_MAX_ACTIVE_WINDOW_RUNS, TWCS_MAX_INACTIVE_WINDOW_FILES, TWCS_MAX_INACTIVE_WINDOW_RUNS,
    TWCS_MAX_OUTPUT_FILE_SIZE, TWCS_TIME_WINDOW,
};
use strum::EnumIter;

use crate::error::{self, Result};
use crate::ir::{Column, Ident};

#[derive(Debug, Builder, Clone, Serialize, Deserialize)]
pub struct AlterTableExpr {
    pub table_name: Ident,
    pub alter_kinds: AlterTableOperation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlterTableOperation {
    /// `ADD [ COLUMN ] <column_def> [location]`
    AddColumn {
        column: Column,
        location: Option<AddColumnLocation>,
    },
    /// `DROP COLUMN <name>`
    DropColumn { name: Ident },
    /// `RENAME <new_table_name>`
    RenameTable { new_table_name: Ident },
    /// `MODIFY COLUMN <column_name> <column_type>`
    ModifyDataType { column: Column },
    /// `SET <table attrs key> = <table attr value>`
    SetTableOptions { options: Vec<AlterTableOption> },
    /// `UNSET <table attrs key>`
    UnsetTableOptions { keys: Vec<String> },
}

#[derive(Debug, EnumIter, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AlterTableOption {
    Ttl(Duration),
    TwcsTimeWindow(Duration),
    TwcsMaxOutputFileSize(ReadableSize),
    TwcsMaxInactiveWindowFiles(u64),
    TwcsMaxActiveWindowFiles(u64),
    TwcsMaxInactiveWindowRuns(u64),
    TwcsMaxActiveWindowRuns(u64),
}

impl AlterTableOption {
    pub fn key(&self) -> &str {
        match self {
            AlterTableOption::Ttl(_) => TTL_KEY,
            AlterTableOption::TwcsTimeWindow(_) => TWCS_TIME_WINDOW,
            AlterTableOption::TwcsMaxOutputFileSize(_) => TWCS_MAX_OUTPUT_FILE_SIZE,
            AlterTableOption::TwcsMaxInactiveWindowFiles(_) => TWCS_MAX_INACTIVE_WINDOW_FILES,
            AlterTableOption::TwcsMaxActiveWindowFiles(_) => TWCS_MAX_ACTIVE_WINDOW_FILES,
            AlterTableOption::TwcsMaxInactiveWindowRuns(_) => TWCS_MAX_INACTIVE_WINDOW_RUNS,
            AlterTableOption::TwcsMaxActiveWindowRuns(_) => TWCS_MAX_ACTIVE_WINDOW_RUNS,
        }
    }

    /// Parses the AlterTableOption from a key-value pair
    fn parse_kv(key: &str, value: &str) -> Result<Self> {
        match key {
            TTL_KEY => {
                let ttl = humantime::parse_duration(value).unwrap();
                Ok(AlterTableOption::Ttl(ttl.into()))
            }
            TWCS_MAX_ACTIVE_WINDOW_RUNS => {
                let runs = value.parse().unwrap();
                Ok(AlterTableOption::TwcsMaxActiveWindowRuns(runs))
            }
            TWCS_MAX_ACTIVE_WINDOW_FILES => {
                let files = value.parse().unwrap();
                Ok(AlterTableOption::TwcsMaxActiveWindowFiles(files))
            }
            TWCS_MAX_INACTIVE_WINDOW_RUNS => {
                let runs = value.parse().unwrap();
                Ok(AlterTableOption::TwcsMaxInactiveWindowRuns(runs))
            }
            TWCS_MAX_INACTIVE_WINDOW_FILES => {
                let files = value.parse().unwrap();
                Ok(AlterTableOption::TwcsMaxInactiveWindowFiles(files))
            }
            TWCS_MAX_OUTPUT_FILE_SIZE => {
                // may be "1M" instead of "1 MiB"
                let value = if value.ends_with("B") {
                    value.to_string()
                } else {
                    format!("{}B", value)
                };
                let size = ReadableSize::from_str(&value).unwrap();
                Ok(AlterTableOption::TwcsMaxOutputFileSize(size))
            }
            TWCS_TIME_WINDOW => {
                let time = humantime::parse_duration(value).unwrap();
                Ok(AlterTableOption::TwcsTimeWindow(time.into()))
            }
            _ => error::UnexpectedSnafu {
                violated: format!("Unknown table option key: {}", key),
            }
            .fail(),
        }
    }

    /// Parses the AlterTableOption from comma-separated string
    pub fn parse_kv_pairs(option_string: &str) -> Result<Vec<Self>> {
        let mut options = vec![];
        for pair in option_string.split(',') {
            let pair = pair.trim();
            let (key, value) = pair.split_once('=').unwrap();
            let key = key.trim();
            let value = value.trim().replace('\'', "");
            // Currently we have only one compaction type, so we ignore it
            // Cautious: COMPACTION_TYPE may be kept even if there are no compaction options enabled
            if key == COMPACTION_TYPE || key == APPEND_MODE_KEY {
                continue;
            } else {
                let option = AlterTableOption::parse_kv(key, &value)?;
                options.push(option);
            }
        }
        Ok(options)
    }
}

impl Display for AlterTableOption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlterTableOption::Ttl(d) => write!(f, "'{}' = '{}'", TTL_KEY, d),
            AlterTableOption::TwcsTimeWindow(d) => write!(f, "'{}' = '{}'", TWCS_TIME_WINDOW, d),
            AlterTableOption::TwcsMaxOutputFileSize(s) => {
                write!(f, "'{}' = '{}'", TWCS_MAX_OUTPUT_FILE_SIZE, s)
            }
            AlterTableOption::TwcsMaxInactiveWindowFiles(u) => {
                write!(f, "'{}' = '{}'", TWCS_MAX_INACTIVE_WINDOW_FILES, u)
            }
            AlterTableOption::TwcsMaxActiveWindowFiles(u) => {
                write!(f, "'{}' = '{}'", TWCS_MAX_ACTIVE_WINDOW_FILES, u)
            }
            AlterTableOption::TwcsMaxInactiveWindowRuns(u) => {
                write!(f, "'{}' = '{}'", TWCS_MAX_INACTIVE_WINDOW_RUNS, u)
            }
            AlterTableOption::TwcsMaxActiveWindowRuns(u) => {
                write!(f, "'{}' = '{}'", TWCS_MAX_ACTIVE_WINDOW_RUNS, u)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_kv_pairs() {
        let option_string =
            "compaction.twcs.max_output_file_size = '1M', compaction.type = 'twcs', ttl = '1day'";
        let options = AlterTableOption::parse_kv_pairs(option_string).unwrap();
        assert_eq!(options.len(), 2);
        assert_eq!(
            options,
            vec![
                AlterTableOption::TwcsMaxOutputFileSize(ReadableSize::from_str("1MB").unwrap()),
                AlterTableOption::Ttl(Duration::new_second(24 * 60 * 60)),
            ]
        );

        let option_string = "compaction.twcs.max_active_window_files = '5030469694939972912',
  compaction.twcs.max_active_window_runs = '8361168990283879099',
  compaction.twcs.max_inactive_window_files = '6028716566907830876',
  compaction.twcs.max_inactive_window_runs = '10622283085591494074',
  compaction.twcs.max_output_file_size = '15686.4PiB',
  compaction.twcs.time_window = '2061999256ms',
  compaction.type = 'twcs',
  ttl = '1month 3days 15h 49m 8s 279ms'";
        let options = AlterTableOption::parse_kv_pairs(option_string).unwrap();
        assert_eq!(options.len(), 7);
        let expected = vec![
            AlterTableOption::TwcsMaxActiveWindowFiles(5030469694939972912),
            AlterTableOption::TwcsMaxActiveWindowRuns(8361168990283879099),
            AlterTableOption::TwcsMaxInactiveWindowFiles(6028716566907830876),
            AlterTableOption::TwcsMaxInactiveWindowRuns(10622283085591494074),
            AlterTableOption::TwcsMaxOutputFileSize(ReadableSize::from_str("15686.4PiB").unwrap()),
            AlterTableOption::TwcsTimeWindow(Duration::new_nanosecond(2_061_999_256_000_000)),
            AlterTableOption::Ttl(Duration::new_millisecond(
                // A month is 2_630_016 seconds
                2_630_016 * 1000
                    + 3 * 24 * 60 * 60 * 1000
                    + 15 * 60 * 60 * 1000
                    + 49 * 60 * 1000
                    + 8 * 1000
                    + 279,
            )),
        ];
        assert_eq!(options, expected);
    }
}
