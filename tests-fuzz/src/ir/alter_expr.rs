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

use common_base::readable_size::ReadableSize;
use common_query::AddColumnLocation;
use common_time::Duration;
use derive_builder::Builder;
use serde::{Deserialize, Serialize};
use store_api::mito_engine_options::{
    TTL_KEY, TWCS_MAX_ACTIVE_WINDOW_FILES, TWCS_MAX_ACTIVE_WINDOW_RUNS,
    TWCS_MAX_INACTIVE_WINDOW_FILES, TWCS_MAX_INACTIVE_WINDOW_RUNS, TWCS_MAX_OUTPUT_FILE_SIZE,
    TWCS_TIME_WINDOW,
};
use strum::EnumIter;

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
}

#[derive(Debug, EnumIter, Clone, Serialize, Deserialize, PartialEq)]
pub enum AlterTableOption {
    TTL(Duration),
    TwcsTimeWindow(Duration),
    TwcsMaxOutputFileSize(ReadableSize),
    TwcsMaxInactiveWindowFiles(u64),
    TwcsMaxActiveWindowFiles(u64),
    TwcsMaxInactiveWindowRuns(u64),
    TwcsMaxActiveWindowRuns(u64),
}

impl Display for AlterTableOption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlterTableOption::TTL(d) => write!(f, "'{}' = '{}'", TTL_KEY, d),
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
