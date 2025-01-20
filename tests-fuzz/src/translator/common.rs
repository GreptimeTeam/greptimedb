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

use super::DslTranslator;
use crate::error::{Error, Result};
use crate::ir::alter_expr::AlterTableOperation;
use crate::ir::{AlterTableExpr, AlterTableOption};

/// Shared translator for `ALTER TABLE` operations.
pub(crate) struct CommonAlterTableTranslator;

impl DslTranslator<AlterTableExpr, String> for CommonAlterTableTranslator {
    type Error = Error;

    fn translate(&self, input: &AlterTableExpr) -> Result<String> {
        Ok(match &input.alter_kinds {
            AlterTableOperation::DropColumn { name } => Self::format_drop(&input.table_name, name),
            AlterTableOperation::SetTableOptions { options } => {
                Self::format_set_table_options(&input.table_name, options)
            }
            AlterTableOperation::UnsetTableOptions { keys } => {
                Self::format_unset_table_options(&input.table_name, keys)
            }
            _ => unimplemented!(),
        })
    }
}

impl CommonAlterTableTranslator {
    fn format_drop(name: impl Display, column: impl Display) -> String {
        format!("ALTER TABLE {name} DROP COLUMN {column};")
    }

    fn format_set_table_options(name: impl Display, options: &[AlterTableOption]) -> String {
        format!(
            "ALTER TABLE {name} SET {};",
            options
                .iter()
                .map(|option| option.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    fn format_unset_table_options(name: impl Display, keys: &[String]) -> String {
        format!(
            "ALTER TABLE {name} UNSET {};",
            keys.iter()
                .map(|key| format!("'{}'", key))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}
