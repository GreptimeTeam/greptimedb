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

use common_query::AddColumnLocation;
use derive_builder::Builder;
use serde::{Deserialize, Serialize};

use crate::ir::{Column, Ident};

#[derive(Debug, Builder, Clone, Serialize, Deserialize)]
pub struct AlterTableExpr {
    pub table_name: Ident,
    pub alter_options: AlterTableOperation,
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
}
