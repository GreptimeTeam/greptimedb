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
use sqlparser::ast::{ColumnDef, Ident, ObjectName, TableConstraint};
use sqlparser_derive::{Visit, VisitMut};

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub struct AlterTable {
    table_name: ObjectName,
    alter_operation: AlterTableOperation,
}

impl AlterTable {
    pub(crate) fn new(table_name: ObjectName, alter_operation: AlterTableOperation) -> Self {
        Self {
            table_name,
            alter_operation,
        }
    }

    pub fn table_name(&self) -> &ObjectName {
        &self.table_name
    }

    pub fn alter_operation(&self) -> &AlterTableOperation {
        &self.alter_operation
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub enum AlterTableOperation {
    /// `ADD <table_constraint>`
    AddConstraint(TableConstraint),
    /// `ADD [ COLUMN ] <column_def> [location]`
    AddColumn {
        column_def: ColumnDef,
        location: Option<AddColumnLocation>,
    },
    /// `DROP COLUMN <name>`
    DropColumn { name: Ident },
    /// `RENAME <new_table_name>`
    RenameTable { new_table_name: String },
}
