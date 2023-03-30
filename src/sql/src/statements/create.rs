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

use std::collections::HashMap;

use crate::ast::{ColumnDef, Ident, ObjectName, SqlOption, TableConstraint, Value as SqlValue};

/// Time index name, used in table constraints.
pub const TIME_INDEX: &str = "__time_index";

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CreateTable {
    /// Create if not exists
    pub if_not_exists: bool,
    pub table_id: u32,
    /// Table name
    pub name: ObjectName,
    pub columns: Vec<ColumnDef>,
    pub engine: String,
    pub constraints: Vec<TableConstraint>,
    /// Table options in `WITH`.
    pub options: Vec<SqlOption>,
    pub partitions: Option<Partitions>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Partitions {
    pub column_list: Vec<Ident>,
    pub entries: Vec<PartitionEntry>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct PartitionEntry {
    pub name: Ident,
    pub value_list: Vec<SqlValue>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CreateDatabase {
    pub name: ObjectName,
    /// Create if not exists
    pub if_not_exists: bool,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CreateExternalTable {
    /// Table name
    pub name: ObjectName,
    pub columns: Vec<ColumnDef>,
    pub constraints: Vec<TableConstraint>,
    /// Table options in `WITH`.
    pub options: HashMap<String, String>,
}
