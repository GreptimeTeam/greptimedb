// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::statements::alter::AlterTable;
use crate::statements::create::{CreateDatabase, CreateTable};
use crate::statements::describe::DescribeTable;
use crate::statements::drop::DropTable;
use crate::statements::explain::Explain;
use crate::statements::insert::Insert;
use crate::statements::query::Query;
use crate::statements::show::{ShowCreateTable, ShowDatabases, ShowTables};

/// Tokens parsed by `DFParser` are converted into these values.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    // Query
    Query(Box<Query>),
    // Insert
    Insert(Box<Insert>),
    /// CREATE TABLE
    CreateTable(CreateTable),
    // DROP TABLE
    DropTable(DropTable),
    // CREATE DATABASE
    CreateDatabase(CreateDatabase),
    /// ALTER TABLE
    Alter(AlterTable),
    // Databases.
    ShowDatabases(ShowDatabases),
    // SHOW TABLES
    ShowTables(ShowTables),
    // SHOW CREATE TABLE
    ShowCreateTable(ShowCreateTable),
    // DESCRIBE TABLE
    DescribeTable(DescribeTable),
    // EXPLAIN QUERY
    Explain(Explain),
    Use(String),
}

/// Comment hints from SQL.
/// It'll be enabled when using `--comment` in mysql client.
/// Eg: `SELECT * FROM system.number LIMIT 1; -- { ErrorCode 25 }`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hint {
    pub error_code: Option<u16>,
    pub comment: String,
    pub prefix: String,
}
