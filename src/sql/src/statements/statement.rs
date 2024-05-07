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

use datafusion_sql::parser::Statement as DfStatement;
use sqlparser::ast::Statement as SpStatement;
use sqlparser_derive::{Visit, VisitMut};

use crate::error::{ConvertToDfStatementSnafu, Error};
use crate::statements::alter::AlterTable;
use crate::statements::create::{
    CreateDatabase, CreateExternalTable, CreateFlow, CreateTable, CreateTableLike, CreateView,
};
use crate::statements::delete::Delete;
use crate::statements::describe::DescribeTable;
use crate::statements::drop::{DropDatabase, DropFlow, DropTable};
use crate::statements::explain::Explain;
use crate::statements::insert::Insert;
use crate::statements::query::Query;
use crate::statements::set_variables::SetVariables;
use crate::statements::show::{
    ShowColumns, ShowCreateTable, ShowDatabases, ShowIndex, ShowKind, ShowTables, ShowVariables,
};
use crate::statements::tql::Tql;
use crate::statements::truncate::TruncateTable;

/// Tokens parsed by `DFParser` are converted into these values.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub enum Statement {
    // Query
    Query(Box<Query>),
    // Insert
    Insert(Box<Insert>),
    // Delete
    Delete(Box<Delete>),
    /// CREATE TABLE
    CreateTable(CreateTable),
    // CREATE EXTERNAL TABLE
    CreateExternalTable(CreateExternalTable),
    // CREATE TABLE ... LIKE
    CreateTableLike(CreateTableLike),
    // CREATE FLOW
    CreateFlow(CreateFlow),
    // DROP FLOW
    DropFlow(DropFlow),
    // CREATE VIEW ... AS
    CreateView(CreateView),
    // DROP TABLE
    DropTable(DropTable),
    // DROP DATABASE
    DropDatabase(DropDatabase),
    // CREATE DATABASE
    CreateDatabase(CreateDatabase),
    /// ALTER TABLE
    Alter(AlterTable),
    // Databases.
    ShowDatabases(ShowDatabases),
    // SHOW TABLES
    ShowTables(ShowTables),
    // SHOW COLUMNS
    ShowColumns(ShowColumns),
    // SHOW CHARSET or SHOW CHARACTER SET
    ShowCharset(ShowKind),
    // SHOW COLLATION
    ShowCollation(ShowKind),
    // SHOW INDEX
    ShowIndex(ShowIndex),
    // SHOW CREATE TABLE
    ShowCreateTable(ShowCreateTable),
    // DESCRIBE TABLE
    DescribeTable(DescribeTable),
    // EXPLAIN QUERY
    Explain(Explain),
    // COPY
    Copy(crate::statements::copy::Copy),
    Tql(Tql),
    // TRUNCATE TABLE
    TruncateTable(TruncateTable),
    // SET VARIABLES
    SetVariables(SetVariables),
    // SHOW VARIABLES
    ShowVariables(ShowVariables),
}

impl Display for Statement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Statement::Query(s) => s.inner.fmt(f),
            Statement::Insert(s) => s.inner.fmt(f),
            Statement::Delete(s) => s.inner.fmt(f),
            Statement::CreateTable(s) => s.fmt(f),
            Statement::CreateExternalTable(s) => s.fmt(f),
            Statement::CreateTableLike(s) => s.fmt(f),
            Statement::CreateFlow(s) => s.fmt(f),
            Statement::DropFlow(s) => s.fmt(f),
            Statement::DropTable(s) => s.fmt(f),
            Statement::DropDatabase(s) => s.fmt(f),
            Statement::CreateDatabase(s) => s.fmt(f),
            Statement::Alter(s) => s.fmt(f),
            Statement::ShowDatabases(s) => s.fmt(f),
            Statement::ShowTables(s) => s.fmt(f),
            Statement::ShowColumns(s) => s.fmt(f),
            Statement::ShowIndex(s) => s.fmt(f),
            Statement::ShowCreateTable(s) => s.fmt(f),
            Statement::DescribeTable(s) => s.fmt(f),
            Statement::Explain(s) => s.fmt(f),
            Statement::Copy(s) => s.fmt(f),
            Statement::Tql(s) => s.fmt(f),
            Statement::TruncateTable(s) => s.fmt(f),
            Statement::SetVariables(s) => s.fmt(f),
            Statement::ShowVariables(s) => s.fmt(f),
            Statement::ShowCharset(kind) => {
                write!(f, "SHOW CHARSET {kind}")
            }
            Statement::ShowCollation(kind) => {
                write!(f, "SHOW COLLATION {kind}")
            }
            Statement::CreateView(_c) => todo!(),
        }
    }
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

impl TryFrom<&Statement> for DfStatement {
    type Error = Error;

    fn try_from(s: &Statement) -> Result<Self, Self::Error> {
        let s = match s {
            Statement::Query(query) => SpStatement::Query(Box::new(query.inner.clone())),
            Statement::Explain(explain) => explain.inner.clone(),
            Statement::Insert(insert) => insert.inner.clone(),
            Statement::Delete(delete) => delete.inner.clone(),
            _ => {
                return ConvertToDfStatementSnafu {
                    statement: format!("{s:?}"),
                }
                .fail();
            }
        };
        Ok(DfStatement::Statement(Box::new(s.into())))
    }
}
