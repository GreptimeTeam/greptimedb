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

use datafusion_sql::parser::Statement as DfStatement;
use sqlparser::ast::Statement as SpStatement;
use sqlparser_derive::{Visit, VisitMut};

use crate::error::{ConvertToDfStatementSnafu, Error};
use crate::statements::alter::AlterTable;
use crate::statements::create::{CreateDatabase, CreateExternalTable, CreateTable};
use crate::statements::delete::Delete;
use crate::statements::describe::DescribeTable;
use crate::statements::drop::DropTable;
use crate::statements::explain::Explain;
use crate::statements::insert::Insert;
use crate::statements::query::Query;
use crate::statements::show::{ShowCreateTable, ShowDatabases, ShowTables};
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
    // COPY
    Copy(crate::statements::copy::Copy),
    Tql(Tql),
    // TRUNCATE TABLE
    TruncateTable(TruncateTable),
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
