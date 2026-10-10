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
use serde::Serialize;
use sqlparser::ast::Statement as SpStatement;
use sqlparser_derive::{Visit, VisitMut};

use crate::error::{ConvertToDfStatementSnafu, Error};
use crate::statements::admin::Admin;
use crate::statements::alter::{AlterDatabase, AlterTable};
use crate::statements::comment::Comment;
use crate::statements::copy::Copy;
use crate::statements::create::{
    CreateDatabase, CreateExternalTable, CreateFlow, CreateTable, CreateTableLike, CreateView,
};
use crate::statements::cursor::{CloseCursor, DeclareCursor, FetchCursor};
use crate::statements::delete::Delete;
use crate::statements::describe::DescribeTable;
#[cfg(feature = "enterprise")]
use crate::statements::drop::UndropTable;
use crate::statements::drop::{DropDatabase, DropFlow, DropTable, DropView};
use crate::statements::explain::ExplainStatement;
use crate::statements::insert::Insert;
use crate::statements::kill::Kill;
use crate::statements::query::Query;
use crate::statements::set_variables::SetVariables;
use crate::statements::show::{
    ShowColumns, ShowCreateDatabase, ShowCreateFlow, ShowCreateTable, ShowCreateView,
    ShowDatabases, ShowFlowStatus, ShowFlows, ShowIndex, ShowKind, ShowProcessList, ShowRegion,
    ShowSearchPath, ShowStatus, ShowTableStatus, ShowTables, ShowVariables, ShowViews,
};
use crate::statements::tql::Tql;
use crate::statements::truncate::TruncateTable;

/// Tokens parsed by `DFParser` are converted into these values.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
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
    // CREATE VIEW ... AS
    CreateView(CreateView),
    // CREATE TRIGGER
    #[cfg(feature = "enterprise")]
    CreateTrigger(crate::statements::create::trigger::CreateTrigger),
    // DROP TABLE
    DropTable(DropTable),
    // UNDROP TABLE
    #[cfg(feature = "enterprise")]
    UndropTable(UndropTable),
    // DROP DATABASE
    DropDatabase(DropDatabase),
    // DROP FLOW
    DropFlow(DropFlow),
    // DROP Trigger
    #[cfg(feature = "enterprise")]
    DropTrigger(crate::statements::drop::trigger::DropTrigger),
    // DROP View
    DropView(DropView),
    // CREATE DATABASE
    CreateDatabase(CreateDatabase),
    /// ALTER TABLE
    AlterTable(AlterTable),
    /// ALTER DATABASE
    AlterDatabase(AlterDatabase),
    /// ALTER TRIGGER
    #[cfg(feature = "enterprise")]
    AlterTrigger(crate::statements::alter::trigger::AlterTrigger),
    // Databases.
    ShowDatabases(ShowDatabases),
    // SHOW TABLES
    ShowTables(ShowTables),
    // SHOW TABLE STATUS
    ShowTableStatus(ShowTableStatus),
    // SHOW COLUMNS
    ShowColumns(ShowColumns),
    // SHOW CHARSET or SHOW CHARACTER SET
    ShowCharset(ShowKind),
    // SHOW COLLATION
    ShowCollation(ShowKind),
    // SHOW INDEX
    ShowIndex(ShowIndex),
    // SHOW REGION
    ShowRegion(ShowRegion),
    // SHOW CREATE DATABASE
    ShowCreateDatabase(ShowCreateDatabase),
    // SHOW CREATE TABLE
    ShowCreateTable(ShowCreateTable),
    // SHOW CREATE FLOW
    ShowCreateFlow(ShowCreateFlow),
    #[cfg(feature = "enterprise")]
    ShowCreateTrigger(crate::statements::show::trigger::ShowCreateTrigger),
    /// SHOW FLOWS
    ShowFlows(ShowFlows),
    /// SHOW FLOW STATUS
    ShowFlowStatus(ShowFlowStatus),
    // SHOW TRIGGERS
    #[cfg(feature = "enterprise")]
    ShowTriggers(crate::statements::show::trigger::ShowTriggers),
    // SHOW CREATE VIEW
    ShowCreateView(ShowCreateView),
    // SHOW STATUS
    ShowStatus(ShowStatus),
    // SHOW SEARCH_PATH
    ShowSearchPath(ShowSearchPath),
    // SHOW VIEWS
    ShowViews(ShowViews),
    // DESCRIBE TABLE
    DescribeTable(DescribeTable),
    // EXPLAIN QUERY
    Explain(Box<ExplainStatement>),
    // COPY
    Copy(Copy),
    // Telemetry Query Language
    Tql(Tql),
    // TRUNCATE TABLE
    TruncateTable(TruncateTable),
    // SET VARIABLES
    SetVariables(SetVariables),
    // SHOW VARIABLES
    ShowVariables(ShowVariables),
    // COMMENT ON
    Comment(Comment),
    // USE
    Use(String),
    // Admin statement(extension)
    Admin(Admin),
    // DECLARE ... CURSOR FOR ...
    DeclareCursor(DeclareCursor),
    // FETCH ... FROM ...
    FetchCursor(FetchCursor),
    // CLOSE
    CloseCursor(CloseCursor),
    // KILL <process>
    Kill(Kill),
    // SHOW PROCESSLIST
    ShowProcesslist(ShowProcessList),
}

impl Statement {
    pub fn is_readonly(&self) -> bool {
        match self {
            // Read-only operations
            Statement::Query(_)
            | Statement::ShowDatabases(_)
            | Statement::ShowTables(_)
            | Statement::ShowTableStatus(_)
            | Statement::ShowColumns(_)
            | Statement::ShowCharset(_)
            | Statement::ShowCollation(_)
            | Statement::ShowIndex(_)
            | Statement::ShowRegion(_)
            | Statement::ShowCreateDatabase(_)
            | Statement::ShowCreateTable(_)
            | Statement::ShowCreateFlow(_)
            | Statement::ShowFlows(_)
            | Statement::ShowFlowStatus(_)
            | Statement::ShowCreateView(_)
            | Statement::ShowStatus(_)
            | Statement::ShowSearchPath(_)
            | Statement::ShowViews(_)
            | Statement::DescribeTable(_)
            | Statement::ShowVariables(_)
            | Statement::ShowProcesslist(_)
            | Statement::FetchCursor(_)
            | Statement::Tql(_) => true,

            #[cfg(feature = "enterprise")]
            Statement::ShowCreateTrigger(_) => true,
            #[cfg(feature = "enterprise")]
            Statement::ShowTriggers(_) => true,

            Statement::Explain(explain) => !explain.analyze || explain.statement.is_readonly(),

            // Write operations
            Statement::Insert(_)
            | Statement::Delete(_)
            | Statement::CreateTable(_)
            | Statement::CreateExternalTable(_)
            | Statement::CreateTableLike(_)
            | Statement::CreateFlow(_)
            | Statement::CreateView(_)
            | Statement::DropTable(_)
            | Statement::DropDatabase(_)
            | Statement::DropFlow(_)
            | Statement::DropView(_)
            | Statement::CreateDatabase(_)
            | Statement::AlterTable(_)
            | Statement::AlterDatabase(_)
            | Statement::Copy(_)
            | Statement::TruncateTable(_)
            | Statement::SetVariables(_)
            | Statement::Comment(_)
            | Statement::Use(_)
            | Statement::DeclareCursor(_)
            | Statement::CloseCursor(_)
            | Statement::Kill(_)
            | Statement::Admin(_) => false,

            #[cfg(feature = "enterprise")]
            Statement::UndropTable(_) => false,

            #[cfg(feature = "enterprise")]
            Statement::AlterTrigger(_) => false,

            #[cfg(feature = "enterprise")]
            Statement::CreateTrigger(_) | Statement::DropTrigger(_) => false,
        }
    }
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
            #[cfg(feature = "enterprise")]
            Statement::CreateTrigger(s) => s.fmt(f),
            Statement::DropFlow(s) => s.fmt(f),
            #[cfg(feature = "enterprise")]
            Statement::DropTrigger(s) => s.fmt(f),
            Statement::DropTable(s) => s.fmt(f),
            #[cfg(feature = "enterprise")]
            Statement::UndropTable(s) => s.fmt(f),
            Statement::DropDatabase(s) => s.fmt(f),
            Statement::DropView(s) => s.fmt(f),
            Statement::CreateDatabase(s) => s.fmt(f),
            Statement::AlterTable(s) => s.fmt(f),
            Statement::AlterDatabase(s) => s.fmt(f),
            #[cfg(feature = "enterprise")]
            Statement::AlterTrigger(s) => s.fmt(f),
            Statement::ShowDatabases(s) => s.fmt(f),
            Statement::ShowTables(s) => s.fmt(f),
            Statement::ShowTableStatus(s) => s.fmt(f),
            Statement::ShowColumns(s) => s.fmt(f),
            Statement::ShowIndex(s) => s.fmt(f),
            Statement::ShowRegion(s) => s.fmt(f),
            Statement::ShowCreateTable(s) => s.fmt(f),
            Statement::ShowCreateFlow(s) => s.fmt(f),
            #[cfg(feature = "enterprise")]
            Statement::ShowCreateTrigger(s) => s.fmt(f),
            Statement::ShowFlows(s) => s.fmt(f),
            Statement::ShowFlowStatus(s) => s.fmt(f),
            #[cfg(feature = "enterprise")]
            Statement::ShowTriggers(s) => s.fmt(f),
            Statement::ShowCreateDatabase(s) => s.fmt(f),
            Statement::ShowCreateView(s) => s.fmt(f),
            Statement::ShowViews(s) => s.fmt(f),
            Statement::ShowStatus(s) => s.fmt(f),
            Statement::ShowSearchPath(s) => s.fmt(f),
            Statement::DescribeTable(s) => s.fmt(f),
            Statement::Explain(s) => s.fmt(f),
            Statement::Copy(s) => s.fmt(f),
            Statement::Tql(s) => s.fmt(f),
            Statement::TruncateTable(s) => s.fmt(f),
            Statement::SetVariables(s) => s.fmt(f),
            Statement::ShowVariables(s) => s.fmt(f),
            Statement::Comment(s) => s.fmt(f),
            Statement::ShowCharset(kind) => {
                write!(f, "SHOW CHARSET {kind}")
            }
            Statement::ShowCollation(kind) => {
                write!(f, "SHOW COLLATION {kind}")
            }
            Statement::CreateView(s) => s.fmt(f),
            Statement::Use(s) => s.fmt(f),
            Statement::Admin(admin) => admin.fmt(f),
            Statement::DeclareCursor(s) => s.fmt(f),
            Statement::FetchCursor(s) => s.fmt(f),
            Statement::CloseCursor(s) => s.fmt(f),
            Statement::Kill(k) => k.fmt(f),
            Statement::ShowProcesslist(s) => s.fmt(f),
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

/// Stack size used for the sqlparser AST deep clone performed by
/// [`TryFrom<&Statement> for DfStatement`].
///
/// The sqlparser AST uses a derived `Clone` implementation, which recurses once
/// per AST level, so cloning a pathologically deep statement (e.g. thousands of
/// chained `UNION ALL` branches) can overflow the caller's stack. The
/// global-worker thread only has a 2MB stack in release builds, so run the
/// conversion on a dedicated stack that fits such statements (see issue #9356).
const DEEP_STATEMENT_CONVERSION_STACK_SIZE: usize = 16 * 1024 * 1024;

impl TryFrom<&Statement> for DfStatement {
    type Error = Error;

    fn try_from(s: &Statement) -> Result<Self, Self::Error> {
        stacker::grow(DEEP_STATEMENT_CONVERSION_STACK_SIZE, || {
            let s = match s {
                Statement::Query(query) => SpStatement::Query(Box::new(query.inner.clone())),
                Statement::Insert(insert) => insert.inner.clone(),
                Statement::Delete(delete) => delete.inner.clone(),
                _ => {
                    return ConvertToDfStatementSnafu {
                        statement: format!("{s:?}"),
                    }
                    .fail();
                }
            };
            Ok(DfStatement::Statement(Box::new(s)))
        })
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::SetExpr;

    use super::*;
    use crate::dialect::GreptimeDbDialect;
    use crate::parser::{ParseOptions, ParserContext};

    fn build_deep_union_all_sql(branches: usize) -> String {
        let mut sql = String::new();
        for i in 0..branches {
            if i > 0 {
                sql.push_str(" UNION ALL ");
            }
            sql.push_str("SELECT number FROM numbers");
        }
        sql
    }

    fn count_union_branches(query: &sqlparser::ast::Query) -> usize {
        let mut branches = 1;
        let mut current: &SetExpr = &query.body;
        while let SetExpr::SetOperation { left, .. } = current {
            branches += 1;
            current = left;
        }
        branches
    }

    #[test]
    fn try_into_df_statement_with_deep_union_all() {
        const BRANCHES: usize = 2048;

        let sql = build_deep_union_all_sql(BRANCHES);

        // Parsing recurses once per `UNION ALL` branch as well, and the thread
        // that runs the test binary has no guaranteed stack size, so pin the
        // parse to an explicit, generous stack too.
        let statement = std::thread::Builder::new()
            .stack_size(DEEP_STATEMENT_CONVERSION_STACK_SIZE)
            .spawn(move || {
                let mut statements = ParserContext::create_with_dialect(
                    &sql,
                    &GreptimeDbDialect {},
                    ParseOptions::default(),
                )
                .unwrap();
                assert_eq!(statements.len(), 1);
                statements.pop().unwrap()
            })
            .unwrap()
            .join()
            .unwrap();

        // Converting deep-clones the sqlparser AST. Its derived `Clone` recursion
        // overflows a 1MB stack for this many branches unless the conversion runs
        // on a bigger stack (see issue #9356). Only the conversion runs on the
        // small stack; the statements are returned back out so that their
        // recursion-heavy drops, which are not stack-protected either, can run on
        // a grown stack below.
        let (df_statement, statement) = std::thread::Builder::new()
            .stack_size(1024 * 1024)
            .spawn(move || {
                let df_statement = DfStatement::try_from(&statement).unwrap();
                (df_statement, statement)
            })
            .unwrap()
            .join()
            .unwrap();

        let SpStatement::Query(query) = (match df_statement {
            DfStatement::Statement(statement) => *statement,
            _ => panic!("expected a plain statement"),
        }) else {
            panic!("expected a query statement");
        };
        assert_eq!(count_union_branches(&query), BRANCHES);

        // Both statements nest `BRANCHES` levels deep; run their recursive drops
        // on a stack that fits them.
        stacker::grow(DEEP_STATEMENT_CONVERSION_STACK_SIZE, move || {
            drop(query);
            drop(statement);
        });
    }
}
