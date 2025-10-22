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

use serde::Serialize;
use sqlparser::ast::ObjectName;
use sqlparser_derive::{Visit, VisitMut};

use crate::statements::OptionMap;
use crate::statements::statement::Statement;

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub enum Copy {
    CopyTable(CopyTable),
    CopyDatabase(CopyDatabase),
    CopyQueryTo(CopyQueryTo),
}

impl Display for Copy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Copy::CopyTable(s) => s.fmt(f),
            Copy::CopyDatabase(s) => s.fmt(f),
            Copy::CopyQueryTo(s) => s.fmt(f),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub struct CopyQueryTo {
    pub query: Box<Statement>,
    pub arg: CopyQueryToArgument,
}

impl Display for CopyQueryTo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "COPY ({}) TO {}", &self.query, &self.arg.location)?;
        if !self.arg.with.is_empty() {
            let options = self.arg.with.kv_pairs();
            write!(f, " WITH ({})", options.join(", "))?;
        }
        if !self.arg.connection.is_empty() {
            let options = self.arg.connection.kv_pairs();
            write!(f, " CONNECTION ({})", options.join(", "))?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub enum CopyTable {
    To(CopyTableArgument),
    From(CopyTableArgument),
}

impl Display for CopyTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "COPY ")?;
        let (with, connection) = match self {
            CopyTable::To(args) => {
                write!(f, "{} TO {}", &args.table_name, &args.location)?;
                (&args.with, &args.connection)
            }
            CopyTable::From(args) => {
                write!(f, "{} FROM {}", &args.table_name, &args.location)?;
                (&args.with, &args.connection)
            }
        };
        if !with.is_empty() {
            let options = with.kv_pairs();
            write!(f, " WITH ({})", options.join(", "))?;
        }
        if !connection.is_empty() {
            let options = connection.kv_pairs();
            write!(f, " CONNECTION ({})", options.join(", "))?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub enum CopyDatabase {
    To(CopyDatabaseArgument),
    From(CopyDatabaseArgument),
}

impl Display for CopyDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "COPY DATABASE ")?;
        let (with, connection) = match self {
            CopyDatabase::To(args) => {
                write!(f, "{} TO {}", &args.database_name, &args.location)?;
                (&args.with, &args.connection)
            }
            CopyDatabase::From(args) => {
                write!(f, "{} FROM {}", &args.database_name, &args.location)?;
                (&args.with, &args.connection)
            }
        };
        if !with.is_empty() {
            let options = with.kv_pairs();
            write!(f, " WITH ({})", options.join(", "))?;
        }
        if !connection.is_empty() {
            let options = connection.kv_pairs();
            write!(f, " CONNECTION ({})", options.join(", "))?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub struct CopyDatabaseArgument {
    pub database_name: ObjectName,
    pub with: OptionMap,
    pub connection: OptionMap,
    pub location: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub struct CopyTableArgument {
    pub table_name: ObjectName,
    pub with: OptionMap,
    pub connection: OptionMap,
    /// Copy tbl [To|From] 'location'.
    pub location: String,
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub struct CopyQueryToArgument {
    pub with: OptionMap,
    pub connection: OptionMap,
    pub location: String,
}

#[cfg(test)]
impl CopyTableArgument {
    pub fn format(&self) -> Option<String> {
        self.with
            .get(common_datasource::file_format::FORMAT_TYPE)
            .map(|v| v.to_string())
            .or_else(|| Some("PARQUET".to_string()))
    }

    pub fn pattern(&self) -> Option<String> {
        self.with
            .get(common_datasource::file_format::FILE_PATTERN)
            .map(|v| v.to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use crate::dialect::GreptimeDbDialect;
    use crate::parser::{ParseOptions, ParserContext};
    use crate::statements::statement::Statement;

    #[test]
    fn test_display_copy_from_tb() {
        let sql = r"copy tbl from 's3://my-bucket/data.parquet'
            with (format = 'parquet', pattern = '.*parquet.*')
            connection(region = 'us-west-2', secret_access_key = '12345678');";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::Copy { .. });

        match &stmts[0] {
            Statement::Copy(copy) => {
                let new_sql = format!("{}", copy);
                assert_eq!(
                    r#"COPY tbl FROM s3://my-bucket/data.parquet WITH (format = 'parquet', pattern = '.*parquet.*') CONNECTION (region = 'us-west-2', secret_access_key = '******')"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }
    }

    #[test]
    fn test_display_copy_to_tb() {
        let sql = r"copy tbl to 's3://my-bucket/data.parquet'
            with (format = 'parquet', pattern = '.*parquet.*')
            connection(region = 'us-west-2', secret_access_key = '12345678');";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::Copy { .. });

        match &stmts[0] {
            Statement::Copy(copy) => {
                let new_sql = format!("{}", copy);
                assert_eq!(
                    r#"COPY tbl TO s3://my-bucket/data.parquet WITH (format = 'parquet', pattern = '.*parquet.*') CONNECTION (region = 'us-west-2', secret_access_key = '******')"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }
    }

    #[test]
    fn test_display_copy_from_db() {
        let sql = r"copy database db1 from 's3://my-bucket/data.parquet'
            with (format = 'parquet', pattern = '.*parquet.*')
            connection(region = 'us-west-2', secret_access_key = '12345678');";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::Copy { .. });

        match &stmts[0] {
            Statement::Copy(copy) => {
                let new_sql = format!("{}", copy);
                assert_eq!(
                    r#"COPY DATABASE db1 FROM s3://my-bucket/data.parquet WITH (format = 'parquet', pattern = '.*parquet.*') CONNECTION (region = 'us-west-2', secret_access_key = '******')"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }
    }

    #[test]
    fn test_display_copy_to_db() {
        let sql = r"copy database db1 to 's3://my-bucket/data.parquet'
            with (format = 'parquet', pattern = '.*parquet.*')
            connection(region = 'us-west-2', secret_access_key = '12345678');";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::Copy { .. });

        match &stmts[0] {
            Statement::Copy(copy) => {
                let new_sql = format!("{}", copy);
                assert_eq!(
                    r#"COPY DATABASE db1 TO s3://my-bucket/data.parquet WITH (format = 'parquet', pattern = '.*parquet.*') CONNECTION (region = 'us-west-2', secret_access_key = '******')"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }
    }

    #[test]
    fn test_display_copy_query_to() {
        let sql = r"copy (SELECT * FROM tbl WHERE ts > 10) to 's3://my-bucket/data.parquet'
            with (format = 'parquet', pattern = '.*parquet.*')
            connection(region = 'us-west-2', secret_access_key = '12345678');";
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::Copy { .. });

        match &stmts[0] {
            Statement::Copy(copy) => {
                let new_sql = format!("{}", copy);
                assert_eq!(
                    r#"COPY (SELECT * FROM tbl WHERE ts > 10) TO s3://my-bucket/data.parquet WITH (format = 'parquet', pattern = '.*parquet.*') CONNECTION (region = 'us-west-2', secret_access_key = '******')"#,
                    &new_sql
                );
            }
            _ => {
                unreachable!();
            }
        }
    }
}
