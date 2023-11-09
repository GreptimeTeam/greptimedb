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

use sqlparser::ast::ObjectName;
use sqlparser_derive::{Visit, VisitMut};

/// SQL structure for `DESCRIBE TABLE`.
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub struct DescribeTable {
    name: ObjectName,
}

impl DescribeTable {
    /// Creates a statement for `DESCRIBE TABLE`
    pub fn new(name: ObjectName) -> Self {
        Self { name }
    }

    pub fn name(&self) -> &ObjectName {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use crate::dialect::GreptimeDbDialect;
    use crate::parser::ParserContext;
    use crate::statements::statement::Statement;
    use crate::util::format_raw_object_name;

    #[test]
    pub fn test_describe_table() {
        let sql = "DESCRIBE TABLE test";
        let stmts: Vec<Statement> =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::DescribeTable { .. });
        match &stmts[0] {
            Statement::DescribeTable(show) => {
                assert_eq!(format_raw_object_name(&show.name), "test");
            }
            _ => {
                unreachable!();
            }
        }
    }

    #[test]
    pub fn test_describe_schema_table() {
        let sql = "DESCRIBE TABLE test_schema.test";
        let stmts: Vec<Statement> =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::DescribeTable { .. });
        match &stmts[0] {
            Statement::DescribeTable(show) => {
                assert_eq!(format_raw_object_name(&show.name), "test_schema.test");
            }
            _ => {
                unreachable!();
            }
        }
    }

    #[test]
    pub fn test_describe_catalog_schema_table() {
        let sql = "DESCRIBE TABLE test_catalog.test_schema.test";
        let stmts: Vec<Statement> =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::DescribeTable { .. });
        match &stmts[0] {
            Statement::DescribeTable(show) => {
                assert_eq!(
                    format_raw_object_name(&show.name),
                    "test_catalog.test_schema.test"
                );
            }
            _ => {
                unreachable!();
            }
        }
    }

    #[test]
    pub fn test_describe_missing_table_name() {
        let sql = "DESCRIBE TABLE";
        assert!(ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).is_err());
    }
}
