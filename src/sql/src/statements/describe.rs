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

/// SQL structure for `DESCRIBE TABLE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTable {
    pub table_name: String,
}

impl DescribeTable {
    /// Creates a statement for `DESCRIBE tABLE`
    pub fn new(table_name: String) -> Self {
        DescribeTable { table_name }
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use sqlparser::dialect::GenericDialect;

    use crate::parser::ParserContext;
    use crate::statements::statement::Statement;

    #[test]
    pub fn test_show_create_table() {
        let sql = "DESCRIBE TABLE test";
        let stmts: Vec<Statement> =
            ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::DescribeTable { .. });
        match &stmts[0] {
            Statement::DescribeTable(show) => {
                let table_name = show.table_name.as_str();
                assert_eq!(table_name, "test");
            }
            _ => {
                unreachable!();
            }
        }
    }
    #[test]
    pub fn test_show_create_missing_table_name() {
        let sql = "DESCRIBE TABLE";
        ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap_err();
    }
}
