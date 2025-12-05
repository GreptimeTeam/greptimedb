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

use snafu::{ResultExt, ensure};
use sqlparser::ast::ObjectName;
use sqlparser::keywords::Keyword;
use sqlparser::tokenizer::Token;

use crate::ast::{Ident, ObjectNamePart};
use crate::error::{self, InvalidSqlSnafu, Result};
use crate::parser::{FLOW, ParserContext};
use crate::statements::comment::{Comment, CommentObject};
use crate::statements::statement::Statement;

impl ParserContext<'_> {
    pub(crate) fn parse_comment(&mut self) -> Result<Statement> {
        let _ = self.parser.next_token(); // consume COMMENT

        if !self.parser.parse_keyword(Keyword::ON) {
            return self.expected("ON", self.parser.peek_token());
        }

        let target_token = self.parser.next_token();
        let comment = match target_token.token {
            Token::Word(word) if word.keyword == Keyword::TABLE => {
                let raw_table =
                    self.parse_object_name()
                        .with_context(|_| error::UnexpectedSnafu {
                            expected: "a table name",
                            actual: self.peek_token_as_string(),
                        })?;
                let table = Self::canonicalize_object_name(raw_table)?;
                CommentObject::Table(table)
            }
            Token::Word(word) if word.keyword == Keyword::COLUMN => {
                self.parse_column_comment_target()?
            }
            Token::Word(word)
                if word.keyword == Keyword::NoKeyword && word.value.eq_ignore_ascii_case(FLOW) =>
            {
                let raw_flow =
                    self.parse_object_name()
                        .with_context(|_| error::UnexpectedSnafu {
                            expected: "a flow name",
                            actual: self.peek_token_as_string(),
                        })?;
                let flow = Self::canonicalize_object_name(raw_flow)?;
                CommentObject::Flow(flow)
            }
            _ => return self.expected("TABLE, COLUMN or FLOW", target_token),
        };

        if !self.parser.parse_keyword(Keyword::IS) {
            return self.expected("IS", self.parser.peek_token());
        }

        let comment_value = if self.parser.parse_keyword(Keyword::NULL) {
            None
        } else {
            Some(
                self.parser
                    .parse_literal_string()
                    .context(error::SyntaxSnafu)?,
            )
        };

        Ok(Statement::Comment(Comment {
            object: comment,
            comment: comment_value,
        }))
    }

    fn parse_column_comment_target(&mut self) -> Result<CommentObject> {
        let raw = self
            .parse_object_name()
            .with_context(|_| error::UnexpectedSnafu {
                expected: "a column reference",
                actual: self.peek_token_as_string(),
            })?;
        let canonical = Self::canonicalize_object_name(raw)?;

        let mut parts = canonical.0;
        ensure!(
            parts.len() >= 2,
            InvalidSqlSnafu {
                msg: "COMMENT ON COLUMN expects <table>.<column>".to_string(),
            }
        );

        let column_part = parts.pop().unwrap();
        let ObjectNamePart::Identifier(column_ident) = column_part else {
            unreachable!("canonicalized object name should only contain identifiers");
        };

        let column = ParserContext::canonicalize_identifier(column_ident);

        let mut table_idents: Vec<Ident> = Vec::with_capacity(parts.len());
        for part in parts {
            match part {
                ObjectNamePart::Identifier(ident) => table_idents.push(ident),
                ObjectNamePart::Function(_) => {
                    unreachable!("canonicalized object name should only contain identifiers")
                }
            }
        }

        ensure!(
            !table_idents.is_empty(),
            InvalidSqlSnafu {
                msg: "Table name is required before column name".to_string(),
            }
        );

        let table = ObjectName::from(table_idents);

        Ok(CommentObject::Column { table, column })
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use crate::dialect::GreptimeDbDialect;
    use crate::parser::{ParseOptions, ParserContext};
    use crate::statements::comment::CommentObject;
    use crate::statements::statement::Statement;

    fn parse(sql: &str) -> Statement {
        let mut stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(stmts.len(), 1);
        stmts.pop().unwrap()
    }

    #[test]
    fn test_parse_comment_on_table() {
        let stmt = parse("COMMENT ON TABLE mytable IS 'test';");
        match stmt {
            Statement::Comment(comment) => {
                assert_matches!(comment.object, CommentObject::Table(ref name) if name.to_string() == "mytable");
                assert_eq!(comment.comment.as_deref(), Some("test"));
            }
            _ => panic!("expected comment statement"),
        }

        let stmt = parse("COMMENT ON TABLE mytable IS NULL;");
        match stmt {
            Statement::Comment(comment) => {
                assert_matches!(comment.object, CommentObject::Table(ref name) if name.to_string() == "mytable");
                assert!(comment.comment.is_none());
            }
            _ => panic!("expected comment statement"),
        }
    }

    #[test]
    fn test_parse_comment_on_column() {
        let stmt = parse("COMMENT ON COLUMN my_schema.my_table.my_col IS 'desc';");
        match stmt {
            Statement::Comment(comment) => match comment.object {
                CommentObject::Column { table, column } => {
                    assert_eq!(table.to_string(), "my_schema.my_table");
                    assert_eq!(column.value, "my_col");
                    assert_eq!(comment.comment.as_deref(), Some("desc"));
                }
                _ => panic!("expected column comment"),
            },
            _ => panic!("expected comment statement"),
        }
    }

    #[test]
    fn test_parse_comment_on_flow() {
        let stmt = parse("COMMENT ON FLOW my_flow IS 'desc';");
        match stmt {
            Statement::Comment(comment) => {
                assert_matches!(comment.object, CommentObject::Flow(ref name) if name.to_string() == "my_flow");
                assert_eq!(comment.comment.as_deref(), Some("desc"));
            }
            _ => panic!("expected comment statement"),
        }
    }
}
