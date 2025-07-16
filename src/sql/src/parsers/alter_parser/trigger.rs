#![allow(dead_code)]
use snafu::ResultExt;
use sqlparser::ast::Ident;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use crate::error::{self, Result};
use crate::parser::ParserContext;
use crate::parsers::create_parser::trigger::{ANNOTATIONS, LABELS, NOTIFY, ON};
use crate::statements::statement::Statement;

/// Some keywords about trigger.
pub const RENAME: &str = "RENAME";
pub const TO: &str = "TO";
pub const SET: &str = "SET";
pub const ADD: &str = "ADD";
pub const MODIFY: &str = "MODIFY";
pub const DROP: &str = "DROP";

impl<'a> ParserContext<'a> {
    /// Parses an `ALTER TRIGGER` statement.
    ///
    /// ```sql
    /// ALTER TRIGGER <trigger_name>
    ///   		[alter_option [alter_option] ...]
    ///
    /// alter_option: {
    ///     	RENAME TO <new_trigger_name>
    ///     	| ON (<query_expression>) EVERY <interval_expression>
    /// 		| [SET] LABELS (<label_name>=<label_val>, ...)
    /// 		| ADD LABELS (<label_name>=<label_val>, ...)
    /// 		| MODIFY LABELS (<label_name>=<label_val>, ...)
    /// 		| DROP LABELS (<label_name1>, <label_name2>, ...)
    /// 		| [SET] ANNOTATIONS (<annotation_name>=<annotation_val>, ...)
    ///     	| ADD ANNOTATIONS (<annotation_name>=<annotation_val>, ...)
    ///     	| MODIFY ANNOTATIONS (<annotation_name>=<annotation_val>, ...)
    /// 		| DROP ANNOTATIONS (<annotation_name1>, <annotation_name2>, ...)
    ///         | ADD NOTIFY
    ///                 WEBHOOK <notify_name1> URL '<url1>' [WITH (<parameter>=<value>, ...)], ...
    /// 		| DROP NOTIFY (<notify_name1>, <notify_name2>)
    /// }
    /// ```
    pub(super) fn parse_alter_trigger(&mut self) -> Result<Statement> {
        let _trigger_name = self.intern_parse_table_name()?;

        let mut may_new_trigger_name = None;
        let mut may_new_query = None;
        let mut may_new_interval = None;
        let mut may_new_labels = None;
        let mut may_new_annotations = None;
        let mut new_notify_channels = None;

        let mut may_add_labels = None;
        let mut may_add_annotations = None;
        let mut add_notify_channels = None;

        let mut may_modify_labels = None;
        let mut may_modify_annotations = None;

        let mut drop_labels = None;
        let mut drop_annotations = None;
        let mut drop_notify_channels = None;

        loop {
            let next_token = self.parser.peek_token();
            match next_token.token {
                Token::Word(w) if w.value.eq_ignore_ascii_case(RENAME) => {
                    self.parser.next_token();
                    let name = self.parse_rename_to(true)?;
                    may_new_trigger_name.replace(name);
                }
                Token::Word(w) if w.value.eq_ignore_ascii_case(ON) => {
                    self.parser.next_token();
                    let (query, interval) = self.parse_trigger_on(true)?;
                    may_new_query.replace(query);
                    may_new_interval.replace(interval);
                }
                Token::Word(w) if w.value.eq_ignore_ascii_case(LABELS) => {
                    self.parser.next_token();
                    let labels = self.parse_trigger_labels(true)?;
                    may_new_labels.replace(labels);
                }
                Token::Word(w) if w.value.eq_ignore_ascii_case(ANNOTATIONS) => {
                    self.parser.next_token();
                    let annotations = self.parse_trigger_annotations(true)?;
                    may_new_annotations.replace(annotations);
                }
                Token::Word(w) if w.value.eq_ignore_ascii_case(NOTIFY) => {
                    self.parser.next_token();
                    let channels = self.parse_trigger_notify(true)?;
                    new_notify_channels.replace(channels);
                }
                Token::Word(w) if w.value.eq_ignore_ascii_case(SET) => {
                    self.parser.next_token();
                    let next_token = self.parser.peek_token();
                    match next_token.token {
                        Token::Word(w) if w.value.eq_ignore_ascii_case(LABELS) => {
                            self.parser.next_token();
                            let labels = self.parse_trigger_labels(true)?;
                            may_new_labels.replace(labels);
                        }
                        Token::Word(w) if w.value.eq_ignore_ascii_case(ANNOTATIONS) => {
                            self.parser.next_token();
                            let annotations = self.parse_trigger_annotations(true)?;
                            may_new_labels.replace(annotations);
                        }
                        Token::Word(w) if w.value.eq_ignore_ascii_case(NOTIFY) => {
                            self.parser.next_token();
                            let channels = self.parse_trigger_notify(true)?;
                            new_notify_channels.replace(channels);
                        }
                        _ => {
                            return self.expected(
                                "`LABELS`, `ANNOTATIONS` or `NOTIFY` keyword after `SET`",
                                next_token,
                            )
                        }
                    }
                }
                Token::Word(w) if w.value.eq_ignore_ascii_case(ADD) => {
                    self.parser.next_token();
                    let next_token = self.parser.peek_token();
                    match next_token.token {
                        Token::Word(w) if w.value.eq_ignore_ascii_case(LABELS) => {
                            self.parser.next_token();
                            let labels = self.parse_trigger_labels(false)?;
                            may_add_labels.replace(labels);
                        }
                        Token::Word(w) if w.value.eq_ignore_ascii_case(ANNOTATIONS) => {
                            self.parser.next_token();
                            let annotations = self.parse_trigger_annotations(false)?;
                            may_add_annotations.replace(annotations);
                        }
                        Token::Word(w) if w.value.eq_ignore_ascii_case(NOTIFY) => {
                            self.parser.next_token();
                            let channels = self.parse_trigger_notify(false)?;
                            add_notify_channels.replace(channels);
                        }
                        _ => {
                            return self.expected(
                                "`LABELS`, `ANNOTATIONS` or `NOTIFY` keyword after `ADD`",
                                next_token,
                            );
                        }
                    }
                }
                Token::Word(w) if w.value.eq_ignore_ascii_case(MODIFY) => {
                    self.parser.next_token();
                    let next_token = self.parser.peek_token();
                    match next_token.token {
                        Token::Word(w) if w.value.eq_ignore_ascii_case(LABELS) => {
                            self.parser.next_token();
                            let labels = self.parse_trigger_labels(false)?;
                            may_modify_labels.replace(labels);
                        }
                        Token::Word(w) if w.value.eq_ignore_ascii_case(ANNOTATIONS) => {
                            self.parser.next_token();
                            let annotations = self.parse_trigger_annotations(false)?;
                            may_modify_annotations.replace(annotations);
                        }
                        _ => {
                            return self.expected(
                                "`LABELS` or `ANNOTATIONS` keyword after `MODIFY`",
                                next_token,
                            );
                        }
                    }
                }
                Token::Word(w) if w.value.eq_ignore_ascii_case(DROP) => {
                    self.parser.next_token();
                    let next_token = self.parser.peek_token();
                    match next_token.token {
                        Token::Word(w) if w.value.eq_ignore_ascii_case(LABELS) => {
                            self.parser.next_token();
                            let names = self.parse_trigger_label_names(false)?;
                            drop_labels.replace(names);
                        }
                        Token::Word(w) if w.value.eq_ignore_ascii_case(ANNOTATIONS) => {
                            self.parser.next_token();
                            let names = self.parse_trigger_annotation_names(false)?;
                            drop_annotations.replace(names);
                        }
                        Token::Word(w) if w.value.eq_ignore_ascii_case(NOTIFY) => {
                            self.parser.next_token();
                            let channels = self.parse_trigger_notify(false)?;
                            drop_notify_channels.replace(channels);
                        }
                        _ => {
                            return self.expected(
                                "`LABELS`, `ANNOTATIONS` or `NOTIFY` keyword after `DROP`",
                                next_token,
                            );
                        }
                    }
                }
                Token::EOF => break,
                _ => {
                    return self.expected(
                        "`ON` or `SET` or `ADD` or `MODIFY` or `DROP` keyword",
                        next_token,
                    );
                }
            }
        }
        todo!("parse_alter_trigger not implemented yet");
    }

    /// The SQL format as follows:
    ///
    /// ```sql
    /// RENAME TO <new_trigger_name>
    /// ```
    ///
    /// ## Parameters
    ///
    /// - `is_first_keyword_matched`: indicates whether the first keyword `ON`
    ///     has been matched.
    fn parse_rename_to(&mut self, is_first_keyword_matched: bool) -> Result<Ident> {
        if !is_first_keyword_matched {
            if let Token::Word(w) = self.parser.peek_token().token
                && w.value.eq_ignore_ascii_case(RENAME)
            {
                self.parser.next_token();
            } else {
                return self.expected("`RENAME` keyword", self.parser.peek_token());
            }
        }

        let next_token = self.parser.peek_token();

        match next_token.token {
            Token::Word(w) if w.value.eq_ignore_ascii_case(TO) => {
                self.parser.next_token();
                self.parser.parse_identifier().context(error::SyntaxSnafu)
            }
            _ => self.expected("`TO` keyword after `RENAME`", next_token),
        }
    }

    /// The SQL format as follows:
    ///
    /// ```sql
    /// LABELS (key1, key2)
    /// ```
    ///
    /// ## Parameters
    ///
    /// - `is_first_keyword_matched`: indicates whether the first keyword `LABELS`
    ///     has been matched.
    fn parse_trigger_label_names(&mut self, is_first_keyword_matched: bool) -> Result<Vec<String>> {
        if !is_first_keyword_matched {
            if let Token::Word(w) = self.parser.peek_token().token
                && w.value.eq_ignore_ascii_case(LABELS)
            {
                self.parser.next_token();
            } else {
                return self.expected("`LABELS` keyword", self.parser.peek_token());
            }
        }

        if let Token::LParen = self.parser.peek_token().token {
            self.parser.next_token();
        } else {
            return self.expected("`(` after `LABELS`", self.parser.peek_token());
        }

        let label_names = self
            .parser
            .parse_comma_separated0(Parser::parse_identifier, Token::RParen)
            .context(error::SyntaxSnafu)?
            .into_iter()
            .map(|ident| ident.value.to_lowercase())
            .collect::<Vec<_>>();

        self.parser
            .expect_token(&Token::RParen)
            .context(error::SyntaxSnafu)?;

        Ok(label_names)
    }

    /// The SQL format as follows:
    ///
    /// ```sql
    /// ANNOTATIONS (key1, key2)
    /// ```
    ///
    /// ## Parameters
    ///
    /// - `is_first_keyword_matched`: indicates whether the first keyword `ANNOTATIONS`
    ///     has been matched.
    fn parse_trigger_annotation_names(
        &mut self,
        is_first_keyword_matched: bool,
    ) -> Result<Vec<String>> {
        if !is_first_keyword_matched {
            if let Token::Word(w) = self.parser.peek_token().token
                && w.value.eq_ignore_ascii_case(ANNOTATIONS)
            {
                self.parser.next_token();
            } else {
                return self.expected("`ANNOTATIONS` keyword", self.parser.peek_token());
            }
        }

        if let Token::LParen = self.parser.peek_token().token {
            self.parser.next_token();
        } else {
            return self.expected("`(` after `ANNOTATIONS`", self.parser.peek_token());
        }

        let annotation_names = self
            .parser
            .parse_comma_separated0(Parser::parse_identifier, Token::RParen)
            .context(error::SyntaxSnafu)?
            .into_iter()
            .map(|ident| ident.value.to_lowercase())
            .collect::<Vec<_>>();

        self.parser
            .expect_token(&Token::RParen)
            .context(error::SyntaxSnafu)?;

        Ok(annotation_names)
    }

    /// The SQL format as follows:
    ///
    /// ```sql
    /// NOTIFY (key1, key2)
    /// ```
    ///
    /// ## Parameters
    ///
    /// - `is_first_keyword_matched`: indicates whether the first keyword `NOTIFY`
    ///     has been matched.
    fn parse_trigger_notify_names(
        &mut self,
        is_first_keyword_matched: bool,
    ) -> Result<Vec<String>> {
        if !is_first_keyword_matched {
            if let Token::Word(w) = self.parser.peek_token().token
                && w.value.eq_ignore_ascii_case(NOTIFY)
            {
                self.parser.next_token();
            } else {
                return self.expected("`NOTIFY` keyword", self.parser.peek_token());
            }
        }

        if let Token::LParen = self.parser.peek_token().token {
            self.parser.next_token();
        } else {
            return self.expected("`(` after `NOTIFY`", self.parser.peek_token());
        }

        let notify_names = self
            .parser
            .parse_comma_separated0(Parser::parse_identifier, Token::RParen)
            .context(error::SyntaxSnafu)?
            .into_iter()
            .map(|ident| ident.value.to_lowercase())
            .collect::<Vec<_>>();

        self.parser
            .expect_token(&Token::RParen)
            .context(error::SyntaxSnafu)?;

        Ok(notify_names)
    }
}

#[cfg(test)]
mod tests {
    use crate::dialect::GreptimeDbDialect;
    use crate::parser::ParserContext;

    #[test]
    fn test_parse_rename_to() {
        let sql = r#"RENAME TO new_trigger_name"#;
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let new_trigger_name = ctx.parse_rename_to(false).unwrap();
        assert_eq!(new_trigger_name.value, "new_trigger_name");
    }

    #[test]
    fn test_parse_trigger_label_names() {
        let sql = r#"LABELS (key1, key2, key3)"#;
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let labels = ctx.parse_trigger_label_names(false).unwrap();
        let expected_labels = vec!["key1".to_string(), "key2".to_string(), "key3".to_string()];
        assert_eq!(labels, expected_labels,);
    }

    #[test]
    fn test_parse_trigger_annotation_names() {
        let sql = r#"ANNOTATIONS (key1, key2, key3)"#;
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let annotations = ctx.parse_trigger_annotation_names(false).unwrap();
        let expected_annotations = vec!["key1".to_string(), "key2".to_string(), "key3".to_string()];
        assert_eq!(annotations, expected_annotations);
    }

    #[test]
    fn test_parse_trigger_notify_names() {
        let sql = r#"NOTIFY (key1, key2, key3)"#;
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let notify_names = ctx.parse_trigger_notify_names(false).unwrap();
        let expected_notify_names =
            vec!["key1".to_string(), "key2".to_string(), "key3".to_string()];
        assert_eq!(notify_names, expected_notify_names);
    }
}
