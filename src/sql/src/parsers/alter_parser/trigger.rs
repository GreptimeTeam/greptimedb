use snafu::{ensure, ResultExt};
use sqlparser::ast::Ident;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use crate::error::{self, DuplicateClauseSnafu, InvalidSqlSnafu, Result};
use crate::parser::ParserContext;
use crate::parsers::create_parser::trigger::{ANNOTATIONS, LABELS, NOTIFY, ON};
use crate::statements::alter::trigger::{
    AlterTrigger, AlterTriggerOperation, AnnotationChange, AnnotationOperations, LabelChange,
    LabelOperations, NotifyChannelChange, NotifyChannelOperations,
};
use crate::statements::create::trigger::NotifyChannel;
use crate::statements::statement::Statement;
use crate::statements::OptionMap;

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
    ///         [alter_option [alter_option] ...]
    ///
    /// alter_option: {
    ///         RENAME TO <new_trigger_name>
    ///         | ON (<query_expression>) EVERY <interval_expression>
    ///         | [SET] LABELS (<label_name>=<label_val>, ...)
    ///         | ADD LABELS (<label_name>=<label_val>, ...)
    ///         | MODIFY LABELS (<label_name>=<label_val>, ...)
    ///         | DROP LABELS (<label_name1>, <label_name2>, ...)
    ///         | [SET] ANNOTATIONS (<annotation_name>=<annotation_val>, ...)
    ///         | ADD ANNOTATIONS (<annotation_name>=<annotation_val>, ...)
    ///         | MODIFY ANNOTATIONS (<annotation_name>=<annotation_val>, ...)
    ///         | DROP ANNOTATIONS (<annotation_name1>, <annotation_name2>, ...)
    ///         | [SET] NOTIFY(
    ///                 WEBHOOK <notify_name1> URL '<url1>' [WITH (<parameter1>=<value1>, ...)],
    ///                 WEBHOOK <notify_name2> URL '<url2>' [WITH (<parameter2>=<value2>, ...)]
    ///         )
    ///         | ADD NOTIFY(
    ///                 WEBHOOK <notify_name1> URL '<url1>' [WITH (<parameter1>=<value1>, ...)],
    ///                 WEBHOOK <notify_name2> URL '<url2>' [WITH (<parameter2>=<value2>, ...)]
    ///         )
    ///         | DROP NOTIFY (<notify_name1>, <notify_name2>)
    /// }
    /// ```
    pub(super) fn parse_alter_trigger(&mut self) -> Result<Statement> {
        let trigger_name = self.intern_parse_table_name()?;

        let mut new_trigger_name = None;
        let mut new_query = None;
        let mut new_interval = None;
        let mut label_ops = None;
        let mut annotation_ops = None;
        let mut notify_ops = None;

        loop {
            let next_token = self.parser.peek_token();
            match next_token.token {
                Token::Word(w) if w.value.eq_ignore_ascii_case(RENAME) => {
                    self.parser.next_token();
                    let name = self.parse_rename_to(true)?;
                    let name = Self::canonicalize_identifier(name);
                    ensure!(
                        new_trigger_name.is_none(),
                        DuplicateClauseSnafu {
                            clause: "RENAME TO"
                        }
                    );
                    new_trigger_name.replace(name.value);
                }
                Token::Word(w) if w.value.eq_ignore_ascii_case(ON) => {
                    self.parser.next_token();
                    let (query, interval) = self.parse_trigger_on(true)?;
                    ensure!(
                        new_query.is_none() && new_interval.is_none(),
                        DuplicateClauseSnafu { clause: ON }
                    );
                    new_query.replace(query);
                    new_interval.replace(interval);
                }
                Token::Word(w) if w.value.eq_ignore_ascii_case(LABELS) => {
                    self.parser.next_token();
                    let labels = self.parse_trigger_labels(true)?;
                    apply_label_replacement(&mut label_ops, labels)?;
                }
                Token::Word(w) if w.value.eq_ignore_ascii_case(ANNOTATIONS) => {
                    self.parser.next_token();
                    let annotations = self.parse_trigger_annotations(true)?;
                    apply_annotation_replacement(&mut annotation_ops, annotations)?;
                }
                Token::Word(w) if w.value.eq_ignore_ascii_case(NOTIFY) => {
                    self.parser.next_token();
                    let channels = self.parse_trigger_notify(true)?;
                    apply_notify_replacement(&mut notify_ops, channels)?;
                }
                Token::Word(w) if w.value.eq_ignore_ascii_case(SET) => {
                    self.parser.next_token();
                    let next_token = self.parser.peek_token();
                    match next_token.token {
                        Token::Word(w) if w.value.eq_ignore_ascii_case(LABELS) => {
                            self.parser.next_token();
                            let labels = self.parse_trigger_labels(true)?;
                            apply_label_replacement(&mut label_ops, labels)?;
                        }
                        Token::Word(w) if w.value.eq_ignore_ascii_case(ANNOTATIONS) => {
                            self.parser.next_token();
                            let annotations = self.parse_trigger_annotations(true)?;
                            apply_annotation_replacement(&mut annotation_ops, annotations)?;
                        }
                        Token::Word(w) if w.value.eq_ignore_ascii_case(NOTIFY) => {
                            self.parser.next_token();
                            let channels = self.parse_trigger_notify(true)?;
                            apply_notify_replacement(&mut notify_ops, channels)?;
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
                            let labels = self.parse_trigger_labels(true)?;
                            apply_label_change(&mut label_ops, LabelChange::Add(labels))?;
                        }
                        Token::Word(w) if w.value.eq_ignore_ascii_case(ANNOTATIONS) => {
                            self.parser.next_token();
                            let annotations = self.parse_trigger_annotations(true)?;
                            apply_annotation_change(
                                &mut annotation_ops,
                                AnnotationChange::Add(annotations),
                            )?;
                        }
                        Token::Word(w) if w.value.eq_ignore_ascii_case(NOTIFY) => {
                            self.parser.next_token();
                            let channels = self.parse_trigger_notify(true)?;
                            apply_notify_change(
                                &mut notify_ops,
                                NotifyChannelChange::Add(channels),
                            )?;
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
                            let labels = self.parse_trigger_labels(true)?;
                            apply_label_change(&mut label_ops, LabelChange::Modify(labels))?;
                        }
                        Token::Word(w) if w.value.eq_ignore_ascii_case(ANNOTATIONS) => {
                            self.parser.next_token();
                            let annotations = self.parse_trigger_annotations(true)?;
                            apply_annotation_change(
                                &mut annotation_ops,
                                AnnotationChange::Modify(annotations),
                            )?;
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
                            let names = self.parse_trigger_label_names(true)?;
                            apply_label_change(&mut label_ops, LabelChange::Drop(names))?;
                        }
                        Token::Word(w) if w.value.eq_ignore_ascii_case(ANNOTATIONS) => {
                            self.parser.next_token();
                            let names = self.parse_trigger_annotation_names(true)?;
                            apply_annotation_change(
                                &mut annotation_ops,
                                AnnotationChange::Drop(names),
                            )?;
                        }
                        Token::Word(w) if w.value.eq_ignore_ascii_case(NOTIFY) => {
                            self.parser.next_token();
                            let channels = self.parse_trigger_notify_names(true)?;
                            apply_notify_change(
                                &mut notify_ops,
                                NotifyChannelChange::Drop(channels),
                            )?;
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

        if new_trigger_name.is_none()
            && new_query.is_none()
            && new_interval.is_none()
            && label_ops.is_none()
            && annotation_ops.is_none()
            && notify_ops.is_none()
        {
            return self.expected("alter option", self.parser.peek_token());
        }

        let operation = AlterTriggerOperation {
            rename: new_trigger_name,
            new_query,
            new_interval,
            label_operations: label_ops,
            annotation_operations: annotation_ops,
            notify_channel_operations: notify_ops,
        };

        let alter_trigger = AlterTrigger {
            trigger_name,
            operation,
        };
        Ok(Statement::AlterTrigger(alter_trigger))
    }

    /// The SQL format as follows:
    ///
    /// ```sql
    /// RENAME TO <new_trigger_name>
    /// ```
    ///
    /// ## Parameters
    ///
    /// - `is_first_keyword_matched`: indicates whether the first keyword `RENAME`
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

fn apply_label_replacement(
    label_ops: &mut Option<LabelOperations>,
    labels: OptionMap,
) -> Result<()> {
    match label_ops {
        Some(LabelOperations::ReplaceAll(_)) => DuplicateClauseSnafu {
            clause: "SET LABELS",
        }
        .fail(),
        Some(LabelOperations::PartialChanges(_)) => InvalidSqlSnafu {
            msg: "SET LABELS cannot be used with ADD/MODIFY/DROP LABELS",
        }
        .fail(),
        None => {
            *label_ops = Some(LabelOperations::ReplaceAll(labels));
            Ok(())
        }
    }
}

fn apply_annotation_replacement(
    annotation_ops: &mut Option<AnnotationOperations>,
    annotations: OptionMap,
) -> Result<()> {
    match annotation_ops {
        Some(AnnotationOperations::ReplaceAll(_)) => DuplicateClauseSnafu {
            clause: "SET ANNOTATIONS",
        }
        .fail(),
        Some(AnnotationOperations::PartialChanges(_)) => InvalidSqlSnafu {
            msg: "SET ANNOTATIONS cannot be used with ADD/MODIFY/DROP ANNOTATIONS",
        }
        .fail(),
        None => {
            *annotation_ops = Some(AnnotationOperations::ReplaceAll(annotations));
            Ok(())
        }
    }
}

fn apply_notify_replacement(
    notify_channel_ops: &mut Option<NotifyChannelOperations>,
    channels: Vec<NotifyChannel>,
) -> Result<()> {
    match notify_channel_ops {
        Some(NotifyChannelOperations::ReplaceAll(_)) => DuplicateClauseSnafu {
            clause: "SET NOTIFY",
        }
        .fail(),
        Some(NotifyChannelOperations::PartialChanges(_)) => InvalidSqlSnafu {
            msg: "SET NOTIFY cannot be used with ADD/DROP NOTIFY",
        }
        .fail(),
        None => {
            *notify_channel_ops = Some(NotifyChannelOperations::ReplaceAll(channels));
            Ok(())
        }
    }
}

fn apply_label_change(
    label_ops: &mut Option<LabelOperations>,
    label_change: LabelChange,
) -> Result<()> {
    match label_ops {
        Some(LabelOperations::ReplaceAll(_)) => InvalidSqlSnafu {
            msg: "SET LABELS cannot be used with ADD/MODIFY/DROP LABELS",
        }
        .fail(),
        Some(LabelOperations::PartialChanges(label_changes)) => {
            label_changes.push(label_change);
            Ok(())
        }
        None => {
            *label_ops = Some(LabelOperations::PartialChanges(vec![label_change]));
            Ok(())
        }
    }
}

fn apply_annotation_change(
    annotation_ops: &mut Option<AnnotationOperations>,
    annotation_change: AnnotationChange,
) -> Result<()> {
    match annotation_ops {
        Some(AnnotationOperations::ReplaceAll(_)) => InvalidSqlSnafu {
            msg: "SET ANNOTATIONS cannot be used with ADD/MODIFY/DROP ANNOTATIONS",
        }
        .fail(),
        Some(AnnotationOperations::PartialChanges(label_changes)) => {
            label_changes.push(annotation_change);
            Ok(())
        }
        None => {
            *annotation_ops = Some(AnnotationOperations::PartialChanges(vec![
                annotation_change,
            ]));
            Ok(())
        }
    }
}

fn apply_notify_change(
    ops: &mut Option<NotifyChannelOperations>,
    change: NotifyChannelChange,
) -> Result<()> {
    match ops {
        Some(NotifyChannelOperations::ReplaceAll(_)) => InvalidSqlSnafu {
            msg: "SET NOTIFY cannot be used with ADD/DROP NOTIFY",
        }
        .fail(),
        Some(NotifyChannelOperations::PartialChanges(changes)) => {
            changes.push(change);
            Ok(())
        }
        None => {
            *ops = Some(NotifyChannelOperations::PartialChanges(vec![change]));
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::dialect::GreptimeDbDialect;
    use crate::parser::ParserContext;
    use crate::parsers::alter_parser::trigger::{apply_label_change, apply_label_replacement};
    use crate::statements::alter::trigger::{LabelChange, LabelOperations};
    use crate::statements::statement::Statement;
    use crate::statements::OptionMap;

    #[test]
    fn test_parse_alter_without_alter_options() {
        // Failed case: No alter options.
        // Note: "ALTER TRIGGER" is matched.
        let sql = r#"public.old_trigger"#;
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let result = ctx.parse_alter_trigger();
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_set_query() {
        // Passed case: SET QUERY.
        // Note: "ALTER TRIGGER" is matched.
        let sql = r#"public.old_trigger ON (SELECT * FROM test_table) EVERY '5 minute'::INTERVAL"#;
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let stmt = ctx.parse_alter_trigger().unwrap();
        let Statement::AlterTrigger(alter) = stmt else {
            panic!("Expected AlterTrigger statement");
        };
        assert!(alter.operation.new_query.is_some());
        assert!(alter.operation.new_interval.is_some());
        assert_eq!(alter.operation.new_interval.unwrap(), 300);
        assert!(alter.operation.rename.is_none());
        assert!(alter.operation.label_operations.is_none());
        assert!(alter.operation.annotation_operations.is_none());
        assert!(alter.operation.notify_channel_operations.is_none());

        // Failed case: multi SET QUERY.
        let sql = r#"public.old_trigger ON (SELECT * FROM test_table) EVERY '5 minute'::INTERVAL
            ON (SELECT * FROM another_table) EVERY '10 minute'::INTERVAL"#;
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let result = ctx.parse_alter_trigger();
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_alter_trigger_rename() {
        // Note: "ALTER TRIGGER" is matched.
        let sql = r#"public.old_trigger RENAME TO `newTrigger`"#;
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let stmt = ctx.parse_alter_trigger().unwrap();
        let Statement::AlterTrigger(alter) = stmt else {
            panic!("Expected AlterTrigger statement");
        };
        let trigger_name = alter.trigger_name.0;
        assert_eq!(trigger_name.len(), 2);
        assert_eq!(
            trigger_name
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join(" "),
            "public old_trigger"
        );
        assert_eq!(alter.operation.rename, Some("newTrigger".to_string()));
    }

    #[test]
    fn test_parse_alter_trigger_labels() {
        // Passed case: SET LABELS.
        // Note: "ALTER TRIGGER" is matched.
        let sql = r#"test_trigger SET LABELS (Key1='value1', 'KEY2'='value2')"#;
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let stmt = ctx.parse_alter_trigger().unwrap();
        let Statement::AlterTrigger(alter) = stmt else {
            panic!("Expected AlterTrigger statement");
        };
        let Some(LabelOperations::ReplaceAll(labels)) = alter.operation.label_operations else {
            panic!("Expected ReplaceAll label operations");
        };
        assert_eq!(labels.get("key1"), Some(&"value1".to_string()));
        assert_eq!(labels.get("KEY2"), Some(&"value2".to_string()));

        // Passed case: multiple ADD/DROP/MODIFY LABELS.
        let sql = r#"test_trigger ADD LABELS (key1='value1') MODIFY LABELS (key2='value2') DROP LABELS ('key3')"#;
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let stmt = ctx.parse_alter_trigger().unwrap();
        let Statement::AlterTrigger(alter) = stmt else {
            panic!("Expected AlterTrigger statement");
        };
        let Some(LabelOperations::PartialChanges(changes)) = alter.operation.label_operations
        else {
            panic!("Expected PartialChanges label operations");
        };
        assert_eq!(changes.len(), 3);
        let expected_changes = vec![
            LabelChange::Add(OptionMap::from([(
                "key1".to_string(),
                "value1".to_string(),
            )])),
            LabelChange::Modify(OptionMap::from([(
                "key2".to_string(),
                "value2".to_string(),
            )])),
            LabelChange::Drop(vec!["key3".to_string()]),
        ];
        assert_eq!(changes, expected_changes);

        // Failed case: Duplicate SET LABELS.
        let sql =
            r#"test_trigger SET LABELS (key1='value1', key2='value2') SET LABELS (key3='value3')"#;
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let result = ctx.parse_alter_trigger();
        assert!(result.is_err());

        // Failed case: SET LABELS with ADD/MODIFY/DROP LABELS.
        let sql =
            r#"test_trigger SET LABELS (key1='value1', key2='value2') ADD LABELS (key3='value3')"#;
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let result = ctx.parse_alter_trigger();
        assert!(result.is_err());
        let sql = r#"test_trigger SET LABELS (key1='value1', key2='value2') DROP LABELS ('key3')"#;
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let result = ctx.parse_alter_trigger();
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_rename_to() {
        let sql = r#"RENAME TO new_trigger_name"#;
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let new_trigger_name = ctx.parse_rename_to(false).unwrap();
        assert_eq!(new_trigger_name.value, "new_trigger_name");
        assert_eq!(new_trigger_name.quote_style, None);

        let sql = r#"RENAME TO "New_Trigger_Name""#;
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let new_trigger_name = ctx.parse_rename_to(false).unwrap();
        assert_eq!(new_trigger_name.value, "New_Trigger_Name");
        assert_eq!(new_trigger_name.quote_style, Some('"'));

        // Failed case: Missing new_trigger_name.
        let sql = r#"RENAME TO"#;
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let result = ctx.parse_rename_to(false);
        assert!(result.is_err());

        // Failed case: Missing `TO` keyword.
        let sql = r#"RENAME"#;
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let result = ctx.parse_rename_to(false);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_trigger_label_names() {
        let sql = r#"LABELS (key1, KEY2, key3)"#;
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let labels = ctx.parse_trigger_label_names(false).unwrap();
        let expected_labels = vec!["key1".to_string(), "key2".to_string(), "key3".to_string()];
        assert_eq!(labels, expected_labels,);

        let sql = r#"LABELS ('key1', `key2`, "key3",)"#;
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let labels = ctx.parse_trigger_label_names(false).unwrap();
        let expected_labels = vec!["key1".to_string(), "key2".to_string(), "key3".to_string()];
        assert_eq!(labels, expected_labels,);

        let sql = r#"LABELS ()"#;
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let labels = ctx.parse_trigger_label_names(false).unwrap();
        assert!(labels.is_empty());
    }

    #[test]
    fn test_parse_trigger_annotation_names() {
        let sql = r#"ANNOTATIONS (key1, KEY2, key3)"#;
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let annotations = ctx.parse_trigger_annotation_names(false).unwrap();
        let expected_annotations = vec!["key1".to_string(), "key2".to_string(), "key3".to_string()];
        assert_eq!(annotations, expected_annotations);

        let sql = r#"ANNOTATIONS (key1, `Key2`, "key3")"#;
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let annotations = ctx.parse_trigger_annotation_names(false).unwrap();
        let expected_annotations = vec!["key1".to_string(), "key2".to_string(), "key3".to_string()];
        assert_eq!(annotations, expected_annotations);

        let sql = r#"ANNOTATIONS ()"#;
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let annotations = ctx.parse_trigger_annotation_names(false).unwrap();
        assert!(annotations.is_empty());
    }

    #[test]
    fn test_parse_trigger_notify_names() {
        let sql = r#"NOTIFY (key1, KEY2, key3)"#;
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let notify_names = ctx.parse_trigger_notify_names(false).unwrap();
        let expected_notify_names =
            vec!["key1".to_string(), "key2".to_string(), "key3".to_string()];
        assert_eq!(notify_names, expected_notify_names);

        let sql = r#"NOTIFY (key1, key2,)"#;
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let notify_names = ctx.parse_trigger_notify_names(false).unwrap();
        let expected_notify_names = vec!["key1".to_string(), "key2".to_string()];
        assert_eq!(notify_names, expected_notify_names);
    }

    #[test]
    fn test_apply_label_changes() {
        let mut label_ops = None;

        let mut labels = OptionMap::default();
        labels.insert("key1".to_string(), "value1".to_string());
        labels.insert("key2".to_string(), "value2".to_string());

        apply_label_replacement(&mut label_ops, labels.clone()).unwrap();
        assert!(label_ops.is_some());

        // Set operations are mutually exclusive.
        let result = apply_label_replacement(&mut label_ops, labels.clone());
        assert!(result.is_err());

        // Set operations and Change operations are mutually exclusive.
        let result =
            apply_label_change(&mut label_ops, LabelChange::Drop(vec!["key1".to_string()]));
        assert!(result.is_err());

        let mut label_ops = None;

        let result = apply_label_change(&mut label_ops, LabelChange::Add(labels.clone()));
        assert!(result.is_ok());

        // Partial changes are not mutually exclusive.
        let result = apply_label_change(&mut label_ops, LabelChange::Modify(labels));
        assert!(result.is_ok());
        let result =
            apply_label_change(&mut label_ops, LabelChange::Drop(vec!["key1".to_string()]));
        assert!(result.is_ok());

        let ops = label_ops.unwrap();
        if let LabelOperations::PartialChanges(changes) = ops {
            assert_eq!(changes.len(), 3);
            assert!(matches!(changes[0], LabelChange::Add(_)));
            assert!(matches!(changes[1], LabelChange::Modify(_)));
            assert!(matches!(changes[2], LabelChange::Drop(_)));
        } else {
            panic!("Expected PartialChanges, got {:?}", ops);
        }
    }
}
