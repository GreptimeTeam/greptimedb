use std::collections::HashMap;

use snafu::{ensure, OptionExt, ResultExt};
use sqlparser::ast::Query;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use crate::error;
use crate::error::Result;
use crate::parser::ParserContext;
use crate::statements::create::trigger::{
    AlertManagerWebhook, ChannelType, CreateTrigger, NotifyChannel,
};
use crate::statements::statement::Statement;
use crate::statements::OptionMap;
use crate::util::parse_option_string;

/// Some keywords about trigger.
pub const ON: &str = "ON";
pub const EVERY: &str = "EVERY";
pub const LABELS: &str = "LABELS";
pub const ANNOTATIONS: &str = "ANNOTATIONS";
pub const NOTIFY: &str = "NOTIFY";
pub const WEBHOOK: &str = "WEBHOOK";
pub const URL: &str = "URL";

const TIMEOUT: &str = "timeout";

fn validate_webhook_option(key: &str) -> bool {
    [TIMEOUT].contains(&key)
}

impl<'a> ParserContext<'a> {
    /// The SQL format as follows:
    ///
    /// ```sql
    /// -- CREATE TRIGGER
    /// [IF NOT EXISTS] <trigger_name>
    ///    ON (<query_expression>)
    ///            EVERY <interval_expression>
    ///    [LABELS (<label_name>=<label_val>, ...)]
    ///    [ANNOTATIONS (<annotation_name>=<annotation_val>, ...)]
    ///    NOTIFY(
    ///            WEBHOOK <notify_name1> URL '<url1>' [WITH (<parameter1>=<value1>, ...)],
    ///            WEBHOOK <notify_name2> URL '<url2>' [WITH (<parameter2>=<value2>, ...)]
    ///     )
    /// ```
    ///
    /// Note: This method does not parse the `CREATE TRIGGER` keywords.
    pub(super) fn parse_create_trigger(&mut self) -> Result<Statement> {
        let if_not_exists = self.parse_if_not_exist()?;
        let trigger_name = self.intern_parse_table_name()?;

        let mut may_query = None;
        let mut may_interval = None;
        let mut may_labels = None;
        let mut may_annotations = None;
        let mut notify_channels = vec![];

        loop {
            let next_token = self.parser.peek_token();
            match next_token.token {
                Token::Word(w) if w.value.eq_ignore_ascii_case(ON) => {
                    self.parser.next_token();
                    let (query, interval) = self.parse_trigger_on(true)?;
                    may_query.replace(query);
                    may_interval.replace(interval);
                }
                Token::Word(w) if w.value.eq_ignore_ascii_case(LABELS) => {
                    self.parser.next_token();
                    let labels = self.parse_trigger_labels(true)?;
                    may_labels.replace(labels);
                }
                Token::Word(w) if w.value.eq_ignore_ascii_case(ANNOTATIONS) => {
                    self.parser.next_token();
                    let annotations = self.parse_trigger_annotations(true)?;
                    may_annotations.replace(annotations);
                }
                Token::Word(w) if w.value.eq_ignore_ascii_case(NOTIFY) => {
                    self.parser.next_token();
                    let channels = self.parse_trigger_notify(true)?;
                    notify_channels.extend(channels);
                }
                Token::EOF => {
                    break;
                }
                _ => {
                    return self.expected(
                        "`ON` or `LABELS` or `ANNOTATIONS` or `NOTIFY` keyword",
                        next_token,
                    );
                }
            }
        }

        let query = may_query.context(error::MissingClauseSnafu { name: ON })?;
        let interval = may_interval.context(error::MissingClauseSnafu { name: ON })?;
        let labels = may_labels.unwrap_or_default();
        let annotations = may_annotations.unwrap_or_default();

        ensure!(
            !notify_channels.is_empty(),
            error::MissingClauseSnafu { name: NOTIFY }
        );

        let create_trigger = CreateTrigger {
            trigger_name,
            if_not_exists,
            query,
            interval,
            labels,
            annotations,
            channels: notify_channels,
        };
        let create_trigger = Statement::CreateTrigger(create_trigger);
        Ok(create_trigger)
    }

    /// The SQL format as follows:
    ///
    /// ```sql
    /// ON (<query_expression>) EVERY <interval_expression>
    /// ```
    ///
    /// ## Parameters
    ///
    /// - `is_first_keyword_matched`: indicates whether the first keyword `ON`
    ///     has been matched.
    fn parse_trigger_on(&mut self, is_first_keyword_matched: bool) -> Result<(Box<Query>, u64)> {
        if !is_first_keyword_matched {
            if let Token::Word(w) = self.parser.peek_token().token
                && w.value.eq_ignore_ascii_case(ON)
            {
                self.parser.next_token();
            } else {
                return self.expected("`ON` keyword", self.parser.peek_token());
            }
        }

        let query = self.parser.parse_query().context(error::SyntaxSnafu)?;

        if let Token::Word(w) = self.parser.peek_token().token
            && w.value.eq_ignore_ascii_case(EVERY)
        {
            self.parser.next_token();
        } else {
            return self.expected("`EVERY` keyword", self.parser.peek_token());
        }

        let interval = self
            .parse_interval()?
            .try_into()
            .context(error::NegativeIntervalSnafu)?;

        Ok((query, interval))
    }

    /// The SQL format as follows:
    ///
    /// ```sql
    /// LABELS (key1 = 'value1', key2 = value2)
    /// ```
    ///
    /// ## Parameters
    ///
    /// - `is_first_keyword_matched`: indicates whether the first keyword `LABELS`
    ///     has been matched.
    fn parse_trigger_labels(&mut self, is_first_keyword_matched: bool) -> Result<OptionMap> {
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
        let options = self
            .parser
            .parse_comma_separated0(Parser::parse_sql_option, Token::RParen)
            .context(error::SyntaxSnafu)?
            .into_iter()
            .map(parse_option_string)
            .collect::<Result<HashMap<String, String>>>()?;
        self.parser
            .expect_token(&Token::RParen)
            .context(error::SyntaxSnafu)?;
        Ok(options.into())
    }

    /// The SQL format as follows:
    ///
    /// ```sql
    /// ANNOTATIONS (key1 = 'value1', key2 = value2)
    /// ```
    ///
    /// ## Parameters
    ///
    /// - `is_first_keyword_matched`: indicates whether the first keyword
    ///     `ANNOTATIONS` has been matched.
    fn parse_trigger_annotations(&mut self, is_first_keyword_matched: bool) -> Result<OptionMap> {
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
            return self.expected("`(` after `LABELS`", self.parser.peek_token());
        }
        let options = self
            .parser
            .parse_comma_separated0(Parser::parse_sql_option, Token::RParen)
            .context(error::SyntaxSnafu)?
            .into_iter()
            .map(parse_option_string)
            .collect::<Result<HashMap<String, String>>>()?;
        self.parser
            .expect_token(&Token::RParen)
            .context(error::SyntaxSnafu)?;
        Ok(options.into())
    }

    /// The SQL format as follows:
    ///
    /// ```sql
    //  NOTIFY(
    //          WEBHOOK <notify_name1> URL '<url1>' [WITH (<parameter1>=<value1>, ...)],
    //          WEBHOOK <notify_name2> URL '<url2>' [WITH (<parameter2>=<value2>, ...)]
    //        )
    /// ```
    ///
    /// ## Parameters
    ///
    /// - `is_first_keyword_matched`: indicates whether the first keyword `NOTIFY`
    ///     has been matched.
    fn parse_trigger_notify(
        &mut self,
        is_first_keyword_matched: bool,
    ) -> Result<Vec<NotifyChannel>> {
        if !is_first_keyword_matched {
            if let Token::Word(w) = self.parser.peek_token().token
                && w.value.eq_ignore_ascii_case(NOTIFY)
            {
                self.parser.next_token();
            } else {
                return self.expected("`NOTIFY` keyword", self.parser.peek_token());
            }
        }

        self.parser
            .expect_token(&Token::LParen)
            .context(error::SyntaxSnafu)?;

        let mut notify_channels = vec![];

        loop {
            let next_token = self.parser.peek_token();
            match next_token.token {
                Token::Word(w) if w.value.eq_ignore_ascii_case(WEBHOOK) => {
                    self.parser.next_token();
                    let notify_channel = self.parse_trigger_notify_webhook(true)?;
                    notify_channels.push(notify_channel);
                }
                Token::RParen => {
                    self.parser.next_token();
                    break;
                }
                _ => {
                    return self.expected("`WEBHOOK` keyword", next_token);
                }
            }

            let next_token = self.parser.peek_token();
            if next_token.token == Token::RParen {
                self.parser.next_token();
                break;
            } else if next_token.token == Token::Comma {
                self.parser.next_token();
                if self.parser.peek_token().token == Token::RParen {
                    self.parser.next_token();
                    break;
                }
            } else {
                return self.expected("`,` or `)`", next_token);
            }
        }

        ensure!(
            !notify_channels.is_empty(),
            error::MissingNotifyChannelSnafu
        );

        Ok(notify_channels)
    }

    /// The SQL format as follows:
    ///
    /// ```sql
    /// WEBHOOK <notify_name> URL '<url>' [WITH (<parameter>=<value>, ...)]
    /// ```
    ///
    /// ## Parameters
    ///
    /// - `is_first_keyword_matched`: indicates whether the first keyword `WEBHOOK`
    ///     has been matched.
    fn parse_trigger_notify_webhook(
        &mut self,
        is_first_keyword_matched: bool,
    ) -> Result<NotifyChannel> {
        if !is_first_keyword_matched {
            if let Token::Word(w) = self.parser.peek_token().token
                && w.value.eq_ignore_ascii_case(WEBHOOK)
            {
                self.parser.next_token();
            } else {
                return self.expected("`WEBHOOK` keyword", self.parser.peek_token());
            }
        }

        let notify_ident = self.parser.parse_identifier().context(error::SyntaxSnafu)?;

        if let Token::Word(w) = self.parser.peek_token().token
            && w.value.eq_ignore_ascii_case(URL)
        {
            self.parser.next_token();
        } else {
            return self.expected("`URL` keyword", self.parser.peek_token());
        }

        let url = self.parser.parse_identifier().context(error::SyntaxSnafu)?;

        let options = self
            .parser
            .parse_options(Keyword::WITH)
            .context(error::SyntaxSnafu)?
            .into_iter()
            .map(parse_option_string)
            .collect::<Result<HashMap<String, String>>>()?;

        for key in options.keys() {
            ensure!(
                validate_webhook_option(key),
                error::InvalidTriggerWebhookOptionSnafu {
                    key: key.to_string()
                }
            );
        }

        let webhook = AlertManagerWebhook {
            url,
            options: options.into(),
        };

        Ok(NotifyChannel {
            name: notify_ident,
            channel_type: ChannelType::Webhook(webhook),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialect::GreptimeDbDialect;
    use crate::statements::create::trigger::ChannelType;

    #[test]
    fn test_parse_create_trigger() {
        // Normal, with all clauses.
        let sql = create_trigger_sql_1();
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let statement = ctx.parse_create_trigger().unwrap();
        let Statement::CreateTrigger(create_trigger) = statement else {
            panic!("Expected CreateTrigger statement");
        };
        verify_create_trigger(&create_trigger);

        // Valid, but shuffle the order of clauses.
        let sql = create_trigger_sql_2();
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let statement = ctx.parse_create_trigger().unwrap();
        let Statement::CreateTrigger(create_trigger) = statement else {
            panic!("Expected CreateTrigger statement");
        };
        verify_create_trigger(&create_trigger);

        // Invalid, since missing notify channels.
        let sql = create_trigger_sql_3();
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let result = ctx.parse_create_trigger();
        assert!(result.is_err());
    }

    fn create_trigger_sql_1() -> &'static str {
        r#"
IF NOT EXISTS cpu_monitor
        ON (SELECT host AS host_label, cpu, memory FROM machine_monitor WHERE cpu > 1)
                EVERY '5 minute'::INTERVAL
        LABELS (label_name=label_val)
        ANNOTATIONS (annotation_name=annotation_val)
        NOTIFY(
                WEBHOOK alert_manager_1 URL 'http://127.0.0.1:9093' WITH (timeout='1m'),
                WEBHOOK alert_manager_2 URL 'http://127.0.0.1:9094' WITH (timeout='2m')
            )"#
    }

    fn create_trigger_sql_2() -> &'static str {
        r#"
IF NOT EXISTS cpu_monitor
        NOTIFY(
                WEBHOOK alert_manager_1 URL 'http://127.0.0.1:9093' WITH (timeout='1m'),
                WEBHOOK alert_manager_2 URL 'http://127.0.0.1:9094' WITH (timeout='2m')
            )
        LABELS (label_name=label_val)
        ANNOTATIONS (annotation_name=annotation_val)
        ON (SELECT host AS host_label, cpu, memory FROM machine_monitor WHERE cpu > 1)
                EVERY '5 minute'::INTERVAL
        "#
    }

    fn create_trigger_sql_3() -> &'static str {
        r#"
IF NOT EXISTS cpu_monitor
        NOTIFY(
            )
        ON (SELECT host AS host_label, cpu, memory FROM machine_monitor WHERE cpu > 1)
                EVERY '5 minute'::INTERVAL
        "#
    }

    fn verify_create_trigger(create_trigger: &CreateTrigger) {
        assert!(create_trigger.if_not_exists);
        assert_eq!(create_trigger.trigger_name.to_string(), "cpu_monitor");
        assert_eq!(
            create_trigger.query.to_string(),
            "(SELECT host AS host_label, cpu, memory FROM machine_monitor WHERE cpu > 1)"
        );
        assert_eq!(create_trigger.interval, 300);
        assert_eq!(create_trigger.labels.len(), 1);
        assert_eq!(
            create_trigger.labels.get("label_name").unwrap(),
            "label_val"
        );
        assert_eq!(create_trigger.annotations.len(), 1);
        assert_eq!(
            create_trigger.annotations.get("annotation_name").unwrap(),
            "annotation_val"
        );
        assert_eq!(create_trigger.channels.len(), 2);

        let channel1 = &create_trigger.channels[0];
        let ChannelType::Webhook(webhook1) = &channel1.channel_type;
        assert_eq!(webhook1.url.to_string(), "'http://127.0.0.1:9093'");
        assert_eq!(webhook1.options.len(), 1);
        assert_eq!(webhook1.options.get("timeout").unwrap(), "1m");

        let channel2 = &create_trigger.channels[1];
        assert_eq!(channel2.name.to_string(), "alert_manager_2");
        let ChannelType::Webhook(webhook2) = &channel2.channel_type;
        assert_eq!(webhook2.url.to_string(), "'http://127.0.0.1:9094'");
        assert_eq!(webhook2.options.len(), 1);
        assert_eq!(webhook2.options.get("timeout").unwrap(), "2m");
    }

    #[test]
    fn test_parse_trigger_on() {
        // Normal.
        let sql = "ON (SELECT * FROM cpu_usage) EVERY '5 minute'::INTERVAL";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let (query, interval) = ctx.parse_trigger_on(false).unwrap();
        assert_eq!(query.to_string(), "(SELECT * FROM cpu_usage)");
        assert_eq!(interval, 300);

        // Invalid, since missing `ON` keyword.
        let sql = "SELECT * FROM cpu_usage EVERY '5 minute'::INTERVAL";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        assert!(ctx.parse_trigger_on(false).is_err());

        // Invalid, since missing `EVERY` keyword.
        let sql = "ON (SELECT * FROM cpu_usage) '5 minute'::INTERVAL";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        assert!(ctx.parse_trigger_on(false).is_err());

        // Invalid, since invalid query expression.
        let sql = "ON (SELECT * cpu_usage) EVERY '5 minute'::INTERVAL";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        assert!(ctx.parse_trigger_on(false).is_err());
    }

    #[test]
    fn test_parse_trigger_labels() {
        // Normal.
        let sql = labels_sql_clause();
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let labels = ctx.parse_trigger_labels(false).unwrap();
        verify_labels(&labels);

        // Empty labels clause. It's valid, since empty labels is allowed.
        let sql = "LABELS ()";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let labels = ctx.parse_trigger_labels(false).unwrap();
        assert!(labels.is_empty());

        // Invalid, since missing ')'
        let sql = "LABELS (";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        assert!(ctx.parse_trigger_labels(false).is_err());

        // Invalid SQL clause, since the value of `name2` is not specified.
        let sql = "LABELS (name1 = val1, name2)";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        assert!(ctx.parse_trigger_labels(false).is_err());
    }

    fn labels_sql_clause() -> &'static str {
        r#"LABELS (name1 = val1, name2 = 'val2')"#
    }

    fn verify_labels(labels: &OptionMap) {
        assert_eq!(labels.len(), 2);
        assert_eq!(labels.get("name1").unwrap(), "val1");
        assert_eq!(labels.get("name2").unwrap(), "val2");
    }

    #[test]
    fn test_parse_trigger_annotations() {
        // Normal.
        let sql = annotations_sql_clause();
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let annotations = ctx.parse_trigger_annotations(false).unwrap();
        verify_annotations(&annotations);

        // Empty annotations clause. It's valid, since empty annotations is allowed.
        let sql = "ANNOTATIONS ()";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let annotations = ctx.parse_trigger_annotations(false).unwrap();
        assert!(annotations.is_empty());

        // Invalid, since missing ')'
        let sql = "ANNOTATIONS (";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        assert!(ctx.parse_trigger_annotations(false).is_err());

        // Invalid, since the value of `name2` is not specified.
        let sql = "ANNOTATIONS (name1 = val1, name2)";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        assert!(ctx.parse_trigger_annotations(false).is_err());
    }

    fn annotations_sql_clause() -> &'static str {
        r#"ANNOTATIONS (name1 = val1, name2 = 'val2')"#
    }

    fn verify_annotations(annotations: &OptionMap) {
        assert_eq!(annotations.len(), 2);
        assert_eq!(annotations.get("name1").unwrap(), "val1");
        assert_eq!(annotations.get("name2").unwrap(), "val2");
    }

    #[test]
    fn test_parse_trigger_notify() {
        let sql = notify_sql_clause_1();
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let notify_channels = ctx.parse_trigger_notify(false).unwrap();
        verify_notify_channels(&notify_channels);

        let sql = notify_sql_clause_2();
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let notify_channels = ctx.parse_trigger_notify(false).unwrap();
        verify_notify_channels(&notify_channels);

        // Invalid, since must specify at least one notify channel.
        let sql = "NOTIFY()";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let result = ctx.parse_trigger_notify(false);
        assert!(result.is_err());
    }

    fn notify_sql_clause_1() -> &'static str {
        r#"NOTIFY(
            WEBHOOK notify_name1 URL 'http://example.com/1' WITH (timeout = '1m'),
            WEBHOOK notify_name2 URL 'http://example.com/2' WITH (timeout = '2m')
        )"#
    }

    fn notify_sql_clause_2() -> &'static str {
        r#"NOTIFY(
            WEBHOOK 'notify_name1' URL 'http://example.com/1' WITH (timeout = '1m'),
            WEBHOOK 'notify_name2' URL 'http://example.com/2' WITH (timeout = '2m'),
        )"#
    }

    fn verify_notify_channels(channels: &[NotifyChannel]) {
        assert_eq!(channels.len(), 2);
        let channel1 = &channels[0];
        assert_eq!(channel1.name.value, "notify_name1");
        let ChannelType::Webhook(webhook1) = &channel1.channel_type;
        assert_eq!(webhook1.url.value, "http://example.com/1");
        assert_eq!(webhook1.options.get("timeout").unwrap(), "1m");
        let channel2 = &channels[1];
        assert_eq!(channel2.name.value, "notify_name2");
        let ChannelType::Webhook(webhook2) = &channel2.channel_type;
        assert_eq!(webhook2.url.value, "http://example.com/2");
        assert_eq!(webhook2.options.get("timeout").unwrap(), "2m");
    }

    #[test]
    fn test_parse_trigger_notify_webhook() {
        // Normal.
        let sql = webhook_sql_clause();
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let channel = ctx.parse_trigger_notify_webhook(false).unwrap();
        verify_notify_webhook(channel);

        // Valid, but without `WITH` clause.
        let sql = "WEBHOOK am URL 'http://example.com'";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let channel = ctx.parse_trigger_notify_webhook(false).unwrap();
        assert_eq!(channel.name.value, "am");
        let ChannelType::Webhook(webhook) = channel.channel_type;
        assert_eq!(webhook.url.value, "http://example.com");
        assert!(webhook.options.is_empty());

        // Valid, but with empty options.
        let sql = "WEBHOOK am URL 'http://example.com' WITH ()";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let channel = ctx.parse_trigger_notify_webhook(false).unwrap();
        assert_eq!(channel.name.value, "am");
        let ChannelType::Webhook(webhook) = channel.channel_type;
        assert_eq!(webhook.url.value, "http://example.com");
        assert!(webhook.options.is_empty());

        // Invalid, since missing URL.
        let sql = "WEBHOOK am";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        assert!(ctx.parse_trigger_notify_webhook(false).is_err());

        // Invalid, since the value of `timeout` is not specified.
        let sql = "WEBHOOK am URL 'http://example.com' WITH (timeout)";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        assert!(ctx.parse_trigger_notify_webhook(false).is_err());

        // Invalid, missing ')' keyword.
        let sql = "WEBHOOK am URL 'http://example.com' WITH (";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        assert!(ctx.parse_trigger_notify_webhook(false).is_err());
    }

    fn webhook_sql_clause() -> &'static str {
        "WEBHOOK am URL 'http://example.com' WITH (timeout = '1m')"
    }

    fn verify_notify_webhook(channel: NotifyChannel) {
        let ChannelType::Webhook(webhook) = channel.channel_type;
        assert_eq!(channel.name.value, "am");
        assert_eq!(webhook.url.value, "http://example.com");
        let options = webhook.options;
        assert_eq!(options.len(), 1);
        assert_eq!(options.get("timeout").unwrap(), "1m");
    }

    #[test]
    fn test_validate_notify_option() {
        assert!(validate_webhook_option(TIMEOUT));
        assert!(!validate_webhook_option("invalid_option"));
    }
}
