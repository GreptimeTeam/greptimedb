use std::time::Duration;

use snafu::{OptionExt, ResultExt, ensure};
use sqlparser::ast::Query;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use crate::error;
use crate::error::Result;
use crate::parser::ParserContext;
use crate::parsers::tql_parser;
use crate::parsers::utils::convert_month_day_nano_to_duration;
use crate::statements::OptionMap;
use crate::statements::create::SqlOrTql;
use crate::statements::create::trigger::{
    AlertManagerWebhook, ChannelType, CreateTrigger, DurationExpr, NotifyChannel, TriggerOn,
};
use crate::statements::statement::Statement;
use crate::util::{location_to_index, parse_option_string};

/// Some keywords about trigger.
pub const ON: &str = "ON";
pub const EVERY: &str = "EVERY";
pub const LABELS: &str = "LABELS";
pub const ANNOTATIONS: &str = "ANNOTATIONS";
pub const NOTIFY: &str = "NOTIFY";
pub const WEBHOOK: &str = "WEBHOOK";
pub const URL: &str = "URL";
pub const FOR: &str = "FOR";
pub const KEEP: &str = "KEEP";
pub const FIRING: &str = "FIRING";

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
    ///     ON (<query_expression>)
    ///         EVERY <interval_expression>
    ///     [FOR <interval_expression>]
    ///     [KEEP FIRING FOR <interval_expression>]
    ///     [LABELS (<label_name>=<label_val>, ...)]
    ///     [ANNOTATIONS (<annotation_name>=<annotation_val>, ...)]
    ///     NOTIFY(
    ///         WEBHOOK <notify_name1> URL '<url1>' [WITH (<parameter1>=<value1>, ...)],
    ///         WEBHOOK <notify_name2> URL '<url2>' [WITH (<parameter2>=<value2>, ...)]
    ///     )
    /// ```
    ///
    /// Note: This method does not parse the `CREATE TRIGGER` keywords.
    pub(super) fn parse_create_trigger(&mut self) -> Result<Statement> {
        let if_not_exists = self.parse_if_not_exist()?;
        let trigger_name = self.intern_parse_table_name()?;

        let mut may_trigger_on = None;
        let mut may_labels = None;
        let mut may_annotations = None;
        let mut notify_channels = vec![];
        let mut r#for = None;
        let mut keep_firing_for = None;

        loop {
            let next_token = self.parser.peek_token();
            match next_token.token {
                Token::Word(w) if w.value.eq_ignore_ascii_case(ON) => {
                    self.parser.next_token();
                    let trigger_on = self.parse_trigger_on(true)?;
                    may_trigger_on.replace(trigger_on);
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
                Token::Word(w) if w.value.eq_ignore_ascii_case(FOR) => {
                    self.parser.next_token();
                    r#for.replace(self.parse_trigger_for(true)?);
                }
                Token::Word(w) if w.value.eq_ignore_ascii_case(KEEP) => {
                    self.parser.next_token();
                    keep_firing_for.replace(self.parse_trigger_keep_firing_for(true)?);
                }
                Token::EOF => break,
                _ => {
                    return self.expected(
                        "`ON` or `LABELS` or `ANNOTATIONS` or `NOTIFY` keyword or `FOR` or `KEEP FIRING FOR`",
                        next_token,
                    );
                }
            }
        }

        let trigger_on = may_trigger_on.context(error::MissingClauseSnafu { name: ON })?;
        let labels = may_labels.unwrap_or_default();
        let annotations = may_annotations.unwrap_or_default();

        ensure!(
            !notify_channels.is_empty(),
            error::MissingClauseSnafu { name: NOTIFY }
        );

        let create_trigger = CreateTrigger {
            trigger_name,
            if_not_exists,
            trigger_on,
            r#for,
            keep_firing_for,
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
    ///
    /// ## Notes
    ///
    /// - The months in the interval expression is prohibited.
    /// - The interval must be at least 1 second. If the parsed interval is less
    ///     than 1 second, **it will be adjusted to 1 second**.
    pub(crate) fn parse_trigger_on(&mut self, is_first_keyword_matched: bool) -> Result<TriggerOn> {
        if !is_first_keyword_matched {
            if let Token::Word(w) = self.parser.peek_token().token
                && w.value.eq_ignore_ascii_case(ON)
            {
                self.parser.next_token();
            } else {
                return self.expected("`ON` keyword", self.parser.peek_token());
            }
        }

        if let Token::LParen = self.parser.peek_token().token {
            self.parser.next_token();
        } else {
            return self.expected("`(` after `ON`", self.parser.peek_token());
        }

        let query = self.parse_parenthesized_sql_or_tql(true)?;

        if let Token::Word(w) = self.parser.peek_token().token
            && w.value.eq_ignore_ascii_case(EVERY)
        {
            self.parser.next_token();
        } else {
            return self.expected("`EVERY` keyword", self.parser.peek_token());
        }

        let (month_day_nano, raw_expr) = self.parse_interval_month_day_nano()?;

        // Trigger Interval (month_day_nano): the months field is prohibited,
        // as the length of a month is ambiguous.
        ensure!(
            month_day_nano.months == 0,
            error::InvalidIntervalSnafu {
                reason: "year and month is not supported in trigger interval".to_string()
            }
        );

        let interval = convert_month_day_nano_to_duration(month_day_nano)?;

        // Ensure the interval is at least 1 second.
        let interval = if interval < Duration::from_secs(1) {
            Duration::from_secs(1)
        } else {
            interval
        };

        let query_interval = DurationExpr {
            duration: interval,
            raw_expr,
        };

        Ok(TriggerOn {
            query,
            query_interval,
        })
    }

    fn parse_parenthesized_sql_or_tql(&mut self, is_lparen_consumed: bool) -> Result<SqlOrTql> {
        if !is_lparen_consumed {
            self.parser
                .expect_token(&Token::LParen)
                .context(error::SyntaxSnafu)?;
        }

        if let Token::Word(w) = self.parser.peek_token().token
            && w.keyword == Keyword::NoKeyword
            && w.quote_style.is_none()
            && w.value.to_uppercase() == tql_parser::TQL
        {
            let (tql, raw_query) = self.parse_parenthesized_tql(true, true, true)?;
            Ok(SqlOrTql::Tql(tql, raw_query))
        } else {
            let (query, raw_query) = self.parse_parenthesized_sql(true)?;
            Ok(SqlOrTql::Sql(query, raw_query))
        }
    }

    fn parse_parenthesized_sql(&mut self, is_lparen_consumed: bool) -> Result<(Query, String)> {
        if !is_lparen_consumed {
            self.parser
                .expect_token(&Token::LParen)
                .context(error::SyntaxSnafu)?;
        }

        let sql_len = self.sql.len();
        let start_loc = self.parser.peek_token().span.start;
        let start_index = location_to_index(self.sql, &start_loc);

        ensure!(
            start_index <= sql_len,
            error::InvalidSqlSnafu {
                msg: format!("Invalid location (index {} > len {})", start_index, sql_len),
            }
        );

        let sql_query = self.parser.parse_query().context(error::SyntaxSnafu)?;

        let end_token = self.parser.peek_token();
        self.parser
            .expect_token(&Token::RParen)
            .context(error::SyntaxSnafu)?;

        let end_index = location_to_index(self.sql, &end_token.span.start);
        ensure!(
            end_index <= sql_len,
            error::InvalidSqlSnafu {
                msg: format!("Invalid location (index {} > len {})", end_index, sql_len),
            }
        );
        let raw_sql = &self.sql[start_index..end_index];

        Ok((*sql_query, raw_sql.trim().to_string()))
    }

    pub(crate) fn parse_trigger_for(
        &mut self,
        is_first_keyword_matched: bool,
    ) -> Result<DurationExpr> {
        if !is_first_keyword_matched {
            if let Token::Word(w) = self.parser.peek_token().token
                && w.value.eq_ignore_ascii_case(FOR)
            {
                self.parser.next_token();
            } else {
                return self.expected("`FOR` keyword", self.parser.peek_token());
            }
        }

        let (month_day_nano, raw_expr) = self.parse_interval_month_day_nano()?;

        // Trigger Interval (month_day_nano): the months field is prohibited,
        // as the length of a month is ambiguous.
        ensure!(
            month_day_nano.months == 0,
            error::InvalidIntervalSnafu {
                reason: "year and month is not supported in trigger FOR duration".to_string()
            }
        );

        let duration = convert_month_day_nano_to_duration(month_day_nano)?;

        let duration = if duration < Duration::from_secs(1) {
            Duration::from_secs(1)
        } else {
            duration
        };

        Ok(DurationExpr { duration, raw_expr })
    }

    pub(crate) fn parse_trigger_keep_firing_for(
        &mut self,
        is_first_keyword_matched: bool,
    ) -> Result<DurationExpr> {
        if !is_first_keyword_matched {
            if let Token::Word(w) = self.parser.peek_token().token
                && w.value.eq_ignore_ascii_case(KEEP)
            {
                self.parser.next_token();
            } else {
                return self.expected("`KEEP` keyword", self.parser.peek_token());
            }
        }

        if let Token::Word(w) = self.parser.peek_token().token
            && w.value.eq_ignore_ascii_case(FIRING)
        {
            self.parser.next_token();
        } else {
            return self.expected("`FIRING` keyword", self.parser.peek_token());
        }

        if let Token::Word(w) = self.parser.peek_token().token
            && w.value.eq_ignore_ascii_case(FOR)
        {
            self.parser.next_token();
        } else {
            return self.expected(
                "`FOR` keyword after `KEEP FIRING`",
                self.parser.peek_token(),
            );
        }

        let (month_day_nano, raw_expr) = self.parse_interval_month_day_nano()?;

        // Trigger Interval (month_day_nano): the months field is prohibited,
        // as the length of a month is ambiguous.
        ensure!(
            month_day_nano.months == 0,
            error::InvalidIntervalSnafu {
                reason: "year and month is not supported in trigger KEEP FIRING FOR duration"
                    .to_string()
            }
        );

        let duration = convert_month_day_nano_to_duration(month_day_nano)?;

        let duration = if duration < Duration::from_secs(1) {
            Duration::from_secs(1)
        } else {
            duration
        };

        Ok(DurationExpr { duration, raw_expr })
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
    pub(crate) fn parse_trigger_labels(
        &mut self,
        is_first_keyword_matched: bool,
    ) -> Result<OptionMap> {
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
            .collect::<Result<Vec<_>>>()?;
        self.parser
            .expect_token(&Token::RParen)
            .context(error::SyntaxSnafu)?;
        Ok(OptionMap::new(options))
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
    pub(crate) fn parse_trigger_annotations(
        &mut self,
        is_first_keyword_matched: bool,
    ) -> Result<OptionMap> {
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
            .collect::<Result<Vec<_>>>()?;
        self.parser
            .expect_token(&Token::RParen)
            .context(error::SyntaxSnafu)?;
        Ok(OptionMap::new(options))
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
    pub(crate) fn parse_trigger_notify(
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
        let notify_ident = Self::canonicalize_identifier(notify_ident);

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
            .collect::<Result<Vec<_>>>()?;

        for (key, _) in options.iter() {
            ensure!(
                validate_webhook_option(key),
                error::InvalidTriggerWebhookOptionSnafu { key: key.clone() }
            );
        }

        let webhook = AlertManagerWebhook {
            url,
            options: OptionMap::new(options),
        };

        Ok(NotifyChannel {
            name: notify_ident,
            channel_type: ChannelType::Webhook(webhook),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

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
        FOR '1ms'::INTERVAL
        KEEP FIRING FOR '10 minute'::INTERVAL
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
        KEEP FIRING FOR '10 minute'::INTERVAL
        FOR '1ms'::INTERVAL
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
            create_trigger.trigger_on.query.to_string(),
            "SELECT host AS host_label, cpu, memory FROM machine_monitor WHERE cpu > 1"
        );
        let TriggerOn {
            query,
            query_interval,
        } = &create_trigger.trigger_on;
        assert_eq!(
            query.to_string(),
            "SELECT host AS host_label, cpu, memory FROM machine_monitor WHERE cpu > 1"
        );
        assert_eq!(query_interval.duration, Duration::from_secs(300));
        assert_eq!(query_interval.raw_expr.clone(), "'5 minute'::INTERVAL");
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

        let r#for = create_trigger.r#for.as_ref().unwrap();
        assert_eq!(r#for.duration, Duration::from_secs(1));
        assert_eq!(r#for.raw_expr, "'1ms'::INTERVAL");
        let keep_firing_for = create_trigger.keep_firing_for.as_ref().unwrap();
        assert_eq!(keep_firing_for.duration, Duration::from_secs(600));
        assert_eq!(keep_firing_for.raw_expr, "'10 minute'::INTERVAL");
    }

    #[test]
    fn test_parse_trigger_on() {
        // Normal.
        let sql = "ON (SELECT * FROM cpu_usage) EVERY '5 minute'::INTERVAL";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let TriggerOn {
            query,
            query_interval: interval,
        } = ctx.parse_trigger_on(false).unwrap();
        assert_eq!(query.to_string(), "SELECT * FROM cpu_usage");
        assert_eq!(interval.duration, Duration::from_secs(300));
        assert_eq!(interval.raw_expr, "'5 minute'::INTERVAL");

        // Invalid, since missing `(` after `ON`.
        let sql = "ON SELECT * FROM cpu_usage EVERY '5 minute'::INTERVAL";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        assert!(ctx.parse_trigger_on(false).is_err());

        // Invalid, since missing `)` after query expression.
        let sql = "ON (SELECT * FROM cpu_usage EVERY '5 minute'::INTERVAL";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        assert!(ctx.parse_trigger_on(false).is_err());

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

        // Invalid, since year is not allowed in trigger interval.
        let sql = "ON (SELECT * FROM cpu_usage) EVERY '1 year'::INTERVAL";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        assert!(ctx.parse_trigger_on(false).is_err());

        // Invalid, since month is not allowed in trigger interval.
        let sql = "ON (SELECT * FROM cpu_usage) EVERY '1 month'::INTERVAL";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        assert!(ctx.parse_trigger_on(false).is_err());

        // Invalid, since the year and month are not allowed in trigger interval.
        let sql = "ON (SELECT * FROM cpu_usage) EVERY '1 year 1 month'::INTERVAL";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        assert!(ctx.parse_trigger_on(false).is_err());

        // Valid, but the interval is less than 1 second, it will be adjusted to 1 second.
        let sql = "ON (SELECT * FROM cpu_usage) EVERY '1ms'::INTERVAL";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let trigger_on = ctx.parse_trigger_on(false).unwrap();
        assert_eq!(trigger_on.query_interval.duration, Duration::from_secs(1));
    }

    #[test]
    fn test_parse_parenthesized_sql_or_tql_sql_branch() {
        let sql = "(SELECT 1)";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let parsed = ctx.parse_parenthesized_sql_or_tql(false).unwrap();
        let expected = "SELECT 1";
        match parsed {
            SqlOrTql::Sql(_, raw) => assert_eq!(raw, expected),
            _ => panic!("Expected SQL branch"),
        }
    }

    #[test]
    fn test_parse_parenthesized_sql_or_tql_tql_branch() {
        let sql = "(TQL EVAL (now() - now(), now(), '1s') cpu_usage)";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let parsed = ctx.parse_parenthesized_sql_or_tql(false).unwrap();
        let expected = "TQL EVAL (now() - now(), now(), '1s') cpu_usage";
        match parsed {
            SqlOrTql::Tql(_, raw) => assert_eq!(raw, expected),
            _ => panic!("Expected TQL branch"),
        }
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

    #[test]
    fn test_parse_trigger_for() {
        // Normal.
        let sql = "FOR '10 minute'::INTERVAL";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let expr = ctx.parse_trigger_for(false).unwrap();
        assert_eq!(expr.duration, Duration::from_secs(600));
        assert_eq!(expr.raw_expr, "'10 minute'::INTERVAL");

        // Invalid, missing FOR keyword.
        let sql = "'10 minute'::INTERVAL";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        assert!(ctx.parse_trigger_for(false).is_err());

        // Invalid, year not allowed.
        let sql = "FOR '1 year'::INTERVAL";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        assert!(ctx.parse_trigger_for(false).is_err());

        // Invalid, month not allowed.
        let sql = "FOR '1 month'::INTERVAL";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        assert!(ctx.parse_trigger_for(false).is_err());

        // Valid, interval less than 1 second is clamped.
        let sql = "FOR '1ms'::INTERVAL";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let expr = ctx.parse_trigger_for(false).unwrap();
        assert_eq!(expr.duration, Duration::from_secs(1));
    }

    #[test]
    fn test_parse_trigger_keep_firing_for() {
        // Normal.
        let sql = "KEEP FIRING FOR '10 minute'::INTERVAL";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let expr = ctx.parse_trigger_keep_firing_for(false).unwrap();
        assert_eq!(expr.duration, Duration::from_secs(600));
        assert_eq!(expr.raw_expr, "'10 minute'::INTERVAL");

        // Invalid, missing KEEP FIRING FOR keywords.
        let sql = "'10 minute'::INTERVAL";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        assert!(ctx.parse_trigger_keep_firing_for(false).is_err());

        // Invalid, year not allowed.
        let sql = "KEEP FIRING FOR '1 year'::INTERVAL";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        assert!(ctx.parse_trigger_keep_firing_for(false).is_err());

        // Invalid, month not allowed.
        let sql = "KEEP FIRING FOR '1 month'::INTERVAL";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        assert!(ctx.parse_trigger_keep_firing_for(false).is_err());

        // Valid, interval less than 1 second is clamped.
        let sql = "KEEP FIRING FOR '1ms'::INTERVAL";
        let mut ctx = ParserContext::new(&GreptimeDbDialect {}, sql).unwrap();
        let expr = ctx.parse_trigger_keep_firing_for(false).unwrap();
        assert_eq!(expr.duration, Duration::from_secs(1));
    }
}
