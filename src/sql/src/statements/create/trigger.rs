use std::fmt::{Display, Formatter};

use itertools::Itertools;
use serde::Serialize;
use sqlparser::ast::Query;
use sqlparser_derive::{Visit, VisitMut};

use crate::ast::{Ident, ObjectName};
use crate::statements::create::{COMMA_SEP, INDENT, LINE_SEP};
use crate::statements::OptionMap;

#[derive(Debug, PartialEq, Eq, Clone, Visit, VisitMut, Serialize)]
pub struct CreateTrigger {
    pub trigger_name: ObjectName,
    pub if_not_exists: bool,
    /// SQL statement executed periodically.
    pub query: Box<Query>,
    /// The interval of exec query. Unit is second.
    pub interval: u64,
    pub labels: OptionMap,
    pub annotations: OptionMap,
    pub channels: Vec<NotifyChannel>,
}

impl Display for CreateTrigger {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE TRIGGER ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }
        writeln!(f, "{}", self.trigger_name)?;
        write!(f, "ON {} ", self.query)?;
        writeln!(f, "EVERY {} SECONDS", self.interval)?;

        if !self.labels.is_empty() {
            let labels = self.labels.kv_pairs();
            writeln!(f, "LABELS ({})", format_list_comma!(labels))?;
        }

        if !self.annotations.is_empty() {
            let annotations = self.annotations.kv_pairs();
            writeln!(f, "ANNOTATIONS ({})", format_list_comma!(annotations))?;
        }

        if !self.channels.is_empty() {
            writeln!(f, "NOTIFY(")?;
            write!(f, "{}", format_list_indent!(self.channels))?;
            write!(f, ")")?;
        }

        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Visit, VisitMut, Serialize)]
pub struct NotifyChannel {
    pub name: Ident,
    pub channel_type: ChannelType,
}

impl Display for NotifyChannel {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.channel_type {
            ChannelType::Webhook(webhook) => {
                write!(f, "WEBHOOK {} URL {}", self.name, webhook.url)?;
                if !webhook.options.is_empty() {
                    let options = webhook.options.kv_pairs();
                    write!(f, " WITH ({})", format_list_comma!(options))?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Visit, VisitMut, Serialize)]
pub enum ChannelType {
    /// Alert manager webhook options.
    Webhook(AlertManagerWebhook),
}

#[derive(Debug, PartialEq, Eq, Clone, Visit, VisitMut, Serialize)]
pub struct AlertManagerWebhook {
    pub url: Ident,
    pub options: OptionMap,
}

#[cfg(test)]
mod tests {
    use crate::dialect::GreptimeDbDialect;
    use crate::parser::{ParseOptions, ParserContext};
    use crate::statements::statement::Statement;

    #[test]
    fn test_display_create_trigger() {
        let sql = r#"CREATE TRIGGER IF NOT EXISTS cpu_monitor
ON (SELECT host AS host_label, cpu, memory FROM machine_monitor WHERE cpu > 2) EVERY '5 minute'::INTERVAL
LABELS (label_name=label_val)
ANNOTATIONS (annotation_name=annotation_val)
NOTIFY
(WEBHOOK alert_manager URL 'http://127.0.0.1:9093' WITH (timeout='1m'))"#;

        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());

        let Statement::CreateTrigger(trigger) = &result[0] else {
            panic!("Expected CreateTrigger statement");
        };
        let formatted = format!("{}", trigger);
        let expected = r#"CREATE TRIGGER IF NOT EXISTS cpu_monitor
ON (SELECT host AS host_label, cpu, memory FROM machine_monitor WHERE cpu > 2) EVERY 300 SECONDS
LABELS (label_name = 'label_val')
ANNOTATIONS (annotation_name = 'annotation_val')
NOTIFY(
  WEBHOOK alert_manager URL 'http://127.0.0.1:9093' WITH (timeout = '1m'))"#;
        assert_eq!(expected, formatted);
    }
}
