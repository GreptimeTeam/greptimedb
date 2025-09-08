use std::fmt::{Display, Formatter};
use std::ops::ControlFlow;
use std::time::Duration;

use itertools::Itertools;
use serde::Serialize;
use sqlparser::ast::{Query, Visit, VisitMut, Visitor, VisitorMut};
use sqlparser_derive::{Visit, VisitMut};

use crate::ast::{Ident, ObjectName};
use crate::statements::OptionMap;
use crate::statements::create::{COMMA_SEP, INDENT, LINE_SEP};

#[derive(Debug, PartialEq, Eq, Clone, Visit, VisitMut, Serialize)]
pub struct CreateTrigger {
    pub trigger_name: ObjectName,
    pub if_not_exists: bool,
    pub trigger_on: TriggerOn,
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
        writeln!(f, "{}", self.trigger_on)?;

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

#[derive(Debug, PartialEq, Eq, Clone, Serialize)]
pub struct TriggerOn {
    pub query: Box<Query>,
    pub interval: Duration,
    pub raw_interval_expr: String,
}

impl Display for TriggerOn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ON {} EVERY {}", self.query, self.raw_interval_expr)
    }
}

impl Visit for TriggerOn {
    fn visit<V: Visitor>(&self, visitor: &mut V) -> ControlFlow<V::Break> {
        Visit::visit(&self.query, visitor)?;
        ControlFlow::Continue(())
    }
}

impl VisitMut for TriggerOn {
    fn visit<V: VisitorMut>(&mut self, visitor: &mut V) -> ControlFlow<V::Break> {
        VisitMut::visit(&mut self.query, visitor)?;
        ControlFlow::Continue(())
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
ON (SELECT host AS host_label, cpu, memory FROM machine_monitor WHERE cpu > 2) EVERY '1day 5 minute'::INTERVAL
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
ON (SELECT host AS host_label, cpu, memory FROM machine_monitor WHERE cpu > 2) EVERY '1day 5 minute'::INTERVAL
LABELS (label_name = 'label_val')
ANNOTATIONS (annotation_name = 'annotation_val')
NOTIFY(
  WEBHOOK alert_manager URL 'http://127.0.0.1:9093' WITH (timeout = '1m'))"#;
        assert_eq!(expected, formatted);
    }
}
