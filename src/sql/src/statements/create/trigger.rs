use std::fmt::{Display, Formatter};
use std::ops::ControlFlow;
use std::time::Duration;

use itertools::Itertools;
use serde::Serialize;
use sqlparser::ast::{Visit, VisitMut, Visitor, VisitorMut};
use sqlparser_derive::{Visit, VisitMut};

use crate::ast::{Ident, ObjectName};
use crate::statements::OptionMap;
use crate::statements::create::{COMMA_SEP, INDENT, SqlOrTql};

#[derive(Debug, PartialEq, Eq, Clone, Visit, VisitMut, Serialize)]
pub struct CreateTrigger {
    pub trigger_name: ObjectName,
    pub if_not_exists: bool,
    pub trigger_on: TriggerOn,
    pub r#for: Option<DurationExpr>,
    pub keep_firing_for: Option<DurationExpr>,
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
        writeln!(f, "  {}", self.trigger_on)?;

        if let Some(r#for) = &self.r#for {
            writeln!(f, "  FOR {}", r#for)?;
        }

        if let Some(keep_firing_for) = &self.keep_firing_for {
            writeln!(f, "  KEEP FIRING FOR {}", keep_firing_for)?;
        }

        if !self.labels.is_empty() {
            let labels = self.labels.kv_pairs();
            writeln!(f, "  LABELS ({})", format_list_comma!(labels))?;
        }

        if !self.annotations.is_empty() {
            let annotations = self.annotations.kv_pairs();
            writeln!(f, "  ANNOTATIONS ({})", format_list_comma!(annotations))?;
        }

        if !self.channels.is_empty() {
            writeln!(f, "  NOTIFY(")?;
            for channel in self.channels.iter() {
                writeln!(f, "  {},", format_indent!(channel))?;
            }
            write!(f, "  )")?;
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

/// Represents a duration expression with its parsed `Duration` and the original
/// raw string. And the struct implements `Visit` and `VisitMut` traits as no-op.
#[derive(Debug, PartialEq, Eq, Clone, Serialize)]
pub struct DurationExpr {
    pub duration: Duration,
    pub raw_expr: String,
}

impl Display for DurationExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.raw_expr.is_empty() {
            // Fallback to display duration if raw_expr is empty.
            // Display in seconds since we limit the min-duration to 1 second
            // in SQL parser.
            write!(f, "{} seconds", self.duration.as_secs())
        } else {
            write!(f, "{}", self.raw_expr)
        }
    }
}

impl Visit for DurationExpr {
    fn visit<V: Visitor>(&self, _visitor: &mut V) -> ControlFlow<V::Break> {
        ControlFlow::Continue(())
    }
}

impl VisitMut for DurationExpr {
    fn visit<V: VisitorMut>(&mut self, _visitor: &mut V) -> ControlFlow<V::Break> {
        ControlFlow::Continue(())
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Visit, VisitMut, Serialize)]
pub struct TriggerOn {
    pub query: SqlOrTql,
    pub query_interval: DurationExpr,
}

impl Display for TriggerOn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ON ({}) EVERY {}", self.query, self.query_interval)
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
    use std::time::Duration;

    use crate::dialect::GreptimeDbDialect;
    use crate::parser::{ParseOptions, ParserContext};
    use crate::statements::create::SqlOrTql;
    use crate::statements::statement::Statement;

    #[test]
    fn test_display_create_trigger() {
        let sql = r#"CREATE TRIGGER IF NOT EXISTS cpu_monitor
ON (SELECT host AS host_label, cpu, memory FROM machine_monitor WHERE cpu > 2) EVERY '1day 5 minute'::INTERVAL
FOR '5 minute'::INTERVAL
KEEP FIRING FOR '10 minute'::INTERVAL
LABELS (label_name=label_val)
ANNOTATIONS (annotation_name=annotation_val)
NOTIFY
(
WEBHOOK alert_manager1 URL 'http://127.0.0.1:9093' WITH (timeout='1m'),
WEBHOOK alert_manager2 URL 'http://127.0.0.1:9093' WITH (timeout='1m')
)"#;

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
  FOR '5 minute'::INTERVAL
  KEEP FIRING FOR '10 minute'::INTERVAL
  LABELS (label_name = 'label_val')
  ANNOTATIONS (annotation_name = 'annotation_val')
  NOTIFY(
    WEBHOOK alert_manager1 URL 'http://127.0.0.1:9093' WITH (timeout = '1m'),
    WEBHOOK alert_manager2 URL 'http://127.0.0.1:9093' WITH (timeout = '1m'),
  )"#;
        assert_eq!(expected, formatted);
    }

    #[test]
    fn test_duration_expr_display() {
        let duration_expr = super::DurationExpr {
            duration: Duration::from_secs(300),
            raw_expr: "'5 minute'::INTERVAL".to_string(),
        };
        assert_eq!(duration_expr.to_string(), "'5 minute'::INTERVAL");

        let duration_expr_no_raw = super::DurationExpr {
            duration: Duration::from_secs(600),
            raw_expr: "".to_string(),
        };
        assert_eq!(duration_expr_no_raw.to_string(), "600 seconds");
    }

    #[test]
    fn test_parse_trigger_with_tql_query() {
        let sql = r#"CREATE TRIGGER cpu_monitor
ON (TQL EVAL (now() - now(), now() - (now() - '10 seconds'::interval), '1s') cpu_usage_total)
EVERY '5 minute'::INTERVAL
NOTIFY(
    WEBHOOK alert_manager URL 'http://127.0.0.1:9093' WITH (timeout='1m')
)
"#;

        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());

        let Statement::CreateTrigger(trigger) = &result[0] else {
            panic!("Expected CreateTrigger statement");
        };

        match &trigger.trigger_on.query {
            SqlOrTql::Tql(_, raw) => {
                assert!(raw.contains("TQL EVAL"));
                assert!(raw.ends_with("cpu_usage_total"));
                assert!(!raw.trim_end().ends_with(')'));
            }
            _ => panic!("Expected TQL query in trigger"),
        }
    }
}
