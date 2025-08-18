use std::fmt::{Display, Formatter};

use serde::Serialize;
use sqlparser::ast::ObjectName;
use sqlparser_derive::{Visit, VisitMut};

use crate::statements::create::trigger::{NotifyChannel, TriggerOn};
use crate::statements::OptionMap;

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub struct AlterTrigger {
    pub trigger_name: ObjectName,
    pub operation: AlterTriggerOperation,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub struct AlterTriggerOperation {
    pub rename: Option<String>,
    pub trigger_on: Option<TriggerOn>,
    pub label_operations: Option<LabelOperations>,
    pub annotation_operations: Option<AnnotationOperations>,
    pub notify_channel_operations: Option<NotifyChannelOperations>,
}

impl Display for AlterTrigger {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ALTER TRIGGER {}", self.trigger_name)?;

        let operation = &self.operation;

        if let Some(new_name) = &operation.rename {
            writeln!(f)?;
            write!(f, "RENAME TO {}", new_name)?;
        }

        if let Some(trigger_on) = &operation.trigger_on {
            writeln!(f)?;
            write!(f, "{}", trigger_on)?;
        }

        if let Some(label_ops) = &operation.label_operations {
            match label_ops {
                LabelOperations::ReplaceAll(map) => {
                    writeln!(f)?;
                    write!(f, "SET LABELS ({})", map.kv_pairs().join(", "))?
                }
                LabelOperations::PartialChanges(changes) => {
                    for change in changes {
                        writeln!(f)?;
                        write!(f, "{}", change)?;
                    }
                }
            }
        }

        if let Some(annotation_ops) = &operation.annotation_operations {
            match annotation_ops {
                AnnotationOperations::ReplaceAll(map) => {
                    writeln!(f)?;
                    write!(f, "SET ANNOTATIONS ({})", map.kv_pairs().join(", "))?
                }
                AnnotationOperations::PartialChanges(changes) => {
                    for change in changes {
                        writeln!(f)?;
                        write!(f, "{}", change)?;
                    }
                }
            }
        }

        if let Some(notify_channel_ops) = &operation.notify_channel_operations {
            match notify_channel_ops {
                NotifyChannelOperations::ReplaceAll(channels) => {
                    if !channels.is_empty() {
                        writeln!(f)?;
                        writeln!(f, "SET NOTIFY")?;
                        for channel in channels {
                            write!(f, "    {}", channel)?;
                        }
                    }
                }
                NotifyChannelOperations::PartialChanges(changes) => {
                    for change in changes {
                        writeln!(f)?;
                        write!(f, "{}", change)?;
                    }
                }
            }
        }

        Ok(())
    }
}

/// The operations which describe how to update labels.
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub enum LabelOperations {
    ReplaceAll(OptionMap),
    PartialChanges(Vec<LabelChange>),
}

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub enum LabelChange {
    /// Add new labels.
    ///
    /// Note: if the labels to add already exists, an error will be reported.
    Add(OptionMap),
    /// Modify existing labels.
    ///
    /// Note: if the labels to update does not exist, an error will be reported.
    Modify(OptionMap),
    /// Drop specified labels.
    ///
    /// Note: if the labels to drop does not exist, an error will be reported.
    Drop(Vec<String>),
}

impl Display for LabelChange {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LabelChange::Add(map) => {
                if map.is_empty() {
                    return Ok(());
                }
                write!(f, "ADD LABELS ({})", map.kv_pairs().join(", "))
            }
            LabelChange::Modify(map) => {
                if map.is_empty() {
                    return Ok(());
                }
                write!(f, "MODIFY LABELS ({})", map.kv_pairs().join(", "))
            }
            LabelChange::Drop(names) => {
                if names.is_empty() {
                    return Ok(());
                }
                write!(f, "DROP LABELS ({})", names.join(", "))
            }
        }
    }
}

/// The operations which describe how to update annotations.
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub enum AnnotationOperations {
    ReplaceAll(OptionMap),
    PartialChanges(Vec<AnnotationChange>),
}

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub enum AnnotationChange {
    /// Add new annotations.
    ///
    /// Note: if the annotations to add already exists, an error will be reported.
    Add(OptionMap),
    /// Modify existing annotations.
    ///
    /// Note: if the annotations to update does not exist, an error will be
    /// reported.
    Modify(OptionMap),
    /// Drop specified annotations.
    ///
    /// Note: if the annotations to drop does not exist, an error will be
    /// reported.
    Drop(Vec<String>),
}

impl Display for AnnotationChange {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AnnotationChange::Add(map) => {
                if map.is_empty() {
                    return Ok(());
                }
                write!(f, "ADD ANNOTATIONS ({})", map.kv_pairs().join(", "))
            }
            AnnotationChange::Modify(map) => {
                if map.is_empty() {
                    return Ok(());
                }
                write!(f, "MODIFY ANNOTATIONS ({})", map.kv_pairs().join(", "))
            }
            AnnotationChange::Drop(names) => {
                if names.is_empty() {
                    return Ok(());
                }
                write!(f, "DROP ANNOTATIONS ({})", names.join(", "))
            }
        }
    }
}

/// The operations which describe how to update notify channels.
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub enum NotifyChannelOperations {
    ReplaceAll(Vec<NotifyChannel>),
    PartialChanges(Vec<NotifyChannelChange>),
}

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub enum NotifyChannelChange {
    /// Add new NotifyChannel's.
    ///
    /// Note: if the NotifyChannel to add already exists, an error will be
    /// reported.
    Add(Vec<NotifyChannel>),
    /// Drop specified NotifyChannel's.
    ///
    /// Note: if the NotifyChannel to drop does not exist, an error will be
    /// reported.
    Drop(Vec<String>),
}

impl Display for NotifyChannelChange {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            NotifyChannelChange::Add(channels) => {
                if channels.is_empty() {
                    return Ok(());
                }
                write!(f, "ADD NOTIFY(")?;
                for (idx, channel) in channels.iter().enumerate() {
                    writeln!(f)?;
                    write!(f, "    {}", channel)?;
                    if idx < channels.len() - 1 {
                        write!(f, ",")?;
                    }
                }
                write!(f, ")")?;
            }
            NotifyChannelChange::Drop(names) => {
                if names.is_empty() {
                    return Ok(());
                }
                write!(f, "DROP NOTIFY ({})", names.join(", "))?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::Ident;

    use super::*;
    use crate::dialect::GreptimeDbDialect;
    use crate::parser::{ParseOptions, ParserContext};
    use crate::statements::create::trigger::{AlertManagerWebhook, ChannelType};
    use crate::statements::statement::Statement;

    #[test]
    fn test_display_label_change() {
        let add = LabelChange::Add(OptionMap::from([("k1".to_string(), "v1".to_string())]));
        let modify = LabelChange::Modify(OptionMap::from([("k2".to_string(), "v2".to_string())]));
        let drop = LabelChange::Drop(vec!["k3".to_string(), "k4".to_string()]);

        assert_eq!(add.to_string(), "ADD LABELS (k1 = 'v1')");
        assert_eq!(modify.to_string(), "MODIFY LABELS (k2 = 'v2')");
        assert_eq!(drop.to_string(), "DROP LABELS (k3, k4)");
    }

    #[test]
    fn test_display_annotation_change() {
        let add = AnnotationChange::Add(OptionMap::from([("a1".to_string(), "v1".to_string())]));
        let modify =
            AnnotationChange::Modify(OptionMap::from([("a2".to_string(), "v2".to_string())]));
        let drop = AnnotationChange::Drop(vec!["a3".to_string(), "a4".to_string()]);

        assert_eq!(add.to_string(), "ADD ANNOTATIONS (a1 = 'v1')");
        assert_eq!(modify.to_string(), "MODIFY ANNOTATIONS (a2 = 'v2')");
        assert_eq!(drop.to_string(), "DROP ANNOTATIONS (a3, a4)");
    }

    #[test]
    fn test_display_notify_channel_change() {
        let add_channel = NotifyChannel {
            name: Ident::new("webhook1"),
            channel_type: ChannelType::Webhook(AlertManagerWebhook {
                url: Ident::new("http://example.com"),
                options: OptionMap::default(),
            }),
        };
        let add = NotifyChannelChange::Add(vec![add_channel]);
        let expected = r#"ADD NOTIFY(
    WEBHOOK webhook1 URL http://example.com)"#;
        assert_eq!(expected, add.to_string(),);

        let drop = NotifyChannelChange::Drop(vec!["webhook2".to_string(), "webhook3".to_string()]);
        assert_eq!(drop.to_string(), "DROP NOTIFY (webhook2, webhook3)");
    }

    #[test]
    fn test_display_alter_trigger() {
        let sql = r#"ALTER TRIGGER my_trigger
ON (SELECT host AS host_label, cpu, memory FROM machine_monitor WHERE cpu > 2) EVERY '5 minute'::INTERVAL
RENAME TO new_trigger
ADD LABELS (k1 = 'v1', k2 = 'v2')
DROP LABELS (k3, k4)
SET ANNOTATIONS (a1 = 'v1', a2 = 'v2')
DROP NOTIFY (webhook1, webhook2)
ADD NOTIFY
    (WEBHOOK webhook3 URL 'http://new3.com',
    WEBHOOK webhook4 URL 'http://new4.com')"#;
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, result.len());

        let Statement::AlterTrigger(trigger) = &result[0] else {
            panic!("Expected AlterTrigger statement");
        };

        let formatted = format!("{}", trigger);
        let expected = r#"ALTER TRIGGER my_trigger
RENAME TO new_trigger
ON (SELECT host AS host_label, cpu, memory FROM machine_monitor WHERE cpu > 2) EVERY '5 minute'::INTERVAL
ADD LABELS (k1 = 'v1', k2 = 'v2')
DROP LABELS (k3, k4)
SET ANNOTATIONS (a1 = 'v1', a2 = 'v2')
DROP NOTIFY (webhook1, webhook2)
ADD NOTIFY(
    WEBHOOK webhook3 URL 'http://new3.com',
    WEBHOOK webhook4 URL 'http://new4.com')"#;
        assert_eq!(formatted, expected);
    }
}
