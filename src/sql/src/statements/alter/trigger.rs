use std::fmt::{Display, Formatter};

use serde::Serialize;
use sqlparser::ast::{ObjectName, Query};
use sqlparser_derive::{Visit, VisitMut};

use crate::statements::create::trigger::NotifyChannel;
use crate::statements::OptionMap;

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub struct AlterTrigger {
    pub trigger_name: ObjectName,
    pub operation: AlterTriggerOperation,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub struct AlterTriggerOperation {
    pub rename: Option<String>,
    pub new_query: Option<Box<Query>>,
    /// The new interval of exec query. Unit is second.
    pub new_interval: Option<u64>,
    pub label_operations: Option<LabelOperations>,
    pub annotation_operations: Option<AnnotationOperations>,
    pub notify_channel_operations: Option<NotifyChannelOperations>,
}

impl Display for AlterTrigger {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "ALTER TRIGGER {}", self.trigger_name)?;

        let operation = &self.operation;

        if let Some(new_name) = &operation.rename {
            writeln!(f, "RENAME TO {}", new_name)?;
        }

        if operation.new_query.is_some() || operation.new_interval.is_some() {
            write!(f, "SET EXEC QUERY")?;
        }

        if let Some(query) = &operation.new_query {
            write!(f, " ({})", query)?;
        }

        if let Some(interval) = operation.new_interval {
            write!(f, " EVERY {} SECONDS", interval)?;
        }

        writeln!(f)?;

        if let Some(label_ops) = &operation.label_operations {
            match label_ops {
                LabelOperations::ReplaceAll(map) => {
                    writeln!(f, "SET LABELS ({})", map.kv_pairs().join(", "))?
                }
                LabelOperations::PartialChanges(changes) => {
                    for change in changes {
                        write!(f, "{}", change)?;
                    }
                }
            }
        }

        if let Some(annotation_ops) = &operation.annotation_operations {
            match annotation_ops {
                AnnotationOperations::ReplaceAll(map) => {
                    writeln!(f, "SET ANNOTATIONS ({})", map.kv_pairs().join(", "))?
                }
                AnnotationOperations::PartialChanges(changes) => {
                    for change in changes {
                        write!(f, "{}", change)?;
                    }
                }
            }
        }

        if let Some(notify_channel_ops) = &operation.notify_channel_operations {
            match notify_channel_ops {
                NotifyChannelOperations::ReplaceAll(channels) => {
                    if !channels.is_empty() {
                        writeln!(f, "SET NOTIFY")?;
                        for channel in channels {
                            write!(f, "    {}", channel)?;
                        }
                    }
                }
                NotifyChannelOperations::PartialChanges(changes) => {
                    for change in changes {
                        write!(f, "{}", change)?;
                    }
                }
            }
        }

        Ok(())
    }
}

/// The operations which describe how to update labels.
///
/// Note: replace all is mutually exclusive with partial changes.
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub enum LabelOperations {
    ReplaceAll(OptionMap),
    PartialChanges(Vec<LabelChange>),
}

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub enum LabelChange {
    /// Add operation will add new labels.
    ///
    /// Note: if the labels to add already exists, an error will be reported.
    Add(OptionMap),
    /// Modify operation will update existing labels.
    ///
    /// Note: if the labels to update does not exist, an error will be reported.
    Modify(OptionMap),
    /// Drop operation will remove specified labels.
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
                writeln!(f, "ADD LABELS ({})", map.kv_pairs().join(", "))
            }
            LabelChange::Modify(map) => {
                if map.is_empty() {
                    return Ok(());
                }
                writeln!(f, "MODIFY LABELS ({})", map.kv_pairs().join(", "))
            }
            LabelChange::Drop(names) => {
                if names.is_empty() {
                    return Ok(());
                }
                writeln!(f, "DROP LABELS ({})", names.join(", "))
            }
        }
    }
}

/// The operations which describe how to update annotations.
///
/// Note: replace all is mutually exclusive with partial changes.
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub enum AnnotationOperations {
    ReplaceAll(OptionMap),
    PartialChanges(Vec<AnnotationChange>),
}

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub enum AnnotationChange {
    /// Add operation will add new annotations.
    ///
    /// Note: if the annotations to add already exists, an error will be reported.
    Add(OptionMap),
    /// Modify operation will update existing annotations.
    ///
    /// Note: if the annotations to update does not exist, an error will be
    /// reported.
    Modify(OptionMap),
    /// Drop operation will remove specified annotations.
    ///
    /// Note: if the annotations to drop does not exist, an error will be reported.
    Drop(Vec<String>),
}

impl Display for AnnotationChange {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AnnotationChange::Add(map) => {
                if map.is_empty() {
                    return Ok(());
                }
                writeln!(f, "ADD ANNOTATIONS ({})", map.kv_pairs().join(", "))
            }
            AnnotationChange::Modify(map) => {
                if map.is_empty() {
                    return Ok(());
                }
                writeln!(f, "MODIFY ANNOTATIONS ({})", map.kv_pairs().join(", "))
            }
            AnnotationChange::Drop(names) => {
                if names.is_empty() {
                    return Ok(());
                }
                writeln!(f, "DROP ANNOTATIONS ({})", names.join(", "))
            }
        }
    }
}

/// The operations which describe how to update notify channels.
///
/// Note: replace all is mutually exclusive with partial changes.
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub enum NotifyChannelOperations {
    ReplaceAll(Vec<NotifyChannel>),
    PartialChanges(Vec<NotifyChannelChange>),
}

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub enum NotifyChannelChange {
    /// Add operation will add new NotifyChannel's.
    ///
    /// Note: if the NotifyChannel to add already exists, an error will be
    /// reported.
    Add(Vec<NotifyChannel>),
    /// Drop operation will remove specified NotifyChannel's.
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
                writeln!(f, "ADD NOTIFY")?;
                for channel in channels {
                    writeln!(f, "    {}", channel)?;
                }
            }
            NotifyChannelChange::Drop(names) => {
                if names.is_empty() {
                    return Ok(());
                }
                writeln!(f, "DROP NOTIFY ({})", names.join(", "))?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use sqlparser::ast::{Ident, ObjectName};

    use super::*;
    use crate::statements::create::trigger::{AlertManagerWebhook, ChannelType};

    #[test]
    fn test_display_alter_trigger() {
        let rename = Some("new_trigger".to_string());

        let new_interval = Some(60);

        let add_labels = HashMap::from([
            ("k1".to_string(), "v1".to_string()),
            ("k2".to_string(), "v2".to_string()),
        ]);
        let add_labels = LabelChange::Add(add_labels.into());
        let remove_labels = LabelChange::Drop(vec!["k3".to_string(), "k4".to_string()]);
        let label_operations = Some(LabelOperations::PartialChanges(vec![add_labels, remove_labels]));

        let set_annotations = HashMap::from([
            ("a1".to_string(), "v1".to_string()),
            ("a2".to_string(), "v2".to_string()),
        ]);
        let annotation_operations = Some(AnnotationOperations::ReplaceAll(set_annotations.into()));

        let add_notify_channel_3 = NotifyChannel {
            name: Ident::new("webhook3"),
            channel_type: ChannelType::Webhook(AlertManagerWebhook {
                url: Ident::new("http://new3.com"),
                options: OptionMap::default(),
            }),
        };
        let add_notify_channel_4 = NotifyChannel {
            name: Ident::new("webhook4"),
            channel_type: ChannelType::Webhook(AlertManagerWebhook {
                url: Ident::new("http://new4.com"),
                options: OptionMap::default(),
            }),
        };
        let notify_channel_operation = vec![
            NotifyChannelChange::Drop(vec!["webhook1".to_string(), "webhook2".to_string()]),
            NotifyChannelChange::Add(vec![add_notify_channel_3, add_notify_channel_4]),
        ];
        let notify_channel_operations = Some(NotifyChannelOperations::PartialChanges(
            notify_channel_operation,
        ));

        let trigger = AlterTrigger {
            trigger_name: ObjectName(vec![Ident::new("my_trigger")]),
            operation: AlterTriggerOperation {
                rename,
                new_query: None,
                new_interval,
                label_operations,
                annotation_operations,
                notify_channel_operations,
            },
        };

        let expected = r#"ALTER TRIGGER my_trigger
RENAME TO new_trigger
SET EXEC QUERY EVERY 60 SECONDS
ADD LABELS (k1 = 'v1', k2 = 'v2')
DROP LABELS (k3, k4)
SET ANNOTATIONS (a1 = 'v1', a2 = 'v2')
DROP NOTIFY (webhook1, webhook2)
ADD NOTIFY
    WEBHOOK webhook3 URL 'http://new3.com'
    WEBHOOK webhook4 URL 'http://new4.com'
"#;
        assert_eq!(trigger.to_string(), expected);
    }
}
