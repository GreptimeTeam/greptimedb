use std::collections::HashMap;
use std::time::Duration;

use api::v1::meta::{
    CreateTriggerTask as PbCreateTriggerTask, DropTriggerTask as PbDropTriggerTask,
};
use api::v1::notify_channel::ChannelType as PbChannelType;
use api::v1::{
    CreateTriggerExpr as PbCreateTriggerExpr, DropTriggerExpr as PbDropTriggerExpr,
    NotifyChannel as PbNotifyChannel, WebhookOptions as PbWebhookOptions,
};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};

use crate::error::{self, Result, TooLargeDurationSnafu};
use crate::rpc::ddl::DdlTask;

// Create trigger
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTriggerTask {
    pub catalog_name: String,
    pub trigger_name: String,
    pub if_not_exists: bool,
    pub sql: String,
    pub channels: Vec<NotifyChannel>,
    pub labels: HashMap<String, String>,
    pub annotations: HashMap<String, String>,
    pub interval: Duration,
    pub raw_interval_expr: Option<String>,
    pub r#for: Option<Duration>,
    pub for_raw_expr: Option<String>,
    pub keep_firing_for: Option<Duration>,
    pub keep_firing_for_raw_expr: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NotifyChannel {
    pub name: String,
    pub channel_type: ChannelType,
}

/// The available channel enum for sending trigger notifications.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ChannelType {
    Webhook(WebhookOptions),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WebhookOptions {
    /// The URL of the AlertManager API endpoint.
    ///
    /// e.g., "http://localhost:9093".
    pub url: String,
    /// Configuration options for the AlertManager webhook. e.g., timeout, etc.
    pub opts: HashMap<String, String>,
}

impl TryFrom<CreateTriggerTask> for PbCreateTriggerTask {
    type Error = error::Error;

    fn try_from(task: CreateTriggerTask) -> Result<Self> {
        let channels = task
            .channels
            .into_iter()
            .map(PbNotifyChannel::from)
            .collect();

        let interval = task.interval.try_into().context(TooLargeDurationSnafu)?;
        let raw_interval_expr = task.raw_interval_expr.unwrap_or_default();

        let r#for = task
            .r#for
            .map(|d| d.try_into().context(TooLargeDurationSnafu))
            .transpose()?;
        let for_raw_expr = task.for_raw_expr.unwrap_or_default();

        let keep_firing_for = task
            .keep_firing_for
            .map(|d| d.try_into().context(TooLargeDurationSnafu))
            .transpose()?;
        let keep_firing_for_raw_expr = task.keep_firing_for_raw_expr.unwrap_or_default();

        let expr = PbCreateTriggerExpr {
            catalog_name: task.catalog_name,
            trigger_name: task.trigger_name,
            create_if_not_exists: task.if_not_exists,
            sql: task.sql,
            channels,
            labels: task.labels,
            annotations: task.annotations,
            interval: Some(interval),
            raw_interval_expr,
            r#for,
            for_raw_expr,
            keep_firing_for,
            keep_firing_for_raw_expr,
        };

        Ok(PbCreateTriggerTask {
            create_trigger: Some(expr),
        })
    }
}

impl TryFrom<PbCreateTriggerTask> for CreateTriggerTask {
    type Error = error::Error;

    fn try_from(task: PbCreateTriggerTask) -> Result<Self> {
        let expr = task.create_trigger.context(error::InvalidProtoMsgSnafu {
            err_msg: "expected create_trigger",
        })?;

        let channels = expr
            .channels
            .into_iter()
            .map(NotifyChannel::try_from)
            .collect::<Result<Vec<_>>>()?;

        let interval = expr.interval.context(error::MissingIntervalSnafu)?;
        let interval = interval.try_into().context(error::NegativeDurationSnafu)?;

        let r#for = expr
            .r#for
            .map(Duration::try_from)
            .transpose()
            .context(error::NegativeDurationSnafu)?;

        let keep_firing_for = expr
            .keep_firing_for
            .map(Duration::try_from)
            .transpose()
            .context(error::NegativeDurationSnafu)?;

        let raw_interval_expr =
            (!expr.raw_interval_expr.is_empty()).then_some(expr.raw_interval_expr);

        let for_raw_expr = (!expr.for_raw_expr.is_empty()).then_some(expr.for_raw_expr);

        let keep_firing_for_raw_expr =
            (!expr.keep_firing_for_raw_expr.is_empty()).then_some(expr.keep_firing_for_raw_expr);

        let task = CreateTriggerTask {
            catalog_name: expr.catalog_name,
            trigger_name: expr.trigger_name,
            if_not_exists: expr.create_if_not_exists,
            sql: expr.sql,
            channels,
            labels: expr.labels,
            annotations: expr.annotations,
            interval,
            raw_interval_expr,
            r#for,
            for_raw_expr,
            keep_firing_for,
            keep_firing_for_raw_expr,
        };
        Ok(task)
    }
}

impl From<NotifyChannel> for PbNotifyChannel {
    fn from(channel: NotifyChannel) -> Self {
        let NotifyChannel { name, channel_type } = channel;

        let channel_type = match channel_type {
            ChannelType::Webhook(options) => PbChannelType::Webhook(PbWebhookOptions {
                url: options.url,
                opts: options.opts,
            }),
        };

        PbNotifyChannel {
            name,
            channel_type: Some(channel_type),
        }
    }
}

impl TryFrom<PbNotifyChannel> for NotifyChannel {
    type Error = error::Error;

    fn try_from(channel: PbNotifyChannel) -> Result<Self> {
        let PbNotifyChannel { name, channel_type } = channel;

        let channel_type = channel_type.context(error::InvalidProtoMsgSnafu {
            err_msg: "expected channel_type",
        })?;

        let channel_type = match channel_type {
            PbChannelType::Webhook(options) => ChannelType::Webhook(WebhookOptions {
                url: options.url,
                opts: options.opts,
            }),
        };
        Ok(NotifyChannel { name, channel_type })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropTriggerTask {
    pub catalog_name: String,
    pub trigger_name: String,
    pub drop_if_exists: bool,
}

impl From<DropTriggerTask> for PbDropTriggerTask {
    fn from(task: DropTriggerTask) -> Self {
        let expr = PbDropTriggerExpr {
            catalog_name: task.catalog_name,
            trigger_name: task.trigger_name,
            drop_if_exists: task.drop_if_exists,
        };

        PbDropTriggerTask {
            drop_trigger: Some(expr),
        }
    }
}

impl TryFrom<PbDropTriggerTask> for DropTriggerTask {
    type Error = error::Error;

    fn try_from(task: PbDropTriggerTask) -> Result<Self> {
        let expr = task.drop_trigger.context(error::InvalidProtoMsgSnafu {
            err_msg: "expected drop_trigger",
        })?;

        Ok(DropTriggerTask {
            catalog_name: expr.catalog_name,
            trigger_name: expr.trigger_name,
            drop_if_exists: expr.drop_if_exists,
        })
    }
}

impl DdlTask {
    /// Creates a [`DdlTask`] to create a trigger.
    pub fn new_create_trigger(expr: CreateTriggerTask) -> Self {
        DdlTask::CreateTrigger(expr)
    }

    /// Creates a [`DdlTask`] to drop a trigger.
    pub fn new_drop_trigger(expr: DropTriggerTask) -> Self {
        DdlTask::DropTrigger(expr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_drop_trigger_task() {
        let original = DropTriggerTask {
            catalog_name: "test_catalog".to_string(),
            trigger_name: "test_trigger".to_string(),
            drop_if_exists: true,
        };

        let pb_task: PbDropTriggerTask = original.clone().into();

        let expr = pb_task.drop_trigger.as_ref().unwrap();
        assert_eq!(expr.catalog_name, "test_catalog");
        assert_eq!(expr.trigger_name, "test_trigger");
        assert!(expr.drop_if_exists);

        let round_tripped = DropTriggerTask::try_from(pb_task).unwrap();

        assert_eq!(original.catalog_name, round_tripped.catalog_name);
        assert_eq!(original.trigger_name, round_tripped.trigger_name);
        assert_eq!(original.drop_if_exists, round_tripped.drop_if_exists);

        // Test invalid case where drop_trigger is None
        let invalid_task = PbDropTriggerTask { drop_trigger: None };
        let result = DropTriggerTask::try_from(invalid_task);
        assert!(result.is_err());
    }

    #[test]
    fn test_convert_create_trigger_task() {
        let original = CreateTriggerTask {
            catalog_name: "test_catalog".to_string(),
            trigger_name: "test_trigger".to_string(),
            if_not_exists: true,
            sql: "SELECT * FROM test".to_string(),
            channels: vec![
                NotifyChannel {
                    name: "channel1".to_string(),
                    channel_type: ChannelType::Webhook(WebhookOptions {
                        url: "http://localhost:9093".to_string(),
                        opts: HashMap::from([("timeout".to_string(), "30s".to_string())]),
                    }),
                },
                NotifyChannel {
                    name: "channel2".to_string(),
                    channel_type: ChannelType::Webhook(WebhookOptions {
                        url: "http://alertmanager:9093".to_string(),
                        opts: HashMap::new(),
                    }),
                },
            ],
            labels: vec![
                ("key1".to_string(), "value1".to_string()),
                ("key2".to_string(), "value2".to_string()),
            ]
            .into_iter()
            .collect(),
            annotations: vec![
                ("summary".to_string(), "Test alert".to_string()),
                ("description".to_string(), "This is a test".to_string()),
            ]
            .into_iter()
            .collect(),
            interval: Duration::from_secs(60),
            raw_interval_expr: Some("'1 minute'::INTERVAL".to_string()),
            r#for: Duration::from_secs(300).into(),
            for_raw_expr: Some("'5 minute'::INTERVAL".to_string()),
            keep_firing_for: Duration::from_secs(600).into(),
            keep_firing_for_raw_expr: Some("'10 minute'::INTERVAL".to_string()),
        };

        let pb_task: PbCreateTriggerTask = original.clone().try_into().unwrap();

        let expr = pb_task.create_trigger.as_ref().unwrap();
        assert_eq!(expr.catalog_name, "test_catalog");
        assert_eq!(expr.trigger_name, "test_trigger");
        assert!(expr.create_if_not_exists);
        assert_eq!(expr.sql, "SELECT * FROM test");
        assert_eq!(expr.channels.len(), 2);
        assert_eq!(expr.labels.len(), 2);
        assert_eq!(expr.labels.get("key1").unwrap(), "value1");
        assert_eq!(expr.labels.get("key2").unwrap(), "value2");
        assert_eq!(expr.annotations.len(), 2);
        assert_eq!(expr.annotations.get("summary").unwrap(), "Test alert");
        assert_eq!(
            expr.annotations.get("description").unwrap(),
            "This is a test"
        );
        let expected: prost_types::Duration = Duration::from_secs(60).try_into().unwrap();
        assert_eq!(expr.interval, Some(expected));

        let round_tripped = CreateTriggerTask::try_from(pb_task).unwrap();

        assert_eq!(original.catalog_name, round_tripped.catalog_name);
        assert_eq!(original.trigger_name, round_tripped.trigger_name);
        assert_eq!(original.if_not_exists, round_tripped.if_not_exists);
        assert_eq!(original.sql, round_tripped.sql);
        assert_eq!(original.channels.len(), round_tripped.channels.len());
        assert_eq!(&original.channels[0], &round_tripped.channels[0]);
        assert_eq!(&original.channels[1], &round_tripped.channels[1]);
        assert_eq!(original.labels, round_tripped.labels);
        assert_eq!(original.annotations, round_tripped.annotations);
        assert_eq!(original.interval, round_tripped.interval);
        assert_eq!(original.raw_interval_expr, round_tripped.raw_interval_expr);
        assert_eq!(original.r#for, round_tripped.r#for);
        assert_eq!(original.for_raw_expr, round_tripped.for_raw_expr);
        assert_eq!(original.keep_firing_for, round_tripped.keep_firing_for);
        assert_eq!(
            original.keep_firing_for_raw_expr,
            round_tripped.keep_firing_for_raw_expr
        );

        // Invalid, since create_trigger is None and it's required.
        let invalid_task = PbCreateTriggerTask {
            create_trigger: None,
        };
        let result = CreateTriggerTask::try_from(invalid_task);
        assert!(result.is_err());
    }

    #[test]
    fn test_convert_notify_channel() {
        let original = NotifyChannel {
            name: "test_channel".to_string(),
            channel_type: ChannelType::Webhook(WebhookOptions {
                url: "http://localhost:9093".to_string(),
                opts: HashMap::new(),
            }),
        };
        let pb_channel: PbNotifyChannel = original.clone().into();
        match pb_channel.channel_type.as_ref().unwrap() {
            PbChannelType::Webhook(options) => {
                assert_eq!(pb_channel.name, "test_channel");
                assert_eq!(options.url, "http://localhost:9093");
                assert!(options.opts.is_empty());
            }
        }
        let round_tripped = NotifyChannel::try_from(pb_channel).unwrap();
        assert_eq!(original, round_tripped);

        // Test with timeout is None.
        let no_timeout = NotifyChannel {
            name: "no_timeout".to_string(),
            channel_type: ChannelType::Webhook(WebhookOptions {
                url: "http://localhost:9093".to_string(),
                opts: HashMap::new(),
            }),
        };
        let pb_no_timeout: PbNotifyChannel = no_timeout.clone().into();
        match pb_no_timeout.channel_type.as_ref().unwrap() {
            PbChannelType::Webhook(options) => {
                assert_eq!(options.url, "http://localhost:9093");
            }
        }
        let round_tripped_no_timeout = NotifyChannel::try_from(pb_no_timeout).unwrap();
        assert_eq!(no_timeout, round_tripped_no_timeout);

        // Invalid, since channel_type is None and it's required.
        let invalid_channel = PbNotifyChannel {
            name: "invalid".to_string(),
            channel_type: None,
        };
        let result = NotifyChannel::try_from(invalid_channel);
        assert!(result.is_err());
    }
}
