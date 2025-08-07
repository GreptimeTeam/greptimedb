use api::v1::notify_channel::ChannelType as PbChannelType;
use api::v1::{
    CreateTriggerExpr as PbCreateTriggerExpr, NotifyChannel as PbNotifyChannel,
    WebhookOptions as PbWebhookOptions,
};
use session::context::QueryContextRef;
use snafu::ensure;
use sql::ast::{ObjectName, ObjectNamePartExt};
use sql::statements::create::trigger::{ChannelType, CreateTrigger};

use crate::error::Result;

pub fn to_create_trigger_task_expr(
    create_trigger: CreateTrigger,
    query_ctx: &QueryContextRef,
) -> Result<PbCreateTriggerExpr> {
    let CreateTrigger {
        trigger_name,
        if_not_exists,
        query,
        interval,
        labels,
        annotations,
        channels,
    } = create_trigger;

    let catalog_name = query_ctx.current_catalog().to_string();
    let trigger_name = sanitize_trigger_name(trigger_name)?;

    let channels = channels
        .into_iter()
        .map(|c| {
            let name = c.name.value;
            match c.channel_type {
                ChannelType::Webhook(am) => PbNotifyChannel {
                    name,
                    channel_type: Some(PbChannelType::Webhook(PbWebhookOptions {
                        url: am.url.value,
                        opts: am.options.into_map(),
                    })),
                },
            }
        })
        .collect::<Vec<_>>();

    let sql = query.to_string();
    let labels = labels.into_map();
    let annotations = annotations.into_map();

    Ok(PbCreateTriggerExpr {
        catalog_name,
        trigger_name,
        create_if_not_exists: if_not_exists,
        sql,
        channels,
        labels,
        annotations,
        interval,
    })
}

fn sanitize_trigger_name(mut trigger_name: ObjectName) -> Result<String> {
    ensure!(
        trigger_name.0.len() == 1,
        crate::error::InvalidTriggerNameSnafu {
            name: trigger_name.to_string(),
        }
    );
    // safety: we've checked trigger_name.0 has exactly one element.
    Ok(trigger_name.0.swap_remove(0).to_string_unquoted())
}

#[cfg(test)]
mod tests {
    use session::context::QueryContext;
    use sql::dialect::GreptimeDbDialect;
    use sql::parser::{ParseOptions, ParserContext};
    use sql::statements::statement::Statement;

    use super::*;

    #[test]
    fn test_sanitize_trigger_name() {
        let name = vec![sql::ast::Ident::new("my_trigger")].into();
        let sanitized = sanitize_trigger_name(name).unwrap();
        assert_eq!(sanitized, "my_trigger");

        let name = vec![sql::ast::Ident::with_quote('`', "my_trigger")].into();
        let sanitized = sanitize_trigger_name(name).unwrap();
        assert_eq!(sanitized, "my_trigger");

        let name = vec![sql::ast::Ident::with_quote('\'', "trigger")].into();
        let sanitized = sanitize_trigger_name(name).unwrap();
        assert_eq!(sanitized, "trigger");
    }

    #[test]
    fn test_to_create_trigger_task_expr() {
        let sql = r#"CREATE TRIGGER IF NOT EXISTS cpu_monitor
ON (SELECT host AS host_label, cpu, memory FROM machine_monitor WHERE cpu > 2) EVERY '5 minute'::INTERVAL
LABELS (label_name=label_val)
ANNOTATIONS (annotation_name=annotation_val)
NOTIFY
(WEBHOOK alert_manager URL 'http://127.0.0.1:9093' WITH (timeout='1m'))"#;

        let stmt =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap()
                .pop()
                .unwrap();

        let Statement::CreateTrigger(stmt) = stmt else {
            unreachable!()
        };

        let query_ctx = QueryContext::arc();
        let expr = to_create_trigger_task_expr(stmt, &query_ctx).unwrap();

        assert_eq!("greptime", expr.catalog_name);
        assert_eq!("cpu_monitor", expr.trigger_name);
        assert!(expr.create_if_not_exists);
        assert_eq!(
            "(SELECT host AS host_label, cpu, memory FROM machine_monitor WHERE cpu > 2)",
            expr.sql
        );
        assert_eq!(300, expr.interval);
        assert_eq!(1, expr.labels.len());
        assert_eq!("label_val", expr.labels.get("label_name").unwrap());
        assert_eq!(1, expr.annotations.len());
        assert_eq!(
            "annotation_val",
            expr.annotations.get("annotation_name").unwrap()
        );
        assert_eq!(1, expr.channels.len());
        let c = &expr.channels[0];
        assert_eq!("alert_manager", c.name,);
        let channel_type = c.channel_type.as_ref().unwrap();
        let PbChannelType::Webhook(am) = &channel_type;
        assert_eq!("http://127.0.0.1:9093", am.url);
        assert_eq!(1, am.opts.len());
        assert_eq!(
            "1m",
            am.opts.get("timeout").expect("Expected timeout option")
        );
    }
}
