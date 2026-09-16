// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use api::v1::meta::HeartbeatResponse;
use async_trait::async_trait;
use common_telemetry::error;

use crate::error::Result;
use crate::heartbeat::mailbox::{IncomingMessage, MailboxRef};

pub mod invalidate_table_cache;
pub mod parse_mailbox_message;
pub mod suspend;
#[cfg(test)]
mod tests;

pub type HeartbeatResponseHandlerExecutorRef = Arc<dyn HeartbeatResponseHandlerExecutor>;
pub type HeartbeatResponseHandlerRef = Arc<dyn HeartbeatResponseHandler>;

pub struct HeartbeatResponseHandlerContext {
    pub mailbox: MailboxRef,
    pub response: HeartbeatResponse,
    pub incoming_message: Option<IncomingMessage>,
}

/// HandleControl
///
/// Controls process of handling heartbeat response.
#[derive(Debug, PartialEq)]
pub enum HandleControl {
    Continue,
    Done,
}

impl HeartbeatResponseHandlerContext {
    pub fn new(mailbox: MailboxRef, response: HeartbeatResponse) -> Self {
        Self {
            mailbox,
            response,
            incoming_message: None,
        }
    }
}

/// HeartbeatResponseHandler
///
/// [`HeartbeatResponseHandler::is_acceptable`] returns true if handler can handle incoming [`HeartbeatResponseHandlerContext`].
///
/// [`HeartbeatResponseHandler::handle`] handles all or part of incoming [`HeartbeatResponseHandlerContext`].
#[async_trait]
pub trait HeartbeatResponseHandler: Send + Sync {
    fn is_acceptable(&self, ctx: &HeartbeatResponseHandlerContext) -> bool;

    async fn handle(&self, ctx: &mut HeartbeatResponseHandlerContext) -> Result<HandleControl>;
}

#[async_trait]
pub trait HeartbeatResponseHandlerExecutor: Send + Sync {
    async fn handle(&self, ctx: HeartbeatResponseHandlerContext) -> Result<()>;
}

pub struct HandlerGroupExecutor {
    handlers: Vec<HeartbeatResponseHandlerRef>,
}

impl HandlerGroupExecutor {
    pub fn new(handlers: Vec<HeartbeatResponseHandlerRef>) -> Self {
        Self { handlers }
    }
}

#[async_trait]
impl HeartbeatResponseHandlerExecutor for HandlerGroupExecutor {
    async fn handle(&self, mut ctx: HeartbeatResponseHandlerContext) -> Result<()> {
        for handler in &self.handlers {
            if !handler.is_acceptable(&ctx) {
                continue;
            }

            match handler.handle(&mut ctx).await {
                Ok(HandleControl::Done) => break,
                Ok(HandleControl::Continue) => {}
                Err(e) => {
                    let mailbox_message_id = ctx
                        .response
                        .mailbox_message
                        .as_ref()
                        .map(|message| message.id);
                    let json_payload_len =
                        ctx.response.mailbox_message.as_ref().and_then(|message| {
                            message.payload.as_ref().map(|payload| match payload {
                                api::v1::meta::mailbox_message::Payload::Json(json) => json.len(),
                            })
                        });
                    error!(
                        %e;
                        "Error while handling heartbeat response: mailbox_message_id={mailbox_message_id:?}, json_payload_len={json_payload_len:?}"
                    );
                    break;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod error_log_tests {
    use std::fmt::Debug;
    use std::sync::{Arc, Mutex};

    use api::v1::meta::mailbox_message::Payload;
    use api::v1::meta::{HeartbeatResponse, MailboxMessage};
    use common_telemetry::tracing::field::{Field, Visit};
    use common_telemetry::tracing::{Event, Subscriber};
    use common_telemetry::tracing_subscriber::layer::{Context, SubscriberExt};
    use common_telemetry::{tracing, tracing_subscriber};

    use super::parse_mailbox_message::ParseMailboxMessageHandler;
    use super::*;
    use crate::heartbeat::mailbox::HeartbeatMailbox;

    #[derive(Clone, Default)]
    struct LogCapture(Arc<Mutex<Vec<String>>>);

    impl<S> tracing_subscriber::Layer<S> for LogCapture
    where
        S: Subscriber,
    {
        fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
            let mut visitor = FieldVisitor::default();
            event.record(&mut visitor);
            self.0.lock().unwrap().push(visitor.fields.join(", "));
        }
    }

    #[derive(Default)]
    struct FieldVisitor {
        fields: Vec<String>,
    }

    impl Visit for FieldVisitor {
        fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
            self.fields.push(format!("{}={value:?}", field.name()));
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            self.fields.push(format!("{}={value:?}", field.name()));
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_handler_error_log_excludes_mailbox_payload() {
        let payload_sentinel = "MALFORMED_PACKED_PAYLOAD_MUST_NOT_BE_LOGGED".repeat(256);
        let payload = format!(
            r#"{{"PackedGcRegions":{{"regions":[],"packed_file_refs_manifest":"{}"}}"#,
            payload_sentinel
        );
        let payload_len = payload.len();
        let capture = LogCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let _guard = tracing::subscriber::set_default(subscriber);
        let (mailbox_tx, _) = tokio::sync::mpsc::channel(1);
        let ctx = HeartbeatResponseHandlerContext::new(
            Arc::new(HeartbeatMailbox::new(mailbox_tx)),
            HeartbeatResponse {
                mailbox_message: Some(MailboxMessage {
                    id: 42,
                    subject: "unsafe subject".to_string(),
                    to: "unsafe recipient".to_string(),
                    from: "unsafe sender".to_string(),
                    payload: Some(Payload::Json(payload)),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );
        let executor = HandlerGroupExecutor::new(vec![Arc::new(ParseMailboxMessageHandler)]);

        executor.handle(ctx).await.unwrap();

        let logs = capture.0.lock().unwrap().join("\n");
        assert!(logs.contains("Error while handling heartbeat response"));
        assert!(logs.contains("mailbox_message_id=Some(42)"));
        assert!(logs.contains(&format!("json_payload_len=Some({payload_len})")));
        assert!(!logs.contains(&payload_sentinel));
    }
}
