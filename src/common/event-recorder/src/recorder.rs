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

use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use api::v1::column_data_type_extension::TypeExt;
use api::v1::value::ValueData;
use api::v1::{
    ColumnDataType, ColumnDataTypeExtension, ColumnSchema, JsonTypeExtension, Row,
    RowInsertRequest, RowInsertRequests, Rows, SemanticType,
};
use async_trait::async_trait;
use backon::{BackoffBuilder, ExponentialBuilder};
use common_telemetry::{debug, error, info, warn};
use common_time::timestamp::{TimeUnit, Timestamp};
use serde::{Deserialize, Serialize};
use store_api::mito_engine_options::{APPEND_MODE_KEY, TTL_KEY};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::error::{MismatchedSchemaSnafu, Result};

/// The default table name for storing the events.
pub const DEFAULT_EVENTS_TABLE_NAME: &str = "events";

/// The column name for the event type.
pub const EVENTS_TABLE_TYPE_COLUMN_NAME: &str = "type";
/// The column name for the event payload.
pub const EVENTS_TABLE_PAYLOAD_COLUMN_NAME: &str = "payload";
/// The column name for the event timestamp.
pub const EVENTS_TABLE_TIMESTAMP_COLUMN_NAME: &str = "timestamp";

/// EventRecorderRef is the reference to the event recorder.
pub type EventRecorderRef = Arc<dyn EventRecorder>;

static EVENTS_TABLE_TTL: OnceLock<String> = OnceLock::new();

/// The time interval for flushing batched events to the event handler.
pub const DEFAULT_FLUSH_INTERVAL_SECONDS: Duration = Duration::from_secs(5);
// The default TTL for the events table.
const DEFAULT_EVENTS_TABLE_TTL: &str = "30d";
// The capacity of the tokio channel for transmitting events to background processor.
const DEFAULT_CHANNEL_SIZE: usize = 2048;
// The size of the buffer for batching events before flushing to event handler.
const DEFAULT_BUFFER_SIZE: usize = 100;
// The maximum number of retry attempts when event handler processing fails.
const DEFAULT_MAX_RETRY_TIMES: u64 = 3;

/// Event trait defines the interface for events that can be recorded and persisted as the system table.
/// By default, the event will be persisted as the system table with the following schema:
///
/// - `type`: the type of the event.
/// - `payload`: the JSON bytes of the event.
/// - `timestamp`: the timestamp of the event.
///
/// The event can also add the extra schema and row to the event by overriding the `extra_schema` and `extra_row` methods.
pub trait Event: Send + Sync + Debug {
    /// Returns the table name of the event.
    fn table_name(&self) -> &str {
        DEFAULT_EVENTS_TABLE_NAME
    }

    /// Returns the type of the event.
    fn event_type(&self) -> &str;

    /// Returns the timestamp of the event. Default to the current time.
    fn timestamp(&self) -> Timestamp {
        Timestamp::current_time(TimeUnit::Nanosecond)
    }

    /// Returns the JSON bytes of the event as the payload. It will use JSON type to store the payload.
    fn json_payload(&self) -> Result<String> {
        Ok("".to_string())
    }

    /// Add the extra schema to the event with the default schema.
    fn extra_schema(&self) -> Vec<ColumnSchema> {
        vec![]
    }

    /// Add the extra row to the event with the default row.
    fn extra_row(&self) -> Result<Row> {
        Ok(Row { values: vec![] })
    }

    /// Returns the event as any type.
    fn as_any(&self) -> &dyn Any;
}

/// Eventable trait defines the interface for objects that can be converted to [Event].
pub trait Eventable: Send + Sync + Debug {
    /// Converts the object to an [Event].
    fn to_event(&self) -> Option<Box<dyn Event>> {
        None
    }
}

/// Returns the hints for the insert operation.
pub fn insert_hints() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            TTL_KEY,
            EVENTS_TABLE_TTL
                .get()
                .map(|s| s.as_str())
                .unwrap_or(DEFAULT_EVENTS_TABLE_TTL),
        ),
        (APPEND_MODE_KEY, "true"),
    ]
}

/// Aggregates events by its `event_type`.
pub fn aggregate_events_by_type(events: &[Box<dyn Event>]) -> HashMap<&str, Vec<&Box<dyn Event>>> {
    let mut event_groups: HashMap<&str, Vec<&Box<dyn Event>>> = HashMap::new();
    for event in events {
        event_groups
            .entry(event.event_type())
            .or_default()
            .push(event);
    }
    event_groups
}

/// Builds the row inserts request for the events that will be persisted to the events table.
pub fn build_row_inserts_request(events: &[&Box<dyn Event>]) -> Result<RowInsertRequests> {
    // Ensure all the events are the same type.
    validate_events(&events)?;

    let mut row_insert_requests = RowInsertRequests {
        inserts: Vec::with_capacity(events.len()),
    };

    // We already validated the events, so it's safe to get the first event to build the schema for the RowInsertRequest.
    let event = &events[0];
    let mut schema = vec![
        ColumnSchema {
            column_name: EVENTS_TABLE_TYPE_COLUMN_NAME.to_string(),
            datatype: ColumnDataType::String.into(),
            semantic_type: SemanticType::Tag.into(),
            ..Default::default()
        },
        ColumnSchema {
            column_name: EVENTS_TABLE_PAYLOAD_COLUMN_NAME.to_string(),
            datatype: ColumnDataType::Binary as i32,
            semantic_type: SemanticType::Field as i32,
            datatype_extension: Some(ColumnDataTypeExtension {
                type_ext: Some(TypeExt::JsonType(JsonTypeExtension::JsonBinary.into())),
            }),
            ..Default::default()
        },
        ColumnSchema {
            column_name: EVENTS_TABLE_TIMESTAMP_COLUMN_NAME.to_string(),
            datatype: ColumnDataType::TimestampNanosecond.into(),
            semantic_type: SemanticType::Timestamp.into(),
            ..Default::default()
        },
    ];
    schema.extend(event.extra_schema());

    let rows = events
        .iter()
        .map(|event| {
            let mut row = Row {
                values: vec![
                    ValueData::StringValue(event.event_type().to_string()).into(),
                    ValueData::BinaryValue(event.json_payload()?.as_bytes().to_vec()).into(),
                    ValueData::TimestampNanosecondValue(event.timestamp().value()).into(),
                ],
            };
            row.values.extend(event.extra_row()?.values);
            Ok(row)
        })
        .collect::<Result<Vec<_>>>()?;

    row_insert_requests.inserts.push(RowInsertRequest {
        table_name: event.table_name().to_string(),
        rows: Some(Rows { schema, rows }),
    });

    Ok(row_insert_requests)
}

// Ensure the events with the same event type have the same extra schema.
#[allow(clippy::borrowed_box)]
fn validate_events(events: &[&Box<dyn Event>]) -> Result<()> {
    // It's safe to get the first event because the events are already grouped by the event type.
    let extra_schema = events[0].extra_schema();
    for event in events {
        if event.extra_schema() != extra_schema {
            MismatchedSchemaSnafu {
                expected: extra_schema.clone(),
                actual: event.extra_schema(),
            }
            .fail()?;
        }
    }
    Ok(())
}

/// EventRecorder trait defines the interface for recording events.
pub trait EventRecorder: Send + Sync + Debug + 'static {
    /// Records an event for persistence and processing by [EventHandler].
    fn record(&self, event: Box<dyn Event>);

    /// Cancels the event recorder.
    fn close(&self);
}

/// EventHandlerOptions is the options for the event handler.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventHandlerOptions {
    /// TTL for the events table that will be used to store the events.
    pub ttl: String,
    /// Append mode for the events table that will be used to store the events.
    pub append_mode: bool,
}

impl Default for EventHandlerOptions {
    fn default() -> Self {
        Self {
            ttl: DEFAULT_EVENTS_TABLE_TTL.to_string(),
            append_mode: true,
        }
    }
}

impl EventHandlerOptions {
    /// Converts the options to the hints for the insert operation.
    pub fn to_hints(&self) -> Vec<(&str, &str)> {
        vec![
            (TTL_KEY, self.ttl.as_str()),
            (
                APPEND_MODE_KEY,
                if self.append_mode { "true" } else { "false" },
            ),
        ]
    }
}

/// EventHandler trait defines the interface for how to handle the event.
#[async_trait]
pub trait EventHandler: Send + Sync + 'static {
    /// Processes and handles incoming events. The [DefaultEventHandlerImpl] implementation forwards events to frontend instances for persistence.
    /// We use `&[Box<dyn Event>]` to avoid consuming the events, so the caller can buffer the events and retry if the handler fails.
    async fn handle(&self, events: &[Box<dyn Event>]) -> Result<()>;

    /// Returns the options for the event handler.
    fn options(&self, _event: &Box<dyn Event>) -> EventHandlerOptions {
        EventHandlerOptions::default()
    }
}

/// Configuration options for the event recorder.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventRecorderOptions {
    /// TTL for the events table that will be used to store the events.
    pub ttl: String,
}

impl Default for EventRecorderOptions {
    fn default() -> Self {
        Self {
            ttl: DEFAULT_EVENTS_TABLE_TTL.to_string(),
        }
    }
}

/// Implementation of [EventRecorder] that records the events and processes them in the background by the [EventHandler].
#[derive(Debug)]
pub struct EventRecorderImpl {
    // The channel to send the events to the background processor.
    tx: Sender<Box<dyn Event>>,
    // The cancel token to cancel the background processor.
    cancel_token: CancellationToken,
    // The background processor to process the events.
    handle: Option<JoinHandle<()>>,
}

impl EventRecorderImpl {
    pub fn new(event_handler: Box<dyn EventHandler>, opts: EventRecorderOptions) -> Self {
        info!("Creating event recorder with options: {:?}", opts);

        let (tx, rx) = channel(DEFAULT_CHANNEL_SIZE);
        let cancel_token = CancellationToken::new();

        let mut recorder = Self {
            tx,
            handle: None,
            cancel_token: cancel_token.clone(),
        };

        let processor = EventProcessor::new(
            rx,
            event_handler,
            DEFAULT_FLUSH_INTERVAL_SECONDS,
            DEFAULT_MAX_RETRY_TIMES,
        )
        .with_cancel_token(cancel_token);

        // Spawn a background task to process the events.
        let handle = tokio::spawn(async move {
            processor.process(DEFAULT_BUFFER_SIZE).await;
        });

        recorder.handle = Some(handle);

        // It only sets the ttl once, so it's safe to skip the error.
        if EVENTS_TABLE_TTL.set(opts.ttl.clone()).is_err() {
            info!(
                "Events table ttl already set to {}, skip setting it",
                opts.ttl
            );
        }

        recorder
    }
}

impl EventRecorder for EventRecorderImpl {
    // Accepts an event and send it to the background handler.
    fn record(&self, event: Box<dyn Event>) {
        if let Err(e) = self.tx.try_send(event) {
            error!("Failed to send event to the background processor: {}", e);
        }
    }

    // Closes the event recorder. It will stop the background processor and flush the buffer.
    fn close(&self) {
        self.cancel_token.cancel();
    }
}

impl Drop for EventRecorderImpl {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
            info!("Aborted the background processor in event recorder");
        }
    }
}

struct EventProcessor {
    rx: Receiver<Box<dyn Event>>,
    event_handler: Box<dyn EventHandler>,
    max_retry_times: u64,
    process_interval: Duration,
    cancel_token: CancellationToken,
}

impl EventProcessor {
    fn new(
        rx: Receiver<Box<dyn Event>>,
        event_handler: Box<dyn EventHandler>,
        process_interval: Duration,
        max_retry_times: u64,
    ) -> Self {
        Self {
            rx,
            event_handler,
            max_retry_times,
            process_interval,
            cancel_token: CancellationToken::new(),
        }
    }

    fn with_cancel_token(mut self, cancel_token: CancellationToken) -> Self {
        self.cancel_token = cancel_token;
        self
    }

    async fn process(mut self, buffer_size: usize) {
        info!("Start the background processor in event recorder to handle the received events.");

        let mut buffer = Vec::with_capacity(buffer_size);
        let mut interval = tokio::time::interval(self.process_interval);

        loop {
            tokio::select! {
                maybe_event = self.rx.recv() => {
                    if let Some(maybe_event) = maybe_event {
                        debug!("Received event: {:?}", maybe_event);

                        if buffer.len() >= buffer_size {
                            debug!(
                                "Flushing events to the event handler because the buffer is full with {} events",
                                buffer.len()
                            );
                            self.flush_events_to_handler(&mut buffer).await;
                        }

                        // Push the event to the buffer, the buffer will be flushed when the interval is triggered or received a closed signal.
                        buffer.push(maybe_event);
                    } else {
                        // When received a closed signal, flush the buffer and exit the loop.
                        self.flush_events_to_handler(&mut buffer).await;
                        break;
                    }
                }
                // Cancel the processor through the cancel token.
                _ = self.cancel_token.cancelled() => {
                    warn!("Received a cancel signal, flushing the buffer and exiting the loop");
                    self.flush_events_to_handler(&mut buffer).await;
                    break;
                }
                // When the interval is triggered, flush the buffer and send the events to the event handler.
                _ = interval.tick() => {
                    self.flush_events_to_handler(&mut buffer).await;
                }
            }
        }
    }

    // NOTE: While we implement a retry mechanism for failed event handling, there is no guarantee that all events will be processed successfully.
    async fn flush_events_to_handler(&self, buffer: &mut Vec<Box<dyn Event>>) {
        if !buffer.is_empty() {
            debug!("Flushing {} events to the event handler", buffer.len());

            let mut backoff = ExponentialBuilder::default()
                .with_min_delay(Duration::from_millis(
                    DEFAULT_FLUSH_INTERVAL_SECONDS.as_millis() as u64 / self.max_retry_times.max(1),
                ))
                .with_max_delay(Duration::from_millis(
                    DEFAULT_FLUSH_INTERVAL_SECONDS.as_millis() as u64,
                ))
                .with_max_times(self.max_retry_times as usize)
                .build();

            loop {
                match self.event_handler.handle(buffer).await {
                    Ok(()) => {
                        debug!("Successfully handled {} events", buffer.len());
                        break;
                    }
                    Err(e) => {
                        if let Some(d) = backoff.next() {
                            warn!(e; "Failed to handle events, retrying...");
                            sleep(d).await;
                            continue;
                        } else {
                            warn!(
                                e; "Failed to handle events after {} retries",
                                self.max_retry_times
                            );
                            break;
                        }
                    }
                }
            }
        }

        // Clear the buffer to prevent unbounded memory growth, regardless of whether event processing succeeded or failed.
        buffer.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct TestEvent {}

    impl Event for TestEvent {
        fn event_type(&self) -> &str {
            "test_event"
        }

        fn json_payload(&self) -> Result<String> {
            Ok("{\"procedure_id\": \"1234567890\"}".to_string())
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    struct TestEventHandlerImpl {}

    #[async_trait]
    impl EventHandler for TestEventHandlerImpl {
        async fn handle(&self, events: &[Box<dyn Event>]) -> Result<()> {
            let event = events
                .first()
                .unwrap()
                .as_any()
                .downcast_ref::<TestEvent>()
                .unwrap();
            assert_eq!(
                event.json_payload().unwrap(),
                "{\"procedure_id\": \"1234567890\"}"
            );
            assert_eq!(event.event_type(), "test_event");
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_event_recorder() {
        let mut event_recorder = EventRecorderImpl::new(
            Box::new(TestEventHandlerImpl {}),
            EventRecorderOptions::default(),
        );
        event_recorder.record(Box::new(TestEvent {}));

        // Sleep for a while to let the event be sent to the event handler.
        sleep(Duration::from_millis(500)).await;

        // Close the event recorder to flush the buffer.
        event_recorder.close();

        // Sleep for a while to let the background task process the event.
        sleep(Duration::from_millis(500)).await;

        if let Some(handle) = event_recorder.handle.take() {
            assert!(handle.await.is_ok());
        }
    }

    struct TestEventHandlerImplShouldPanic {}

    #[async_trait]
    impl EventHandler for TestEventHandlerImplShouldPanic {
        async fn handle(&self, events: &[Box<dyn Event>]) -> Result<()> {
            let event = events
                .first()
                .unwrap()
                .as_any()
                .downcast_ref::<TestEvent>()
                .unwrap();

            // Set the incorrect payload and event type to trigger the panic.
            assert_eq!(
                event.json_payload().unwrap(),
                "{\"procedure_id\": \"should_panic\"}"
            );
            assert_eq!(event.event_type(), "should_panic");
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_event_recorder_should_panic() {
        let mut event_recorder = EventRecorderImpl::new(
            Box::new(TestEventHandlerImplShouldPanic {}),
            EventRecorderOptions::default(),
        );

        event_recorder.record(Box::new(TestEvent {}));

        // Sleep for a while to let the event be sent to the event handler.
        sleep(Duration::from_millis(500)).await;

        // Close the event recorder to flush the buffer.
        event_recorder.close();

        // Sleep for a while to let the background task process the event.
        sleep(Duration::from_millis(500)).await;

        if let Some(handle) = event_recorder.handle.take() {
            assert!(handle.await.unwrap_err().is_panic());
        }
    }
}
