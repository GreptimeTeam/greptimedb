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
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use api::v1::{RowInsertRequest, RowInsertRequests};
use async_trait::async_trait;
use client::{Client, Database};
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_PRIVATE_SCHEMA_NAME};
use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_grpc::channel_manager::ChannelManager;
use common_meta::peer::PeerLookupServiceRef;
use common_telemetry::{debug, error, info};
use snafu::ResultExt;
use store_api::mito_engine_options::{APPEND_MODE_KEY, TTL_KEY};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::sleep;

use crate::cluster::MetaPeerClientRef;
use crate::error::{
    InsertEventsSnafu, KvBackendSnafu, NoAvailableFrontendSnafu, Result, SendEventSnafu,
};
use crate::lease::MetaPeerLookupService;

mod region_migration_event;

pub use region_migration_event::*;

// TODO(zyy17): Make these constants configurable.
const DEFAULT_EVENTS_CHANNEL_SIZE: usize = 2048;
const DEFAULT_EVENTS_BATCH_SIZE: usize = 100;
const DEFAULT_EVENTS_TABLE_TTL: &str = "30d";
const DEFAULT_EVENTS_PROCESS_INTERVAL_SECONDS: u64 = 5;
const DEFAULT_EVENTS_MAX_RETRY_TIMES: u64 = 3;

/// EventRecorderRef is the reference to the event recorder.
pub type EventRecorderRef = Arc<dyn EventRecorder>;

/// Event trait defines the interface for events that can be recorded and persisted as the system table.
pub trait Event: Send + Sync + Debug + 'static {
    /// Returns the name of the event.
    fn name(&self) -> &str;

    /// Generates the row inserts request based on the event. The request will be sent to the frontend by the event handler.
    fn to_row_insert(&self) -> RowInsertRequest;

    /// Returns the event as any type.
    fn as_any(&self) -> &dyn Any;
}

/// EventRecorder trait defines the interface for recording events.
pub trait EventRecorder: Send + Sync + 'static {
    /// Records an event for persistence and processing by [EventHandler].
    fn record(&self, event: Box<dyn Event>) -> Result<()>;

    /// Constructs and returns an [EventHandler] instance responsible for processing received event.
    fn build_event_handler(&self) -> Box<dyn EventHandler>;
}

/// EventHandler trait defines the interface for how to handle the event.
#[async_trait]
pub trait EventHandler: Send + Sync + 'static {
    /// Processes and handles incoming events. The [DefaultEventHandlerImpl] implementation forwards events to frontend instances for persistence.
    /// We use `&[Box<dyn Event>]` to avoid consuming the events, so the caller can buffer the events and retry if the handler fails.
    async fn handle(&self, events: &[Box<dyn Event>]) -> Result<()>;
}

/// Implementation of [EventRecorder] that records the events and forwards them to frontend instances for persistence as system tables.
#[derive(Clone)]
pub struct EventRecorderImpl {
    // The channel to send the events to the background processor.
    tx: Sender<Box<dyn Event>>,
    // The peer lookup service to fetch the available frontend addresses.
    peer_lookup_service: PeerLookupServiceRef,
    // The background processor to process the events.
    _handle: Option<Arc<JoinHandle<()>>>,
}

impl EventRecorder for EventRecorderImpl {
    // Accepts an event and send it to the background handler.
    fn record(&self, event: Box<dyn Event>) -> Result<()> {
        self.tx
            .try_send(event)
            .map_err(|e| BoxedError::new(PlainError::new(e.to_string(), StatusCode::Internal)))
            .context(SendEventSnafu)
    }

    // The default event handler implementation sends the received events to the frontend instances.
    fn build_event_handler(&self) -> Box<dyn EventHandler> {
        Box::new(DefaultEventHandlerImpl {
            peer_lookup_service: self.peer_lookup_service.clone(),
            channel_manager: ChannelManager::new(),
        })
    }
}

impl EventRecorderImpl {
    /// Creates a new event recorder to record important events and persist them to the database.
    pub fn new(meta_peer_client: MetaPeerClientRef) -> Self {
        let (tx, rx) = channel(DEFAULT_EVENTS_CHANNEL_SIZE);
        let peer_lookup_service = Arc::new(MetaPeerLookupService::new(meta_peer_client));

        let mut recorder = Self {
            tx,
            peer_lookup_service,
            _handle: None,
        };

        let processor = EventProcessor::new(rx, recorder.build_event_handler());

        // Spawn a background task to process the events.
        let handle = tokio::spawn(async move {
            processor.process().await;
        });

        recorder._handle = Some(Arc::new(handle));

        recorder
    }
}

// DefaultEventHandlerImpl is the default event handler implementation. It sends the received events to the frontend instances.
struct DefaultEventHandlerImpl {
    peer_lookup_service: PeerLookupServiceRef,
    channel_manager: ChannelManager,
}

#[async_trait]
impl EventHandler for DefaultEventHandlerImpl {
    async fn handle(&self, events: &[Box<dyn Event>]) -> Result<()> {
        debug!("Received {} events: {:?}", events.len(), events);
        let database_client = self.build_database_client().await?;
        let row_inserts = RowInsertRequests {
            inserts: events.iter().map(|e| e.to_row_insert()).collect(),
        };
        debug!("Inserting events: {:?}", row_inserts);

        database_client
            .row_inserts_with_hints(
                row_inserts,
                &[
                    (TTL_KEY, DEFAULT_EVENTS_TABLE_TTL),
                    (APPEND_MODE_KEY, "true"),
                ],
            )
            .await
            .context(InsertEventsSnafu)?;

        Ok(())
    }
}

impl DefaultEventHandlerImpl {
    async fn build_database_client(&self) -> Result<Database> {
        let frontends = self
            .peer_lookup_service
            .active_frontends()
            .await
            .context(KvBackendSnafu)?;

        if frontends.is_empty() {
            return NoAvailableFrontendSnafu.fail();
        }

        let urls = frontends
            .into_iter()
            .map(|peer| peer.addr)
            .collect::<Vec<_>>();

        debug!("Available frontend addresses: {:?}", urls);

        Ok(Database::new(
            DEFAULT_CATALOG_NAME,
            DEFAULT_PRIVATE_SCHEMA_NAME,
            Client::with_manager_and_urls(self.channel_manager.clone(), urls),
        ))
    }
}

impl Drop for EventRecorderImpl {
    fn drop(&mut self) {
        if let Some(handle) = self._handle.take() {
            handle.abort();
            info!("Aborted the background processor in event recorder");
        }
    }
}

struct EventProcessor {
    rx: Receiver<Box<dyn Event>>,
    event_handler: Box<dyn EventHandler>,
}

impl EventProcessor {
    fn new(rx: Receiver<Box<dyn Event>>, event_handler: Box<dyn EventHandler>) -> Self {
        Self { rx, event_handler }
    }

    async fn process(mut self) {
        info!("Start the background processor in event recorder to handle the received events.");

        let mut buffer = Vec::with_capacity(DEFAULT_EVENTS_BATCH_SIZE);
        let mut interval =
            tokio::time::interval(Duration::from_secs(DEFAULT_EVENTS_PROCESS_INTERVAL_SECONDS));

        loop {
            tokio::select! {
                maybe_event = self.rx.recv() => {
                    if let Some(maybe_event) = maybe_event {
                        debug!("Received event: {:?}", maybe_event);
                        // Push the event to the buffer, the buffer will be flushed when the interval is triggered or received a closed signal.
                        buffer.push(maybe_event);
                    } else {
                        // When received a closed signal, flush the buffer and exit the loop.
                        self.flush_events_to_handler(&mut buffer).await;
                        break;
                    }
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

            // Use the fixed interval to retry the failed events.
            let mut retry = 1;
            while retry <= DEFAULT_EVENTS_MAX_RETRY_TIMES {
                if let Err(e) = self.event_handler.handle(buffer).await {
                    error!(
                        e; "Failed to handle events, retrying... ({}/{})",
                        retry, DEFAULT_EVENTS_MAX_RETRY_TIMES
                    );
                    retry += 1;
                    sleep(Duration::from_millis(
                        DEFAULT_EVENTS_PROCESS_INTERVAL_SECONDS * 1000
                            / DEFAULT_EVENTS_MAX_RETRY_TIMES,
                    ))
                    .await;
                } else {
                    info!("Successfully handled {} events", buffer.len());
                    break;
                }

                if retry > DEFAULT_EVENTS_MAX_RETRY_TIMES {
                    error!(
                        "Failed to handle {} events after {} retries",
                        buffer.len(),
                        DEFAULT_EVENTS_MAX_RETRY_TIMES
                    );
                }
            }
        }

        // Clear the buffer to prevent unbounded memory growth, regardless of whether event processing succeeded or failed.
        buffer.clear();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use common_meta::peer::Peer;
    use common_procedure::ProcedureId;
    use store_api::storage::RegionId;
    use tokio::sync::mpsc::{channel, Receiver, Sender};
    use tokio::task::JoinHandle;

    use super::*;
    use crate::procedure::region_migration::{
        RegionMigrationProcedureTask, RegionMigrationTriggerReason,
    };

    const TEST_PROCEDURE_ID: &str = "422cd478-60c6-48ec-82ca-fae095c0f3a7";
    const TEST_REGION_ID: RegionId = RegionId::new(1, 0);

    struct TestEventRecorderImpl {
        tx: Sender<Box<dyn Event>>,
        _handle: Option<Arc<JoinHandle<()>>>,
    }

    impl EventRecorder for TestEventRecorderImpl {
        fn record(&self, event: Box<dyn Event>) -> Result<()> {
            if let Err(e) = self.tx.try_send(event) {
                error!(e; "Failed to send event");
            }
            Ok(())
        }

        fn build_event_handler(&self) -> Box<dyn EventHandler> {
            Box::new(TestEventHandlerImpl {})
        }
    }

    impl TestEventRecorderImpl {
        fn new() -> Self {
            let (tx, rx) = channel(DEFAULT_EVENTS_CHANNEL_SIZE);
            let mut recorder = Self { tx, _handle: None };

            let processor = TestEventProcessor::new(rx, recorder.build_event_handler());

            // Spawn a background task to process the events.
            let handle = tokio::spawn(async move {
                processor.process().await;
            });

            recorder._handle = Some(Arc::new(handle));

            recorder
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
                .downcast_ref::<RegionMigrationEvent>()
                .unwrap();
            assert_eq!(
                event.procedure_id,
                ProcedureId::parse_str(TEST_PROCEDURE_ID).unwrap()
            );
            assert_eq!(event.task.region_id, TEST_REGION_ID);
            assert_eq!(
                event.task.trigger_reason,
                RegionMigrationTriggerReason::Manual
            );
            Ok(())
        }
    }

    struct TestEventProcessor {
        rx: Receiver<Box<dyn Event>>,
        event_handler: Box<dyn EventHandler>,
    }

    impl TestEventProcessor {
        fn new(rx: Receiver<Box<dyn Event>>, event_handler: Box<dyn EventHandler>) -> Self {
            Self { rx, event_handler }
        }

        async fn process(mut self) {
            while let Some(event) = self.rx.recv().await {
                self.event_handler.handle(&[event]).await.unwrap()
            }
        }
    }

    #[tokio::test]
    async fn test_event_recorder() {
        let recorder = TestEventRecorderImpl::new();
        let task = RegionMigrationProcedureTask::new(
            TEST_REGION_ID,
            Peer::new(0, "127.0.0.1:3000"),
            Peer::new(1, "127.0.0.1:3001"),
            Duration::from_secs(10),
            RegionMigrationTriggerReason::Manual,
        );
        let event = Box::new(RegionMigrationEvent::new(
            task,
            ProcedureId::parse_str(TEST_PROCEDURE_ID).unwrap(),
            RegionMigrationStatus::Running,
        ));
        recorder.record(event).unwrap();
    }
}
