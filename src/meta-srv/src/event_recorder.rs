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

use api::v1::RowInsertRequests;
use async_trait::async_trait;
use client::{Client, Database};
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_PRIVATE_SCHEMA_NAME};
use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_meta::cluster::{NodeInfo, NodeInfoKey, Role as ClusterRole};
use common_meta::kv_backend::ResettableKvBackendRef;
use common_meta::rpc::store::RangeRequest;
use common_telemetry::{debug, error, info};
use snafu::ResultExt;
use store_api::mito_engine_options::{APPEND_MODE_KEY, TTL_KEY};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::JoinHandle;

use crate::error::{InsertEventsSnafu, KvBackendSnafu, Result, SendEventSnafu};

mod region_migration_event;

pub use region_migration_event::*;

const DEFAULT_EVENTS_CHANNEL_SIZE: usize = 2048;
const DEFAULT_EVENTS_TABLE_TTL: &str = "30d";

/// EventRecorderRef is the reference to the event recorder.
pub type EventRecorderRef = Arc<dyn EventRecorder>;

/// Event trait defines the interface for events that can be recorded and persisted as the system table.
pub trait Event: Send + Sync + Debug + 'static {
    /// Returns the name of the event.
    fn name(&self) -> &str;

    /// Generates the row inserts request based on the event. The request will be sent to the frontend by the event handler.
    fn to_row_inserts(&self) -> RowInsertRequests;

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
    async fn handle(&self, event: Box<dyn Event>) -> Result<()>;
}

/// Implementation of [EventRecorder] that records the events and forwards them to frontend instances for persistence as system tables.
#[derive(Clone)]
pub struct EventRecorderImpl {
    // The channel to send the events to the background processor.
    tx: Sender<Box<dyn Event>>,
    // The in-memory key-value backend to fetch the available frontend addresses.
    in_memory_key: ResettableKvBackendRef,
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
            in_memory_key: self.in_memory_key.clone(),
        })
    }
}

impl EventRecorderImpl {
    /// Creates a new event recorder to record important events and persist them to the database.
    pub fn new(in_memory_key: ResettableKvBackendRef) -> Self {
        let (tx, rx) = channel(DEFAULT_EVENTS_CHANNEL_SIZE);

        let mut recorder = Self {
            tx,
            in_memory_key,
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
    in_memory_key: ResettableKvBackendRef,
}

#[async_trait]
impl EventHandler for DefaultEventHandlerImpl {
    async fn handle(&self, event: Box<dyn Event>) -> Result<()> {
        let database_client = self.build_database_client().await?;
        let row_inserts = event.to_row_inserts();

        debug!("Inserting event: {:?}", row_inserts);

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
        // Build a range request to get all available frontend addresses.
        let range_request = RangeRequest::new()
            .with_prefix(NodeInfoKey::key_prefix_with_role(ClusterRole::Frontend));
        let response = self
            .in_memory_key
            .range(range_request)
            .await
            .context(KvBackendSnafu)?;

        let mut urls = Vec::with_capacity(response.kvs.len());
        for kv in response.kvs {
            let node_info = NodeInfo::try_from(kv.value).context(KvBackendSnafu)?;
            urls.push(node_info.peer.addr);
        }

        debug!("Available frontend addresses: {:?}", urls);

        Ok(Database::new(
            DEFAULT_CATALOG_NAME,
            DEFAULT_PRIVATE_SCHEMA_NAME,
            Client::with_urls(urls),
        ))
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
        while let Some(event) = self.rx.recv().await {
            debug!("Received event: {:?}", event);
            if let Err(e) = self.event_handler.handle(event).await {
                error!(e; "Failed to handle event");
            }
        }
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
        async fn handle(&self, event: Box<dyn Event>) -> Result<()> {
            let event = event
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
                self.event_handler.handle(event).await.unwrap()
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
