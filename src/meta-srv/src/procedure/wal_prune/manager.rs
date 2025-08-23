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

use std::collections::hash_set::Entry;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, RwLock};

use common_procedure::{watcher, ProcedureId, ProcedureManagerRef, ProcedureWithId};
use common_telemetry::{debug, error, info, warn};
use futures::future::join_all;
use snafu::{OptionExt, ResultExt};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Semaphore;

use crate::define_ticker;
use crate::error::{self, Result};
use crate::metrics::METRIC_META_REMOTE_WAL_PRUNE_EXECUTE;
use crate::procedure::wal_prune::utils::{find_pruneable_entry_id_for_topic, should_trigger_prune};
use crate::procedure::wal_prune::{Context as WalPruneContext, WalPruneProcedure};

pub type WalPruneTickerRef = Arc<WalPruneTicker>;

/// Tracks running [WalPruneProcedure]s and the resources they hold.
/// A [WalPruneProcedure] is holding a semaphore permit to limit the number of concurrent procedures.
///
/// TODO(CookiePie): Similar to [RegionMigrationProcedureTracker], maybe can refactor to a unified framework.
#[derive(Clone)]
pub struct WalPruneProcedureTracker {
    running_procedures: Arc<RwLock<HashSet<String>>>,
}

impl WalPruneProcedureTracker {
    /// Insert a running [WalPruneProcedure] for the given topic name and
    /// consume acquire a semaphore permit for the given topic name.
    pub fn insert_running_procedure(&self, topic_name: String) -> Option<WalPruneProcedureGuard> {
        let mut running_procedures = self.running_procedures.write().unwrap();
        match running_procedures.entry(topic_name.clone()) {
            Entry::Occupied(_) => None,
            Entry::Vacant(entry) => {
                entry.insert();
                Some(WalPruneProcedureGuard {
                    topic_name,
                    running_procedures: self.running_procedures.clone(),
                })
            }
        }
    }

    /// Number of running [WalPruneProcedure]s.
    pub fn len(&self) -> usize {
        self.running_procedures.read().unwrap().len()
    }
}

/// [WalPruneProcedureGuard] is a guard for [WalPruneProcedure].
/// It is used to track the running [WalPruneProcedure]s.
/// When the guard is dropped, it will remove the topic name from the running procedures and release the semaphore.
pub struct WalPruneProcedureGuard {
    topic_name: String,
    running_procedures: Arc<RwLock<HashSet<String>>>,
}

impl Drop for WalPruneProcedureGuard {
    fn drop(&mut self) {
        let mut running_procedures = self.running_procedures.write().unwrap();
        running_procedures.remove(&self.topic_name);
    }
}

/// Event is used to notify the [WalPruneManager] to do some work.
///
/// - `Tick`: Trigger a submission of [WalPruneProcedure] to prune remote WAL.
pub enum Event {
    Tick,
}

impl Debug for Event {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Event::Tick => write!(f, "Tick"),
        }
    }
}

define_ticker!(
    /// [WalPruneTicker] is a ticker that periodically sends [Event]s to the [WalPruneManager].
    /// It is used to trigger the [WalPruneManager] to submit [WalPruneProcedure]s.
    WalPruneTicker,
    event_type = Event,
    event_value = Event::Tick
);

/// [WalPruneManager] manages all remote WAL related tasks in metasrv.
///
/// [WalPruneManager] is responsible for:
/// 1. Registering [WalPruneProcedure] loader in the procedure manager.
/// 2. Periodically receive [Event::Tick] to submit [WalPruneProcedure] to prune remote WAL.
/// 3. Use a semaphore to limit the number of concurrent [WalPruneProcedure]s.
pub(crate) struct WalPruneManager {
    /// Receives [Event]s.
    receiver: Receiver<Event>,
    /// Procedure manager.
    procedure_manager: ProcedureManagerRef,
    /// Tracker for running [WalPruneProcedure]s.
    tracker: WalPruneProcedureTracker,
    /// Semaphore to limit the number of concurrent [WalPruneProcedure]s.
    semaphore: Arc<Semaphore>,

    /// Context for [WalPruneProcedure].
    wal_prune_context: WalPruneContext,
}

impl WalPruneManager {
    /// Returns a new empty [`WalPruneManager`].
    pub fn new(
        parallelism: usize,
        receiver: Receiver<Event>,
        procedure_manager: ProcedureManagerRef,
        wal_prune_context: WalPruneContext,
    ) -> Self {
        Self {
            receiver,
            procedure_manager,
            wal_prune_context,
            tracker: WalPruneProcedureTracker {
                running_procedures: Arc::new(RwLock::new(HashSet::new())),
            },
            semaphore: Arc::new(Semaphore::new(parallelism)),
        }
    }

    /// Start the [WalPruneManager]. It will register [WalPruneProcedure] loader in the procedure manager.
    pub async fn try_start(mut self) -> Result<()> {
        let context = self.wal_prune_context.clone();
        let tracker = self.tracker.clone();
        self.procedure_manager
            .register_loader(
                WalPruneProcedure::TYPE_NAME,
                Box::new(move |json| {
                    let tracker = tracker.clone();
                    WalPruneProcedure::from_json(json, &context, tracker).map(|p| Box::new(p) as _)
                }),
            )
            .context(error::RegisterProcedureLoaderSnafu {
                type_name: WalPruneProcedure::TYPE_NAME,
            })?;
        common_runtime::spawn_global(async move {
            self.run().await;
        });
        info!("WalPruneProcedureManager Started.");
        Ok(())
    }

    /// Returns a mpsc channel with a buffer capacity of 1024 for sending and receiving `Event` messages.
    pub(crate) fn channel() -> (Sender<Event>, Receiver<Event>) {
        tokio::sync::mpsc::channel(1024)
    }

    /// Runs the main loop. Performs actions on received events.
    ///
    /// - `Tick`: Submit `limit` [WalPruneProcedure]s to prune remote WAL.
    pub(crate) async fn run(&mut self) {
        while let Some(event) = self.receiver.recv().await {
            match event {
                Event::Tick => self.handle_tick_request().await.unwrap_or_else(|e| {
                    error!(e; "Failed to handle tick request");
                }),
            }
        }
    }

    /// Submits a [WalPruneProcedure] for the given topic name.
    pub async fn wait_procedure(
        &self,
        topic_name: &str,
        prunable_entry_id: u64,
    ) -> Result<ProcedureId> {
        let guard = self
            .tracker
            .insert_running_procedure(topic_name.to_string())
            .with_context(|| error::PruneTaskAlreadyRunningSnafu { topic: topic_name })?;

        let procedure = WalPruneProcedure::new(
            self.wal_prune_context.clone(),
            Some(guard),
            topic_name.to_string(),
            prunable_entry_id,
        );
        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));
        let procedure_id = procedure_with_id.id;
        METRIC_META_REMOTE_WAL_PRUNE_EXECUTE
            .with_label_values(&[topic_name])
            .inc();
        let procedure_manager = self.procedure_manager.clone();
        let mut watcher = procedure_manager
            .submit(procedure_with_id)
            .await
            .context(error::SubmitProcedureSnafu)?;
        watcher::wait(&mut watcher)
            .await
            .context(error::WaitProcedureSnafu)?;

        Ok(procedure_id)
    }

    async fn try_prune(&self, topic_name: &str) -> Result<()> {
        let table_metadata_manager = self.wal_prune_context.table_metadata_manager.clone();
        let leader_region_registry = self.wal_prune_context.leader_region_registry.clone();
        let prunable_entry_id = find_pruneable_entry_id_for_topic(
            &table_metadata_manager,
            &leader_region_registry,
            topic_name,
        )
        .await?;
        let Some(prunable_entry_id) = prunable_entry_id else {
            debug!(
                "No prunable entry id found for topic {}, skipping prune",
                topic_name
            );
            return Ok(());
        };
        let current = table_metadata_manager
            .topic_name_manager()
            .get(topic_name)
            .await
            .context(error::TableMetadataManagerSnafu)?
            .map(|v| v.into_inner().pruned_entry_id);
        if !should_trigger_prune(current, prunable_entry_id) {
            debug!(
                "No need to prune topic {}, current pruned entry id: {:?}, prunable entry id: {}",
                topic_name, current, prunable_entry_id
            );
            return Ok(());
        }

        self.wait_procedure(topic_name, prunable_entry_id)
            .await
            .map(|_| ())
    }

    async fn handle_tick_request(&self) -> Result<()> {
        let topics = self.retrieve_sorted_topics().await?;
        let mut tasks = Vec::with_capacity(topics.len());
        for topic_name in topics.iter() {
            tasks.push(async {
                let _permit = self.semaphore.acquire().await.unwrap();
                match self.try_prune(topic_name).await {
                    Ok(_) => {}
                    Err(error::Error::PruneTaskAlreadyRunning { topic, .. }) => {
                        warn!("Prune task for topic {} is already running", topic);
                    }
                    Err(e) => {
                        error!(
                            "Failed to submit prune task for topic {}: {}",
                            topic_name.clone(),
                            e
                        );
                    }
                }
            });
        }

        join_all(tasks).await;
        Ok(())
    }

    /// Retrieve topics from the table metadata manager.
    /// Since [WalPruneManager] submits procedures depending on the order of the topics, we should sort the topics.
    /// TODO(CookiePie): Can register topics in memory instead of retrieving from the table metadata manager every time.
    async fn retrieve_sorted_topics(&self) -> Result<Vec<String>> {
        self.wal_prune_context
            .table_metadata_manager
            .topic_name_manager()
            .range()
            .await
            .context(error::TableMetadataManagerSnafu)
    }
}

#[cfg(test)]
mod test {
    use std::assert_matches::assert_matches;
    use std::time::Duration;

    use common_meta::key::topic_name::TopicNameKey;
    use common_meta::leadership_notifier::LeadershipChangeListener;
    use common_wal::maybe_skip_kafka_integration_test;
    use common_wal::test_util::get_kafka_endpoints;
    use tokio::time::{sleep, timeout};

    use super::*;
    use crate::procedure::test_util::new_wal_prune_metadata;
    use crate::procedure::wal_prune::test_util::TestEnv;

    #[tokio::test]
    async fn test_wal_prune_ticker() {
        common_telemetry::init_default_ut_logging();
        let (tx, mut rx) = WalPruneManager::channel();
        let interval = Duration::from_millis(50);
        let ticker = WalPruneTicker::new(interval, tx);
        assert_eq!(ticker.name(), "WalPruneTicker");

        for _ in 0..2 {
            ticker.start();
            // wait a bit longer to make sure not all ticks are skipped
            sleep(4 * interval).await;
            assert!(!rx.is_empty());
            while let Ok(event) = rx.try_recv() {
                assert_matches!(event, Event::Tick);
            }
        }
        ticker.stop();
    }

    #[tokio::test]
    async fn test_wal_prune_tracker_and_guard() {
        let tracker = WalPruneProcedureTracker {
            running_procedures: Arc::new(RwLock::new(HashSet::new())),
        };
        let topic_name = uuid::Uuid::new_v4().to_string();
        {
            let guard = tracker
                .insert_running_procedure(topic_name.clone())
                .unwrap();
            assert_eq!(guard.topic_name, topic_name);
            assert_eq!(guard.running_procedures.read().unwrap().len(), 1);

            let result = tracker.insert_running_procedure(topic_name.clone());
            assert!(result.is_none());
        }
        assert_eq!(tracker.running_procedures.read().unwrap().len(), 0);
    }

    async fn mock_wal_prune_manager(
        broker_endpoints: Vec<String>,
        limit: usize,
    ) -> (Sender<Event>, WalPruneManager) {
        let test_env = TestEnv::new();
        let (tx, rx) = WalPruneManager::channel();
        let wal_prune_context = test_env.build_wal_prune_context(broker_endpoints).await;
        (
            tx,
            WalPruneManager::new(
                limit,
                rx,
                test_env.procedure_manager.clone(),
                wal_prune_context,
            ),
        )
    }

    async fn mock_topics(manager: &WalPruneManager, topics: &[String]) {
        let topic_name_keys = topics
            .iter()
            .map(|topic| TopicNameKey::new(topic))
            .collect::<Vec<_>>();
        manager
            .wal_prune_context
            .table_metadata_manager
            .topic_name_manager()
            .batch_put(topic_name_keys)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_wal_prune_manager() {
        maybe_skip_kafka_integration_test!();
        let broker_endpoints = get_kafka_endpoints();
        let limit = 6;
        let (tx, manager) = mock_wal_prune_manager(broker_endpoints, limit).await;
        let topics = (0..limit * 2)
            .map(|_| uuid::Uuid::new_v4().to_string())
            .collect::<Vec<_>>();
        mock_topics(&manager, &topics).await;

        let tracker = manager.tracker.clone();
        let handler =
            common_runtime::spawn_global(async move { manager.try_start().await.unwrap() });
        handler.await.unwrap();

        tx.send(Event::Tick).await.unwrap();
        // Wait for at least one procedure to be submitted.
        timeout(Duration::from_millis(100), async move { tracker.len() > 0 })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_find_pruneable_entry_id_for_topic_none() {
        let test_env = TestEnv::new();
        let prunable_entry_id = find_pruneable_entry_id_for_topic(
            &test_env.table_metadata_manager,
            &test_env.leader_region_registry,
            "test_topic",
        )
        .await
        .unwrap();
        assert!(prunable_entry_id.is_none());
    }

    #[tokio::test]
    async fn test_find_pruneable_entry_id_for_topic_some() {
        let test_env = TestEnv::new();
        let topic = "test_topic";
        let expected_prunable_entry_id = new_wal_prune_metadata(
            test_env.table_metadata_manager.clone(),
            test_env.leader_region_registry.clone(),
            2,
            5,
            &[3, 10, 23, 50, 52, 82, 130],
            topic.to_string(),
        )
        .await;
        let prunable_entry_id = find_pruneable_entry_id_for_topic(
            &test_env.table_metadata_manager,
            &test_env.leader_region_registry,
            topic,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(prunable_entry_id, expected_prunable_entry_id);
    }
}
