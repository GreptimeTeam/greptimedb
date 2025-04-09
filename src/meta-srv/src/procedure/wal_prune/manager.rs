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
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use common_meta::key::TableMetadataManagerRef;
use common_meta::leadership_notifier::LeadershipChangeListener;
use common_procedure::{watcher, ProcedureId, ProcedureManagerRef, ProcedureWithId};
use common_runtime::JoinHandle;
use common_telemetry::{error, info, warn};
use itertools::Itertools;
use snafu::ResultExt;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{interval, MissedTickBehavior};

use crate::error::{RegisterProcedureLoaderSnafu, Result, TableMetadataManagerSnafu};
use crate::metrics::METRIC_META_REMOTE_WAL_PRUNE_EXECUTE;
use crate::procedure::wal_prune::{Context as WalPruneContext, WalPruneProcedure};

/// Default limit of submitting [WalPruneProcedure]s. We prune `limit` topics at a time.
pub const DEFAULT_WAL_PRUNE_LIMIT: usize = 10;
/// Default interval of [WalPruneTicker].
pub const DEFAULT_WAL_PRUNE_INTERVAL: Duration = Duration::from_secs(60 * 5);
/// Default flush threshold of [WalPruneProcedure].
pub const DEFAULT_WAL_PRUNE_FLUSH_THRESHOLD: Option<u64> = None;

pub type WalPruneTickerRef = Arc<WalPruneTicker>;

/// Tracks running [WalPruneProcedure]s.
/// Currently we only track the topic name of the running procedures.
///
/// TODO(CookiePie): Similar to [RegionMigrationProcedureTracker], maybe can refactor to a unified framework.
pub(crate) struct WalPruneProcedureTracker {
    running_procedures: Arc<RwLock<HashSet<String>>>,
}

impl WalPruneProcedureTracker {
    pub(crate) fn insert_running_procedure(
        &self,
        topic_name: String,
    ) -> Option<WalPruneProcedureGuard> {
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
}

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
pub(crate) enum Event {
    Tick,
}

pub(crate) struct WalPruneTicker {
    /// Handle of ticker thread.
    pub(crate) tick_handle: Mutex<Option<JoinHandle<()>>>,
    /// The interval of tick.
    pub(crate) tick_interval: Duration,
    /// Sends [Event]s.
    pub(crate) sender: Sender<Event>,
}

#[async_trait::async_trait]
impl LeadershipChangeListener for WalPruneTicker {
    fn name(&self) -> &'static str {
        "WalPruneTicker"
    }

    async fn on_leader_start(&self) -> common_meta::error::Result<()> {
        self.start();
        Ok(())
    }

    async fn on_leader_stop(&self) -> common_meta::error::Result<()> {
        self.stop();
        Ok(())
    }
}

/// TODO(CookiePie): Similar to [RegionSupervisorTicker], maybe can refactor to a unified framework.
impl WalPruneTicker {
    pub(crate) fn new(tick_interval: Duration, sender: Sender<Event>) -> Self {
        Self {
            tick_handle: Mutex::new(None),
            tick_interval,
            sender,
        }
    }

    /// Starts the ticker.
    pub fn start(&self) {
        let mut handle = self.tick_handle.lock().unwrap();
        if handle.is_none() {
            let sender = self.sender.clone();
            let tick_interval = self.tick_interval;
            let ticker_loop = tokio::spawn(async move {
                let mut interval = interval(tick_interval);
                interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
                loop {
                    interval.tick().await;
                    if sender.send(Event::Tick).await.is_err() {
                        info!("EventReceiver is dropped, tick loop is stopped");
                        break;
                    }
                }
            });
            *handle = Some(ticker_loop);
        }
    }

    /// Stops the ticker.
    pub fn stop(&self) {
        let mut handle = self.tick_handle.lock().unwrap();
        if let Some(handle) = handle.take() {
            handle.abort();
        }
    }
}

impl Drop for WalPruneTicker {
    fn drop(&mut self) {
        self.stop();
    }
}

/// [WalPruneManager] manages all remote WAL related tasks in metasrv.
///
/// [WalPruneManager] is responsible for:
/// 1. Periodically submit [WalPruneProcedure] to prune remote WAL.
pub(crate) struct WalPruneManager {
    /// Table metadata manager to restore topics from kvbackend.
    table_metadata_manager: TableMetadataManagerRef,
    /// Limit of submitting [WalPruneProcedure]s. Since each topic has a [WalPruneProcedure], the limit is the number of topics.
    limit: usize,
    /// Receives [Event]s.
    receiver: Receiver<Event>,
    /// Procedure manager.
    procedure_manager: ProcedureManagerRef,
    /// Tracker for running [WalPruneProcedure]s.
    tracker: WalPruneProcedureTracker,

    /// Context for [WalPruneProcedure].
    wal_prune_context: WalPruneContext,
    trigger_flush_threshold: Option<u64>,
}

impl WalPruneManager {
    /// Returns a new empty [WalPruneManager].
    pub fn new(
        table_metadata_manager: TableMetadataManagerRef,
        limit: usize,
        receiver: Receiver<Event>,
        procedure_manager: ProcedureManagerRef,
        wal_prune_context: WalPruneContext,
        trigger_flush_threshold: Option<u64>,
    ) -> Self {
        Self {
            table_metadata_manager,
            limit,
            receiver,
            procedure_manager,
            wal_prune_context,
            tracker: WalPruneProcedureTracker {
                running_procedures: Arc::new(RwLock::new(HashSet::new())),
            },
            trigger_flush_threshold,
        }
    }

    /// Initializes the [WalPruneManager]. It will register [WalPruneProcedure] loader in the procedure manager.
    pub async fn init(&self) -> Result<()> {
        let context = self.wal_prune_context.clone();
        self.procedure_manager
            .register_loader(
                WalPruneProcedure::TYPE_NAME,
                Box::new(move |json| {
                    WalPruneProcedure::from_json(json, &context).map(|p| Box::new(p) as _)
                }),
            )
            .context(RegisterProcedureLoaderSnafu {
                type_name: WalPruneProcedure::TYPE_NAME,
            })
    }

    /// Returns a mpsc channel with a buffer capacity of 1024 for sending and receiving `Event` messages.
    pub(crate) fn channel() -> (Sender<Event>, Receiver<Event>) {
        tokio::sync::mpsc::channel(1024)
    }

    /// Runs the main loop. Performs actions on received events.
    ///
    /// - `Tick`: Submit `limit` [WalPruneProcedure]s to prune remote WAL.
    pub(crate) async fn run(&mut self) {
        let mut offset = 0;

        while let Some(event) = self.receiver.recv().await {
            match event {
                Event::Tick => {
                    let topics = self.retrieve_sorted_topics().await;
                    if let Err(e) = topics {
                        error!(e; "Failed to retrieve topics from table metadata manager");
                        continue;
                    }
                    let topics = topics.unwrap();
                    for topic_name in topics.iter().cycle().skip(offset).take(self.limit) {
                        if self.submit_procedure(topic_name).await.is_none() {
                            warn!("A remote WAL prune procedure for topic {topic_name} is already running");
                        }
                    }
                    offset += self.limit;
                }
            }
        }
    }

    /// Submits a [WalPruneProcedure] for the given topic name.
    pub async fn submit_procedure(&self, topic_name: &str) -> Option<ProcedureId> {
        let guard = self
            .tracker
            .insert_running_procedure(topic_name.to_string())?;

        let procedure = WalPruneProcedure::new(
            topic_name.to_string(),
            self.wal_prune_context.clone(),
            self.trigger_flush_threshold,
            Some(guard),
        );
        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));
        let procedure_id = procedure_with_id.id;
        METRIC_META_REMOTE_WAL_PRUNE_EXECUTE
            .with_label_values(&[topic_name])
            .inc();
        let topic_name = topic_name.to_string();
        let procedure_manager = self.procedure_manager.clone();
        common_runtime::spawn_global(async move {
            let watcher = &mut match procedure_manager.submit(procedure_with_id).await {
                Ok(watcher) => watcher,
                Err(e) => {
                    info!("Failed to submit procedure: {}", e);
                    return;
                }
            };

            if let Err(e) = watcher::wait(watcher).await {
                error!(e; "Failed to wait for wal prune procedure {procedure_id} for {topic_name}")
            }
        });

        Some(procedure_id)
    }

    /// Retrieve topics from the table metadata manager.
    /// Since [WalPruneManager] submits procedures depending on the order of the topics, we should sort the topics.
    /// TODO(CookiePie): Can register topics in memory instead of retrieving from the table metadata manager every time.
    async fn retrieve_sorted_topics(&self) -> Result<Vec<String>> {
        Ok(self
            .table_metadata_manager
            .topic_name_manager()
            .range()
            .await
            .context(TableMetadataManagerSnafu)?
            .into_iter()
            .sorted()
            .collect::<Vec<_>>())
    }
}

#[cfg(test)]
mod test {
    use common_meta::wal_options_allocator::build_kafka_topic_creator;
    use common_wal::config::kafka::common::{KafkaConnectionConfig, KafkaTopicConfig};
    use common_wal::config::kafka::MetasrvKafkaConfig;
    use common_wal::test_util::run_test_with_kafka_wal;

    use super::*;
    use crate::procedure::wal_prune::test_util::TestEnv;

    #[tokio::test]
    async fn test_insert_running_procedure() {
        run_test_with_kafka_wal(|broker_endpoints| {
            Box::pin(async {
                let test_env = TestEnv::new();
                let kafka_topic = KafkaTopicConfig {
                    replication_factor: broker_endpoints.len() as i16,
                    ..Default::default()
                };
                let config = MetasrvKafkaConfig {
                    connection: KafkaConnectionConfig {
                        broker_endpoints,
                        ..Default::default()
                    },
                    kafka_topic,
                    ..Default::default()
                };
                let topic_creator = build_kafka_topic_creator(&config).await.unwrap();
                let wal_prune_context = WalPruneContext {
                    client: topic_creator.client().clone(),
                    table_metadata_manager: test_env.table_metadata_manager.clone(),
                    leader_region_registry: test_env.leader_region_registry.clone(),
                    server_addr: test_env.server_addr.to_string(),
                    mailbox: test_env.mailbox.mailbox().clone(),
                };
                let (_tx, rx) = WalPruneManager::channel();
                let manager = WalPruneManager::new(
                    test_env.table_metadata_manager.clone(),
                    DEFAULT_WAL_PRUNE_LIMIT,
                    rx,
                    test_env.procedure_manager.clone(),
                    wal_prune_context,
                    None,
                );

                manager
                    .tracker
                    .running_procedures
                    .write()
                    .unwrap()
                    .insert("test_topic1".to_string());

                let result = manager.submit_procedure("test_topic1").await;
                assert!(result.is_none());
            })
        })
        .await;
    }
}
