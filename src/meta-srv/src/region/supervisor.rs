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

use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common_meta::key::{TableMetadataManager, TableMetadataManagerRef, MAINTENANCE_KEY};
use common_meta::kv_backend::KvBackendRef;
use common_meta::peer::Peer;
use common_meta::rpc::router::RegionRoute;
use common_meta::{ClusterId, DatanodeId};
use common_runtime::JoinHandle;
use common_telemetry::{error, info, warn};
use common_time::util::current_time_millis;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::RegionId;
use tokio::sync::mpsc::{Receiver, Sender};

use super::failure_detector::RegionFailureDetector;
use crate::error::{self, Result};
use crate::failure_detector::PhiAccrualFailureDetectorOptions;
use crate::handler::node_stat::Stat;
use crate::metasrv::{SelectorContext, SelectorRef};
use crate::procedure::region_migration::manager::RegionMigrationManagerRef;
use crate::procedure::region_migration::RegionMigrationProcedureTask;
use crate::selector::SelectorOptions;

#[derive(Debug)]
pub(crate) struct DatanodeHeartbeat {
    cluster_id: ClusterId,
    datanode_id: DatanodeId,
    // TODO(weny): Considers collecting the memtable size in regions.
    regions: Vec<RegionId>,
    timestamp: i64,
}

impl From<&Stat> for DatanodeHeartbeat {
    fn from(value: &Stat) -> Self {
        DatanodeHeartbeat {
            cluster_id: value.cluster_id,
            datanode_id: value.id,
            regions: value.region_stats.iter().map(|x| x.id).collect(),
            timestamp: value.timestamp_millis,
        }
    }
}

pub(crate) enum Event {
    Tick,
    HeartbeatArrived(DatanodeHeartbeat),
    Clear,
    #[cfg(test)]
    Dump(tokio::sync::oneshot::Sender<RegionFailureDetector>),
}

impl Debug for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tick => write!(f, "Tick"),
            Self::HeartbeatArrived(arg0) => f.debug_tuple("HeartbeatArrived").field(arg0).finish(),
            Self::Clear => write!(f, "Clear"),
            #[cfg(test)]
            Self::Dump(_) => f.debug_struct("Dump").finish(),
        }
    }
}

pub type RegionSupervisorTickerRef = Arc<RegionSupervisorTicker>;

/// A background job to generate [`Event::Tick`] type events.
#[derive(Debug)]
pub struct RegionSupervisorTicker {
    /// The [`Option`] wrapper allows us to abort the job while dropping the [`RegionSupervisor`].
    tick_handle: Mutex<Option<JoinHandle<()>>>,

    /// The interval of tick.
    tick_interval: Duration,

    /// Sends [Event]s.
    sender: Sender<Event>,
}

impl RegionSupervisorTicker {
    /// Starts the ticker.
    pub fn start(&self) {
        let mut handle = self.tick_handle.lock().unwrap();
        if handle.is_none() {
            let sender = self.sender.clone();
            let tick_interval = self.tick_interval;
            let ticker_loop = tokio::spawn(async move {
                if let Err(err) = sender.send(Event::Clear).await {
                    warn!(err; "EventReceiver is dropped, failed to send Event::Clear");
                    return;
                }
                loop {
                    tokio::time::sleep(tick_interval).await;
                    if let Err(err) = sender.send(Event::Tick).await {
                        warn!(err; "EventReceiver is dropped, tick loop is stopped");
                        break;
                    }
                }
            });
            *handle = Some(ticker_loop);
        }
    }

    /// Stops the ticker.
    pub fn stop(&self) {
        let handle = self.tick_handle.lock().unwrap().take();
        if let Some(handle) = handle {
            handle.abort();
            info!("The tick loop is stopped.");
        }
    }
}

impl Drop for RegionSupervisorTicker {
    fn drop(&mut self) {
        self.stop();
    }
}

pub type RegionSupervisorRef = Arc<RegionSupervisor>;

/// The default tick interval.
pub const DEFAULT_TICK_INTERVAL: Duration = Duration::from_secs(1);

pub struct RegionSupervisor {
    /// Used to detect the failure of regions.
    failure_detector: RegionFailureDetector,
    /// The interval of tick
    tick_interval: Duration,
    /// Receives [Event]s.
    receiver: Receiver<Event>,
    sender: Sender<Event>,
    selector_context: SelectorContext,
    /// Candidate node selector.
    selector: SelectorRef,
    /// Region migration manager.
    region_migration_manager: RegionMigrationManagerRef,
    // TODO(weny): find a better way
    kv_backend: KvBackendRef,
    table_metadata_manager: TableMetadataManagerRef,
}

pub(crate) struct HeartbeatSender {
    sender: Sender<Event>,
}

impl HeartbeatSender {
    pub(crate) async fn send(&self, heartbeat: DatanodeHeartbeat) {
        if let Err(e) = self.sender.send(Event::HeartbeatArrived(heartbeat)).await {
            error!(e; "RegionSupervisor is stop receiving heartbeat");
        }
    }
}

#[cfg(test)]
impl RegionSupervisor {
    /// Returns the [Event] sender.
    pub(crate) fn sender(&self) -> Sender<Event> {
        self.sender.clone()
    }
}

impl RegionSupervisor {
    pub(crate) fn new(
        options: PhiAccrualFailureDetectorOptions,
        tick_interval: Duration,
        selector_context: SelectorContext,
        selector: SelectorRef,
        region_migration_manager: RegionMigrationManagerRef,
        kv_backend: KvBackendRef,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(1024);
        Self {
            failure_detector: RegionFailureDetector::new(options),
            tick_interval,
            receiver: rx,
            sender: tx,
            selector_context,
            selector,
            region_migration_manager,
            table_metadata_manager: Arc::new(TableMetadataManager::new(kv_backend.clone())),
            kv_backend,
        }
    }

    /// Returns the [HeartbeatSender].
    pub(crate) fn heartbeat_sender(&self) -> HeartbeatSender {
        HeartbeatSender {
            sender: self.sender.clone(),
        }
    }

    /// Returns the [RegionSupervisorTicker].
    pub(crate) fn ticker(&self) -> RegionSupervisorTickerRef {
        Arc::new(RegionSupervisorTicker {
            tick_interval: self.tick_interval,
            sender: self.sender.clone(),
            tick_handle: Mutex::new(None),
        })
    }

    /// Runs the main loop.
    pub(crate) async fn run(&mut self) {
        while let Some(event) = self.receiver.recv().await {
            match event {
                Event::Tick => {
                    let regions = self.detect_region_failure();
                    self.handle_region_failures(regions).await;
                }
                Event::HeartbeatArrived(heartbeat) => self.on_heartbeat_arrived(heartbeat),
                Event::Clear => self.clear(),
                #[cfg(test)]
                Event::Dump(sender) => {
                    let _ = sender.send(self.failure_detector.dump());
                }
            }
        }
        info!("RegionSupervisor is stopped!");
    }

    async fn handle_region_failures(&self, regions: Vec<(ClusterId, DatanodeId, RegionId)>) {
        if regions.is_empty() {
            return;
        }
        match self.is_maintenance_mode().await {
            Ok(false) => {}
            Ok(true) => {
                info!("Maintenance mode is enabled, skip failover");
                return;
            }
            Err(err) => {
                error!(err; "Failed to check maintenance mode");
                return;
            }
        }

        let table_ids = regions
            .iter()
            .map(|(_, _, region_id)| region_id.table_id())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();

        let table_routes = match self
            .table_metadata_manager
            .table_route_manager()
            .batch_get_physical_table_routes(&table_ids)
            .await
            .context(error::TableMetadataManagerSnafu)
        {
            Ok(table_routes) => table_routes,
            Err(err) => {
                error!(err; "Failed to retrieves table routes: {table_ids:?}");
                return;
            }
        };

        warn!(
            "Detects region failures: {:?}",
            regions
                .iter()
                .map(|(_, datanode, region)| (datanode, region))
                .collect::<Vec<_>>()
        );
        for (cluster_id, datanode_id, region_id) in regions {
            match table_routes.get(&region_id.table_id()) {
                Some(route) => {
                    match self
                        .handle_region_failure(
                            cluster_id,
                            datanode_id,
                            region_id,
                            &route.region_routes,
                        )
                        .await
                    {
                        Ok(_) => {
                            self.failure_detector
                                .remove(&(cluster_id, datanode_id, region_id))
                        }
                        Err(err) => {
                            error!(err; "Failed to execute region failover for region: {region_id}, datanode: {datanode_id}");
                        }
                    }
                }
                None => {
                    info!(
                        "Skipping to execute region failover for region: {}, target table: {} is not exists",
                        region_id,
                        region_id.table_id()
                    );
                    self.failure_detector
                        .remove(&(cluster_id, datanode_id, region_id));
                }
            }
        }
    }

    async fn handle_region_failure(
        &self,
        cluster_id: ClusterId,
        datanode_id: DatanodeId,
        region_id: RegionId,
        region_routes: &[RegionRoute],
    ) -> Result<()> {
        let region_leader_peer = region_routes
            .iter()
            .find_map(|region| {
                if region.region.id == region_id {
                    region.leader_peer.clone()
                } else {
                    None
                }
            })
            .context(error::RegionLeaderNotFoundSnafu { region_id })?;
        ensure!(
            region_leader_peer.id == datanode_id,
            error::UnexpectedSnafu {
                violated: format!(
                    "Region leader peer is changed, expected: Datanode {}, actual: Datanode {}",
                    datanode_id, region_leader_peer.id
                )
            }
        );
        self.do_failover(cluster_id, region_leader_peer, region_id)
            .await?;

        Ok(())
    }

    pub(crate) async fn is_maintenance_mode(&self) -> Result<bool> {
        self.kv_backend
            .exists(MAINTENANCE_KEY.as_bytes())
            .await
            .context(error::KvBackendSnafu)
    }

    async fn do_failover(
        &self,
        cluster_id: ClusterId,
        from_peer: Peer,
        region_id: RegionId,
    ) -> Result<()> {
        let task = self.region_migration_manager.tracker().get(region_id);
        match task {
            Some(task) => {
                info!(
                    "Region is migrating to Datanode({}), skipping the region failover",
                    task.to_peer.id
                );
            }
            None => {
                let mut peers = self
                    .selector
                    .select(
                        cluster_id,
                        &self.selector_context,
                        SelectorOptions {
                            min_required_items: 1,
                            allow_duplication: false,
                        },
                    )
                    .await?;
                let to_peer = peers.remove(0);
                let task = RegionMigrationProcedureTask {
                    cluster_id,
                    region_id,
                    from_peer,
                    to_peer,
                    replay_timeout: Duration::from_secs(60),
                };
                self.region_migration_manager.submit_procedure(task).await?;
            }
        };

        Ok(())
    }

    /// Detects the failure of regions.
    fn detect_region_failure(&self) -> Vec<(ClusterId, DatanodeId, RegionId)> {
        self.failure_detector
            .iter()
            .filter_map(|e| {
                // Intentionally not place `current_time_millis()` out of the iteration.
                // The failure detection determination should be happened "just in time",
                // i.e., failed or not has to be compared with the most recent "now".
                // Besides, it might reduce the false positive of failure detection,
                // because during the iteration, heartbeats are coming in as usual,
                // and the `phi`s are still updating.
                if !e.failure_detector().is_available(current_time_millis()) {
                    Some(*e.region_ident())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    }

    /// Updates the state of corresponding failure detectors.
    fn on_heartbeat_arrived(&self, heartbeat: DatanodeHeartbeat) {
        for region_id in heartbeat.regions {
            let ident = (heartbeat.cluster_id, heartbeat.datanode_id, region_id);
            let mut detector = self.failure_detector.region_failure_detector(ident);
            detector.heartbeat(heartbeat.timestamp);
        }
    }

    fn clear(&self) {
        self.failure_detector.clear();
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use common_meta::peer::Peer;
    use common_time::util::current_time_millis;
    use rand::Rng;
    use store_api::storage::RegionId;
    use tokio::sync::oneshot;
    use tokio::time::sleep;

    use crate::procedure::region_migration::manager::RegionMigrationManager;
    use crate::procedure::region_migration::test_util::TestingEnv;
    use crate::region::supervisor::{
        DatanodeHeartbeat, Event, RegionSupervisor, RegionSupervisorTicker,
    };
    use crate::selector::test_utils::{new_test_selector_context, RandomNodeSelector};

    fn new_test_supervisor() -> RegionSupervisor {
        let env = TestingEnv::new();
        let selector_context = new_test_selector_context();
        let selector = Arc::new(RandomNodeSelector::new(vec![Peer::empty(1)]));
        let context_factory = env.context_factory();
        let region_migration_manager = Arc::new(RegionMigrationManager::new(
            env.procedure_manager().clone(),
            context_factory,
        ));
        let kv_backend = env.kv_backend();

        RegionSupervisor::new(
            Default::default(),
            Duration::from_secs(1),
            selector_context,
            selector,
            region_migration_manager,
            kv_backend,
        )
    }

    #[tokio::test]
    async fn test_heartbeat() {
        let mut supervisor = new_test_supervisor();
        let sender = supervisor.sender();
        tokio::spawn(async move { supervisor.run().await });

        sender
            .send(Event::HeartbeatArrived(DatanodeHeartbeat {
                cluster_id: 0,
                datanode_id: 0,
                regions: vec![RegionId::new(1, 1)],
                timestamp: 100,
            }))
            .await
            .unwrap();
        let (tx, rx) = oneshot::channel();
        sender.send(Event::Dump(tx)).await.unwrap();
        let detector = rx.await.unwrap();
        assert!(detector.contains(&(0, 0, RegionId::new(1, 1))));

        // Clear up
        sender.send(Event::Clear).await.unwrap();
        let (tx, rx) = oneshot::channel();
        sender.send(Event::Dump(tx)).await.unwrap();
        assert!(rx.await.unwrap().is_empty());

        fn generate_heartbeats(datanode_id: u64, region_ids: Vec<u32>) -> Vec<DatanodeHeartbeat> {
            let mut rng = rand::thread_rng();
            let start = current_time_millis();
            (0..2000)
                .map(|i| DatanodeHeartbeat {
                    timestamp: start + i * 1000 + rng.gen_range(0..100),
                    cluster_id: 0,
                    datanode_id,
                    regions: region_ids
                        .iter()
                        .map(|number| RegionId::new(0, *number))
                        .collect(),
                })
                .collect::<Vec<_>>()
        }

        let heartbeats = generate_heartbeats(100, vec![1, 2, 3]);
        let last_heartbeat_time = heartbeats.last().unwrap().timestamp;
        for heartbeat in heartbeats {
            sender
                .send(Event::HeartbeatArrived(heartbeat))
                .await
                .unwrap();
        }

        let (tx, rx) = oneshot::channel();
        sender.send(Event::Dump(tx)).await.unwrap();
        let detector = rx.await.unwrap();
        assert_eq!(detector.len(), 3);

        for e in detector.iter() {
            let fd = e.failure_detector();
            let acceptable_heartbeat_pause_millis = fd.acceptable_heartbeat_pause_millis() as i64;
            let start = last_heartbeat_time;

            // Within the "acceptable_heartbeat_pause_millis" period, phi is zero ...
            for i in 1..=acceptable_heartbeat_pause_millis / 1000 {
                let now = start + i * 1000;
                assert_eq!(fd.phi(now), 0.0);
            }

            // ... then in less than two seconds, phi is above the threshold.
            // The same effect can be seen in the diagrams in Akka's document.
            let now = start + acceptable_heartbeat_pause_millis + 1000;
            assert!(fd.phi(now) < fd.threshold() as _);
            let now = start + acceptable_heartbeat_pause_millis + 2000;
            assert!(fd.phi(now) > fd.threshold() as _);
        }
    }

    #[tokio::test]
    async fn test_supervisor_ticker() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(128);
        let ticker = RegionSupervisorTicker {
            tick_handle: Mutex::new(None),
            tick_interval: Duration::from_millis(10),
            sender: tx,
        };
        ticker.start();
        sleep(Duration::from_millis(100)).await;
        ticker.stop();
        assert!(!rx.is_empty());
        while let Ok(event) = rx.try_recv() {
            assert_matches!(event, Event::Tick | Event::Clear);
        }

        // It's ok if we start the ticker again.
        ticker.start();
        sleep(Duration::from_millis(100)).await;
        ticker.stop();
        assert!(!rx.is_empty());
        while let Ok(event) = rx.try_recv() {
            assert_matches!(event, Event::Tick | Event::Clear);
        }
    }
}
