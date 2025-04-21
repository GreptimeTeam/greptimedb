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

use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use common_meta::datanode::Stat;
use common_meta::ddl::{DetectingRegion, RegionFailureDetectorController};
use common_meta::key::maintenance::MaintenanceModeManagerRef;
use common_meta::leadership_notifier::LeadershipChangeListener;
use common_meta::peer::PeerLookupServiceRef;
use common_meta::DatanodeId;
use common_runtime::JoinHandle;
use common_telemetry::{error, info, warn};
use common_time::util::current_time_millis;
use error::Error::{MigrationRunning, RegionLeaderChanged, TableRouteNotFound};
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionId;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{interval, MissedTickBehavior};

use crate::error::{self, Result};
use crate::failure_detector::PhiAccrualFailureDetectorOptions;
use crate::metasrv::{SelectorContext, SelectorRef};
use crate::procedure::region_migration::manager::RegionMigrationManagerRef;
use crate::procedure::region_migration::RegionMigrationProcedureTask;
use crate::region::failure_detector::RegionFailureDetector;
use crate::selector::SelectorOptions;

/// `DatanodeHeartbeat` represents the heartbeat signal sent from a datanode.
/// It includes identifiers for the cluster and datanode, a list of regions being monitored,
/// and a timestamp indicating when the heartbeat was sent.
#[derive(Debug)]
pub(crate) struct DatanodeHeartbeat {
    datanode_id: DatanodeId,
    // TODO(weny): Considers collecting the memtable size in regions.
    regions: Vec<RegionId>,
    timestamp: i64,
}

impl From<&Stat> for DatanodeHeartbeat {
    fn from(value: &Stat) -> Self {
        DatanodeHeartbeat {
            datanode_id: value.id,
            regions: value.region_stats.iter().map(|x| x.id).collect(),
            timestamp: value.timestamp_millis,
        }
    }
}

/// `Event` represents various types of events that can be processed by the region supervisor.
/// These events are crucial for managing state transitions and handling specific scenarios
/// in the region lifecycle.
///
/// Variants:
/// - `Tick`: This event is used to trigger region failure detection periodically.
/// - `HeartbeatArrived`: This event presents the metasrv received [`DatanodeHeartbeat`] from the datanodes.
/// - `Clear`: This event is used to reset the state of the supervisor, typically used
///   when a system-wide reset or reinitialization is needed.
/// - `Dump`: (Available only in test) This event triggers a dump of the
///   current state for debugging purposes. It allows developers to inspect the internal state
///   of the supervisor during tests.
pub(crate) enum Event {
    Tick,
    RegisterFailureDetectors(Vec<DetectingRegion>),
    DeregisterFailureDetectors(Vec<DetectingRegion>),
    HeartbeatArrived(DatanodeHeartbeat),
    Clear,
    #[cfg(test)]
    Dump(tokio::sync::oneshot::Sender<RegionFailureDetector>),
}

#[cfg(test)]
impl Event {
    pub(crate) fn into_region_failure_detectors(self) -> Vec<DetectingRegion> {
        match self {
            Self::RegisterFailureDetectors(detecting_regions) => detecting_regions,
            _ => unreachable!(),
        }
    }
}

impl Debug for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tick => write!(f, "Tick"),
            Self::HeartbeatArrived(arg0) => f.debug_tuple("HeartbeatArrived").field(arg0).finish(),
            Self::Clear => write!(f, "Clear"),
            Self::RegisterFailureDetectors(arg0) => f
                .debug_tuple("RegisterFailureDetectors")
                .field(arg0)
                .finish(),
            Self::DeregisterFailureDetectors(arg0) => f
                .debug_tuple("DeregisterFailureDetectors")
                .field(arg0)
                .finish(),
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

#[async_trait]
impl LeadershipChangeListener for RegionSupervisorTicker {
    fn name(&self) -> &'static str {
        "RegionSupervisorTicker"
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

impl RegionSupervisorTicker {
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
                if let Err(err) = sender.send(Event::Clear).await {
                    warn!(err; "EventReceiver is dropped, failed to send Event::Clear");
                    return;
                }
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

/// The [`RegionSupervisor`] is used to detect Region failures
/// and initiate Region failover upon detection, ensuring uninterrupted region service.
pub struct RegionSupervisor {
    /// Used to detect the failure of regions.
    failure_detector: RegionFailureDetector,
    /// Receives [Event]s.
    receiver: Receiver<Event>,
    /// The context of [`SelectorRef`]
    selector_context: SelectorContext,
    /// Candidate node selector.
    selector: SelectorRef,
    /// Region migration manager.
    region_migration_manager: RegionMigrationManagerRef,
    /// The maintenance mode manager.
    maintenance_mode_manager: MaintenanceModeManagerRef,
    /// Peer lookup service
    peer_lookup: PeerLookupServiceRef,
}

/// Controller for managing failure detectors for regions.
#[derive(Debug, Clone)]
pub struct RegionFailureDetectorControl {
    sender: Sender<Event>,
}

impl RegionFailureDetectorControl {
    pub(crate) fn new(sender: Sender<Event>) -> Self {
        Self { sender }
    }
}

#[async_trait::async_trait]
impl RegionFailureDetectorController for RegionFailureDetectorControl {
    async fn register_failure_detectors(&self, detecting_regions: Vec<DetectingRegion>) {
        if let Err(err) = self
            .sender
            .send(Event::RegisterFailureDetectors(detecting_regions))
            .await
        {
            error!(err; "RegionSupervisor has stop receiving heartbeat.");
        }
    }

    async fn deregister_failure_detectors(&self, detecting_regions: Vec<DetectingRegion>) {
        if let Err(err) = self
            .sender
            .send(Event::DeregisterFailureDetectors(detecting_regions))
            .await
        {
            error!(err; "RegionSupervisor has stop receiving heartbeat.");
        }
    }
}

/// [`HeartbeatAcceptor`] forwards heartbeats to [`RegionSupervisor`].
#[derive(Clone)]
pub(crate) struct HeartbeatAcceptor {
    sender: Sender<Event>,
}

impl HeartbeatAcceptor {
    pub(crate) fn new(sender: Sender<Event>) -> Self {
        Self { sender }
    }

    /// Accepts heartbeats from datanodes.
    pub(crate) async fn accept(&self, heartbeat: DatanodeHeartbeat) {
        if let Err(err) = self.sender.send(Event::HeartbeatArrived(heartbeat)).await {
            error!(err; "RegionSupervisor has stop receiving heartbeat.");
        }
    }
}

impl RegionSupervisor {
    /// Returns a mpsc channel with a buffer capacity of 1024 for sending and receiving `Event` messages.
    pub(crate) fn channel() -> (Sender<Event>, Receiver<Event>) {
        tokio::sync::mpsc::channel(1024)
    }

    pub(crate) fn new(
        event_receiver: Receiver<Event>,
        options: PhiAccrualFailureDetectorOptions,
        selector_context: SelectorContext,
        selector: SelectorRef,
        region_migration_manager: RegionMigrationManagerRef,
        maintenance_mode_manager: MaintenanceModeManagerRef,
        peer_lookup: PeerLookupServiceRef,
    ) -> Self {
        Self {
            failure_detector: RegionFailureDetector::new(options),
            receiver: event_receiver,
            selector_context,
            selector,
            region_migration_manager,
            maintenance_mode_manager,
            peer_lookup,
        }
    }

    /// Runs the main loop.
    pub(crate) async fn run(&mut self) {
        while let Some(event) = self.receiver.recv().await {
            match event {
                Event::Tick => {
                    let regions = self.detect_region_failure();
                    self.handle_region_failures(regions).await;
                }
                Event::RegisterFailureDetectors(detecting_regions) => {
                    self.register_failure_detectors(detecting_regions).await
                }
                Event::DeregisterFailureDetectors(detecting_regions) => {
                    self.deregister_failure_detectors(detecting_regions).await
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

    async fn register_failure_detectors(&self, detecting_regions: Vec<DetectingRegion>) {
        let ts_millis = current_time_millis();
        for region in detecting_regions {
            // The corresponding region has `acceptable_heartbeat_pause_millis` to send heartbeat from datanode.
            self.failure_detector
                .maybe_init_region_failure_detector(region, ts_millis);
        }
    }

    async fn deregister_failure_detectors(&self, detecting_regions: Vec<DetectingRegion>) {
        for region in detecting_regions {
            self.failure_detector.remove(&region)
        }
    }

    async fn handle_region_failures(&self, mut regions: Vec<(DatanodeId, RegionId)>) {
        if regions.is_empty() {
            return;
        }
        match self.is_maintenance_mode_enabled().await {
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

        let migrating_regions = regions
            .extract_if(.., |(_, region_id)| {
                self.region_migration_manager.tracker().contains(*region_id)
            })
            .collect::<Vec<_>>();

        for (datanode_id, region_id) in migrating_regions {
            self.failure_detector.remove(&(datanode_id, region_id));
        }

        warn!("Detects region failures: {:?}", regions);
        for (datanode_id, region_id) in regions {
            match self.do_failover(datanode_id, region_id).await {
                Ok(_) => self.failure_detector.remove(&(datanode_id, region_id)),
                Err(err) => {
                    error!(err; "Failed to execute region failover for region: {region_id}, datanode: {datanode_id}");
                }
            }
        }
    }

    pub(crate) async fn is_maintenance_mode_enabled(&self) -> Result<bool> {
        self.maintenance_mode_manager
            .maintenance_mode()
            .await
            .context(error::MaintenanceModeManagerSnafu)
    }

    async fn do_failover(&self, datanode_id: DatanodeId, region_id: RegionId) -> Result<()> {
        let from_peer = self
            .peer_lookup
            .datanode(datanode_id)
            .await
            .context(error::LookupPeerSnafu {
                peer_id: datanode_id,
            })?
            .context(error::PeerUnavailableSnafu {
                peer_id: datanode_id,
            })?;
        let mut peers = self
            .selector
            .select(
                &self.selector_context,
                SelectorOptions {
                    min_required_items: 1,
                    allow_duplication: false,
                },
            )
            .await?;
        let to_peer = peers.remove(0);
        if to_peer.id == from_peer.id {
            warn!(
                "Skip failover for region: {region_id}, from_peer: {from_peer}, trying to failover to the same peer."
            );
            return Ok(());
        }
        let from_peer_id = from_peer.id;
        let task = RegionMigrationProcedureTask {
            region_id,
            from_peer,
            to_peer,
            timeout: Duration::from_secs(60),
        };

        if let Err(err) = self.region_migration_manager.submit_procedure(task).await {
            return match err {
                // Returns Ok if it's running or table is dropped.
                MigrationRunning { .. } => Ok(()),
                TableRouteNotFound { .. } => {
                    self.deregister_failure_detectors(vec![(datanode_id, region_id)])
                        .await;
                    info!(
                        "Table route is not found, the table is dropped, removed failover detector for region: {}, datanode: {}",
                        region_id, from_peer_id
                    );
                    Ok(())
                }
                RegionLeaderChanged { .. } => {
                    self.deregister_failure_detectors(vec![(datanode_id, region_id)])
                        .await;
                    info!(
                        "Region leader changed, removed failover detector for region: {}, datanode: {}",
                        region_id, from_peer_id
                    );
                    Ok(())
                }
                err => Err(err),
            };
        };

        Ok(())
    }

    /// Detects the failure of regions.
    fn detect_region_failure(&self) -> Vec<(DatanodeId, RegionId)> {
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
            let detecting_region = (heartbeat.datanode_id, region_id);
            let mut detector = self
                .failure_detector
                .region_failure_detector(detecting_region);
            detector.heartbeat(heartbeat.timestamp);
        }
    }

    fn clear(&self) {
        self.failure_detector.clear();
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::assert_matches::assert_matches;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use common_meta::ddl::RegionFailureDetectorController;
    use common_meta::key::maintenance;
    use common_meta::peer::Peer;
    use common_meta::test_util::NoopPeerLookupService;
    use common_time::util::current_time_millis;
    use rand::Rng;
    use store_api::storage::RegionId;
    use tokio::sync::mpsc::Sender;
    use tokio::sync::oneshot;
    use tokio::time::sleep;

    use crate::procedure::region_migration::manager::RegionMigrationManager;
    use crate::procedure::region_migration::test_util::TestingEnv;
    use crate::region::supervisor::{
        DatanodeHeartbeat, Event, RegionFailureDetectorControl, RegionSupervisor,
        RegionSupervisorTicker,
    };
    use crate::selector::test_utils::{new_test_selector_context, RandomNodeSelector};

    pub(crate) fn new_test_supervisor() -> (RegionSupervisor, Sender<Event>) {
        let env = TestingEnv::new();
        let selector_context = new_test_selector_context();
        let selector = Arc::new(RandomNodeSelector::new(vec![Peer::empty(1)]));
        let context_factory = env.context_factory();
        let region_migration_manager = Arc::new(RegionMigrationManager::new(
            env.procedure_manager().clone(),
            context_factory,
        ));
        let maintenance_mode_manager =
            Arc::new(maintenance::MaintenanceModeManager::new(env.kv_backend()));
        let peer_lookup = Arc::new(NoopPeerLookupService);
        let (tx, rx) = RegionSupervisor::channel();

        (
            RegionSupervisor::new(
                rx,
                Default::default(),
                selector_context,
                selector,
                region_migration_manager,
                maintenance_mode_manager,
                peer_lookup,
            ),
            tx,
        )
    }

    #[tokio::test]
    async fn test_heartbeat() {
        let (mut supervisor, sender) = new_test_supervisor();
        tokio::spawn(async move { supervisor.run().await });

        sender
            .send(Event::HeartbeatArrived(DatanodeHeartbeat {
                datanode_id: 0,
                regions: vec![RegionId::new(1, 1)],
                timestamp: 100,
            }))
            .await
            .unwrap();
        let (tx, rx) = oneshot::channel();
        sender.send(Event::Dump(tx)).await.unwrap();
        let detector = rx.await.unwrap();
        assert!(detector.contains(&(0, RegionId::new(1, 1))));

        // Clear up
        sender.send(Event::Clear).await.unwrap();
        let (tx, rx) = oneshot::channel();
        sender.send(Event::Dump(tx)).await.unwrap();
        assert!(rx.await.unwrap().is_empty());

        fn generate_heartbeats(datanode_id: u64, region_ids: Vec<u32>) -> Vec<DatanodeHeartbeat> {
            let mut rng = rand::rng();
            let start = current_time_millis();
            (0..2000)
                .map(|i| DatanodeHeartbeat {
                    timestamp: start + i * 1000 + rng.random_range(0..100),
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
        // It's ok if we start the ticker again.
        for _ in 0..2 {
            ticker.start();
            sleep(Duration::from_millis(100)).await;
            ticker.stop();
            assert!(!rx.is_empty());
            while let Ok(event) = rx.try_recv() {
                assert_matches!(event, Event::Tick | Event::Clear);
            }
        }
    }

    #[tokio::test]
    async fn test_region_failure_detector_controller() {
        let (mut supervisor, sender) = new_test_supervisor();
        let controller = RegionFailureDetectorControl::new(sender.clone());
        tokio::spawn(async move { supervisor.run().await });
        let detecting_region = (1, RegionId::new(1, 1));
        controller
            .register_failure_detectors(vec![detecting_region])
            .await;

        let (tx, rx) = oneshot::channel();
        sender.send(Event::Dump(tx)).await.unwrap();
        let detector = rx.await.unwrap();
        let region_detector = detector.region_failure_detector(detecting_region).clone();

        // Registers failure detector again
        controller
            .register_failure_detectors(vec![detecting_region])
            .await;
        let (tx, rx) = oneshot::channel();
        sender.send(Event::Dump(tx)).await.unwrap();
        let detector = rx.await.unwrap();
        let got = detector.region_failure_detector(detecting_region).clone();
        assert_eq!(region_detector, got);

        controller
            .deregister_failure_detectors(vec![detecting_region])
            .await;
        let (tx, rx) = oneshot::channel();
        sender.send(Event::Dump(tx)).await.unwrap();
        assert!(rx.await.unwrap().is_empty());
    }
}
