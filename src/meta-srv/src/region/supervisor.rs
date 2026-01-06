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

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use common_meta::DatanodeId;
use common_meta::datanode::Stat;
use common_meta::ddl::{DetectingRegion, RegionFailureDetectorController};
use common_meta::key::runtime_switch::RuntimeSwitchManagerRef;
use common_meta::key::table_route::{TableRouteKey, TableRouteValue};
use common_meta::key::{MetadataKey, MetadataValue};
use common_meta::kv_backend::KvBackendRef;
use common_meta::leadership_notifier::LeadershipChangeListener;
use common_meta::peer::{Peer, PeerResolverRef};
use common_meta::range_stream::{DEFAULT_PAGE_SIZE, PaginationStream};
use common_meta::rpc::store::RangeRequest;
use common_runtime::JoinHandle;
use common_telemetry::{debug, error, info, warn};
use common_time::util::current_time_millis;
use futures::{StreamExt, TryStreamExt};
use snafu::{ResultExt, ensure};
use store_api::storage::RegionId;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tokio::time::{MissedTickBehavior, interval, interval_at};

use crate::discovery::utils::accept_ingest_workload;
use crate::error::{self, Result};
use crate::failure_detector::PhiAccrualFailureDetectorOptions;
use crate::metasrv::{RegionStatAwareSelectorRef, SelectTarget, SelectorContext, SelectorRef};
use crate::procedure::region_migration::manager::{
    RegionMigrationManagerRef, RegionMigrationTriggerReason, SubmitRegionMigrationTaskResult,
};
use crate::procedure::region_migration::utils::RegionMigrationTaskBatch;
use crate::procedure::region_migration::{
    DEFAULT_REGION_MIGRATION_TIMEOUT, RegionMigrationProcedureTask,
};
use crate::region::failure_detector::RegionFailureDetector;
use crate::selector::SelectorOptions;
use crate::state::StateRef;

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
/// - `InitializeAllRegions`: This event is used to initialize all region failure detectors.
/// - `RegisterFailureDetectors`: This event is used to register failure detectors for regions.
/// - `DeregisterFailureDetectors`: This event is used to deregister failure detectors for regions.
/// - `HeartbeatArrived`: This event presents the metasrv received [`DatanodeHeartbeat`] from the datanodes.
/// - `Clear`: This event is used to reset the state of the supervisor, typically used
///   when a system-wide reset or reinitialization is needed.
/// - `Dump`: (Available only in test) This event triggers a dump of the
///   current state for debugging purposes. It allows developers to inspect the internal state
///   of the supervisor during tests.
pub(crate) enum Event {
    Tick,
    InitializeAllRegions(tokio::sync::oneshot::Sender<()>),
    RegisterFailureDetectors(Vec<DetectingRegion>),
    DeregisterFailureDetectors(Vec<DetectingRegion>),
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
            Self::InitializeAllRegions(_) => write!(f, "InspectAndRegisterRegions"),
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

    /// The [`Option`] wrapper allows us to abort the job while dropping the [`RegionSupervisor`].
    initialization_handle: Mutex<Option<JoinHandle<()>>>,

    /// The interval of tick.
    tick_interval: Duration,

    /// The delay before initializing all region failure detectors.
    initialization_delay: Duration,

    /// The retry period for initializing all region failure detectors.
    initialization_retry_period: Duration,

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
    pub(crate) fn new(
        tick_interval: Duration,
        initialization_delay: Duration,
        initialization_retry_period: Duration,
        sender: Sender<Event>,
    ) -> Self {
        info!(
            "RegionSupervisorTicker is created, tick_interval: {:?}, initialization_delay: {:?}, initialization_retry_period: {:?}",
            tick_interval, initialization_delay, initialization_retry_period
        );
        Self {
            tick_handle: Mutex::new(None),
            initialization_handle: Mutex::new(None),
            tick_interval,
            initialization_delay,
            initialization_retry_period,
            sender,
        }
    }

    /// Starts the ticker.
    pub fn start(&self) {
        let mut handle = self.tick_handle.lock().unwrap();
        if handle.is_none() {
            let sender = self.sender.clone();
            let tick_interval = self.tick_interval;
            let initialization_delay = self.initialization_delay;

            let mut initialization_interval = interval_at(
                tokio::time::Instant::now() + initialization_delay,
                self.initialization_retry_period,
            );
            initialization_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let initialization_handler = common_runtime::spawn_global(async move {
                loop {
                    initialization_interval.tick().await;
                    let (tx, rx) = oneshot::channel();
                    if sender.send(Event::InitializeAllRegions(tx)).await.is_err() {
                        info!(
                            "EventReceiver is dropped, region failure detectors initialization loop is stopped"
                        );
                        break;
                    }
                    if rx.await.is_ok() {
                        info!("All region failure detectors are initialized.");
                        break;
                    }
                }
            });
            *self.initialization_handle.lock().unwrap() = Some(initialization_handler);

            let sender = self.sender.clone();
            let ticker_loop = tokio::spawn(async move {
                let mut tick_interval = interval(tick_interval);
                tick_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

                if let Err(err) = sender.send(Event::Clear).await {
                    warn!(err; "EventReceiver is dropped, failed to send Event::Clear");
                    return;
                }
                loop {
                    tick_interval.tick().await;
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
        let initialization_handler = self.initialization_handle.lock().unwrap().take();
        if let Some(initialization_handler) = initialization_handler {
            initialization_handler.abort();
            info!("The initialization loop is stopped.");
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
/// The default initialization retry period.
pub const DEFAULT_INITIALIZATION_RETRY_PERIOD: Duration = Duration::from_secs(60);

/// Selector for region supervisor.
pub enum RegionSupervisorSelector {
    NaiveSelector(SelectorRef),
    RegionStatAwareSelector(RegionStatAwareSelectorRef),
}

/// The [`RegionSupervisor`] is used to detect Region failures
/// and initiate Region failover upon detection, ensuring uninterrupted region service.
pub struct RegionSupervisor {
    /// Used to detect the failure of regions.
    failure_detector: RegionFailureDetector,
    /// Tracks the number of failovers for each region.
    failover_counts: HashMap<DetectingRegion, u32>,
    /// Receives [Event]s.
    receiver: Receiver<Event>,
    /// The context of [`SelectorRef`]
    selector_context: SelectorContext,
    /// Candidate node selector.
    selector: RegionSupervisorSelector,
    /// Region migration manager.
    region_migration_manager: RegionMigrationManagerRef,
    /// The maintenance mode manager.
    runtime_switch_manager: RuntimeSwitchManagerRef,
    /// Peer resolver
    peer_resolver: PeerResolverRef,
    /// The kv backend.
    kv_backend: KvBackendRef,
    /// The meta state, used to check if the current metasrv is the leader.
    state: Option<StateRef>,
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

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        event_receiver: Receiver<Event>,
        options: PhiAccrualFailureDetectorOptions,
        selector_context: SelectorContext,
        selector: RegionSupervisorSelector,
        region_migration_manager: RegionMigrationManagerRef,
        runtime_switch_manager: RuntimeSwitchManagerRef,
        peer_resolver: PeerResolverRef,
        kv_backend: KvBackendRef,
    ) -> Self {
        Self {
            failure_detector: RegionFailureDetector::new(options),
            failover_counts: HashMap::new(),
            receiver: event_receiver,
            selector_context,
            selector,
            region_migration_manager,
            runtime_switch_manager,
            peer_resolver,
            kv_backend,
            state: None,
        }
    }

    /// Sets the meta state.
    pub(crate) fn with_state(mut self, state: StateRef) -> Self {
        self.state = Some(state);
        self
    }

    /// Runs the main loop.
    pub(crate) async fn run(&mut self) {
        while let Some(event) = self.receiver.recv().await {
            if let Some(state) = self.state.as_ref()
                && !state.read().unwrap().is_leader()
            {
                warn!(
                    "The current metasrv is not the leader, ignore {:?} event",
                    event
                );
                continue;
            }

            match event {
                Event::InitializeAllRegions(sender) => {
                    match self.is_maintenance_mode_enabled().await {
                        Ok(false) => {}
                        Ok(true) => {
                            warn!(
                                "Skipping initialize all regions since maintenance mode is enabled."
                            );
                            continue;
                        }
                        Err(err) => {
                            error!(err; "Failed to check maintenance mode during initialize all regions.");
                            continue;
                        }
                    }

                    if let Err(err) = self.initialize_all().await {
                        error!(err; "Failed to initialize all regions.");
                    } else {
                        // Ignore the error.
                        let _ = sender.send(());
                    }
                }
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
                Event::Clear => {
                    self.clear();
                    info!("Region supervisor is initialized.");
                }
                #[cfg(test)]
                Event::Dump(sender) => {
                    let _ = sender.send(self.failure_detector.dump());
                }
            }
        }
        info!("RegionSupervisor is stopped!");
    }

    async fn initialize_all(&self) -> Result<()> {
        let now = Instant::now();
        let regions = self.regions();
        let req = RangeRequest::new().with_prefix(TableRouteKey::range_prefix());
        let stream = PaginationStream::new(self.kv_backend.clone(), req, DEFAULT_PAGE_SIZE, |kv| {
            TableRouteKey::from_bytes(&kv.key).map(|v| (v.table_id, kv.value))
        })
        .into_stream();

        let mut stream = stream
            .map_ok(|(_, value)| {
                TableRouteValue::try_from_raw_value(&value)
                    .context(error::TableMetadataManagerSnafu)
            })
            .boxed();
        let mut detecting_regions = Vec::new();
        while let Some(route) = stream
            .try_next()
            .await
            .context(error::TableMetadataManagerSnafu)?
        {
            let route = route?;
            if !route.is_physical() {
                continue;
            }

            let physical_table_route = route.into_physical_table_route();
            physical_table_route
                .region_routes
                .iter()
                .for_each(|region_route| {
                    if !regions.contains(&region_route.region.id)
                        && let Some(leader_peer) = &region_route.leader_peer
                    {
                        detecting_regions.push((leader_peer.id, region_route.region.id));
                    }
                });
        }

        let num_detecting_regions = detecting_regions.len();
        if !detecting_regions.is_empty() {
            self.register_failure_detectors(detecting_regions).await;
        }

        info!(
            "Initialize {} region failure detectors, elapsed: {:?}",
            num_detecting_regions,
            now.elapsed()
        );

        Ok(())
    }

    async fn register_failure_detectors(&self, detecting_regions: Vec<DetectingRegion>) {
        let ts_millis = current_time_millis();
        for region in detecting_regions {
            // The corresponding region has `acceptable_heartbeat_pause_millis` to send heartbeat from datanode.
            self.failure_detector
                .maybe_init_region_failure_detector(region, ts_millis);
        }
    }

    async fn deregister_failure_detectors(&mut self, detecting_regions: Vec<DetectingRegion>) {
        for region in detecting_regions {
            self.failure_detector.remove(&region);
            self.failover_counts.remove(&region);
        }
    }

    async fn handle_region_failures(&mut self, mut regions: Vec<(DatanodeId, RegionId)>) {
        if regions.is_empty() {
            return;
        }
        match self.is_maintenance_mode_enabled().await {
            Ok(false) => {}
            Ok(true) => {
                warn!(
                    "Skipping failover since maintenance mode is enabled. Detected region failures: {:?}",
                    regions
                );
                return;
            }
            Err(err) => {
                error!(err; "Failed to check maintenance mode");
                return;
            }
        }

        // Extracts regions that are migrating(failover), which means they are already being triggered failover.
        let migrating_regions = regions
            .extract_if(.., |(_, region_id)| {
                self.region_migration_manager.tracker().contains(*region_id)
            })
            .collect::<Vec<_>>();

        for (datanode_id, region_id) in migrating_regions {
            debug!(
                "Removed region failover for region: {region_id}, datanode: {datanode_id} because it's migrating"
            );
        }

        if regions.is_empty() {
            // If all detected regions are failover or migrating, just return.
            return;
        }

        let mut grouped_regions: HashMap<u64, Vec<RegionId>> =
            HashMap::with_capacity(regions.len());
        for (datanode_id, region_id) in regions {
            grouped_regions
                .entry(datanode_id)
                .or_default()
                .push(region_id);
        }

        for (datanode_id, regions) in grouped_regions {
            warn!(
                "Detects region failures on datanode: {}, regions: {:?}",
                datanode_id, regions
            );
            // We can't use `grouped_regions.keys().cloned().collect::<Vec<_>>()` here
            // because there may be false positives in failure detection on the datanode.
            // So we only consider the datanode that reports the failure.
            let failed_datanodes = [datanode_id];
            match self
                .generate_failover_tasks(datanode_id, &regions, &failed_datanodes)
                .await
            {
                Ok(tasks) => {
                    let mut grouped_tasks: HashMap<(u64, u64), Vec<_>> = HashMap::new();
                    for (task, count) in tasks {
                        grouped_tasks
                            .entry((task.from_peer.id, task.to_peer.id))
                            .or_default()
                            .push((task, count));
                    }

                    for ((from_peer_id, to_peer_id), tasks) in grouped_tasks {
                        if tasks.is_empty() {
                            continue;
                        }
                        let task = RegionMigrationTaskBatch::from_tasks(tasks);
                        let region_ids = task.region_ids.clone();
                        if let Err(err) = self.do_failover_tasks(task).await {
                            error!(err; "Failed to execute region failover for regions: {:?}, from_peer: {}, to_peer: {}", region_ids, from_peer_id, to_peer_id);
                        }
                    }
                }
                Err(err) => error!(err; "Failed to generate failover tasks"),
            }
        }
    }

    pub(crate) async fn is_maintenance_mode_enabled(&self) -> Result<bool> {
        self.runtime_switch_manager
            .maintenance_mode()
            .await
            .context(error::RuntimeSwitchManagerSnafu)
    }

    async fn select_peers(
        &self,
        from_peer_id: DatanodeId,
        regions: &[RegionId],
        failure_datanodes: &[DatanodeId],
    ) -> Result<Vec<(RegionId, Peer)>> {
        let exclude_peer_ids = HashSet::from_iter(failure_datanodes.iter().cloned());
        match &self.selector {
            RegionSupervisorSelector::NaiveSelector(selector) => {
                let opt = SelectorOptions {
                    min_required_items: regions.len(),
                    allow_duplication: true,
                    exclude_peer_ids,
                    workload_filter: Some(accept_ingest_workload),
                };
                let peers = selector.select(&self.selector_context, opt).await?;
                ensure!(
                    peers.len() == regions.len(),
                    error::NoEnoughAvailableNodeSnafu {
                        required: regions.len(),
                        available: peers.len(),
                        select_target: SelectTarget::Datanode,
                    }
                );
                let region_peers = regions
                    .iter()
                    .zip(peers)
                    .map(|(region_id, peer)| (*region_id, peer))
                    .collect::<Vec<_>>();

                Ok(region_peers)
            }
            RegionSupervisorSelector::RegionStatAwareSelector(selector) => {
                let peers = selector
                    .select(
                        &self.selector_context,
                        from_peer_id,
                        regions,
                        exclude_peer_ids,
                    )
                    .await?;
                ensure!(
                    peers.len() == regions.len(),
                    error::NoEnoughAvailableNodeSnafu {
                        required: regions.len(),
                        available: peers.len(),
                        select_target: SelectTarget::Datanode,
                    }
                );

                Ok(peers)
            }
        }
    }

    async fn generate_failover_tasks(
        &mut self,
        from_peer_id: DatanodeId,
        regions: &[RegionId],
        failed_datanodes: &[DatanodeId],
    ) -> Result<Vec<(RegionMigrationProcedureTask, u32)>> {
        let mut tasks = Vec::with_capacity(regions.len());
        let from_peer = self
            .peer_resolver
            .datanode(from_peer_id)
            .await
            .ok()
            .flatten()
            .unwrap_or_else(|| Peer::empty(from_peer_id));

        let region_peers = self
            .select_peers(from_peer_id, regions, failed_datanodes)
            .await?;

        for (region_id, peer) in region_peers {
            let count = *self
                .failover_counts
                .entry((from_peer_id, region_id))
                .and_modify(|count| *count += 1)
                .or_insert(1);
            let task = RegionMigrationProcedureTask {
                region_id,
                from_peer: from_peer.clone(),
                to_peer: peer,
                timeout: DEFAULT_REGION_MIGRATION_TIMEOUT * count,
                trigger_reason: RegionMigrationTriggerReason::Failover,
            };
            tasks.push((task, count));
        }

        Ok(tasks)
    }

    async fn do_failover_tasks(&mut self, task: RegionMigrationTaskBatch) -> Result<()> {
        let from_peer_id = task.from_peer.id;
        let to_peer_id = task.to_peer.id;
        let timeout = task.timeout;
        let trigger_reason = task.trigger_reason;
        let result = self
            .region_migration_manager
            .submit_region_migration_task(task)
            .await?;
        self.handle_submit_region_migration_task_result(
            from_peer_id,
            to_peer_id,
            timeout,
            trigger_reason,
            result,
        )
        .await
    }

    async fn handle_submit_region_migration_task_result(
        &mut self,
        from_peer_id: DatanodeId,
        to_peer_id: DatanodeId,
        timeout: Duration,
        trigger_reason: RegionMigrationTriggerReason,
        result: SubmitRegionMigrationTaskResult,
    ) -> Result<()> {
        if !result.migrated.is_empty() {
            let detecting_regions = result
                .migrated
                .iter()
                .map(|region_id| (from_peer_id, *region_id))
                .collect::<Vec<_>>();
            self.deregister_failure_detectors(detecting_regions).await;
            info!(
                "Region has been migrated to target peer: {}, removed failover detectors for regions: {:?}",
                to_peer_id, result.migrated,
            )
        }
        if !result.migrating.is_empty() {
            info!(
                "Region is still migrating, skipping failover for regions: {:?}",
                result.migrating
            );
        }
        if !result.table_not_found.is_empty() {
            let detecting_regions = result
                .table_not_found
                .iter()
                .map(|region_id| (from_peer_id, *region_id))
                .collect::<Vec<_>>();
            self.deregister_failure_detectors(detecting_regions).await;
            info!(
                "Table is not found, removed failover detectors for regions: {:?}",
                result.table_not_found
            );
        }
        if !result.leader_changed.is_empty() {
            let detecting_regions = result
                .leader_changed
                .iter()
                .map(|region_id| (from_peer_id, *region_id))
                .collect::<Vec<_>>();
            self.deregister_failure_detectors(detecting_regions).await;
            info!(
                "Region's leader peer changed, removed failover detectors for regions: {:?}",
                result.leader_changed
            );
        }
        if !result.peer_conflict.is_empty() {
            info!(
                "Region has peer conflict, ignore failover for regions: {:?}",
                result.peer_conflict
            );
        }
        if !result.submitted.is_empty() {
            info!(
                "Failover for regions: {:?}, from_peer: {}, to_peer: {}, procedure_id: {:?}, timeout: {:?}, trigger_reason: {:?}",
                result.submitted,
                from_peer_id,
                to_peer_id,
                result.procedure_id,
                timeout,
                trigger_reason,
            );
        }

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

    /// Returns all regions that registered in the failure detector.
    fn regions(&self) -> HashSet<RegionId> {
        self.failure_detector
            .iter()
            .map(|e| e.region_ident().1)
            .collect::<HashSet<_>>()
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
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use common_meta::ddl::RegionFailureDetectorController;
    use common_meta::ddl::test_util::{
        test_create_logical_table_task, test_create_physical_table_task,
    };
    use common_meta::key::table_route::{
        LogicalTableRouteValue, PhysicalTableRouteValue, TableRouteValue,
    };
    use common_meta::key::{TableMetadataManager, runtime_switch};
    use common_meta::peer::Peer;
    use common_meta::rpc::router::{Region, RegionRoute};
    use common_meta::test_util::NoopPeerResolver;
    use common_telemetry::info;
    use common_time::util::current_time_millis;
    use rand::Rng;
    use store_api::storage::RegionId;
    use tokio::sync::mpsc::Sender;
    use tokio::sync::oneshot;
    use tokio::time::sleep;

    use super::RegionSupervisorSelector;
    use crate::procedure::region_migration::RegionMigrationTriggerReason;
    use crate::procedure::region_migration::manager::{
        RegionMigrationManager, SubmitRegionMigrationTaskResult,
    };
    use crate::procedure::region_migration::test_util::TestingEnv;
    use crate::region::supervisor::{
        DatanodeHeartbeat, Event, RegionFailureDetectorControl, RegionSupervisor,
        RegionSupervisorTicker,
    };
    use crate::selector::test_utils::{RandomNodeSelector, new_test_selector_context};

    pub(crate) fn new_test_supervisor() -> (RegionSupervisor, Sender<Event>) {
        let env = TestingEnv::new();
        let selector_context = new_test_selector_context();
        let selector = Arc::new(RandomNodeSelector::new(vec![Peer::empty(1)]));
        let context_factory = env.context_factory();
        let region_migration_manager = Arc::new(RegionMigrationManager::new(
            env.procedure_manager().clone(),
            context_factory,
        ));
        let runtime_switch_manager =
            Arc::new(runtime_switch::RuntimeSwitchManager::new(env.kv_backend()));
        let peer_resolver = Arc::new(NoopPeerResolver);
        let (tx, rx) = RegionSupervisor::channel();
        let kv_backend = env.kv_backend();

        (
            RegionSupervisor::new(
                rx,
                Default::default(),
                selector_context,
                RegionSupervisorSelector::NaiveSelector(selector),
                region_migration_manager,
                runtime_switch_manager,
                peer_resolver,
                kv_backend,
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
            initialization_handle: Mutex::new(None),
            tick_interval: Duration::from_millis(10),
            initialization_delay: Duration::from_millis(100),
            initialization_retry_period: Duration::from_millis(100),
            sender: tx,
        };
        // It's ok if we start the ticker again.
        for _ in 0..2 {
            ticker.start();
            sleep(Duration::from_millis(100)).await;
            ticker.stop();
            assert!(!rx.is_empty());
            while let Ok(event) = rx.try_recv() {
                assert_matches!(
                    event,
                    Event::Tick | Event::Clear | Event::InitializeAllRegions(_)
                );
            }
            assert!(ticker.initialization_handle.lock().unwrap().is_none());
            assert!(ticker.tick_handle.lock().unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn test_initialize_all_regions_event_handling() {
        common_telemetry::init_default_ut_logging();
        let (tx, mut rx) = tokio::sync::mpsc::channel(128);
        let ticker = RegionSupervisorTicker {
            tick_handle: Mutex::new(None),
            initialization_handle: Mutex::new(None),
            tick_interval: Duration::from_millis(1000),
            initialization_delay: Duration::from_millis(50),
            initialization_retry_period: Duration::from_millis(50),
            sender: tx,
        };
        ticker.start();
        sleep(Duration::from_millis(60)).await;
        let handle = tokio::spawn(async move {
            let mut counter = 0;
            while let Some(event) = rx.recv().await {
                if let Event::InitializeAllRegions(tx) = event {
                    if counter == 0 {
                        // Ignore the first event
                        counter += 1;
                        continue;
                    }
                    tx.send(()).unwrap();
                    info!("Responded initialize all regions event");
                    break;
                }
            }
            rx
        });

        let rx = handle.await.unwrap();
        for _ in 0..3 {
            sleep(Duration::from_millis(100)).await;
            assert!(rx.is_empty());
        }
    }

    #[tokio::test]
    async fn test_initialize_all_regions() {
        common_telemetry::init_default_ut_logging();
        let (mut supervisor, sender) = new_test_supervisor();
        let table_metadata_manager = TableMetadataManager::new(supervisor.kv_backend.clone());

        // Create a physical table metadata
        let table_id = 1024;
        let mut create_physical_table_task = test_create_physical_table_task("my_physical_table");
        create_physical_table_task.set_table_id(table_id);
        let table_info = create_physical_table_task.table_info;
        let table_route = PhysicalTableRouteValue::new(vec![RegionRoute {
            region: Region {
                id: RegionId::new(table_id, 0),
                ..Default::default()
            },
            leader_peer: Some(Peer::empty(1)),
            ..Default::default()
        }]);
        let table_route_value = TableRouteValue::Physical(table_route);
        table_metadata_manager
            .create_table_metadata(table_info, table_route_value, HashMap::new())
            .await
            .unwrap();

        // Create a logical table metadata
        let logical_table_id = 1025;
        let mut test_create_logical_table_task = test_create_logical_table_task("my_logical_table");
        test_create_logical_table_task.set_table_id(logical_table_id);
        let table_info = test_create_logical_table_task.table_info;
        let table_route = LogicalTableRouteValue::new(1024, vec![RegionId::new(1025, 0)]);
        let table_route_value = TableRouteValue::Logical(table_route);
        table_metadata_manager
            .create_table_metadata(table_info, table_route_value, HashMap::new())
            .await
            .unwrap();
        tokio::spawn(async move { supervisor.run().await });
        let (tx, rx) = oneshot::channel();
        sender.send(Event::InitializeAllRegions(tx)).await.unwrap();
        assert!(rx.await.is_ok());

        let (tx, rx) = oneshot::channel();
        sender.send(Event::Dump(tx)).await.unwrap();
        let detector = rx.await.unwrap();
        assert_eq!(detector.len(), 1);
        assert!(detector.contains(&(1, RegionId::new(1024, 0))));
    }

    #[tokio::test]
    async fn test_initialize_all_regions_with_maintenance_mode() {
        common_telemetry::init_default_ut_logging();
        let (mut supervisor, sender) = new_test_supervisor();

        supervisor
            .runtime_switch_manager
            .set_maintenance_mode()
            .await
            .unwrap();
        tokio::spawn(async move { supervisor.run().await });
        let (tx, rx) = oneshot::channel();
        sender.send(Event::InitializeAllRegions(tx)).await.unwrap();
        // The sender is dropped, so the receiver will receive an error.
        assert!(rx.await.is_err());
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

    #[tokio::test]
    async fn test_handle_submit_region_migration_task_result_migrated() {
        common_telemetry::init_default_ut_logging();
        let (mut supervisor, _) = new_test_supervisor();
        let region_id = RegionId::new(1, 1);
        let detecting_region = (1, region_id);
        supervisor
            .register_failure_detectors(vec![detecting_region])
            .await;
        supervisor.failover_counts.insert(detecting_region, 1);
        let result = SubmitRegionMigrationTaskResult {
            migrated: vec![region_id],
            ..Default::default()
        };
        supervisor
            .handle_submit_region_migration_task_result(
                1,
                2,
                Duration::from_millis(1000),
                RegionMigrationTriggerReason::Manual,
                result,
            )
            .await
            .unwrap();
        assert!(!supervisor.failure_detector.contains(&detecting_region));
        assert!(supervisor.failover_counts.is_empty());
    }

    #[tokio::test]
    async fn test_handle_submit_region_migration_task_result_migrating() {
        common_telemetry::init_default_ut_logging();
        let (mut supervisor, _) = new_test_supervisor();
        let region_id = RegionId::new(1, 1);
        let detecting_region = (1, region_id);
        supervisor
            .register_failure_detectors(vec![detecting_region])
            .await;
        supervisor.failover_counts.insert(detecting_region, 1);
        let result = SubmitRegionMigrationTaskResult {
            migrating: vec![region_id],
            ..Default::default()
        };
        supervisor
            .handle_submit_region_migration_task_result(
                1,
                2,
                Duration::from_millis(1000),
                RegionMigrationTriggerReason::Manual,
                result,
            )
            .await
            .unwrap();
        assert!(supervisor.failure_detector.contains(&detecting_region));
        assert!(supervisor.failover_counts.contains_key(&detecting_region));
    }

    #[tokio::test]
    async fn test_handle_submit_region_migration_task_result_table_not_found() {
        common_telemetry::init_default_ut_logging();
        let (mut supervisor, _) = new_test_supervisor();
        let region_id = RegionId::new(1, 1);
        let detecting_region = (1, region_id);
        supervisor
            .register_failure_detectors(vec![detecting_region])
            .await;
        supervisor.failover_counts.insert(detecting_region, 1);
        let result = SubmitRegionMigrationTaskResult {
            table_not_found: vec![region_id],
            ..Default::default()
        };
        supervisor
            .handle_submit_region_migration_task_result(
                1,
                2,
                Duration::from_millis(1000),
                RegionMigrationTriggerReason::Manual,
                result,
            )
            .await
            .unwrap();
        assert!(!supervisor.failure_detector.contains(&detecting_region));
        assert!(supervisor.failover_counts.is_empty());
    }

    #[tokio::test]
    async fn test_handle_submit_region_migration_task_result_leader_changed() {
        common_telemetry::init_default_ut_logging();
        let (mut supervisor, _) = new_test_supervisor();
        let region_id = RegionId::new(1, 1);
        let detecting_region = (1, region_id);
        supervisor
            .register_failure_detectors(vec![detecting_region])
            .await;
        supervisor.failover_counts.insert(detecting_region, 1);
        let result = SubmitRegionMigrationTaskResult {
            leader_changed: vec![region_id],
            ..Default::default()
        };
        supervisor
            .handle_submit_region_migration_task_result(
                1,
                2,
                Duration::from_millis(1000),
                RegionMigrationTriggerReason::Manual,
                result,
            )
            .await
            .unwrap();
        assert!(!supervisor.failure_detector.contains(&detecting_region));
        assert!(supervisor.failover_counts.is_empty());
    }

    #[tokio::test]
    async fn test_handle_submit_region_migration_task_result_peer_conflict() {
        common_telemetry::init_default_ut_logging();
        let (mut supervisor, _) = new_test_supervisor();
        let region_id = RegionId::new(1, 1);
        let detecting_region = (1, region_id);
        supervisor
            .register_failure_detectors(vec![detecting_region])
            .await;
        supervisor.failover_counts.insert(detecting_region, 1);
        let result = SubmitRegionMigrationTaskResult {
            peer_conflict: vec![region_id],
            ..Default::default()
        };
        supervisor
            .handle_submit_region_migration_task_result(
                1,
                2,
                Duration::from_millis(1000),
                RegionMigrationTriggerReason::Manual,
                result,
            )
            .await
            .unwrap();
        assert!(supervisor.failure_detector.contains(&detecting_region));
        assert!(supervisor.failover_counts.contains_key(&detecting_region));
    }

    #[tokio::test]
    async fn test_handle_submit_region_migration_task_result_submitted() {
        common_telemetry::init_default_ut_logging();
        let (mut supervisor, _) = new_test_supervisor();
        let region_id = RegionId::new(1, 1);
        let detecting_region = (1, region_id);
        supervisor
            .register_failure_detectors(vec![detecting_region])
            .await;
        supervisor.failover_counts.insert(detecting_region, 1);
        let result = SubmitRegionMigrationTaskResult {
            submitted: vec![region_id],
            ..Default::default()
        };
        supervisor
            .handle_submit_region_migration_task_result(
                1,
                2,
                Duration::from_millis(1000),
                RegionMigrationTriggerReason::Manual,
                result,
            )
            .await
            .unwrap();
        assert!(supervisor.failure_detector.contains(&detecting_region));
        assert!(supervisor.failover_counts.contains_key(&detecting_region));
    }
}
