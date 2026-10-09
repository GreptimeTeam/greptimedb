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
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use snafu::{ResultExt, ensure};
use store_api::storage::RegionId;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tokio::time::{MissedTickBehavior, interval, interval_at};

use crate::discovery::utils::accept_ingest_workload;
use crate::error::{self, Result};
use crate::failure_detector::PhiAccrualFailureDetectorOptions;
use crate::metasrv::{RegionStatAwareSelectorRef, SelectTarget, SelectorContext, SelectorRef};
use crate::metrics::METRIC_META_HEARTBEAT_DROPPED;
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
/// - `ContinueInitializeAllRegions`: This event is used to continue an initialization scan
///   that was split into batches, and is enqueued by the supervisor itself.
/// - `RegisterFailureDetectors`: This event is used to register failure detectors for regions.
/// - `ResetFailureDetectors`: This event is used to reset failure detectors for regions.
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
    ContinueInitializeAllRegions,
    RegisterFailureDetectors(Vec<DetectingRegion>),
    DeregisterFailureDetectors(Vec<DetectingRegion>),
    ResetFailureDetectors(Vec<DetectingRegion>),
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
            Self::ContinueInitializeAllRegions => write!(f, "ContinueInitializeAllRegions"),
            Self::RegisterFailureDetectors(arg0) => f
                .debug_tuple("RegisterFailureDetectors")
                .field(arg0)
                .finish(),
            Self::ResetFailureDetectors(arg0) => {
                f.debug_tuple("ResetFailureDetectors").field(arg0).finish()
            }
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

/// The number of table routes the initialization scan processes per event.
///
/// Bounds how long a single [`Event::InitializeAllRegions`] can occupy the supervisor loop, so
/// events queued behind it (heartbeats in particular) are handled between batches rather than
/// after the whole scan.
const DEFAULT_INITIALIZE_ALL_BATCH_SIZE: usize = 256;

/// An in-flight [`Event::InitializeAllRegions`] scan, kept alive across events.
///
/// The scan is split into batches: an event processes at most
/// [`RegionSupervisor::initialize_batch_size`] routes and then hands the rest back through
/// [`Event::ContinueInitializeAllRegions`]. The route stream is stored rather than rebuilt,
/// because that is the only way to keep the pagination position —
/// [`PaginationStream`] consumes itself and does not expose its cursor.
struct InitializeAllScan {
    /// The table route stream, resumed across batches.
    routes: BoxStream<'static, Result<TableRouteValue>>,
    /// Regions already known to the failure detector when the scan started.
    ///
    /// Sampled once, matching the single-pass scan this replaced: a region registered after
    /// the scan started is not re-registered. That is safe because registration is idempotent.
    known_regions: HashSet<RegionId>,
    /// Signalled once the scan is exhausted.
    ///
    /// Dropped without firing when the scan errors out or is abandoned, which is what makes
    /// the ticker that requested the scan retry.
    sender: oneshot::Sender<()>,
    /// Number of detectors registered so far, for the completion log.
    registered: usize,
    /// When the scan started, for the completion log.
    started_at: Instant,
}

/// How one batch of an initialization scan ended.
enum BatchOutcome {
    /// The scan is finished, or was abandoned: no further batch is run.
    Completed,
    /// The continuation is queued: the loop returns to `recv()`.
    Suspended,
    /// The continuation could not be queued: the batch did not yield, and the rest of the scan
    /// runs inline.
    NotYielded,
}

/// Why one batch stopped pulling routes from the scan's stream.
enum PullEnd {
    /// The batch limit was reached; the stream may have more routes.
    BatchLimit,
    /// The route stream is exhausted.
    Exhausted,
    /// Pulling a route failed.
    Failed,
}

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
    /// Sends [`Event`]s to this supervisor's own queue, used to hand the rest of a batched
    /// initialization scan back to the loop. The hand-back must use `try_send`, since the loop
    /// owns the [`Receiver`] and awaiting capacity here would deadlock.
    self_sender: Sender<Event>,
    /// Number of table routes the initialization scan processes per event.
    initialize_batch_size: usize,
    /// Page size used to scan table routes during initialization.
    initialize_page_size: usize,
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

    async fn reset_failure_detectors(&self, detecting_regions: Vec<DetectingRegion>) {
        if let Err(err) = self
            .sender
            .send(Event::ResetFailureDetectors(detecting_regions))
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
    ///
    /// Never waits for queue capacity: the region lease has already been decided
    /// earlier in the heartbeat handler chain, so a full supervisor queue drops the
    /// heartbeat (counted in [`METRIC_META_HEARTBEAT_DROPPED`]) instead of parking
    /// the response path. Dropping is only unsafe if the queue stays full long enough
    /// for the failure detector to age out every datanode: with the default phi
    /// threshold that takes ~13.5s of continuous drops.
    pub(crate) fn accept(&self, heartbeat: DatanodeHeartbeat) {
        let datanode_id = heartbeat.datanode_id;
        match self.sender.try_send(Event::HeartbeatArrived(heartbeat)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                METRIC_META_HEARTBEAT_DROPPED
                    .with_label_values(&["full"])
                    .inc();
                warn!(
                    "Dropping heartbeat because the region supervisor event queue is full, datanode_id: {}, queue_capacity: {}",
                    datanode_id,
                    self.sender.max_capacity()
                );
            }
            Err(TrySendError::Closed(err)) => {
                METRIC_META_HEARTBEAT_DROPPED
                    .with_label_values(&["closed"])
                    .inc();
                error!(err; "RegionSupervisor has stop receiving heartbeat.");
            }
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
        self_sender: Sender<Event>,
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
            self_sender,
            initialize_batch_size: DEFAULT_INITIALIZE_ALL_BATCH_SIZE,
            initialize_page_size: DEFAULT_PAGE_SIZE,
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

    /// Overrides the number of table routes processed per initialization event.
    ///
    /// A zero batch size is clamped to one where the batch runs.
    #[cfg(test)]
    pub(crate) fn with_initialize_batch_size(mut self, batch_size: usize) -> Self {
        self.initialize_batch_size = batch_size;
        self
    }

    /// Overrides the page size of the initialization scan.
    ///
    /// A zero page size needs no clamp here: [`PaginationStream`] falls back to its own
    /// default.
    #[cfg(test)]
    pub(crate) fn with_initialize_page_size(mut self, page_size: usize) -> Self {
        self.initialize_page_size = page_size;
        self
    }

    /// Runs the main loop.
    pub(crate) async fn run(&mut self) {
        // The in-flight initialization scan, kept across events so its pagination position
        // survives. It lives here rather than on `self` because the underlying route stream is
        // not `Sync`, which a struct field would force.
        let mut init_scan = None;

        while let Some(event) = self.receiver.recv().await {
            if let Some(state) = self.state.as_ref()
                && !state.read().unwrap().is_leader()
            {
                warn!(
                    "The current metasrv is not the leader, ignore {:?} event",
                    event
                );
                // Dropping the in-flight scan also drops its completion signal, which is what
                // the ticker waits on to decide whether to retry.
                init_scan = None;
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

                    init_scan = Some(self.begin_initialize_all(sender));
                    self.drain_initialize_all(&mut init_scan).await;
                }
                Event::ContinueInitializeAllRegions => {
                    self.drain_initialize_all(&mut init_scan).await
                }
                Event::Tick => {
                    let regions = self.detect_region_failure();
                    self.handle_region_failures(regions).await;
                }
                Event::RegisterFailureDetectors(detecting_regions) => {
                    self.register_failure_detectors(detecting_regions).await
                }
                Event::ResetFailureDetectors(detecting_regions) => {
                    self.reset_failure_detectors(detecting_regions).await
                }
                Event::DeregisterFailureDetectors(detecting_regions) => {
                    self.deregister_failure_detectors(detecting_regions).await
                }
                Event::HeartbeatArrived(heartbeat) => self.on_heartbeat_arrived(heartbeat),
                Event::Clear => {
                    init_scan = None;
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

    /// Starts an initialization scan.
    ///
    /// The caller replaces any scan already in flight with the returned one, which drops the
    /// superseded scan's `sender` without firing it. That is deliberate: the ticker that
    /// requested the superseded scan then retries, and its route snapshot is re-sampled here
    /// anyway.
    fn begin_initialize_all(&self, sender: oneshot::Sender<()>) -> InitializeAllScan {
        let req = RangeRequest::new().with_prefix(TableRouteKey::range_prefix());
        let routes: BoxStream<'static, Result<TableRouteValue>> = PaginationStream::new(
            self.kv_backend.clone(),
            req,
            self.initialize_page_size,
            |kv| TableRouteKey::from_bytes(&kv.key).map(|v| (v.table_id, kv.value)),
        )
        .into_stream()
        // Flattened here rather than by `try_next` + `?` in the loop, because the scan
        // keeps the stream in a struct field and so has to fix its item type up front.
        .map(|res| {
            let (_, value) = res.context(error::TableMetadataManagerSnafu)?;
            TableRouteValue::try_from_raw_value(&value).context(error::TableMetadataManagerSnafu)
        })
        .boxed();

        InitializeAllScan {
            routes,
            known_regions: self.regions(),
            sender,
            registered: 0,
            started_at: Instant::now(),
        }
    }

    /// Processes one batch of the in-flight initialization scan.
    async fn step_initialize_all(&self, init_scan: &mut Option<InitializeAllScan>) -> BatchOutcome {
        // Taken out of the slot so the `&self` calls below do not overlap a borrow of it.
        let Some(mut scan) = init_scan.take() else {
            // A continuation for a scan that is already gone, e.g. one that was queued when a
            // newer scan replaced it.
            return BatchOutcome::Completed;
        };

        let mut detecting_regions = Vec::new();
        let mut pull_end = PullEnd::BatchLimit;
        // A zero batch never polls the route stream, so the scan would re-enqueue itself
        // forever without making progress.
        for _ in 0..self.initialize_batch_size.max(1) {
            match scan.routes.try_next().await {
                Ok(Some(route)) => {
                    if !route.is_physical() {
                        continue;
                    }

                    let physical_table_route = route.into_physical_table_route();
                    physical_table_route
                        .region_routes
                        .iter()
                        .for_each(|region_route| {
                            if !scan.known_regions.contains(&region_route.region.id)
                                && let Some(leader_peer) = &region_route.leader_peer
                            {
                                detecting_regions.push((leader_peer.id, region_route.region.id));
                            }
                        });
                }
                Ok(None) => {
                    pull_end = PullEnd::Exhausted;
                    break;
                }
                Err(err) => {
                    error!(err; "Failed to scan table routes during initialize all regions.");
                    pull_end = PullEnd::Failed;
                    break;
                }
            }
        }

        scan.registered += detecting_regions.len();
        if !detecting_regions.is_empty() {
            self.register_failure_detectors(detecting_regions).await;
        }

        match pull_end {
            PullEnd::Exhausted => {
                info!(
                    "Initialize {} region failure detectors, elapsed: {:?}",
                    scan.registered,
                    scan.started_at.elapsed()
                );
                // Ignore the error.
                let _ = scan.sender.send(());
                BatchOutcome::Completed
            }
            // Dropping `scan` here drops `sender` without firing it, so the ticker retries.
            PullEnd::Failed => BatchOutcome::Completed,
            PullEnd::BatchLimit => {
                let outcome = match self
                    .self_sender
                    .try_send(Event::ContinueInitializeAllRegions)
                {
                    Ok(()) => BatchOutcome::Suspended,
                    // The queue is full, so the continuation cannot be queued: the batch does
                    // not yield and the scan keeps running inline rather than dropping the
                    // routes it has left. That is the pre-batching behaviour, and it holds
                    // only while the queue stays full.
                    Err(TrySendError::Full(_)) => BatchOutcome::NotYielded,
                    Err(TrySendError::Closed(_)) => BatchOutcome::Completed,
                };
                *init_scan = Some(scan);
                outcome
            }
        }
    }

    /// Runs batches until the scan completes, suspends itself, or cannot be re-queued.
    async fn drain_initialize_all(&self, init_scan: &mut Option<InitializeAllScan>) {
        while let BatchOutcome::NotYielded = self.step_initialize_all(init_scan).await {}
    }

    async fn register_failure_detectors(&self, detecting_regions: Vec<DetectingRegion>) {
        let ts_millis = current_time_millis();
        for region in detecting_regions {
            // The corresponding region has `acceptable_heartbeat_pause_millis` to send heartbeat from datanode.
            self.failure_detector
                .maybe_init_region_failure_detector(region, ts_millis);
        }
    }

    async fn reset_failure_detectors(&mut self, detecting_regions: Vec<DetectingRegion>) {
        let ts_millis = current_time_millis();
        for region in detecting_regions {
            self.failure_detector
                .reset_region_failure_detector(region, ts_millis);
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
                    extensions: Default::default(),
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
        if !result.region_not_found.is_empty() {
            let detecting_regions = result
                .region_not_found
                .iter()
                .map(|region_id| (from_peer_id, *region_id))
                .collect::<Vec<_>>();
            self.deregister_failure_detectors(detecting_regions).await;
            info!(
                "Region route not found, removed failover detectors for regions: {:?}",
                result.region_not_found
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
    use std::any::Any;
    use std::assert_matches;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, RwLock};
    use std::time::Duration;

    use common_meta::ddl::RegionFailureDetectorController;
    use common_meta::ddl::test_util::{
        test_create_logical_table_task, test_create_physical_table_task,
    };
    use common_meta::key::table_route::{
        LogicalTableRouteValue, PhysicalTableRouteValue, TableRouteKey, TableRouteValue,
    };
    use common_meta::key::{TableMetadataManager, runtime_switch};
    use common_meta::kv_backend::txn::{Txn, TxnResponse};
    use common_meta::kv_backend::{KvBackend, KvBackendRef, TxnService};
    use common_meta::peer::Peer;
    use common_meta::rpc::router::{Region, RegionRoute};
    use common_meta::rpc::store::{
        BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse,
        BatchPutRequest, BatchPutResponse, DeleteRangeRequest, DeleteRangeResponse, PutRequest,
        PutResponse, RangeRequest, RangeResponse,
    };
    use common_meta::test_util::NoopPeerResolver;
    use common_telemetry::info;
    use common_time::util::current_time_millis;
    use rand::Rng;
    use store_api::storage::RegionId;
    use tokio::sync::mpsc::Sender;
    use tokio::sync::{Notify, Semaphore, oneshot};
    use tokio::time::sleep;

    use super::RegionSupervisorSelector;
    use crate::metrics::METRIC_META_HEARTBEAT_DROPPED;
    use crate::procedure::region_migration::RegionMigrationTriggerReason;
    use crate::procedure::region_migration::manager::{
        RegionMigrationManager, SubmitRegionMigrationTaskResult,
    };
    use crate::procedure::region_migration::test_util::TestingEnv;
    use crate::region::supervisor::{
        DatanodeHeartbeat, Event, HeartbeatAcceptor, RegionFailureDetectorControl,
        RegionSupervisor, RegionSupervisorTicker,
    };
    use crate::selector::test_utils::{RandomNodeSelector, new_test_selector_context};
    use crate::state::{State, StateRef, become_follower, become_leader};

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
                tx.clone(),
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
        let table_route = LogicalTableRouteValue::new(1024);
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

    /// Creates `count` physical tables from `first_table_id`, each holding a single region led
    /// by `Peer::empty(1)`.
    async fn create_physical_tables(
        table_metadata_manager: &TableMetadataManager,
        first_table_id: u32,
        count: u32,
    ) {
        for table_id in first_table_id..first_table_id + count {
            let mut task = test_create_physical_table_task(&format!("physical_{table_id}"));
            task.set_table_id(table_id);
            let table_route = PhysicalTableRouteValue::new(vec![RegionRoute {
                region: Region {
                    id: RegionId::new(table_id, 0),
                    ..Default::default()
                },
                leader_peer: Some(Peer::empty(1)),
                ..Default::default()
            }]);
            table_metadata_manager
                .create_table_metadata(
                    task.table_info,
                    TableRouteValue::Physical(table_route),
                    HashMap::new(),
                )
                .await
                .unwrap();
        }
    }

    /// KV backend that blocks reads of table routes until the test hands out a pass.
    struct ScanGateBackend {
        inner: KvBackendRef,
        passes: Arc<Semaphore>,
        entered: Arc<Notify>,
    }

    impl ScanGateBackend {
        fn new(inner: KvBackendRef) -> (Self, Arc<Semaphore>, Arc<Notify>) {
            let passes = Arc::new(Semaphore::new(0));
            let entered = Arc::new(Notify::new());
            let backend = Self {
                inner,
                passes: passes.clone(),
                entered: entered.clone(),
            };
            (backend, passes, entered)
        }
    }

    #[async_trait::async_trait]
    impl TxnService for ScanGateBackend {
        type Error = common_meta::error::Error;

        async fn txn(&self, txn: Txn) -> Result<TxnResponse, Self::Error> {
            self.inner.txn(txn).await
        }

        fn max_txn_ops(&self) -> usize {
            self.inner.max_txn_ops()
        }
    }

    #[async_trait::async_trait]
    impl KvBackend for ScanGateBackend {
        fn name(&self) -> &str {
            "test_scan_gate"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        async fn range(&self, req: RangeRequest) -> Result<RangeResponse, Self::Error> {
            if !req.key.starts_with(&TableRouteKey::range_prefix()) {
                return self.inner.range(req).await;
            }

            self.entered.notify_one();
            // Forgetting the permit consumes it for good, so one `add_permits(1)` lets exactly
            // one more table-route read through.
            self.passes.clone().acquire_owned().await.unwrap().forget();
            self.inner.range(req).await
        }

        async fn put(&self, req: PutRequest) -> Result<PutResponse, Self::Error> {
            self.inner.put(req).await
        }

        async fn batch_put(&self, req: BatchPutRequest) -> Result<BatchPutResponse, Self::Error> {
            self.inner.batch_put(req).await
        }

        async fn batch_get(&self, req: BatchGetRequest) -> Result<BatchGetResponse, Self::Error> {
            self.inner.batch_get(req).await
        }

        async fn delete_range(
            &self,
            req: DeleteRangeRequest,
        ) -> Result<DeleteRangeResponse, Self::Error> {
            self.inner.delete_range(req).await
        }

        async fn batch_delete(
            &self,
            req: BatchDeleteRequest,
        ) -> Result<BatchDeleteResponse, Self::Error> {
            self.inner.batch_delete(req).await
        }
    }

    /// Builds a supervisor whose table-route reads are gated by [`ScanGateBackend`].
    ///
    /// The tables are created through the ungated backend first, so setup cannot block on the
    /// gate. Returns the supervisor, the event sender, and the two pacing handles.
    async fn new_gated_test_supervisor(
        first_table_id: u32,
        table_count: u32,
    ) -> (RegionSupervisor, Sender<Event>, Arc<Semaphore>, Arc<Notify>) {
        let env = TestingEnv::new();
        let raw = env.kv_backend();
        create_physical_tables(
            &TableMetadataManager::new(raw.clone()),
            first_table_id,
            table_count,
        )
        .await;

        let (gated, passes, entered) = ScanGateBackend::new(raw);
        let gated: KvBackendRef = Arc::new(gated);

        let selector_context = new_test_selector_context();
        let selector = Arc::new(RandomNodeSelector::new(vec![Peer::empty(1)]));
        let region_migration_manager = Arc::new(RegionMigrationManager::new(
            env.procedure_manager().clone(),
            env.context_factory(),
        ));
        let runtime_switch_manager =
            Arc::new(runtime_switch::RuntimeSwitchManager::new(gated.clone()));
        let peer_resolver = Arc::new(NoopPeerResolver);
        let (tx, rx) = RegionSupervisor::channel();
        let supervisor = RegionSupervisor::new(
            rx,
            tx.clone(),
            Default::default(),
            selector_context,
            RegionSupervisorSelector::NaiveSelector(selector),
            region_migration_manager,
            runtime_switch_manager,
            peer_resolver,
            gated,
        );

        (supervisor, tx, passes, entered)
    }

    fn filler_heartbeat() -> DatanodeHeartbeat {
        DatanodeHeartbeat {
            datanode_id: 1,
            regions: vec![],
            timestamp: 0,
        }
    }

    // Regression test for the batching added for issue #9419: the initialization scan must
    // cover every table route across batches without restarting or silently truncating.
    #[tokio::test]
    async fn test_initialize_all_regions_spans_batches() {
        common_telemetry::init_default_ut_logging();
        let (supervisor, sender) = new_test_supervisor();
        create_physical_tables(
            &TableMetadataManager::new(supervisor.kv_backend.clone()),
            1024,
            5,
        )
        .await;

        // Five routes and a batch of two: finishing takes three batches. A scan that ran only
        // its first batch would register two detectors and stop.
        let mut supervisor = supervisor.with_initialize_batch_size(2);
        tokio::spawn(async move { supervisor.run().await });

        let (tx, rx) = oneshot::channel();
        sender.send(Event::InitializeAllRegions(tx)).await.unwrap();
        assert!(rx.await.is_ok());

        let (tx, rx) = oneshot::channel();
        sender.send(Event::Dump(tx)).await.unwrap();
        let detector = rx.await.unwrap();
        assert_eq!(detector.len(), 5);
        for i in 0..5 {
            assert!(detector.contains(&(1, RegionId::new(1024 + i, 0))));
        }
    }

    // Regression test for the batching added for issue #9419: an event queued behind the scan
    // must be handled between batches, not after the whole scan.
    //
    // The scan is gated so the assertion cannot race it: while the scan is parked in a read,
    // it cannot have completed, so observing an answered dump is proof that the loop got back
    // to the queue.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_initialize_all_regions_yields_to_queued_events_between_batches() {
        common_telemetry::init_default_ut_logging();
        let (supervisor, sender, passes, entered) = new_gated_test_supervisor(1024, 3).await;
        let mut supervisor = supervisor
            .with_initialize_batch_size(1)
            .with_initialize_page_size(1);
        tokio::spawn(async move { supervisor.run().await });

        let (init_tx, mut init_rx) = oneshot::channel();
        sender
            .send(Event::InitializeAllRegions(init_tx))
            .await
            .unwrap();

        // Run the first batch, then let the scan park inside the second batch's read.
        entered.notified().await;
        passes.add_permits(1);
        entered.notified().await;

        // Queue a heartbeat and a dump behind the parked scan.
        sender
            .send(Event::HeartbeatArrived(filler_heartbeat()))
            .await
            .unwrap();
        let (dump_tx, dump_rx) = oneshot::channel();
        sender.send(Event::Dump(dump_tx)).await.unwrap();

        // Finish the second batch. The rest of the scan is handed back through the back of the
        // queue, so the heartbeat and the dump -- queued earlier -- are handled first.
        passes.add_permits(1);
        let detector = dump_rx.await.unwrap();
        assert!(init_rx.try_recv().is_err());
        assert_eq!(detector.len(), 2);
        assert!(detector.contains(&(1, RegionId::new(1024, 0))));
        assert!(detector.contains(&(1, RegionId::new(1025, 0))));

        // Let the rest of the scan finish.
        passes.add_permits(64);
        assert!(
            tokio::time::timeout(Duration::from_secs(30), init_rx)
                .await
                .unwrap()
                .is_ok()
        );
    }

    // Regression test for the batching added for issue #9419: when the continuation cannot be
    // queued, the scan must continue inline instead of losing the remaining routes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_initialize_all_regions_survives_a_full_queue() {
        common_telemetry::init_default_ut_logging();
        let (supervisor, sender, passes, entered) = new_gated_test_supervisor(1024, 5).await;
        let mut supervisor = supervisor
            .with_initialize_batch_size(2)
            .with_initialize_page_size(1);
        tokio::spawn(async move { supervisor.run().await });

        let (init_tx, init_rx) = oneshot::channel();
        sender
            .send(Event::InitializeAllRegions(init_tx))
            .await
            .unwrap();
        entered.notified().await;

        // Fill the queue so the continuation has nowhere to go.
        let mut queued = 0;
        while sender
            .try_send(Event::HeartbeatArrived(filler_heartbeat()))
            .is_ok()
        {
            queued += 1;
        }
        assert_eq!(queued, sender.max_capacity());

        // The scan must run to completion inline rather than dropping the routes it has left.
        passes.add_permits(64);
        tokio::time::timeout(Duration::from_secs(30), init_rx)
            .await
            .unwrap()
            .unwrap();

        // Wait for the queued events to drain, then confirm every region was registered.
        let (dump_tx, dump_rx) = oneshot::channel();
        tokio::time::timeout(Duration::from_secs(30), sender.send(Event::Dump(dump_tx)))
            .await
            .unwrap()
            .unwrap();
        let detector = tokio::time::timeout(Duration::from_secs(30), dump_rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(detector.len(), 5);
    }

    // Regression test for the batching added for issue #9419: losing leadership mid-scan must
    // abandon the scan and drop its completion signal. A signal that is merely parked would
    // leave the ticker waiting on it forever, with no retry.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_initialize_all_regions_aborts_on_leadership_loss() {
        common_telemetry::init_default_ut_logging();
        let (supervisor, sender, passes, entered) = new_gated_test_supervisor(1024, 3).await;
        let state: StateRef = Arc::new(RwLock::new(State::leader("test".into(), false)));
        let mut supervisor = supervisor
            .with_state(state.clone())
            .with_initialize_batch_size(1)
            .with_initialize_page_size(1);
        tokio::spawn(async move { supervisor.run().await });

        let (init_tx, init_rx) = oneshot::channel();
        sender
            .send(Event::InitializeAllRegions(init_tx))
            .await
            .unwrap();

        // Lose leadership while the scan is suspended, then let it finish its batch. Its
        // continuation is queued, and the loop drops it along with the scan.
        entered.notified().await;
        state.write().unwrap().next_state(become_follower());
        passes.add_permits(1);

        assert!(
            tokio::time::timeout(Duration::from_secs(30), init_rx)
                .await
                .unwrap()
                .is_err()
        );

        // Starting over as leader must work.
        state.write().unwrap().next_state(become_leader(false));
        passes.add_permits(64);
        let (init_tx, init_rx) = oneshot::channel();
        sender
            .send(Event::InitializeAllRegions(init_tx))
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(30), init_rx)
                .await
                .unwrap()
                .is_ok()
        );
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
    async fn test_handle_submit_region_migration_task_result_region_not_found() {
        common_telemetry::init_default_ut_logging();
        let (mut supervisor, _) = new_test_supervisor();
        let region_id = RegionId::new(1, 1);
        let detecting_region = (1, region_id);
        supervisor
            .register_failure_detectors(vec![detecting_region])
            .await;
        supervisor.failover_counts.insert(detecting_region, 1);
        let result = SubmitRegionMigrationTaskResult {
            region_not_found: vec![region_id],
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

    // Regression test for issue #9419: a busy region supervisor used to park the heartbeat
    // response path, withholding region lease grants from every datanode.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_heartbeat_accept_does_not_block_on_full_queue() {
        let (mut supervisor, sender, passes, entered) = new_gated_test_supervisor(1024, 1).await;
        tokio::spawn(async move { supervisor.run().await });

        // Busy the supervisor: the initialization scan blocks inside its first table-route read.
        let (init_done_tx, init_done_rx) = oneshot::channel();
        sender
            .send(Event::InitializeAllRegions(init_done_tx))
            .await
            .unwrap();
        entered.notified().await;

        // Fill the bounded event queue while the supervisor is blocked.
        let capacity = sender.max_capacity();
        let mut queued = 0;
        while sender
            .try_send(Event::HeartbeatArrived(filler_heartbeat()))
            .is_ok()
        {
            queued += 1;
        }
        assert_eq!(queued, capacity);
        assert_eq!(sender.capacity(), 0);

        // The heartbeat must be dropped instead of blocking, and counted as such.
        let acceptor = HeartbeatAcceptor::new(sender.clone());
        let dropped_before = METRIC_META_HEARTBEAT_DROPPED
            .with_label_values(&["full"])
            .get();
        acceptor.accept(filler_heartbeat());
        assert_eq!(
            METRIC_META_HEARTBEAT_DROPPED
                .with_label_values(&["full"])
                .get(),
            dropped_before + 1
        );
        assert_eq!(sender.capacity(), 0);

        // Release the scan and let the supervisor drain the queue.
        passes.add_permits(1);
        let _ = init_done_rx.await;
        tokio::time::timeout(Duration::from_secs(30), async {
            while sender.capacity() < capacity {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        // Once there is room again, heartbeats go through instead of being dropped.
        // That an accepted heartbeat reaches the detector is covered by
        // `failure_handler::tests::test_handle_heartbeat`.
        let dropped_before = METRIC_META_HEARTBEAT_DROPPED
            .with_label_values(&["full"])
            .get();
        acceptor.accept(filler_heartbeat());
        assert_eq!(
            METRIC_META_HEARTBEAT_DROPPED
                .with_label_values(&["full"])
                .get(),
            dropped_before,
            "a heartbeat accepted with queue room must not be counted as dropped"
        );
    }
}
