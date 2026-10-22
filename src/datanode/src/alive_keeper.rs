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

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use api::v1::meta::GrantedRegion;
use async_trait::async_trait;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_meta::error::InvalidProtoMsgSnafu;
use common_meta::heartbeat::handler::{
    HandleControl, HeartbeatResponseHandler, HeartbeatResponseHandlerContext,
};
use common_telemetry::{debug, error, info, trace, warn};
use snafu::OptionExt;
use store_api::region_engine::{RegionLeaseGrant, RegionRole, SettableRegionRoleState};
use store_api::region_request::{RegionCloseRequest, RegionRequest};
use store_api::storage::RegionId;
#[cfg(test)]
use tokio::sync::oneshot;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

use crate::error::{self, Result};
use crate::event_listener::{RegionServerEvent, RegionServerEventReceiver};
use crate::region_server::RegionServer;

/// [RegionAliveKeeper] manages all [CountdownTaskHandle]s.
///
/// [RegionAliveKeeper] starts a [CountdownTask] for each region. When the deadline is reached,
/// the status of region be set to "readonly", ensures there is no side-effect in the entity system.
///
/// The deadline is controlled by the meta server. Datanode will send its opened regions info to meta sever
/// via heartbeat. If the meta server decides some region could be resided in this Datanode,
/// it will renew the lease of region, a deadline of [CountdownTask] will be reset.
pub struct RegionAliveKeeper {
    region_server: RegionServer,
    tasks: Arc<Mutex<HashMap<RegionId, Arc<CountdownTaskHandle>>>>,
    heartbeat_interval_millis: Arc<AtomicU64>,
    started: Arc<AtomicBool>,

    /// The epoch when [RegionAliveKeeper] is created. It's used to get a monotonically non-decreasing
    /// elapsed time when submitting heartbeats to the meta server (because [Instant] is monotonically
    /// non-decreasing). The heartbeat requests will carry the duration since this epoch, and the
    /// duration acts like an "invariant point" for region's keep alive lease.
    epoch: Instant,

    countdown_task_handler_ext: Option<CountdownTaskHandlerExtRef>,
}

impl RegionAliveKeeper {
    /// Returns an empty [RegionAliveKeeper].
    pub fn new(
        region_server: RegionServer,
        countdown_task_handler_ext: Option<CountdownTaskHandlerExtRef>,
        heartbeat_interval: Duration,
    ) -> Self {
        Self {
            region_server,
            tasks: Arc::new(Mutex::new(HashMap::new())),
            heartbeat_interval_millis: Arc::new(AtomicU64::new(
                heartbeat_interval.as_millis() as u64
            )),
            started: Arc::new(AtomicBool::new(false)),
            epoch: Instant::now(),
            countdown_task_handler_ext,
        }
    }

    /// Update the heartbeat interval with the value received from Metasrv.
    pub fn update_heartbeat_interval(&self, heartbeat_interval_millis: u64) {
        self.heartbeat_interval_millis
            .store(heartbeat_interval_millis, Ordering::Relaxed);
    }

    async fn find_handle(&self, region_id: RegionId) -> Option<Arc<CountdownTaskHandle>> {
        self.tasks.lock().await.get(&region_id).cloned()
    }

    /// Add the countdown task for a specific region.
    /// It will be ignored if the task exists.
    pub async fn register_region(&self, region_id: RegionId) {
        let handle = Arc::new(CountdownTaskHandle::new(
            self.region_server.clone(),
            self.countdown_task_handler_ext.clone(),
            region_id,
        ));

        let should_start = {
            let mut handles = self.tasks.lock().await;

            // Check if already exists, return early if so
            if handles.contains_key(&region_id) {
                return;
            }

            // Insert new handle
            handles.insert(region_id, handle.clone());

            // Return whether we should start (check state inside lock)
            self.started.load(Ordering::Relaxed)
        };

        if should_start {
            handle
                .start(self.heartbeat_interval_millis.load(Ordering::Relaxed))
                .await;
            info!("Region alive countdown for region {region_id} is started!");
        } else {
            info!(
                "Region alive countdown for region {region_id} is registered but not started yet!"
            );
        }
    }

    /// Removes the countdown task for a specific region.
    pub async fn deregister_region(&self, region_id: RegionId) {
        if self.tasks.lock().await.remove(&region_id).is_some() {
            info!("Deregister alive countdown for region {region_id}")
        }
    }

    /// Renews the lease of regions to `deadline`, granted for the heartbeat sent at `heartbeat_sent_at`.
    async fn renew_region_leases(
        &self,
        regions: &[GrantedRegion],
        heartbeat_sent_at: Instant,
        deadline: Instant,
    ) {
        for region in regions {
            let (role, region_id) = (region.role().into(), RegionId::from(region.region_id));
            if let Some(handle) = self.find_handle(region_id).await {
                handle
                    .reset_deadline(role, heartbeat_sent_at, deadline, region.extensions.clone())
                    .await;
            } else {
                warn!(
                    "Trying to renew the lease for region {region_id}, the keeper handler is not found!"
                );
                // Else the region alive keeper might be triggered by lagging messages, we can safely ignore it.
            }
        }
    }

    async fn close_staled_region(&self, region_id: RegionId) {
        info!("Closing staled region: {region_id}");
        let request = RegionRequest::Close(RegionCloseRequest::default());
        if let Err(e) = self.region_server.handle_request(region_id, request).await
            && e.status_code() != StatusCode::RegionNotFound
        {
            let _ = self
                .region_server
                .set_region_role(region_id, RegionRole::Follower);
            error!(e; "Failed to close staled region {}, convert region to follower.", region_id);
        }
    }

    /// Closes staled regions.
    async fn close_staled_regions(&self, regions: &[u64]) {
        for region_id in regions {
            self.close_staled_region(RegionId::from_u64(*region_id))
                .await;
        }
    }

    #[cfg(test)]
    async fn deadline(&self, region_id: RegionId) -> Option<Instant> {
        let mut deadline = None;
        if let Some(handle) = self.find_handle(region_id).await {
            let (s, r) = oneshot::channel();
            if handle.tx.send(CountdownCommand::Deadline(s)).await.is_ok() {
                deadline = r.await.ok()
            }
        }
        deadline
    }

    pub async fn start(
        self: &Arc<Self>,
        event_receiver: Option<RegionServerEventReceiver>,
    ) -> Result<()> {
        self.started.store(true, Ordering::Relaxed);

        if let Some(mut event_receiver) = event_receiver {
            let keeper = self.clone();
            // Initializers region alive keeper.
            // It makes sure all opened regions are registered to `RegionAliveKeeper.`
            loop {
                match event_receiver.0.try_recv() {
                    Ok(RegionServerEvent::Registered(region_id)) => {
                        keeper.register_region(region_id).await;
                    }
                    Ok(RegionServerEvent::Deregistered(region_id)) => {
                        keeper.deregister_region(region_id).await;
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        return error::UnexpectedSnafu {
                            violated: "RegionServerEventSender closed",
                        }
                        .fail();
                    }
                    Err(mpsc::error::TryRecvError::Empty) => {
                        break;
                    }
                }
            }
            let running = self.started.clone();

            // Watches changes
            common_runtime::spawn_global(async move {
                loop {
                    if !running.load(Ordering::Relaxed) {
                        info!("RegionAliveKeeper stopped! Quits the watch loop!");
                        break;
                    }

                    match event_receiver.0.recv().await {
                        Some(RegionServerEvent::Registered(region_id)) => {
                            keeper.register_region(region_id).await;
                        }
                        Some(RegionServerEvent::Deregistered(region_id)) => {
                            keeper.deregister_region(region_id).await;
                        }
                        None => {
                            info!("RegionServerEventSender closed! Quits the watch loop!");
                            break;
                        }
                    }
                }
            });
        }

        let tasks = self.tasks.lock().await;
        let interval = self.heartbeat_interval_millis.load(Ordering::Relaxed);
        for task in tasks.values() {
            task.start(interval).await;
        }

        info!(
            "RegionAliveKeeper is started with region {:?}",
            tasks.keys().map(|x| x.to_string()).collect::<Vec<_>>(),
        );

        Ok(())
    }

    pub fn epoch(&self) -> Instant {
        self.epoch
    }
}

#[async_trait]
impl HeartbeatResponseHandler for RegionAliveKeeper {
    fn is_acceptable(&self, ctx: &HeartbeatResponseHandlerContext) -> bool {
        ctx.response.region_lease.is_some()
    }

    async fn handle(
        &self,
        ctx: &mut HeartbeatResponseHandlerContext,
    ) -> common_meta::error::Result<HandleControl> {
        let region_lease = ctx
            .response
            .region_lease
            .as_ref()
            .context(InvalidProtoMsgSnafu {
                err_msg: "'region_lease' is missing in heartbeat response",
            })?;
        let start_instant = self.epoch + Duration::from_millis(region_lease.duration_since_epoch);
        let lease = Duration::from_secs(region_lease.lease_seconds);
        let deadline = start_instant + lease;

        // Regions created on this node write under the node lease until their first grant.
        if let Some(mito) = self.region_server.mito_engine() {
            mito.renew_node_lease(start_instant.into_std(), lease);
        }
        self.renew_region_leases(&region_lease.regions, start_instant, deadline)
            .await;
        self.close_staled_regions(&region_lease.closeable_region_ids)
            .await;

        Ok(HandleControl::Continue)
    }
}

#[derive(Debug)]
enum CountdownCommand {
    /// Start this countdown task. The first deadline will be set to
    /// 4 * `heartbeat_interval_millis`
    Start(u64),
    /// Reset countdown deadline to the given instance.
    /// (NextRole, HeartbeatSentAt, Deadline, ExtensionInfo)
    Reset((RegionRole, Instant, Instant, HashMap<String, Vec<u8>>)),
    /// Returns the current deadline of the countdown task.
    #[cfg(test)]
    Deadline(oneshot::Sender<Instant>),
}

pub type CountdownTaskHandlerExtRef = Arc<dyn CountdownTaskHandlerExt>;

/// Extension trait for [CountdownTaskHandlerExt] to reset deadline of a region.
#[async_trait]
pub trait CountdownTaskHandlerExt: Send + Sync {
    async fn reset_deadline(
        &self,
        region_server: &RegionServer,
        region_id: RegionId,
        role: RegionRole,
        deadline: Instant,
        extension_info: HashMap<String, Vec<u8>>,
    );
}

struct CountdownTaskHandle {
    tx: mpsc::Sender<CountdownCommand>,
    handler: JoinHandle<()>,
    region_id: RegionId,
}

impl CountdownTaskHandle {
    /// Creates a new [CountdownTaskHandle] and starts the countdown task.
    fn new(
        region_server: RegionServer,
        handler_ext: Option<CountdownTaskHandlerExtRef>,
        region_id: RegionId,
    ) -> Self {
        let (tx, rx) = mpsc::channel(1024);

        let mut countdown_task = CountdownTask {
            region_server,
            handler_ext,
            region_id,
            rx,
        };
        let handler = common_runtime::spawn_hb(async move {
            countdown_task.run().await;
        });

        Self {
            tx,
            handler,
            region_id,
        }
    }

    /// Starts the [CountdownTask],
    /// it will be ignored if the task started.
    async fn start(&self, heartbeat_interval_millis: u64) {
        if let Err(e) = self
            .tx
            .send(CountdownCommand::Start(heartbeat_interval_millis))
            .await
        {
            warn!(
                "Failed to start region alive keeper countdown: {e}. \
                Maybe the task is stopped due to region been closed."
            );
        }
    }

    #[cfg(test)]
    async fn deadline(&self) -> Option<Instant> {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(CountdownCommand::Deadline(tx)).await.is_ok() {
            return rx.await.ok();
        }
        None
    }

    async fn reset_deadline(
        &self,
        role: RegionRole,
        heartbeat_sent_at: Instant,
        deadline: Instant,
        extension_info: HashMap<String, Vec<u8>>,
    ) {
        if let Err(e) = self
            .tx
            .send(CountdownCommand::Reset((
                role,
                heartbeat_sent_at,
                deadline,
                extension_info,
            )))
            .await
        {
            warn!(
                "Failed to reset region alive keeper deadline: {e}. \
                Maybe the task is stopped due to region been closed."
            );
        }
    }
}

impl Drop for CountdownTaskHandle {
    fn drop(&mut self) {
        debug!(
            "Aborting region alive countdown task for region {}",
            self.region_id
        );
        self.handler.abort();
    }
}

struct CountdownTask {
    region_server: RegionServer,
    region_id: RegionId,
    handler_ext: Option<CountdownTaskHandlerExtRef>,
    rx: mpsc::Receiver<CountdownCommand>,
}

impl CountdownTask {
    async fn run(&mut self) {
        // 30 years. See `Instant::far_future`.
        let far_future = Instant::now() + Duration::from_secs(86400 * 365 * 30);

        // Make sure the alive countdown is not gonna happen before heartbeat task is started (the
        // "start countdown" command will be sent from heartbeat task).
        let countdown = tokio::time::sleep_until(far_future);
        tokio::pin!(countdown);
        let region_id = self.region_id;

        let mut started = false;
        loop {
            tokio::select! {
                command = self.rx.recv() => {
                    match command {
                        Some(CountdownCommand::Start(heartbeat_interval_millis)) => {
                            if !started {
                                // Set first deadline in 4 heartbeats (roughly after 12 seconds from now if heartbeat
                                // interval is set to default 3 seconds), to make Datanode and Metasrv more tolerable to
                                // network or other jitters during startup.
                                let first_deadline = Instant::now() + Duration::from_millis(heartbeat_interval_millis) * 4;
                                countdown.set(tokio::time::sleep_until(first_deadline));
                                started = true;
                            }
                        },
                        Some(CountdownCommand::Reset((role, heartbeat_sent_at, deadline, extension_info))) => {
                            let prev_role_leader = self.region_server.is_region_leader(self.region_id).unwrap_or(false);
                            let grant = RegionLeaseGrant {
                                role,
                                heartbeat_sent_at: heartbeat_sent_at.into_std(),
                                deadline: deadline.into_std(),
                            };
                            match self.region_server.set_region_role_by_lease(self.region_id, grant) {
                                Err(err) => {
                                    if err.status_code() == StatusCode::RegionNotFound {
                                        // Table metadata in metasrv is deleted after its regions are dropped.
                                        // The datanode may still receive lease renewal responses that depend on the metadata
                                        // during the short period before it is removed.
                                        warn!(err; "Failed to set region role to {role} for region {region_id}");
                                    } else {
                                        error!(err; "Failed to set region role to {role} for region {region_id}");
                                    }
                                }
                                // A leader grant for a heartbeat sent before a downgrade must not make the region writable again.
                                Ok(false) => {
                                    info!("Ignore the stale {role} lease of region {region_id}");
                                    continue;
                                }
                                // Finalize leadership: persist backfilled metadata.
                                Ok(true) => {
                                    if !prev_role_leader && role == RegionRole::Leader
                                        && let Err(err) = self
                                            .region_server
                                            .set_region_role_state_gracefully(self.region_id, SettableRegionRoleState::Leader)
                                            .await
                                    {
                                        error!(err; "Failed to set region role state gracefully to {role} for region {region_id}");
                                    }
                                }
                            }

                            if let Some(ext_handler) = self.handler_ext.as_ref() {
                                ext_handler.reset_deadline(
                                    &self.region_server,
                                    self.region_id,
                                    role,
                                    deadline,
                                    extension_info,
                                ).await;
                            }
                            trace!(
                                "Reset deadline of region {region_id} to approximately {} seconds later.",
                                (deadline - Instant::now()).as_secs_f32(),
                            );
                            countdown.set(tokio::time::sleep_until(deadline));
                        },
                        None => {
                            info!(
                                "The handle of countdown task for region {region_id}\
                                is dropped, RegionAliveKeeper out."
                            );
                            break;
                        },
                        #[cfg(test)]
                        Some(CountdownCommand::Deadline(tx)) => {
                            let _ = tx.send(countdown.deadline());
                        }
                    }
                }
                () = &mut countdown => {
                    warn!("The region {region_id} lease is expired, convert region to follower.");
                    if let Err(err) = self.region_server.set_region_role(self.region_id, RegionRole::Follower) {
                        error!(err; "Failed to set region role to follower for region {region_id}");
                    }
                    // resets the countdown.
                    let far_future = Instant::now() + Duration::from_secs(86400 * 365 * 30);
                    countdown.as_mut().reset(far_future);
                }
            }
        }
    }
}

#[cfg(test)]
mod test {

    use api::region::RegionResponse;
    use api::v1::meta::{HeartbeatResponse, RegionLease};
    use api::v1::{ColumnSchema, Rows};
    use common_meta::heartbeat::mailbox::HeartbeatMailbox;
    use common_recordbatch::RecordBatches;
    use mito2::config::MitoConfig;
    use mito2::engine::MitoEngine;
    use mito2::engine::listener::WriteWalListener;
    use mito2::test_util::{CreateRequestBuilder, TestEnv, build_rows, rows_schema};
    use store_api::region_engine::RegionEngine;
    use store_api::region_request::{
        EnterStagingRequest, RegionCatchupRequest, RegionPutRequest, StagingPartitionDirective,
    };
    use store_api::storage::ScanRequest;

    use super::*;
    use crate::tests::mock_region_server;

    /// Returns a region server whose Mito engine needs a lease to acknowledge writes, as on a
    /// datanode the metasrv controls.
    async fn lease_region_server(
        env: &mut TestEnv,
        listener: Option<Arc<WriteWalListener>>,
    ) -> (RegionServer, MitoEngine) {
        let mut region_server = mock_region_server();
        let engine = env
            .create_engine_with(
                MitoConfig::default(),
                None,
                listener.map(|listener| listener as _),
                None,
            )
            .await;
        engine.require_write_lease();
        region_server.register_engine(Arc::new(engine.clone()));
        (region_server, engine)
    }

    async fn create_region(region_server: &RegionServer, region_id: RegionId) -> Vec<ColumnSchema> {
        let request = CreateRequestBuilder::new().build();
        let schema = rows_schema(&request);
        region_server
            .handle_request(region_id, RegionRequest::Create(request))
            .await
            .unwrap();
        schema
    }

    async fn put_row(
        region_server: &RegionServer,
        region_id: RegionId,
        schema: &[ColumnSchema],
    ) -> Result<RegionResponse> {
        region_server
            .handle_request(
                region_id,
                RegionRequest::Put(RegionPutRequest {
                    rows: Rows {
                        schema: schema.to_vec(),
                        rows: build_rows(0, 1),
                    },
                    hint: None,
                    partition_expr_version: None,
                    skip_wal: false,
                }),
            )
            .await
    }

    fn leader_grant(region_id: RegionId) -> GrantedRegion {
        GrantedRegion {
            region_id: region_id.as_u64(),
            role: api::v1::meta::RegionRole::Leader.into(),
            extensions: HashMap::new(),
        }
    }

    /// Handles a heartbeat response with region leases for a heartbeat sent now.
    async fn handle_lease_response(
        alive_keeper: &RegionAliveKeeper,
        regions: Vec<GrantedRegion>,
        lease_seconds: u64,
    ) {
        let (tx, _rx) = mpsc::channel(1);
        let response = HeartbeatResponse {
            region_lease: Some(RegionLease {
                regions,
                duration_since_epoch: alive_keeper.epoch().elapsed().as_millis() as u64,
                lease_seconds,
                closeable_region_ids: vec![],
            }),
            ..Default::default()
        };
        let mut ctx =
            HeartbeatResponseHandlerContext::new(Arc::new(HeartbeatMailbox::new(tx)), response);
        assert!(alive_keeper.is_acceptable(&ctx));
        alive_keeper.handle(&mut ctx).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn region_alive_keeper() {
        common_telemetry::init_default_ut_logging();
        let mut region_server = mock_region_server();
        let mut engine_env = TestEnv::with_prefix("region-alive-keeper").await;
        let engine = engine_env.create_engine(MitoConfig::default()).await;
        let engine = Arc::new(engine);
        region_server.register_engine(engine.clone());

        let alive_keeper = Arc::new(RegionAliveKeeper::new(
            region_server.clone(),
            None,
            Duration::from_millis(100),
        ));

        let region_id = RegionId::new(1024, 1);
        let builder = CreateRequestBuilder::new();
        region_server
            .handle_request(region_id, RegionRequest::Create(builder.build()))
            .await
            .unwrap();
        region_server
            .set_region_role(region_id, RegionRole::Leader)
            .unwrap();

        // Register a region before starting.
        alive_keeper.register_region(region_id).await;
        assert!(alive_keeper.find_handle(region_id).await.is_some());

        info!("Start the keeper");
        alive_keeper.start(None).await.unwrap();

        // The started alive keeper should assign deadline to this region.
        let deadline = alive_keeper.deadline(region_id).await.unwrap();
        assert!(deadline >= Instant::now());
        assert_eq!(engine.role(region_id).unwrap(), RegionRole::Leader);

        info!("Wait for lease expired");
        // Sleep to wait lease expired.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(alive_keeper.find_handle(region_id).await.is_some());
        assert_eq!(engine.role(region_id).unwrap(), RegionRole::Follower);

        info!("Renew the region lease");
        // Renew lease then sleep.
        alive_keeper
            .renew_region_leases(
                &[GrantedRegion {
                    region_id: region_id.as_u64(),
                    role: api::v1::meta::RegionRole::Leader.into(),
                    extensions: HashMap::new(),
                }],
                Instant::now(),
                Instant::now() + Duration::from_millis(200),
            )
            .await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(alive_keeper.find_handle(region_id).await.is_some());
        let deadline = alive_keeper.deadline(region_id).await.unwrap();
        assert!(deadline >= Instant::now());
        assert_eq!(engine.role(region_id).unwrap(), RegionRole::Leader);

        info!("Wait for lease expired");
        // Sleep to wait lease expired.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(alive_keeper.find_handle(region_id).await.is_some());
        assert_eq!(engine.role(region_id).unwrap(), RegionRole::Follower);

        let deadline = alive_keeper.deadline(region_id).await.unwrap();
        assert!(deadline > Instant::now() + Duration::from_secs(86400 * 365 * 29));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn countdown_task() {
        let region_server = mock_region_server();

        let countdown_handle =
            CountdownTaskHandle::new(region_server, None, RegionId::new(9999, 2));

        // If countdown task is not started, its deadline is set to far future.
        assert!(
            countdown_handle.deadline().await.unwrap()
                > Instant::now() + Duration::from_secs(86400 * 365 * 29)
        );

        // The first deadline should be set to 4 * heartbeat_interval_millis.
        // We assert it to be greater than 3 * heartbeat_interval_millis to avoid flaky test.
        let heartbeat_interval_millis = 100;
        countdown_handle.start(heartbeat_interval_millis).await;
        assert!(
            countdown_handle.deadline().await.unwrap()
                > Instant::now() + Duration::from_millis(heartbeat_interval_millis * 3)
        );
        tokio::time::sleep(Duration::from_millis(heartbeat_interval_millis * 5)).await;

        // No effect.
        countdown_handle.start(heartbeat_interval_millis).await;
        assert!(
            countdown_handle.deadline().await.unwrap()
                > Instant::now() + Duration::from_secs(86400 * 365 * 29)
        );

        // Reset deadline.
        countdown_handle
            .reset_deadline(
                RegionRole::Leader,
                Instant::now(),
                Instant::now() + Duration::from_millis(heartbeat_interval_millis * 5),
                HashMap::new(),
            )
            .await;
        assert!(
            countdown_handle.deadline().await.unwrap()
                > Instant::now() + Duration::from_millis(heartbeat_interval_millis * 4)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn renew_staging_leader_keeps_region_in_staging() {
        let mut region_server = mock_region_server();
        let mut engine_env = TestEnv::with_prefix("region-alive-keeper-staging").await;
        let engine = engine_env.create_engine(MitoConfig::default()).await;
        let engine = Arc::new(engine);
        region_server.register_engine(engine.clone());

        let alive_keeper = Arc::new(RegionAliveKeeper::new(
            region_server.clone(),
            None,
            Duration::from_millis(100),
        ));

        let region_id = RegionId::new(1024, 2);
        region_server
            .handle_request(
                region_id,
                RegionRequest::Create(CreateRequestBuilder::new().build()),
            )
            .await
            .unwrap();
        region_server
            .handle_request(
                region_id,
                RegionRequest::EnterStaging(EnterStagingRequest {
                    partition_directive: StagingPartitionDirective::RejectAllWrites,
                }),
            )
            .await
            .unwrap();

        alive_keeper.register_region(region_id).await;
        alive_keeper
            .renew_region_leases(
                &[GrantedRegion {
                    region_id: region_id.as_u64(),
                    role: api::v1::meta::RegionRole::StagingLeader.into(),
                    extensions: HashMap::new(),
                }],
                Instant::now(),
                Instant::now() + Duration::from_millis(3000),
            )
            .await;

        assert_eq!(engine.role(region_id).unwrap(), RegionRole::StagingLeader);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn renew_staging_leader_exit_into_leader() {
        common_telemetry::init_default_ut_logging();
        let mut region_server = mock_region_server();
        let mut engine_env = TestEnv::with_prefix("region-alive-keeper-staging-exit").await;
        let engine = engine_env.create_engine(MitoConfig::default()).await;
        let engine = Arc::new(engine);
        region_server.register_engine(engine.clone());

        let alive_keeper = Arc::new(RegionAliveKeeper::new(
            region_server.clone(),
            None,
            Duration::from_millis(100),
        ));

        let region_id = RegionId::new(1024, 2);
        region_server
            .handle_request(
                region_id,
                RegionRequest::Create(CreateRequestBuilder::new().build()),
            )
            .await
            .unwrap();
        region_server
            .handle_request(
                region_id,
                RegionRequest::EnterStaging(EnterStagingRequest {
                    partition_directive: StagingPartitionDirective::RejectAllWrites,
                }),
            )
            .await
            .unwrap();

        alive_keeper.register_region(region_id).await;
        alive_keeper
            .renew_region_leases(
                &[GrantedRegion {
                    region_id: region_id.as_u64(),
                    role: api::v1::meta::RegionRole::Leader.into(),
                    extensions: HashMap::new(),
                }],
                Instant::now(),
                Instant::now() + Duration::from_millis(3000),
            )
            .await;

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(engine.role(region_id).unwrap(), RegionRole::Leader);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn renew_staging_leader_does_not_promote_normal_leader_into_staging() {
        let mut region_server = mock_region_server();
        let mut engine_env = TestEnv::with_prefix("region-alive-keeper-non-staging").await;
        let engine = engine_env.create_engine(MitoConfig::default()).await;
        let engine = Arc::new(engine);
        region_server.register_engine(engine.clone());

        let alive_keeper = Arc::new(RegionAliveKeeper::new(
            region_server.clone(),
            None,
            Duration::from_millis(100),
        ));

        let region_id = RegionId::new(1024, 4);
        region_server
            .handle_request(
                region_id,
                RegionRequest::Create(CreateRequestBuilder::new().build()),
            )
            .await
            .unwrap();
        region_server
            .set_region_role(region_id, RegionRole::Leader)
            .unwrap();

        alive_keeper.register_region(region_id).await;
        alive_keeper
            .renew_region_leases(
                &[GrantedRegion {
                    region_id: region_id.as_u64(),
                    role: api::v1::meta::RegionRole::StagingLeader.into(),
                    extensions: HashMap::new(),
                }],
                Instant::now(),
                Instant::now() + Duration::from_millis(3000),
            )
            .await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(engine.role(region_id).unwrap(), RegionRole::Leader);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn created_region_writes_under_node_lease() {
        common_telemetry::init_default_ut_logging();
        let mut env = TestEnv::with_prefix("region-alive-keeper-node-lease").await;
        let (region_server, _engine) = lease_region_server(&mut env, None).await;
        let alive_keeper =
            RegionAliveKeeper::new(region_server.clone(), None, Duration::from_millis(100));
        let region_id = RegionId::new(1024, 1);
        let schema = create_region(&region_server, region_id).await;

        // No heartbeat response has carried region leases yet.
        let err = put_row(&region_server, region_id, &schema)
            .await
            .unwrap_err();
        assert!(err.is_retryable(), "{err:?}");

        // A response without region leases does not renew the node lease.
        let (tx, _rx) = mpsc::channel(1);
        let ctx = HeartbeatResponseHandlerContext::new(
            Arc::new(HeartbeatMailbox::new(tx)),
            HeartbeatResponse::default(),
        );
        assert!(!alive_keeper.is_acceptable(&ctx));

        // A response with region leases renews it, though it grants nothing to the region. A
        // region keeps the node lease it was created under, so only a region created after the
        // response writes under it, until it expires.
        handle_lease_response(&alive_keeper, vec![], 1).await;
        let err = put_row(&region_server, region_id, &schema)
            .await
            .unwrap_err();
        assert!(err.is_retryable(), "{err:?}");
        let region_id = RegionId::new(1024, 2);
        let schema = create_region(&region_server, region_id).await;
        put_row(&region_server, region_id, &schema).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let err = put_row(&region_server, region_id, &schema)
            .await
            .unwrap_err();
        assert!(err.is_retryable(), "{err:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn created_region_keeps_node_lease_of_creation() {
        common_telemetry::init_default_ut_logging();
        let mut env = TestEnv::with_prefix("region-alive-keeper-created-lease-cap").await;
        let (region_server, _engine) = lease_region_server(&mut env, None).await;
        let alive_keeper =
            RegionAliveKeeper::new(region_server.clone(), None, Duration::from_millis(100));
        let lease = Duration::from_secs(4);
        handle_lease_response(&alive_keeper, vec![], lease.as_secs()).await;
        let renewed = Instant::now();
        // The region is created half a lease after that response.
        tokio::time::sleep(lease / 2).await;
        let creating = Instant::now();
        let region_id = RegionId::new(1024, 1);
        let schema = create_region(&region_server, region_id).await;
        put_row(&region_server, region_id, &schema).await.unwrap();

        // Responses keep renewing the node lease without granting the region anything. The
        // region keeps the node lease it was created under, which expires one lease after the
        // response before its creation, earlier than one lease after the creation.
        while renewed.elapsed() < lease + Duration::from_millis(200) {
            handle_lease_response(&alive_keeper, vec![], lease.as_secs()).await;
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        handle_lease_response(&alive_keeper, vec![], lease.as_secs()).await;
        let err = put_row(&region_server, region_id, &schema)
            .await
            .unwrap_err();
        assert!(
            format!("{err:?}").contains("does not hold a valid leader lease"),
            "{err:?}"
        );
        // The write was rejected within one lease of the creation.
        assert!(creating.elapsed() < lease);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn catchup_reopen_keeps_region_lease() {
        common_telemetry::init_default_ut_logging();
        let mut env = TestEnv::with_prefix("region-alive-keeper-reopen-lease").await;
        let (region_server, engine) = lease_region_server(&mut env, None).await;
        let region_id = RegionId::new(1024, 1);
        let schema = create_region(&region_server, region_id).await;
        // A Leader grant without a node lease: the upgrade below seeds nothing from the node
        // lease, so only the kept record lets the reopened region write.
        let now = std::time::Instant::now();
        let grant = RegionLeaseGrant {
            role: RegionRole::Leader,
            heartbeat_sent_at: now,
            deadline: now + Duration::from_secs(10),
        };
        assert!(
            region_server
                .set_region_role_by_lease(region_id, grant)
                .unwrap()
        );
        put_row(&region_server, region_id, &schema).await.unwrap();

        // The catchup reopens the region, as its memtable is not empty.
        region_server
            .set_region_role(region_id, RegionRole::Follower)
            .unwrap();
        let region = engine.find_region(region_id).unwrap();
        region_server
            .handle_request(
                region_id,
                RegionRequest::Catchup(RegionCatchupRequest {
                    set_writable: true,
                    entry_id: None,
                    metadata_entry_id: None,
                    checkpoint: None,
                }),
            )
            .await
            .unwrap();
        assert!(!Arc::ptr_eq(
            &region,
            &engine.find_region(region_id).unwrap()
        ));
        // The reopened region still holds the Leader grant.
        put_row(&region_server, region_id, &schema).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn writes_fail_after_lease_expiry_without_countdown() {
        common_telemetry::init_default_ut_logging();
        let mut env = TestEnv::with_prefix("region-alive-keeper-no-countdown").await;
        let listener = Arc::new(WriteWalListener::default());
        let (region_server, engine) = lease_region_server(&mut env, Some(listener.clone())).await;
        let alive_keeper =
            RegionAliveKeeper::new(region_server.clone(), None, Duration::from_millis(100));
        let region_id = RegionId::new(1024, 1);
        let schema = create_region(&region_server, region_id).await;
        alive_keeper.register_region(region_id).await;
        let deadline = Instant::now() + Duration::from_secs(2);
        alive_keeper
            .renew_region_leases(&[leader_grant(region_id)], Instant::now(), deadline)
            .await;
        alive_keeper.deadline(region_id).await.unwrap();
        // No lease comes anymore, and the countdown task stops.
        alive_keeper.deregister_region(region_id).await;

        // The lease expires while a write is in the WAL.
        listener.pause_next();
        let write = {
            let region_server = region_server.clone();
            let schema = schema.clone();
            tokio::spawn(async move { put_row(&region_server, region_id, &schema).await })
        };
        listener.wait_appended().await;
        tokio::time::sleep_until(deadline).await;
        listener.resume();
        let err = write.await.unwrap().unwrap_err();
        assert!(err.is_retryable(), "{err:?}");
        assert!(
            format!("{err:?}").contains("lost its leader lease while writing to the WAL"),
            "{err:?}"
        );
        // The write is in the WAL, so it is applied.
        let stream = engine
            .scan_to_stream(region_id, ScanRequest::default())
            .await
            .unwrap();
        let batches = RecordBatches::try_collect(stream).await.unwrap();
        assert_eq!(
            1,
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>()
        );

        // Without the countdown task, the region is still a leader but rejects writes.
        assert_eq!(engine.role(region_id), Some(RegionRole::Leader));
        let err = put_row(&region_server, region_id, &schema)
            .await
            .unwrap_err();
        assert!(err.is_retryable(), "{err:?}");
        assert!(
            format!("{err:?}").contains("does not hold a valid leader lease"),
            "{err:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stale_leader_lease_after_downgrade_is_ignored() {
        common_telemetry::init_default_ut_logging();
        let mut env = TestEnv::with_prefix("region-alive-keeper-stale-lease").await;
        let (region_server, engine) = lease_region_server(&mut env, None).await;
        let alive_keeper =
            RegionAliveKeeper::new(region_server.clone(), None, Duration::from_millis(100));
        let region_id = RegionId::new(1024, 1);
        let schema = create_region(&region_server, region_id).await;
        alive_keeper.register_region(region_id).await;
        let sent_before_downgrade = Instant::now();
        alive_keeper
            .renew_region_leases(
                &[leader_grant(region_id)],
                sent_before_downgrade,
                sent_before_downgrade + Duration::from_secs(10),
            )
            .await;
        alive_keeper.deadline(region_id).await.unwrap();

        region_server
            .set_region_role_state_gracefully(region_id, SettableRegionRoleState::DowngradingLeader)
            .await
            .unwrap();

        // A Leader grant for a heartbeat sent before the downgrade arrives late.
        alive_keeper
            .renew_region_leases(
                &[leader_grant(region_id)],
                sent_before_downgrade,
                sent_before_downgrade + Duration::from_secs(10),
            )
            .await;
        alive_keeper.deadline(region_id).await.unwrap();
        assert_eq!(engine.role(region_id), Some(RegionRole::DowngradingLeader));
        let err = put_row(&region_server, region_id, &schema)
            .await
            .unwrap_err();
        assert!(err.is_retryable(), "{err:?}");

        // After a rollback, the Leader grant for a later heartbeat applies.
        alive_keeper
            .renew_region_leases(
                &[leader_grant(region_id)],
                Instant::now(),
                Instant::now() + Duration::from_secs(10),
            )
            .await;
        alive_keeper.deadline(region_id).await.unwrap();
        assert_eq!(engine.role(region_id), Some(RegionRole::Leader));
        put_row(&region_server, region_id, &schema).await.unwrap();
    }
}
