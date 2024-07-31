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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

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
use store_api::region_engine::RegionRole;
use store_api::region_request::{RegionCloseRequest, RegionRequest};
use store_api::storage::RegionId;
#[cfg(test)]
use tokio::sync::oneshot;
use tokio::sync::{mpsc, Mutex};
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
    heartbeat_interval_millis: u64,
    started: Arc<AtomicBool>,

    /// The epoch when [RegionAliveKeeper] is created. It's used to get a monotonically non-decreasing
    /// elapsed time when submitting heartbeats to the meta server (because [Instant] is monotonically
    /// non-decreasing). The heartbeat requests will carry the duration since this epoch, and the
    /// duration acts like an "invariant point" for region's keep alive lease.
    epoch: Instant,
}

impl RegionAliveKeeper {
    /// Returns an empty [RegionAliveKeeper].
    pub fn new(region_server: RegionServer, heartbeat_interval_millis: u64) -> Self {
        Self {
            region_server,
            tasks: Arc::new(Mutex::new(HashMap::new())),
            heartbeat_interval_millis,
            started: Arc::new(AtomicBool::new(false)),
            epoch: Instant::now(),
        }
    }

    async fn find_handle(&self, region_id: RegionId) -> Option<Arc<CountdownTaskHandle>> {
        self.tasks.lock().await.get(&region_id).cloned()
    }

    /// Add the countdown task for a specific region.
    /// It will be ignored if the task exists.
    pub async fn register_region(&self, region_id: RegionId) {
        if self.find_handle(region_id).await.is_some() {
            return;
        }

        let handle = Arc::new(CountdownTaskHandle::new(
            self.region_server.clone(),
            region_id,
        ));

        let mut handles = self.tasks.lock().await;
        let _ = handles.insert(region_id, handle.clone());

        if self.started.load(Ordering::Relaxed) {
            handle.start(self.heartbeat_interval_millis).await;

            info!("Region alive countdown for region {region_id} is started!",);
        } else {
            info!(
                "Region alive countdown for region {region_id} is registered but not started yet!",
            );
        }
    }

    /// Removes the countdown task for a specific region.
    pub async fn deregister_region(&self, region_id: RegionId) {
        if self.tasks.lock().await.remove(&region_id).is_some() {
            info!("Deregister alive countdown for region {region_id}")
        }
    }

    /// Renews the lease of regions to `deadline`.
    async fn renew_region_leases(&self, regions: &[GrantedRegion], deadline: Instant) {
        for region in regions {
            let (role, region_id) = (region.role().into(), RegionId::from(region.region_id));
            if let Some(handle) = self.find_handle(region_id).await {
                handle.reset_deadline(role, deadline).await;
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
        let request = RegionRequest::Close(RegionCloseRequest {});
        if let Err(e) = self.region_server.handle_request(region_id, request).await {
            if e.status_code() != StatusCode::RegionNotFound {
                let _ = self.region_server.set_writable(region_id, false);
                error!(e; "Failed to close staled region {}, set region to readonly.",region_id);
            }
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
                        .fail()
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
        for task in tasks.values() {
            task.start(self.heartbeat_interval_millis).await;
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
        let deadline = start_instant + Duration::from_secs(region_lease.lease_seconds);

        self.renew_region_leases(&region_lease.regions, deadline)
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
    /// (NextRole, Deadline)
    Reset((RegionRole, Instant)),
    /// Returns the current deadline of the countdown task.
    #[cfg(test)]
    Deadline(oneshot::Sender<Instant>),
}

struct CountdownTaskHandle {
    tx: mpsc::Sender<CountdownCommand>,
    handler: JoinHandle<()>,
    region_id: RegionId,
}

impl CountdownTaskHandle {
    /// Creates a new [CountdownTaskHandle] and starts the countdown task.
    fn new(region_server: RegionServer, region_id: RegionId) -> Self {
        let (tx, rx) = mpsc::channel(1024);

        let mut countdown_task = CountdownTask {
            region_server,
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

    async fn reset_deadline(&self, role: RegionRole, deadline: Instant) {
        if let Err(e) = self
            .tx
            .send(CountdownCommand::Reset((role, deadline)))
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
                        Some(CountdownCommand::Reset((role, deadline))) => {
                            let _ = self.region_server.set_writable(self.region_id, role.writable());
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
                    warn!("The region {region_id} lease is expired, set region to readonly.");
                    let _ = self.region_server.set_writable(self.region_id, false);
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

    use mito2::config::MitoConfig;
    use mito2::test_util::{CreateRequestBuilder, TestEnv};
    use store_api::region_engine::RegionEngine;

    use super::*;
    use crate::tests::mock_region_server;

    #[tokio::test(flavor = "multi_thread")]
    async fn region_alive_keeper() {
        common_telemetry::init_default_ut_logging();
        let mut region_server = mock_region_server();
        let mut engine_env = TestEnv::with_prefix("region-alive-keeper");
        let engine = Arc::new(engine_env.create_engine(MitoConfig::default()).await);
        region_server.register_engine(engine.clone());

        let alive_keeper = Arc::new(RegionAliveKeeper::new(region_server.clone(), 100));

        let region_id = RegionId::new(1024, 1);
        let builder = CreateRequestBuilder::new();
        region_server
            .handle_request(region_id, RegionRequest::Create(builder.build()))
            .await
            .unwrap();
        region_server.set_writable(region_id, true).unwrap();

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
                }],
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

        let countdown_handle = CountdownTaskHandle::new(region_server, RegionId::new(9999, 2));

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
                Instant::now() + Duration::from_millis(heartbeat_interval_millis * 5),
            )
            .await;
        assert!(
            countdown_handle.deadline().await.unwrap()
                > Instant::now() + Duration::from_millis(heartbeat_interval_millis * 4)
        );
    }
}
