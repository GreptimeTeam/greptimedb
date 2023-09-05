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
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_meta::error::InvalidProtoMsgSnafu;
use common_meta::heartbeat::handler::{
    HandleControl, HeartbeatResponseHandler, HeartbeatResponseHandlerContext,
};
use common_telemetry::{debug, error, info, warn};
use snafu::OptionExt;
use store_api::region_request::{RegionCloseRequest, RegionRequest};
use store_api::storage::RegionId;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

use crate::region_server::RegionServer;

const MAX_CLOSE_RETRY_TIMES: usize = 10;

/// [RegionAliveKeeper] manages all [CountdownTaskHandle]s.
///
/// [RegionAliveKeeper] starts a [CountdownTask] for each region. When deadline is reached,
/// the region will be closed.
/// The deadline is controlled by Metasrv. It works like "lease" for regions: a Datanode submits its
/// opened regions to Metasrv, in heartbeats. If Metasrv decides some region could be resided in this
/// Datanode, it will "extend" the region's "lease", with a deadline for [RegionAliveKeeper] to
/// countdown.
pub struct RegionAliveKeeper {
    region_server: RegionServer,
    tasks: Arc<Mutex<HashMap<RegionId, Arc<CountdownTaskHandle>>>>,
    heartbeat_interval_millis: u64,
    started: AtomicBool,

    /// The epoch when [RegionAliveKeepers] is created. It's used to get a monotonically non-decreasing
    /// elapsed time when submitting heartbeats to Metasrv (because [Instant] is monotonically
    /// non-decreasing). The heartbeat request will carry the duration since this epoch, and the
    /// duration acts like an "invariant point" for region's keep alive lease.
    epoch: Instant,
}

impl RegionAliveKeeper {
    pub fn new(region_server: RegionServer, heartbeat_interval_millis: u64) -> Self {
        Self {
            region_server,
            tasks: Arc::new(Mutex::new(HashMap::new())),
            heartbeat_interval_millis,
            started: AtomicBool::new(false),
            epoch: Instant::now(),
        }
    }

    async fn find_handle(&self, region_id: RegionId) -> Option<Arc<CountdownTaskHandle>> {
        self.tasks.lock().await.get(&region_id).cloned()
    }

    pub async fn register_region(&self, region_id: RegionId) {
        if self.find_handle(region_id).await.is_some() {
            return;
        }

        let tasks = Arc::downgrade(&self.tasks);
        let on_task_finished = async move {
            if let Some(x) = tasks.upgrade() {
                let _ = x.lock().await.remove(&region_id);
            } // Else the countdown task handles map could be dropped because the keeper is dropped.
        };
        let handle = Arc::new(CountdownTaskHandle::new(
            self.region_server.clone(),
            region_id,
            move |result: Option<bool>| {
                info!(
                    "Deregister region: {region_id} after countdown task finished, result: {result:?}",
                );
                on_task_finished
            },
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

    pub async fn deregister_region(&self, region_id: RegionId) {
        if self.tasks.lock().await.remove(&region_id).is_some() {
            info!("Deregister alive countdown for region {region_id}")
        }
    }

    async fn keep_lived(&self, designated_regions: Vec<RegionId>, deadline: Instant) {
        for region_id in designated_regions {
            if let Some(handle) = self.find_handle(region_id).await {
                handle.reset_deadline(deadline).await;
            }
            // Else the region alive keeper might be triggered by lagging messages, we can safely ignore it.
        }
    }

    pub async fn deadline(&self, region_id: RegionId) -> Option<Instant> {
        let mut deadline = None;
        if let Some(handle) = self.find_handle(region_id).await {
            let (s, r) = oneshot::channel();
            if handle.tx.send(CountdownCommand::Deadline(s)).await.is_ok() {
                deadline = r.await.ok()
            }
        }
        deadline
    }

    pub async fn start(&self) {
        let tasks = self.tasks.lock().await;
        for task in tasks.values() {
            task.start(self.heartbeat_interval_millis).await;
        }
        self.started.store(true, Ordering::Relaxed);

        info!(
            "RegionAliveKeeper is started with region {:?}",
            tasks.keys().map(|x| x.to_string()).collect::<Vec<_>>(),
        );
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
        let region_ids = region_lease
            .region_ids
            .iter()
            .map(|id| RegionId::from_u64(*id))
            .collect();
        self.keep_lived(region_ids, deadline).await;
        Ok(HandleControl::Continue)
    }
}

#[derive(Debug)]
enum CountdownCommand {
    Start(u64),
    Reset(Instant),
    Deadline(oneshot::Sender<Instant>),
}

struct CountdownTaskHandle {
    tx: mpsc::Sender<CountdownCommand>,
    handler: JoinHandle<()>,
    region_id: RegionId,
}

impl CountdownTaskHandle {
    /// Creates a new [CountdownTaskHandle] and starts the countdown task.
    /// # Params
    /// - `on_task_finished`: a callback to be invoked when the task is finished. Note that it will not
    ///   be invoked if the task is cancelled (by dropping the handle). This is because we want something
    ///   meaningful to be done when the task is finished, e.g. deregister the handle from the map.
    ///   While dropping the handle does not necessarily mean the task is finished.
    fn new<Fut>(
        region_server: RegionServer,
        region_id: RegionId,
        on_task_finished: impl FnOnce(Option<bool>) -> Fut + Send + 'static,
    ) -> Self
    where
        Fut: Future<Output = ()> + Send,
    {
        let (tx, rx) = mpsc::channel(1024);

        let mut countdown_task = CountdownTask {
            region_server,
            region_id,
            rx,
        };
        let handler = common_runtime::spawn_bg(async move {
            let result = countdown_task.run().await;
            on_task_finished(result).await;
        });

        Self {
            tx,
            handler,
            region_id,
        }
    }

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

    async fn reset_deadline(&self, deadline: Instant) {
        if let Err(e) = self.tx.send(CountdownCommand::Reset(deadline)).await {
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
    // returns true if region closed successfully
    async fn run(&mut self) -> Option<bool> {
        // 30 years. See `Instant::far_future`.
        let far_future = Instant::now() + Duration::from_secs(86400 * 365 * 30);

        // Make sure the alive countdown is not gonna happen before heartbeat task is started (the
        // "start countdown" command will be sent from heartbeat task).
        let countdown = tokio::time::sleep_until(far_future);
        tokio::pin!(countdown);

        let region_id = self.region_id;
        loop {
            tokio::select! {
                command = self.rx.recv() => {
                    match command {
                        Some(CountdownCommand::Start(heartbeat_interval_millis)) => {
                            // Set first deadline in 4 heartbeats (roughly after 20 seconds from now if heartbeat
                            // interval is set to default 5 seconds), to make Datanode and Metasrv more tolerable to
                            // network or other jitters during startup.
                            let first_deadline = Instant::now() + Duration::from_millis(heartbeat_interval_millis) * 4;
                            countdown.set(tokio::time::sleep_until(first_deadline));
                        },
                        Some(CountdownCommand::Reset(deadline)) => {
                            if countdown.deadline() < deadline {
                                debug!(
                                    "Reset deadline of region {region_id} to approximately {} seconds later",
                                    (deadline - Instant::now()).as_secs_f32(),
                                );
                                countdown.set(tokio::time::sleep_until(deadline));
                            }
                            // Else the countdown could be either:
                            // - not started yet;
                            // - during startup protection;
                            // - received a lagging heartbeat message.
                            // All can be safely ignored.
                        },
                        None => {
                            info!(
                                "The handle of countdown task for region {region_id}\
                                is dropped, RegionAliveKeeper out."
                            );
                            break;
                        },
                        Some(CountdownCommand::Deadline(tx)) => {
                            let _ = tx.send(countdown.deadline());
                        }
                    }
                }
                () = &mut countdown => {
                    let result = self.close_region().await;
                    info!(
                        "Region {region_id} is closed, result: {result:?}. \
                        RegionAliveKeeper out.",
                    );
                    return Some(result);
                }
            }
        }
        None
    }

    /// Returns if the region is closed successfully.
    async fn close_region(&self) -> bool {
        for retry in 0..MAX_CLOSE_RETRY_TIMES {
            let request = RegionRequest::Close(RegionCloseRequest {});
            match self
                .region_server
                .handle_request(self.region_id, request)
                .await
            {
                Ok(_) => return true,
                Err(e) if e.status_code() == StatusCode::RegionNotFound => return true,
                // If region is failed to close, immediately retry. Maybe we should panic instead?
                Err(e) => error!(e;
                    "Retry {retry}, failed to close region {}. \
                    For the integrity of data, retry closing and retry without wait.",
                    self.region_id,
                ),
            }
        }
        false
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::tests::mock_region_server;

    #[tokio::test(flavor = "multi_thread")]
    async fn region_alive_keeper() {
        let region_server = mock_region_server();
        let alive_keeper = RegionAliveKeeper::new(region_server, 300);
        let region_id = RegionId::new(1, 2);

        // register a region before starting
        alive_keeper.register_region(region_id).await;
        assert!(alive_keeper.find_handle(region_id).await.is_some());

        alive_keeper.start().await;

        // started alive keeper should assign deadline to this region
        let deadline = alive_keeper.deadline(region_id).await.unwrap();
        assert!(deadline >= Instant::now());

        // extend lease then sleep
        alive_keeper
            .keep_lived(vec![region_id], Instant::now() + Duration::from_millis(500))
            .await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(alive_keeper.find_handle(region_id).await.is_some());
        let deadline = alive_keeper.deadline(region_id).await.unwrap();
        assert!(deadline >= Instant::now());

        // sleep to wait lease expired
        tokio::time::sleep(Duration::from_millis(1000)).await;
        assert!(alive_keeper.find_handle(region_id).await.is_none());
    }
}
