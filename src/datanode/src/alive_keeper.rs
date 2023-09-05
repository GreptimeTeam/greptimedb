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
        self.tasks.lock().await.remove(&region_id).map(|_| {
            info!("Deregister alive countdown for region {region_id}");
        });
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

#[cfg(feature = "testeeeed")]
mod test {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use api::v1::meta::{HeartbeatResponse, RegionLease};
    use common_meta::heartbeat::mailbox::HeartbeatMailbox;
    use datatypes::schema::RawSchema;
    use table::engine::manager::MemoryTableEngineManager;
    use table::engine::TableEngine;
    use table::requests::{CreateTableRequest, TableOptions};
    use table::test_util::EmptyTable;

    use super::*;
    use crate::remote::mock::MockTableEngine;

    async fn prepare_keepers() -> (TableIdent, RegionAliveKeeper) {
        let table_engine = Arc::new(MockTableEngine::default());
        let table_engine_manager = Arc::new(MemoryTableEngineManager::new(table_engine));
        let keepers = RegionAliveKeeper::new(table_engine_manager, 5000);

        let catalog = "my_catalog";
        let schema = "my_schema";
        let table = "my_table";
        let table_ident = TableIdent {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            table_id: 1,
            engine: "MockTableEngine".to_string(),
        };
        let table = EmptyTable::table(CreateTableRequest {
            id: 1,
            catalog_name: catalog.to_string(),
            schema_name: schema.to_string(),
            table_name: table.to_string(),
            desc: None,
            schema: RawSchema {
                column_schemas: vec![],
                timestamp_index: None,
                version: 0,
            },
            region_numbers: vec![1, 2, 3],
            primary_key_indices: vec![],
            create_if_not_exists: false,
            table_options: TableOptions::default(),
            engine: "MockTableEngine".to_string(),
        });
        let catalog_manager = MemoryCatalogManager::new_with_table(table.clone());
        keepers
            .register_table(table_ident.clone(), table, catalog_manager)
            .await
            .unwrap();
        assert!(keepers
            .keepers
            .lock()
            .await
            .contains_key(&table_ident.table_id));

        (table_ident, keepers)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_heartbeat_response() {
        let (table_ident, keepers) = prepare_keepers().await;

        keepers.start().await;
        let startup_protection_until = Instant::now() + Duration::from_secs(21);

        let duration_since_epoch = (Instant::now() - keepers.epoch).as_millis() as _;
        let lease_seconds = 100;
        let response = HeartbeatResponse {
            region_lease: Some(RegionLease {
                region_ids: vec![
                    RegionId::new(table_ident.table_id, 1).as_u64(),
                    RegionId::new(table_ident.table_id, 3).as_u64(),
                ], // Not extending region 2's lease time.
                duration_since_epoch,
                lease_seconds,
            }),
            ..Default::default()
        };
        let keep_alive_until = keepers.epoch
            + Duration::from_millis(duration_since_epoch)
            + Duration::from_secs(lease_seconds);

        let (tx, _) = mpsc::channel(8);
        let mailbox = Arc::new(HeartbeatMailbox::new(tx));
        let mut ctx = HeartbeatResponseHandlerContext::new(mailbox, response);

        assert!(keepers.handle(&mut ctx).await.unwrap() == HandleControl::Continue);

        // sleep to wait for background task spawned in `handle`
        tokio::time::sleep(Duration::from_secs(1)).await;

        async fn test(
            keeper: &Arc<RegionAliveKeeperDeprecated>,
            region_number: RegionNumber,
            startup_protection_until: Instant,
            keep_alive_until: Instant,
            is_kept_live: bool,
        ) {
            let deadline = keeper.deadline(region_number).await.unwrap();
            if is_kept_live {
                assert!(deadline > startup_protection_until && deadline == keep_alive_until);
            } else {
                assert!(deadline <= startup_protection_until);
            }
        }

        let keeper = &keepers
            .keepers
            .lock()
            .await
            .get(&table_ident.table_id)
            .cloned()
            .unwrap();

        // Test region 1 and 3 is kept lived. Their deadlines are updated to desired instant.
        test(keeper, 1, startup_protection_until, keep_alive_until, true).await;
        test(keeper, 3, startup_protection_until, keep_alive_until, true).await;

        // Test region 2 is not kept lived. It's deadline is not updated: still during startup protection period.
        test(keeper, 2, startup_protection_until, keep_alive_until, false).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_region_alive_keepers() {
        let (table_ident, keepers) = prepare_keepers().await;

        keepers
            .register_region(&RegionIdent {
                cluster_id: 1,
                datanode_id: 1,
                table_ident: table_ident.clone(),
                region_number: 4,
            })
            .await;

        keepers.start().await;
        for keeper in keepers.keepers.lock().await.values() {
            let regions = {
                let handles = keeper.countdown_task_handles.lock().await;
                handles.keys().copied().collect::<Vec<_>>()
            };
            for region in regions {
                // assert countdown tasks are started
                let deadline = keeper.deadline(region).await.unwrap();
                assert!(deadline <= Instant::now() + Duration::from_secs(20));
            }
        }

        keepers
            .deregister_region(&RegionIdent {
                cluster_id: 1,
                datanode_id: 1,
                table_ident: table_ident.clone(),
                region_number: 1,
            })
            .await;
        let mut regions = keepers
            .find_keeper(table_ident.table_id)
            .await
            .unwrap()
            .countdown_task_handles
            .lock()
            .await
            .keys()
            .copied()
            .collect::<Vec<_>>();
        regions.sort();
        assert_eq!(regions, vec![2, 3, 4]);

        let keeper = keepers.deregister_table(&table_ident).await.unwrap();
        assert!(Arc::try_unwrap(keeper).is_ok(), "keeper is not dropped");
        assert!(keepers.keepers.lock().await.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_region_alive_keeper() {
        let table_engine = Arc::new(MockTableEngine::default());
        let table_ident = TableIdent {
            catalog: "my_catalog".to_string(),
            schema: "my_schema".to_string(),
            table: "my_table".to_string(),
            table_id: 1024,
            engine: "mito".to_string(),
        };
        let catalog_manager = MemoryCatalogManager::with_default_setup();
        let keeper =
            RegionAliveKeeperDeprecated::new(table_engine, catalog_manager, table_ident, 1000);

        let region = 1;
        assert!(keeper.find_handle(&region).await.is_none());
        keeper.register_region(region).await;
        let _ = keeper.find_handle(&region).await.unwrap();

        let ten_seconds_later = || Instant::now() + Duration::from_secs(10);

        keeper.keep_lived(vec![1, 2, 3], ten_seconds_later()).await;
        assert!(keeper.find_handle(&2).await.is_none());
        assert!(keeper.find_handle(&3).await.is_none());

        let far_future = Instant::now() + Duration::from_secs(86400 * 365 * 29);
        // assert if keeper is not started, keep_lived is of no use
        assert!(keeper.deadline(region).await.unwrap() > far_future);

        keeper.start().await;
        keeper.keep_lived(vec![1, 2, 3], ten_seconds_later()).await;
        // assert keep_lived works if keeper is started
        assert!(keeper.deadline(region).await.unwrap() <= ten_seconds_later());

        let handle = keeper.deregister_region(region).await.unwrap();
        assert!(Arc::try_unwrap(handle).is_ok(), "handle is not dropped");
        assert!(keeper.find_handle(&region).await.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_countdown_task_handle() {
        let table_engine = Arc::new(MockTableEngine::default());
        let table_ident = TableIdent {
            catalog: "my_catalog".to_string(),
            schema: "my_schema".to_string(),
            table: "my_table".to_string(),
            table_id: 1024,
            engine: "mito".to_string(),
        };
        let finished = Arc::new(AtomicBool::new(false));
        let finished_clone = finished.clone();
        let handle = CountdownTaskHandle::new(
            table_engine.clone(),
            table_ident.clone(),
            1,
            |_| async move { finished_clone.store(true, Ordering::Relaxed) },
        );
        let tx = handle.tx.clone();

        // assert countdown task is running
        tx.send(CountdownCommand::Start(5000)).await.unwrap();
        assert!(!finished.load(Ordering::Relaxed));

        drop(handle);
        tokio::time::sleep(Duration::from_secs(1)).await;

        // assert countdown task is stopped
        assert!(tx
            .try_send(CountdownCommand::Reset(
                Instant::now() + Duration::from_secs(10)
            ))
            .is_err());
        // assert `on_task_finished` is not called (because the task is aborted by the handle's drop)
        assert!(!finished.load(Ordering::Relaxed));

        let finished = Arc::new(AtomicBool::new(false));
        let finished_clone = finished.clone();
        let handle = CountdownTaskHandle::new(table_engine, table_ident, 1, |_| async move {
            finished_clone.store(true, Ordering::Relaxed)
        });
        handle.tx.send(CountdownCommand::Start(100)).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        // assert `on_task_finished` is called when task is finished normally
        assert!(finished.load(Ordering::Relaxed));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_countdown_task_run() {
        let ctx = &EngineContext::default();
        let catalog = "my_catalog";
        let schema = "my_schema";
        let table = "my_table";
        let table_id = 1;
        let request = CreateTableRequest {
            id: table_id,
            catalog_name: catalog.to_string(),
            schema_name: schema.to_string(),
            table_name: table.to_string(),
            desc: None,
            schema: RawSchema {
                column_schemas: vec![],
                timestamp_index: None,
                version: 0,
            },
            region_numbers: vec![],
            primary_key_indices: vec![],
            create_if_not_exists: false,
            table_options: TableOptions::default(),
            engine: "mito".to_string(),
        };

        let table_engine = Arc::new(MockTableEngine::default());
        let _ = table_engine.create_table(ctx, request).await.unwrap();

        let table_ident = TableIdent {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            table_id,
            engine: "mito".to_string(),
        };
        let (tx, rx) = mpsc::channel(10);
        let mut task = CountdownTask {
            table_engine: table_engine.clone(),
            table_ident,
            region: 1,
            rx,
        };
        let _handle = common_runtime::spawn_bg(async move {
            task.run().await;
        });

        async fn deadline(tx: &mpsc::Sender<CountdownCommand>) -> Instant {
            let (s, r) = oneshot::channel();
            tx.send(CountdownCommand::Deadline(s)).await.unwrap();
            r.await.unwrap()
        }

        // if countdown task is not started, its deadline is set to far future
        assert!(deadline(&tx).await > Instant::now() + Duration::from_secs(86400 * 365 * 29));

        // start countdown in 250ms * 4 = 1s
        tx.send(CountdownCommand::Start(250)).await.unwrap();
        // assert deadline is correctly set
        assert!(deadline(&tx).await <= Instant::now() + Duration::from_secs(1));

        // reset countdown in 1.5s
        tx.send(CountdownCommand::Reset(
            Instant::now() + Duration::from_millis(1500),
        ))
        .await
        .unwrap();

        // assert the table is closed after deadline is reached
        assert!(table_engine.table_exists(ctx, table_id));
        // spare 500ms for the task to close the table
        tokio::time::sleep(Duration::from_millis(2000)).await;
        assert!(!table_engine.table_exists(ctx, table_id));
    }
}
