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
use common_meta::error::InvalidProtoMsgSnafu;
use common_meta::heartbeat::handler::{
    HandleControl, HeartbeatResponseHandler, HeartbeatResponseHandlerContext,
};
use common_meta::ident::TableIdent;
use common_meta::RegionIdent;
use common_telemetry::{debug, error, info, warn};
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionNumber;
use table::engine::manager::TableEngineManagerRef;
use table::engine::{CloseTableResult, EngineContext, TableEngineRef};
use table::requests::CloseTableRequest;
use table::TableRef;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

use crate::error::{Result, TableEngineNotFoundSnafu};

/// [RegionAliveKeepers] manages all [RegionAliveKeeper] in a scope of tables.
pub struct RegionAliveKeepers {
    table_engine_manager: TableEngineManagerRef,
    keepers: Arc<Mutex<HashMap<TableIdent, Arc<RegionAliveKeeper>>>>,
    heartbeat_interval_millis: u64,
    started: AtomicBool,

    /// The epoch when [RegionAliveKeepers] is created. It's used to get a monotonically non-decreasing
    /// elapsed time when submitting heartbeats to Metasrv (because [Instant] is monotonically
    /// non-decreasing). The heartbeat request will carry the duration since this epoch, and the
    /// duration acts like an "invariant point" for region's keep alive lease.
    epoch: Instant,
}

impl RegionAliveKeepers {
    pub fn new(
        table_engine_manager: TableEngineManagerRef,
        heartbeat_interval_millis: u64,
    ) -> Self {
        Self {
            table_engine_manager,
            keepers: Arc::new(Mutex::new(HashMap::new())),
            heartbeat_interval_millis,
            started: AtomicBool::new(false),
            epoch: Instant::now(),
        }
    }

    pub async fn find_keeper(&self, table_ident: &TableIdent) -> Option<Arc<RegionAliveKeeper>> {
        self.keepers.lock().await.get(table_ident).cloned()
    }

    pub async fn register_table(&self, table_ident: TableIdent, table: TableRef) -> Result<()> {
        let keeper = self.find_keeper(&table_ident).await;
        if keeper.is_some() {
            return Ok(());
        }

        let table_engine = self
            .table_engine_manager
            .engine(&table_ident.engine)
            .context(TableEngineNotFoundSnafu {
                engine_name: &table_ident.engine,
            })?;

        let keeper = Arc::new(RegionAliveKeeper::new(
            table_engine,
            table_ident.clone(),
            self.heartbeat_interval_millis,
        ));
        for r in table.table_info().meta.region_numbers.iter() {
            keeper.register_region(*r).await;
        }

        let mut keepers = self.keepers.lock().await;
        keepers.insert(table_ident.clone(), keeper.clone());

        if self.started.load(Ordering::Relaxed) {
            keeper.start().await;

            info!("RegionAliveKeeper for table {table_ident} is started!");
        } else {
            info!("RegionAliveKeeper for table {table_ident} is registered but not started yet!");
        }
        Ok(())
    }

    pub async fn deregister_table(
        &self,
        table_ident: &TableIdent,
    ) -> Option<Arc<RegionAliveKeeper>> {
        self.keepers.lock().await.remove(table_ident).map(|x| {
            info!("Deregister RegionAliveKeeper for table {table_ident}");
            x
        })
    }

    pub async fn register_region(&self, region_ident: &RegionIdent) {
        let table_ident = &region_ident.table_ident;
        let Some(keeper) = self.find_keeper(table_ident).await else {
            // Alive keeper could be affected by lagging msg, just warn and ignore.
            warn!("Alive keeper for region {region_ident} is not found!");
            return;
        };
        keeper.register_region(region_ident.region_number).await
    }

    pub async fn deregister_region(&self, region_ident: &RegionIdent) {
        let table_ident = &region_ident.table_ident;
        let Some(keeper) = self.find_keeper(table_ident).await else {
            // Alive keeper could be affected by lagging msg, just warn and ignore.
            warn!("Alive keeper for region {region_ident} is not found!");
            return;
        };
        let _ = keeper.deregister_region(region_ident.region_number).await;
    }

    pub async fn start(&self) {
        let keepers = self.keepers.lock().await;
        for keeper in keepers.values() {
            keeper.start().await;
        }
        self.started.store(true, Ordering::Relaxed);

        info!(
            "RegionAliveKeepers for tables {:?} are started!",
            keepers.keys().map(|x| x.to_string()).collect::<Vec<_>>(),
        );
    }

    pub fn epoch(&self) -> Instant {
        self.epoch
    }
}

#[async_trait]
impl HeartbeatResponseHandler for RegionAliveKeepers {
    fn is_acceptable(&self, ctx: &HeartbeatResponseHandlerContext) -> bool {
        !ctx.response.region_leases.is_empty()
    }

    async fn handle(
        &self,
        ctx: &mut HeartbeatResponseHandlerContext,
    ) -> common_meta::error::Result<HandleControl> {
        let leases = ctx.response.region_leases.drain(..).collect::<Vec<_>>();
        for lease in leases {
            let table_ident: TableIdent = match lease
                .table_ident
                .context(InvalidProtoMsgSnafu {
                    err_msg: "'table_ident' is missing in RegionLease",
                })
                .and_then(|x| x.try_into())
            {
                Ok(x) => x,
                Err(e) => {
                    error!(e; "");
                    continue;
                }
            };

            let Some(keeper) = self.keepers.lock().await.get(&table_ident).cloned() else {
                // Alive keeper could be affected by lagging msg, just warn and ignore.
                warn!("Alive keeper for table {table_ident} is not found!");
                continue;
            };

            let start_instant = self.epoch + Duration::from_millis(lease.duration_since_epoch);
            let deadline = start_instant + Duration::from_secs(lease.lease_seconds);
            keeper.keep_lived(lease.regions, deadline).await;
        }
        Ok(HandleControl::Continue)
    }
}

/// [RegionAliveKeeper] starts a countdown for each region in a table. When deadline is reached,
/// the region will be closed.
/// The deadline is controlled by Metasrv. It works like "lease" for regions: a Datanode submits its
/// opened regions to Metasrv, in heartbeats. If Metasrv decides some region could be resided in this
/// Datanode, it will "extend" the region's "lease", with a deadline for [RegionAliveKeeper] to
/// countdown.
pub struct RegionAliveKeeper {
    table_engine: TableEngineRef,
    table_ident: TableIdent,
    countdown_task_handles: Arc<Mutex<HashMap<RegionNumber, Arc<CountdownTaskHandle>>>>,
    heartbeat_interval_millis: u64,
    started: AtomicBool,
}

impl RegionAliveKeeper {
    fn new(
        table_engine: TableEngineRef,
        table_ident: TableIdent,
        heartbeat_interval_millis: u64,
    ) -> Self {
        Self {
            table_engine,
            table_ident,
            countdown_task_handles: Arc::new(Mutex::new(HashMap::new())),
            heartbeat_interval_millis,
            started: AtomicBool::new(false),
        }
    }

    async fn find_handle(&self, region: &RegionNumber) -> Option<Arc<CountdownTaskHandle>> {
        self.countdown_task_handles
            .lock()
            .await
            .get(region)
            .cloned()
    }

    async fn register_region(&self, region: RegionNumber) {
        if self.find_handle(&region).await.is_some() {
            return;
        }

        let countdown_task_handles = Arc::downgrade(&self.countdown_task_handles);
        let on_task_finished = async move {
            if let Some(x) = countdown_task_handles.upgrade() {
                x.lock().await.remove(&region);
            } // Else the countdown task handles map could be dropped because the keeper is dropped.
        };
        let handle = Arc::new(CountdownTaskHandle::new(
            self.table_engine.clone(),
            self.table_ident.clone(),
            region,
            || on_task_finished,
        ));

        let mut handles = self.countdown_task_handles.lock().await;
        handles.insert(region, handle.clone());

        if self.started.load(Ordering::Relaxed) {
            handle.start(self.heartbeat_interval_millis).await;

            info!(
                "Region alive countdown for region {region} in table {} is started!",
                self.table_ident
            );
        } else {
            info!(
                "Region alive countdown for region {region} in table {} is registered but not started yet!",
                self.table_ident
            );
        }
    }

    async fn deregister_region(&self, region: RegionNumber) -> Option<Arc<CountdownTaskHandle>> {
        self.countdown_task_handles
            .lock()
            .await
            .remove(&region)
            .map(|x| {
                info!(
                    "Deregister alive countdown for region {region} in table {}",
                    self.table_ident
                );
                x
            })
    }

    async fn start(&self) {
        let handles = self.countdown_task_handles.lock().await;
        for handle in handles.values() {
            handle.start(self.heartbeat_interval_millis).await;
        }

        self.started.store(true, Ordering::Relaxed);
        info!(
            "Region alive countdowns for regions {:?} in table {} are started!",
            handles.keys().copied().collect::<Vec<_>>(),
            self.table_ident
        );
    }

    async fn keep_lived(&self, designated_regions: Vec<RegionNumber>, deadline: Instant) {
        for region in designated_regions {
            if let Some(handle) = self.find_handle(&region).await {
                handle.reset_deadline(deadline).await;
            }
            // Else the region alive keeper might be triggered by lagging messages, we can safely ignore it.
        }
    }

    pub async fn deadline(&self, region: RegionNumber) -> Option<Instant> {
        let mut deadline = None;
        if let Some(handle) = self.find_handle(&region).await {
            let (s, r) = oneshot::channel();
            if handle.tx.send(CountdownCommand::Deadline(s)).await.is_ok() {
                deadline = r.await.ok()
            }
        }
        deadline
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
    table_ident: TableIdent,
    region: RegionNumber,
}

impl CountdownTaskHandle {
    /// Creates a new [CountdownTaskHandle] and starts the countdown task.
    /// # Params
    /// - `on_task_finished`: a callback to be invoked when the task is finished. Note that it will not
    ///   be invoked if the task is cancelled (by dropping the handle). This is because we want something
    ///   meaningful to be done when the task is finished, e.g. deregister the handle from the map.
    ///   While dropping the handle does not necessarily mean the task is finished.
    fn new<Fut>(
        table_engine: TableEngineRef,
        table_ident: TableIdent,
        region: RegionNumber,
        on_task_finished: impl FnOnce() -> Fut + Send + 'static,
    ) -> Self
    where
        Fut: Future<Output = ()> + Send,
    {
        let (tx, rx) = mpsc::channel(1024);

        let mut countdown_task = CountdownTask {
            table_engine,
            table_ident: table_ident.clone(),
            region,
            rx,
        };
        let handler = common_runtime::spawn_bg(async move {
            countdown_task.run().await;
            on_task_finished().await;
        });

        Self {
            tx,
            handler,
            table_ident,
            region,
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
            "Aborting region alive countdown task for region {} in table {}",
            self.region, self.table_ident,
        );
        self.handler.abort();
    }
}

struct CountdownTask {
    table_engine: TableEngineRef,
    table_ident: TableIdent,
    region: RegionNumber,
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

        let region = &self.region;
        let table_ident = &self.table_ident;
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
                                    "Reset deadline of region {region} of table {table_ident} to approximately {} seconds later",
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
                                "The handle of countdown task for region {region} of table {table_ident} \
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
                    warn!(
                        "Region {region} of table {table_ident} is closed, result: {result:?}. \
                        RegionAliveKeeper out.",
                    );
                    break;
                }
            }
        }
    }

    async fn close_region(&self) -> CloseTableResult {
        let ctx = EngineContext::default();
        let region = self.region;
        let table_ident = &self.table_ident;
        loop {
            let request = CloseTableRequest {
                catalog_name: table_ident.catalog.clone(),
                schema_name: table_ident.schema.clone(),
                table_name: table_ident.table.clone(),
                region_numbers: vec![region],
                flush: true,
            };
            match self.table_engine.close_table(&ctx, request).await {
                Ok(result) => return result,
                // If region is failed to close, immediately retry. Maybe we should panic instead?
                Err(e) => error!(e;
                    "Failed to close region {region} of table {table_ident}. \
                    For the integrity of data, retry closing and retry without wait.",
                ),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use api::v1::meta::{HeartbeatResponse, RegionLease};
    use common_meta::heartbeat::mailbox::HeartbeatMailbox;
    use datatypes::schema::RawSchema;
    use table::engine::manager::MemoryTableEngineManager;
    use table::engine::{TableEngine, TableReference};
    use table::requests::{CreateTableRequest, TableOptions};
    use table::test_util::EmptyTable;

    use super::*;
    use crate::remote::mock::MockTableEngine;

    async fn prepare_keepers() -> (TableIdent, RegionAliveKeepers) {
        let table_engine = Arc::new(MockTableEngine::default());
        let table_engine_manager = Arc::new(MemoryTableEngineManager::new(table_engine));
        let keepers = RegionAliveKeepers::new(table_engine_manager, 5000);

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
        let table = Arc::new(EmptyTable::new(CreateTableRequest {
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
        }));
        keepers
            .register_table(table_ident.clone(), table)
            .await
            .unwrap();
        assert!(keepers.keepers.lock().await.contains_key(&table_ident));

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
            region_leases: vec![RegionLease {
                table_ident: Some(table_ident.clone().into()),
                regions: vec![1, 3], // Not extending region 2's lease time.
                duration_since_epoch,
                lease_seconds,
            }],
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
            keeper: &Arc<RegionAliveKeeper>,
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
            .get(&table_ident)
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
            .find_keeper(&table_ident)
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
        let keeper = RegionAliveKeeper::new(table_engine, table_ident, 1000);

        let region = 1;
        assert!(keeper.find_handle(&region).await.is_none());
        keeper.register_region(region).await;
        assert!(keeper.find_handle(&region).await.is_some());

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
            || async move { finished_clone.store(true, Ordering::Relaxed) },
        );
        let tx = handle.tx.clone();

        // assert countdown task is running
        assert!(tx.send(CountdownCommand::Start(5000)).await.is_ok());
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
        let handle = CountdownTaskHandle::new(table_engine, table_ident, 1, || async move {
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
        let request = CreateTableRequest {
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
            region_numbers: vec![],
            primary_key_indices: vec![],
            create_if_not_exists: false,
            table_options: TableOptions::default(),
            engine: "mito".to_string(),
        };
        let table_ref = TableReference::full(catalog, schema, table);

        let table_engine = Arc::new(MockTableEngine::default());
        table_engine.create_table(ctx, request).await.unwrap();

        let table_ident = TableIdent {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            table_id: 1024,
            engine: "mito".to_string(),
        };
        let (tx, rx) = mpsc::channel(10);
        let mut task = CountdownTask {
            table_engine: table_engine.clone(),
            table_ident,
            region: 1,
            rx,
        };
        common_runtime::spawn_bg(async move {
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
        assert!(table_engine.table_exists(ctx, &table_ref));
        // spare 500ms for the task to close the table
        tokio::time::sleep(Duration::from_millis(2000)).await;
        assert!(!table_engine.table_exists(ctx, &table_ref));
    }
}
