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
use std::sync::Arc;

use common_meta::ident::TableIdent;
use common_meta::RegionIdent;
use common_telemetry::{debug, error, info, warn};
use snafu::ResultExt;
use store_api::storage::RegionNumber;
use table::engine::manager::TableEngineManagerRef;
use table::engine::{CloseTableResult, EngineContext, TableEngineRef};
use table::requests::CloseTableRequest;
use table::TableRef;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

use crate::error::{Result, TableEngineNotFoundSnafu};

/// [RegionAliveKeepers] manages all [RegionAliveKeeper] in a scope of tables.
pub struct RegionAliveKeepers {
    table_engine_manager: TableEngineManagerRef,
    keepers: Arc<Mutex<HashMap<TableIdent, Arc<RegionAliveKeeper>>>>,
}

impl RegionAliveKeepers {
    pub fn new(table_engine_manager: TableEngineManagerRef) -> Self {
        Self {
            table_engine_manager,
            keepers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn find_keeper(&self, table_ident: &TableIdent) -> Option<Arc<RegionAliveKeeper>> {
        self.keepers.lock().await.get(table_ident).cloned()
    }

    pub(crate) async fn register_table(
        &self,
        table_ident: TableIdent,
        table: TableRef,
    ) -> Result<()> {
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

        let keeper = Arc::new(RegionAliveKeeper::new(table_engine, table_ident.clone()));
        for r in table.table_info().meta.region_numbers.iter() {
            keeper.register_region(*r).await;
        }

        info!("Register RegionAliveKeeper for table {table_ident}");
        self.keepers.lock().await.insert(table_ident, keeper);
        Ok(())
    }

    pub(crate) async fn deregister_table(&self, table_ident: &TableIdent) {
        if self.keepers.lock().await.remove(table_ident).is_some() {
            info!("Deregister RegionAliveKeeper for table {table_ident}");
        }
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
        keeper.deregister_region(region_ident.region_number).await
    }

    pub async fn start(&self, heartbeat_interval_millis: u64) {
        for keeper in self.keepers.lock().await.values() {
            keeper.start(heartbeat_interval_millis).await;
        }
    }
}

/// [RegionAliveKeeper] starts a countdown for each region in a table. When deadline is reached,
/// the region will be closed.
/// The deadline is controlled by Metasrv. It works like "lease" for regions: a Datanode submits its
/// opened regions to Metasrv, in heartbeats. If Metasrv decides some region could be resided in this
/// Datanode, it will "extend" the region's "lease", with a deadline for [RegionAliveKeeper] to
/// countdown.
struct RegionAliveKeeper {
    table_engine: TableEngineRef,
    table_ident: TableIdent,
    countdown_task_handles: Arc<Mutex<HashMap<RegionNumber, Arc<CountdownTaskHandle>>>>,
}

impl RegionAliveKeeper {
    fn new(table_engine: TableEngineRef, table_ident: TableIdent) -> Self {
        Self {
            table_engine,
            table_ident,
            countdown_task_handles: Arc::new(Mutex::new(HashMap::new())),
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

        let countdown_task_handles = self.countdown_task_handles.clone();
        let on_task_finished = async move {
            let _ = countdown_task_handles.lock().await.remove(&region);
        };
        let handle = Arc::new(CountdownTaskHandle::new(
            self.table_engine.clone(),
            self.table_ident.clone(),
            region,
            || on_task_finished,
        ));

        self.countdown_task_handles
            .lock()
            .await
            .insert(region, handle);
        info!(
            "Register alive countdown for new region {region} in table {}",
            self.table_ident
        )
    }

    async fn deregister_region(&self, region: RegionNumber) {
        if self
            .countdown_task_handles
            .lock()
            .await
            .remove(&region)
            .is_some()
        {
            info!(
                "Deregister alive countdown for region {region} in table {}",
                self.table_ident
            )
        }
    }

    async fn start(&self, heartbeat_interval_millis: u64) {
        for handle in self.countdown_task_handles.lock().await.values() {
            handle.start(heartbeat_interval_millis).await;
        }
        info!(
            "RegionAliveKeeper for table {} is started!",
            self.table_ident
        )
    }

    async fn keep_lived(&self, designated_regions: Vec<RegionNumber>, deadline: Instant) {
        for region in designated_regions {
            if let Some(handle) = self.find_handle(&region).await {
                handle.reset_deadline(deadline).await;
            }
            // Else the region alive keeper might be triggered by lagging messages, we can safely ignore it.
        }
    }
}

#[derive(Debug)]
enum CountdownCommand {
    Start(u64),
    Reset(Instant),

    #[cfg(test)]
    Deadline(tokio::sync::oneshot::Sender<Instant>),
}

struct CountdownTaskHandle {
    tx: mpsc::Sender<CountdownCommand>,
    handler: JoinHandle<()>,
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
            table_ident,
            region,
            rx,
        };
        let handler = common_runtime::spawn_bg(async move {
            countdown_task.run().await;
            on_task_finished().await;
        });

        Self { tx, handler }
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
        self.handler.abort()
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
                                debug!("Reset deadline to region {region} of table {table_ident} to {deadline:?}");
                                countdown.set(tokio::time::sleep_until(deadline));
                            }
                            // Else the countdown could be not started yet, or during startup protection.
                            // Can be safely ignored.
                        },
                        None => {
                            info!(
                                "The handle of countdown task for region {region} of table {table_ident} \
                                is dropped, RegionAliveKeeper out."
                            );
                            break;
                        },

                        #[cfg(test)]
                        Some(CountdownCommand::Deadline(tx)) => {
                            tx.send(countdown.deadline()).unwrap()
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

    use datatypes::schema::RawSchema;
    use table::engine::manager::MemoryTableEngineManager;
    use table::engine::{TableEngine, TableReference};
    use table::requests::{CreateTableRequest, TableOptions};
    use table::test_util::EmptyTable;
    use tokio::sync::oneshot;

    use super::*;
    use crate::remote::mock::MockTableEngine;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_region_alive_keepers() {
        let table_engine = Arc::new(MockTableEngine::default());
        let table_engine_manager = Arc::new(MemoryTableEngineManager::new(table_engine));
        let keepers = RegionAliveKeepers::new(table_engine_manager);

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

        keepers
            .register_region(&RegionIdent {
                cluster_id: 1,
                datanode_id: 1,
                table_ident: table_ident.clone(),
                region_number: 4,
            })
            .await;

        keepers.start(5000).await;
        for keeper in keepers.keepers.lock().await.values() {
            for handle in keeper.countdown_task_handles.lock().await.values() {
                // assert countdown tasks are started
                assert!(deadline(&handle.tx).await <= Instant::now() + Duration::from_secs(20));
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

        keepers.deregister_table(&table_ident).await;
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
        let keeper = RegionAliveKeeper::new(table_engine, table_ident);

        let region = 1;
        assert!(keeper.find_handle(&region).await.is_none());
        keeper.register_region(region).await;
        assert!(keeper.find_handle(&region).await.is_some());

        let sender = &keeper
            .countdown_task_handles
            .lock()
            .await
            .get(&region)
            .unwrap()
            .tx
            .clone();

        let ten_seconds_later = || Instant::now() + Duration::from_secs(10);

        keeper.keep_lived(vec![1, 2, 3], ten_seconds_later()).await;
        assert!(keeper.find_handle(&2).await.is_none());
        assert!(keeper.find_handle(&3).await.is_none());

        let far_future = Instant::now() + Duration::from_secs(86400 * 365 * 29);
        // assert if keeper is not started, keep_lived is of no use
        assert!(deadline(sender).await > far_future);

        keeper.start(1000).await;
        keeper.keep_lived(vec![1, 2, 3], ten_seconds_later()).await;
        // assert keep_lived works if keeper is started
        assert!(deadline(sender).await <= ten_seconds_later());

        keeper.deregister_region(region).await;
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

    async fn deadline(tx: &mpsc::Sender<CountdownCommand>) -> Instant {
        let (s, r) = oneshot::channel();
        tx.send(CountdownCommand::Deadline(s)).await.unwrap();
        r.await.unwrap()
    }
}
