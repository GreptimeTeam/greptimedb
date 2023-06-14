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

use common_meta::instruction::TableIdent;
use common_telemetry::{debug, error, info, warn};
use store_api::storage::RegionNumber;
use table::engine::{CloseTableResult, EngineContext, TableEngineRef};
use table::requests::CloseTableRequest;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

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
    fn new<F>(
        table_engine: TableEngineRef,
        table_ident: TableIdent,
        region: RegionNumber,
        on_task_finished: F,
    ) -> Self
    where
        F: FnOnce() + Send + 'static,
    {
        let (tx, rx) = mpsc::channel(1024);

        let mut countdown_task = CountdownTask {
            table_engine,
            table_ident,
            region,
            rx,
        };
        let handler = common_runtime::spawn_bg(async move {
            countdown_task.run(on_task_finished).await;
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
    async fn run<F>(&mut self, on_task_finished: F)
    where
        F: FnOnce() + Send + 'static,
    {
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
                            // Else we have received a past deadline, it could be the following
                            // possible reasons:
                            // 1. the clock drift happened in Metasrv or Datanode;
                            // 2. some messages are lagged;
                            // 3. during the period of Datanode startup.
                            // We can safely ignore case 2 and 3. However, case 1 is catastrophic.
                            // We must think of a way to resolve it, maybe using logical clock, or
                            // simply fire an alarm for it? For now, we can tolerate that, because it's
                            // seconds resolution to deadline, and clock drift is less likely
                            // to happen in that resolution.
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

        on_task_finished();
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
    use table::engine::{TableEngine, TableReference};
    use table::requests::{CreateTableRequest, TableOptions};

    use super::*;
    use crate::remote::mock::MockTableEngine;

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
        let handle =
            CountdownTaskHandle::new(table_engine.clone(), table_ident.clone(), 1, move || {
                finished_clone.store(true, Ordering::Relaxed)
            });
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
        let handle = CountdownTaskHandle::new(table_engine, table_ident, 1, move || {
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
            task.run(|| ()).await;
        });

        async fn deadline(tx: &mpsc::Sender<CountdownCommand>) -> Instant {
            let (s, r) = tokio::sync::oneshot::channel();
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
