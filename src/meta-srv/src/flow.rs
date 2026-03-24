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

use std::sync::Arc;
use std::time::Duration;

use common_meta::ddl_manager::DdlManagerRef;
use common_meta::key::table_name::TableNameKey;
use common_telemetry::{error, info};
use futures::TryStreamExt;
use snafu::ResultExt;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::define_ticker;
use crate::error::{PendingFlowReconcileSnafu, Result};

const PENDING_FLOW_RECONCILE_INTERVAL: Duration = Duration::from_secs(10);

pub enum Event {
    Tick,
}

pub type PendingFlowReconcileTickerRef = Arc<PendingFlowReconcileTicker>;

define_ticker!(
    PendingFlowReconcileTicker,
    event_type = Event,
    event_value = Event::Tick
);

pub struct PendingFlowReconcileManager {
    ddl_manager: DdlManagerRef,
    receiver: Receiver<Event>,
}

impl PendingFlowReconcileManager {
    pub fn new(ddl_manager: DdlManagerRef) -> (Self, PendingFlowReconcileTicker) {
        let (sender, receiver) = Self::channel();
        (
            Self {
                ddl_manager,
                receiver,
            },
            PendingFlowReconcileTicker::new(PENDING_FLOW_RECONCILE_INTERVAL, sender),
        )
    }

    fn channel() -> (Sender<Event>, Receiver<Event>) {
        tokio::sync::mpsc::channel(8)
    }

    pub fn try_start(mut self) -> Result<()> {
        common_runtime::spawn_global(async move { self.run().await });
        info!("Pending flow reconcile manager started");
        Ok(())
    }

    async fn run(&mut self) {
        while let Some(event) = self.receiver.recv().await {
            match event {
                Event::Tick => {
                    if let Err(e) = self.handle_tick().await {
                        error!(e; "Failed to reconcile pending flows");
                    }
                }
            }
        }
    }

    async fn handle_tick(&self) -> Result<()> {
        let ddl_context = self.ddl_manager.create_context();
        let ddl_manager = self.ddl_manager.clone();
        ddl_context
            .flow_metadata_manager
            .flow_info_manager()
            .flow_infos()
            .try_for_each(move |(flow_id, flow_info)| {
                let ddl_context = ddl_context.clone();
                let ddl_manager = ddl_manager.clone();
                async move {
                    if !flow_info.is_pending() {
                        return Ok(());
                    }

                    let current_flow_info = ddl_context
                        .flow_metadata_manager
                        .flow_info_manager()
                        .get_raw(flow_id)
                        .await;
                    let current_flow_info = match current_flow_info {
                        Ok(current_flow_info) => current_flow_info,
                        Err(e) => {
                            error!(e; "Failed to load flow metadata for pending flow {}", flow_id);
                            return Ok(());
                        }
                    };
                    let Some(current_flow_info) = current_flow_info else {
                        return Ok(());
                    };
                    if !current_flow_info.get_inner_ref().is_pending() {
                        return Ok(());
                    }

                    let unresolved_source_table_names = current_flow_info
                        .get_inner_ref()
                        .unresolved_source_table_names();
                    if !unresolved_source_table_names.is_empty() {
                        let unresolved_table_keys = unresolved_source_table_names
                            .iter()
                            .map(|name| {
                                TableNameKey::new(
                                    &name.catalog_name,
                                    &name.schema_name,
                                    &name.table_name,
                                )
                            })
                            .collect::<Vec<_>>();
                        let resolved_tables = ddl_context
                            .table_metadata_manager
                            .table_name_manager()
                            .batch_get(unresolved_table_keys)
                            .await;
                        let resolved_tables = match resolved_tables {
                            Ok(resolved_tables) => resolved_tables,
                            Err(e) => {
                                error!(e; "Failed to resolve source tables for pending flow {}", flow_id);
                                return Ok(());
                            }
                        };
                        if resolved_tables.iter().all(|table_id| table_id.is_none()) {
                            return Ok(());
                        }
                    }

                    if let Err(e) = ddl_manager
                        .submit_activate_pending_flow_task(
                            flow_id,
                            current_flow_info.get_inner_ref().catalog_name().clone(),
                        )
                        .await
                    {
                        error!(e; "Failed to reconcile pending flow {}", flow_id);
                    }

                    Ok(())
                }
            })
            .await
            .context(PendingFlowReconcileSnafu)?;

        Ok(())
    }
}
