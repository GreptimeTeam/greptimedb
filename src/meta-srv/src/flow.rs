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
        let flow_infos = ddl_context
            .flow_metadata_manager
            .flow_info_manager()
            .flow_infos()
            .try_collect::<Vec<_>>()
            .await
            .context(PendingFlowReconcileSnafu)?;
        let pending_flows = flow_infos
            .into_iter()
            .filter_map(|(flow_id, flow_info)| {
                flow_info
                    .is_pending()
                    .then_some((flow_id, flow_info.catalog_name().clone()))
            })
            .collect::<Vec<_>>();

        for (flow_id, catalog_name) in pending_flows {
            let current_flow_info = ddl_context
                .flow_metadata_manager
                .flow_info_manager()
                .get_raw(flow_id)
                .await
                .context(PendingFlowReconcileSnafu)?;
            let Some(current_flow_info) = current_flow_info else {
                continue;
            };
            if !current_flow_info.get_inner_ref().is_pending() {
                continue;
            }
            let _ = self
                .ddl_manager
                .submit_activate_pending_flow_task(flow_id, catalog_name)
                .await
                .context(PendingFlowReconcileSnafu)?;
        }

        Ok(())
    }
}
