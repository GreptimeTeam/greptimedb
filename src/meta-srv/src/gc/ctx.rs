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
use std::time::Duration;

use common_meta::datanode::RegionStat;
use common_meta::key::TableMetadataManagerRef;
use common_meta::key::table_repart::TableRepartValue;
use common_meta::key::table_route::PhysicalTableRouteValue;
use common_procedure::{ProcedureManagerRef, ProcedureWithId, watcher};
use common_telemetry::debug;
use snafu::{OptionExt as _, ResultExt as _};
use store_api::storage::{GcReport, RegionId};
use table::metadata::TableId;

use crate::cluster::MetaPeerClientRef;
use crate::error::{self, Result, TableMetadataManagerSnafu};
use crate::gc::Region2Peers;
use crate::gc::procedure::BatchGcProcedure;
use crate::service::mailbox::MailboxRef;

#[async_trait::async_trait]
pub(crate) trait SchedulerCtx: Send + Sync {
    async fn get_table_to_region_stats(&self) -> Result<HashMap<TableId, Vec<RegionStat>>>;

    async fn get_table_reparts(&self) -> Result<Vec<(TableId, TableRepartValue)>>;

    async fn get_table_route(
        &self,
        table_id: TableId,
    ) -> Result<(TableId, PhysicalTableRouteValue)>;

    async fn batch_get_table_route(
        &self,
        table_ids: &[TableId],
    ) -> Result<HashMap<TableId, PhysicalTableRouteValue>>;

    async fn gc_regions(
        &self,
        region_ids: &[RegionId],
        full_file_listing: bool,
        timeout: Duration,
        region_routes_override: Region2Peers,
    ) -> Result<GcReport>;
}

pub(crate) struct DefaultGcSchedulerCtx {
    /// The metadata manager.
    pub(crate) table_metadata_manager: TableMetadataManagerRef,
    /// Procedure manager.
    pub(crate) procedure_manager: ProcedureManagerRef,
    /// For getting `RegionStats`.
    pub(crate) meta_peer_client: MetaPeerClientRef,
    /// The mailbox to send messages.
    pub(crate) mailbox: MailboxRef,
    /// The server address.
    pub(crate) server_addr: String,
}

impl DefaultGcSchedulerCtx {
    pub fn try_new(
        table_metadata_manager: TableMetadataManagerRef,
        procedure_manager: ProcedureManagerRef,
        meta_peer_client: MetaPeerClientRef,
        mailbox: MailboxRef,
        server_addr: String,
    ) -> Result<Self> {
        Ok(Self {
            table_metadata_manager,
            procedure_manager,
            meta_peer_client,
            mailbox,
            server_addr,
        })
    }
}

#[async_trait::async_trait]
impl SchedulerCtx for DefaultGcSchedulerCtx {
    async fn get_table_to_region_stats(&self) -> Result<HashMap<TableId, Vec<RegionStat>>> {
        let dn_stats = self.meta_peer_client.get_all_dn_stat_kvs().await?;
        let mut table_to_region_stats: HashMap<TableId, Vec<RegionStat>> = HashMap::new();
        for (_dn_id, stats) in dn_stats {
            let stats = stats.stats;

            let Some(latest_stat) = stats.iter().max_by_key(|s| s.timestamp_millis).cloned() else {
                continue;
            };

            for region_stat in latest_stat.region_stats {
                table_to_region_stats
                    .entry(region_stat.id.table_id())
                    .or_default()
                    .push(region_stat);
            }
        }
        Ok(table_to_region_stats)
    }

    async fn get_table_reparts(&self) -> Result<Vec<(TableId, TableRepartValue)>> {
        self.table_metadata_manager
            .table_repart_manager()
            .table_reparts()
            .await
            .context(TableMetadataManagerSnafu)
    }

    async fn get_table_route(
        &self,
        table_id: TableId,
    ) -> Result<(TableId, PhysicalTableRouteValue)> {
        self.table_metadata_manager
            .table_route_manager()
            .get_physical_table_route(table_id)
            .await
            .context(TableMetadataManagerSnafu)
    }

    async fn batch_get_table_route(
        &self,
        table_ids: &[TableId],
    ) -> Result<HashMap<TableId, PhysicalTableRouteValue>> {
        self.table_metadata_manager
            .table_route_manager()
            .batch_get_physical_table_routes(table_ids)
            .await
            .context(TableMetadataManagerSnafu)
    }

    async fn gc_regions(
        &self,
        region_ids: &[RegionId],
        full_file_listing: bool,
        timeout: Duration,
        region_routes_override: Region2Peers,
    ) -> Result<GcReport> {
        self.gc_regions_inner(
            region_ids,
            full_file_listing,
            timeout,
            region_routes_override,
        )
        .await
    }
}

impl DefaultGcSchedulerCtx {
    async fn gc_regions_inner(
        &self,
        region_ids: &[RegionId],
        full_file_listing: bool,
        timeout: Duration,
        region_routes_override: Region2Peers,
    ) -> Result<GcReport> {
        debug!(
            "Sending GC instruction for {} regions (full_file_listing: {})",
            region_ids.len(),
            full_file_listing
        );

        let procedure = BatchGcProcedure::new(
            self.mailbox.clone(),
            self.table_metadata_manager.clone(),
            self.server_addr.clone(),
            region_ids.to_vec(),
            full_file_listing,
            timeout,
            region_routes_override,
        );
        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        let id = procedure_with_id.id;

        let mut watcher = self
            .procedure_manager
            .submit(procedure_with_id)
            .await
            .context(error::SubmitProcedureSnafu)?;
        let res = watcher::wait(&mut watcher)
            .await
            .context(error::WaitProcedureSnafu)?
            .with_context(|| error::UnexpectedSnafu {
                violated: format!(
                    "GC procedure {id} successfully completed but no result returned"
                ),
            })?;

        let gc_report = BatchGcProcedure::cast_result(res)?;

        Ok(gc_report)
    }
}
