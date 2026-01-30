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

use std::any::Any;

use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::{info, warn};
use serde::{Deserialize, Serialize};
use snafu::ensure;

use crate::ddl::utils::region_metadata_lister::RegionMetadataLister;
use crate::ddl::utils::table_info::get_all_table_info_values_by_table_ids;
use crate::error::{self, Result};
use crate::metrics;
use crate::reconciliation::reconcile_logical_tables::reconcile_regions::ReconcileRegions;
use crate::reconciliation::reconcile_logical_tables::{
    ReconcileLogicalTablesContext, ReconcileLogicalTablesProcedure, State,
};
use crate::reconciliation::utils::{
    check_column_metadatas_consistent, need_update_logical_table_info,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct ResolveTableMetadatas;

#[async_trait::async_trait]
#[typetag::serde]
impl State for ResolveTableMetadatas {
    async fn next(
        &mut self,
        ctx: &mut ReconcileLogicalTablesContext,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let table_names = ctx
            .persistent_ctx
            .logical_tables
            .iter()
            .map(|t| t.table_ref())
            .collect::<Vec<_>>();
        let table_ids = &ctx.persistent_ctx.logical_table_ids;

        let mut create_tables = vec![];
        let mut update_table_infos = vec![];

        let table_info_values = get_all_table_info_values_by_table_ids(
            ctx.table_metadata_manager.table_info_manager(),
            table_ids,
            &table_names,
        )
        .await?;

        // Safety: The physical table route is set in `ReconciliationStart` state.
        let region_routes = &ctx
            .persistent_ctx
            .physical_table_route
            .as_ref()
            .unwrap()
            .region_routes;
        let region_metadata_lister = RegionMetadataLister::new(ctx.region_rpc.clone());
        let mut metadata_consistent_count = 0;
        let mut metadata_inconsistent_count = 0;
        let mut create_tables_count = 0;
        for (table_id, table_info_value) in table_ids.iter().zip(table_info_values.iter()) {
            let region_metadatas = {
                let _timer = metrics::METRIC_META_RECONCILIATION_LIST_REGION_METADATA_DURATION
                    .with_label_values(&[metrics::TABLE_TYPE_LOGICAL])
                    .start_timer();
                region_metadata_lister
                    .list(*table_id, region_routes)
                    .await?
            };

            ensure!(!region_metadatas.is_empty(), {
                metrics::METRIC_META_RECONCILIATION_STATS
                    .with_label_values(&[
                        ReconcileLogicalTablesProcedure::TYPE_NAME,
                        metrics::TABLE_TYPE_LOGICAL,
                        metrics::STATS_TYPE_NO_REGION_METADATA,
                    ])
                    .inc();

                error::UnexpectedSnafu {
                    err_msg: format!(
                        "No region metadata found for table: {}, table_id: {}",
                        table_info_value.table_info.name, table_id
                    ),
                }
            });

            if region_metadatas.iter().any(|r| r.is_none()) {
                create_tables_count += 1;
                create_tables.push((*table_id, table_info_value.table_info.clone()));
                continue;
            }

            // Safety: The physical table route is set in `ReconciliationStart` state.
            let region_metadatas = region_metadatas
                .into_iter()
                .map(|r| r.unwrap())
                .collect::<Vec<_>>();
            if let Some(column_metadatas) = check_column_metadatas_consistent(&region_metadatas) {
                metadata_consistent_count += 1;
                if need_update_logical_table_info(&table_info_value.table_info, &column_metadatas) {
                    update_table_infos.push((*table_id, column_metadatas));
                }
            } else {
                metadata_inconsistent_count += 1;
                // If the logical regions have inconsistent column metadatas, it won't affect read and write.
                // It's safe to continue if the column metadatas of the logical table are inconsistent.
                warn!(
                    "Found inconsistent column metadatas for table: {}, table_id: {}. Remaining the inconsistent column metadatas",
                    table_info_value.table_info.name, table_id
                );
            }
        }

        let table_id = ctx.table_id();
        let table_name = ctx.table_name();
        info!(
            "Resolving table metadatas for physical table: {}, table_id: {}, updating table infos: {:?}, creating tables: {:?}",
            table_name,
            table_id,
            update_table_infos
                .iter()
                .map(|(table_id, _)| *table_id)
                .collect::<Vec<_>>(),
            create_tables
                .iter()
                .map(|(table_id, _)| *table_id)
                .collect::<Vec<_>>(),
        );
        ctx.persistent_ctx.update_table_infos = update_table_infos;
        ctx.persistent_ctx.create_tables = create_tables;
        // Update metrics.
        let metrics = ctx.mut_metrics();
        metrics.column_metadata_consistent_count = metadata_consistent_count;
        metrics.column_metadata_inconsistent_count = metadata_inconsistent_count;
        metrics.create_tables_count = create_tables_count;
        Ok((Box::new(ReconcileRegions), Status::executing(true)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
