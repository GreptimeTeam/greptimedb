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

use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::info;
use serde::{Deserialize, Serialize};
use store_api::metadata::ColumnMetadata;
use tonic::async_trait;

use crate::error::Result;
use crate::key::DeserializedValueWithBytes;
use crate::key::table_info::TableInfoValue;
use crate::reconciliation::reconcile_table::reconciliation_end::ReconciliationEnd;
use crate::reconciliation::reconcile_table::{ReconcileTableContext, State};
use crate::rpc::router::region_distribution;

/// Updates the table info with the new column metadatas.
#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateTableInfo {
    table_info_value: DeserializedValueWithBytes<TableInfoValue>,
    column_metadatas: Vec<ColumnMetadata>,
    /// A region repair must retire the old write layouts even if the catalog schema is identical.
    #[serde(default)]
    refresh_sparse_keys: bool,
}

impl UpdateTableInfo {
    pub fn new(
        table_info_value: DeserializedValueWithBytes<TableInfoValue>,
        column_metadatas: Vec<ColumnMetadata>,
        refresh_sparse_keys: bool,
    ) -> Self {
        Self {
            table_info_value,
            column_metadatas,
            refresh_sparse_keys,
        }
    }
}

#[async_trait]
#[typetag::serde]
impl State for UpdateTableInfo {
    async fn next(
        &mut self,
        ctx: &mut ReconcileTableContext,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let new_table_meta = match &ctx.volatile_ctx.table_meta {
            Some(table_meta) => table_meta.clone(),
            None => ctx.build_table_meta(&self.column_metadatas)?,
        };

        let region_routes = &ctx
            .persistent_ctx
            .physical_table_route
            .as_ref()
            .unwrap()
            .region_routes;
        let region_distribution = region_distribution(region_routes);
        let current_table_info_value = ctx.persistent_ctx.table_info_value.as_ref().unwrap();
        if new_table_meta == current_table_info_value.table_info.meta && !self.refresh_sparse_keys {
            info!(
                "Table info is already up to date for table: {}, table_id: {}",
                ctx.table_name(),
                ctx.table_id()
            );
            ctx.volatile_ctx
                .result_summary
                .mark_table_info_phase_completed();
            return Ok((Box::new(ReconciliationEnd), Status::executing(true)));
        }

        let mut new_table_info = current_table_info_value.table_info.clone();
        new_table_info.meta = new_table_meta;
        info!(
            "Updating table info for table: {}, table_id: {}. new table meta: {:?}, current table meta: {:?}",
            ctx.table_name(),
            ctx.table_id(),
            new_table_info.meta,
            current_table_info_value.table_info.meta,
        );
        ctx.publish_table_info(new_table_info, Some(region_distribution))
            .await?;
        ctx.volatile_ctx
            .result_summary
            .mark_table_info_phase_completed();

        Ok((Box::new(ReconciliationEnd), Status::executing(true)))
    }
}
