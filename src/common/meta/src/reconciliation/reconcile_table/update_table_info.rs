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
use common_telemetry::info;
use serde::{Deserialize, Serialize};
use store_api::metadata::ColumnMetadata;
use tonic::async_trait;

use crate::cache_invalidator::Context as CacheContext;
use crate::error::Result;
use crate::instruction::CacheIdent;
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
}

impl UpdateTableInfo {
    pub fn new(
        table_info_value: DeserializedValueWithBytes<TableInfoValue>,
        column_metadatas: Vec<ColumnMetadata>,
    ) -> Self {
        Self {
            table_info_value,
            column_metadatas,
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
        let new_table_info = {
            let mut new_table_info = current_table_info_value.table_info.clone();
            new_table_info.meta = new_table_meta;
            new_table_info
        };

        if new_table_info.meta == current_table_info_value.table_info.meta {
            info!(
                "Table info is already up to date for table: {}, table_id: {}",
                ctx.table_name(),
                ctx.table_id()
            );
            return Ok((Box::new(ReconciliationEnd), Status::executing(true)));
        }

        info!(
            "Updating table info for table: {}, table_id: {}. new table meta: {:?}, current table meta: {:?}",
            ctx.table_name(),
            ctx.table_id(),
            new_table_info.meta,
            current_table_info_value.table_info.meta,
        );
        ctx.table_metadata_manager
            .update_table_info(
                current_table_info_value,
                Some(region_distribution),
                new_table_info,
                None,
            )
            .await?;

        let table_ref = ctx.table_name().table_ref();
        let table_id = ctx.table_id();
        let cache_ctx = CacheContext {
            subject: Some(format!(
                "Invalidate table cache by reconciling table {}, table_id: {}",
                table_ref, table_id,
            )),
        };
        ctx.cache_invalidator
            .invalidate(
                &cache_ctx,
                &[
                    CacheIdent::TableName(table_ref.into()),
                    CacheIdent::TableId(table_id),
                ],
            )
            .await?;
        // Update metrics.
        let metrics = ctx.mut_metrics();
        metrics.update_table_info = true;

        Ok((Box::new(ReconciliationEnd), Status::executing(true)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
