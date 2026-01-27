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
use std::collections::HashMap;

use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::info;
use serde::{Deserialize, Serialize};
use store_api::metadata::ColumnMetadata;
use store_api::storage::TableId;
use table::metadata::TableInfo;
use table::table_name::TableName;
use table::table_reference::TableReference;

use crate::cache_invalidator::Context as CacheContext;
use crate::ddl::utils::table_info::{
    batch_update_table_info_values, get_all_table_info_values_by_table_ids,
};
use crate::error::Result;
use crate::instruction::CacheIdent;
use crate::reconciliation::reconcile_logical_tables::reconciliation_end::ReconciliationEnd;
use crate::reconciliation::reconcile_logical_tables::{ReconcileLogicalTablesContext, State};
use crate::reconciliation::utils::build_table_meta_from_column_metadatas;

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateTableInfos;

#[async_trait::async_trait]
#[typetag::serde]
impl State for UpdateTableInfos {
    async fn next(
        &mut self,
        ctx: &mut ReconcileLogicalTablesContext,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        if ctx.persistent_ctx.update_table_infos.is_empty() {
            return Ok((Box::new(ReconciliationEnd), Status::executing(false)));
        }

        let all_table_names = ctx
            .persistent_ctx
            .logical_table_ids
            .iter()
            .cloned()
            .zip(
                ctx.persistent_ctx
                    .logical_tables
                    .iter()
                    .map(|t| t.table_ref()),
            )
            .collect::<HashMap<_, _>>();
        let table_ids = ctx
            .persistent_ctx
            .update_table_infos
            .iter()
            .map(|(table_id, _)| *table_id)
            .collect::<Vec<_>>();
        let table_names = table_ids
            .iter()
            .map(|table_id| *all_table_names.get(table_id).unwrap())
            .collect::<Vec<_>>();
        let table_info_values = get_all_table_info_values_by_table_ids(
            ctx.table_metadata_manager.table_info_manager(),
            &table_ids,
            &table_names,
        )
        .await?;

        let mut table_info_values_to_update =
            Vec::with_capacity(ctx.persistent_ctx.update_table_infos.len());
        for ((table_id, column_metadatas), table_info_value) in ctx
            .persistent_ctx
            .update_table_infos
            .iter()
            .zip(table_info_values.into_iter())
        {
            let new_table_info = Self::build_new_table_info(
                *table_id,
                column_metadatas,
                &table_info_value.table_info,
            )?;
            table_info_values_to_update.push((table_info_value, new_table_info));
        }
        let table_id = ctx.table_id();
        let table_name = ctx.table_name();

        let updated_table_info_num = table_info_values_to_update.len();
        batch_update_table_info_values(&ctx.table_metadata_manager, table_info_values_to_update)
            .await?;

        info!(
            "Updated table infos for logical tables: {:?}, physical table: {}, table_id: {}",
            ctx.persistent_ctx
                .update_table_infos
                .iter()
                .map(|(table_id, _)| table_id)
                .collect::<Vec<_>>(),
            table_id,
            table_name,
        );

        let cache_ctx = CacheContext {
            subject: Some(format!(
                "Invalidate table by reconcile logical tables, physical_table_id: {}",
                table_id
            )),
        };
        let idents = Self::build_cache_ident_keys(table_id, table_name, &table_ids, &table_names);
        ctx.cache_invalidator
            .invalidate(&cache_ctx, &idents)
            .await?;

        ctx.persistent_ctx.update_table_infos.clear();
        // Update metrics.
        let metrics = ctx.mut_metrics();
        metrics.update_table_info_count = updated_table_info_num;
        Ok((Box::new(ReconciliationEnd), Status::executing(false)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl UpdateTableInfos {
    fn build_new_table_info(
        table_id: TableId,
        column_metadatas: &[ColumnMetadata],
        table_info: &TableInfo,
    ) -> Result<TableInfo> {
        let table_ref = table_info.table_ref();
        let table_meta = build_table_meta_from_column_metadatas(
            table_id,
            table_ref,
            &table_info.meta,
            None,
            column_metadatas,
        )?;

        let mut new_table_info = table_info.clone();
        new_table_info.meta = table_meta;
        new_table_info.ident.version = table_info.ident.version + 1;
        new_table_info.sort_columns();

        Ok(new_table_info)
    }

    fn build_cache_ident_keys(
        physical_table_id: TableId,
        physical_table_name: &TableName,
        table_ids: &[TableId],
        table_names: &[TableReference],
    ) -> Vec<CacheIdent> {
        let mut cache_keys = Vec::with_capacity(table_ids.len() * 2 + 2);
        cache_keys.push(CacheIdent::TableId(physical_table_id));
        cache_keys.push(CacheIdent::TableName(physical_table_name.clone()));
        cache_keys.extend(
            table_ids
                .iter()
                .map(|table_id| CacheIdent::TableId(*table_id)),
        );
        cache_keys.extend(
            table_names
                .iter()
                .map(|table_ref| CacheIdent::TableName((*table_ref).into())),
        );

        cache_keys
    }
}
