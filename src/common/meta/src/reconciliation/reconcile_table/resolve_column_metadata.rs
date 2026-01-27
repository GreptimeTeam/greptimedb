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

use async_trait::async_trait;
use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::info;
use serde::{Deserialize, Serialize};
use snafu::OptionExt;
use store_api::metadata::RegionMetadata;
use strum::AsRefStr;

use crate::error::{self, MissingColumnIdsSnafu, Result};
use crate::reconciliation::reconcile_table::reconcile_regions::ReconcileRegions;
use crate::reconciliation::reconcile_table::update_table_info::UpdateTableInfo;
use crate::reconciliation::reconcile_table::{ReconcileTableContext, State};
use crate::reconciliation::utils::{
    ResolveColumnMetadataResult, build_column_metadata_from_table_info,
    check_column_metadatas_consistent, resolve_column_metadatas_with_latest,
    resolve_column_metadatas_with_metasrv,
};

/// Strategy for resolving column metadata inconsistencies.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, Default, AsRefStr)]
pub enum ResolveStrategy {
    #[default]
    /// Trusts the latest column metadata from datanode.
    UseLatest,

    /// Always uses the column metadata from metasrv.
    UseMetasrv,

    /// Aborts the resolution process if inconsistencies are detected.
    AbortOnConflict,
}

impl From<api::v1::meta::ResolveStrategy> for ResolveStrategy {
    fn from(strategy: api::v1::meta::ResolveStrategy) -> Self {
        match strategy {
            api::v1::meta::ResolveStrategy::UseMetasrv => Self::UseMetasrv,
            api::v1::meta::ResolveStrategy::UseLatest => Self::UseLatest,
            api::v1::meta::ResolveStrategy::AbortOnConflict => Self::AbortOnConflict,
        }
    }
}

/// State responsible for resolving inconsistencies in column metadata across physical regions.
#[derive(Debug, Serialize, Deserialize)]
pub struct ResolveColumnMetadata {
    strategy: ResolveStrategy,
    region_metadata: Vec<RegionMetadata>,
}

impl ResolveColumnMetadata {
    pub fn new(strategy: ResolveStrategy, region_metadata: Vec<RegionMetadata>) -> Self {
        Self {
            strategy,
            region_metadata,
        }
    }
}

#[async_trait]
#[typetag::serde]
impl State for ResolveColumnMetadata {
    async fn next(
        &mut self,
        ctx: &mut ReconcileTableContext,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let table_id = ctx.persistent_ctx.table_id;
        let table_name = &ctx.persistent_ctx.table_name;

        let table_info_value = ctx
            .table_metadata_manager
            .table_info_manager()
            .get(table_id)
            .await?
            .with_context(|| error::TableNotFoundSnafu {
                table_name: table_name.to_string(),
            })?;
        ctx.persistent_ctx.table_info_value = Some(table_info_value);

        if let Some(column_metadatas) = check_column_metadatas_consistent(&self.region_metadata) {
            // Safety: fetched in the above.
            let table_info_value = ctx.persistent_ctx.table_info_value.clone().unwrap();
            info!(
                "Column metadatas are consistent for table: {}, table_id: {}.",
                table_name, table_id
            );

            // Update metrics.
            ctx.mut_metrics().resolve_column_metadata_result =
                Some(ResolveColumnMetadataResult::Consistent);
            return Ok((
                Box::new(UpdateTableInfo::new(table_info_value, column_metadatas)),
                Status::executing(false),
            ));
        };

        match self.strategy {
            ResolveStrategy::UseMetasrv => {
                let table_info_value = ctx.persistent_ctx.table_info_value.as_ref().unwrap();
                let name_to_ids = table_info_value
                    .table_info
                    .name_to_ids()
                    .context(MissingColumnIdsSnafu)?;
                let column_metadata = build_column_metadata_from_table_info(
                    table_info_value.table_info.meta.schema.column_schemas(),
                    &table_info_value.table_info.meta.primary_key_indices,
                    &name_to_ids,
                )?;

                let region_ids =
                    resolve_column_metadatas_with_metasrv(&column_metadata, &self.region_metadata)?;

                // Update metrics.
                let metrics = ctx.mut_metrics();
                metrics.resolve_column_metadata_result =
                    Some(ResolveColumnMetadataResult::Inconsistent(self.strategy));
                Ok((
                    Box::new(ReconcileRegions::new(column_metadata, region_ids)),
                    Status::executing(true),
                ))
            }
            ResolveStrategy::UseLatest => {
                let (column_metadatas, region_ids) =
                    resolve_column_metadatas_with_latest(&self.region_metadata)?;

                // Update metrics.
                let metrics = ctx.mut_metrics();
                metrics.resolve_column_metadata_result =
                    Some(ResolveColumnMetadataResult::Inconsistent(self.strategy));
                Ok((
                    Box::new(ReconcileRegions::new(column_metadatas, region_ids)),
                    Status::executing(true),
                ))
            }
            ResolveStrategy::AbortOnConflict => {
                let table_name = table_name.to_string();

                // Update metrics.
                let metrics = ctx.mut_metrics();
                metrics.resolve_column_metadata_result =
                    Some(ResolveColumnMetadataResult::Inconsistent(self.strategy));
                error::ColumnMetadataConflictsSnafu {
                    table_name,
                    table_id,
                }
                .fail()
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
