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
use std::collections::{HashMap, HashSet};

use api::v1::column_def::try_as_column_def;
use api::v1::region::region_request::Body;
use api::v1::region::{
    AlterRequest, RegionColumnDef, RegionRequest, RegionRequestHeader, SyncColumns, alter_request,
};
use api::v1::{ColumnDef, SemanticType};
use async_trait::async_trait;
use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::info;
use common_telemetry::tracing_context::TracingContext;
use futures::future;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use store_api::metadata::ColumnMetadata;
use store_api::metric_engine_consts::TABLE_COLUMN_METADATA_EXTENSION_KEY;
use store_api::storage::{ColumnId, RegionId};

use crate::ddl::utils::{add_peer_context_if_needed, extract_column_metadatas};
use crate::error::{ConvertColumnDefSnafu, Result, UnexpectedSnafu};
use crate::reconciliation::reconcile_table::reconciliation_end::ReconciliationEnd;
use crate::reconciliation::reconcile_table::update_table_info::UpdateTableInfo;
use crate::reconciliation::reconcile_table::{ReconcileTableContext, State};
use crate::rpc::router::{find_leaders, region_distribution};

#[derive(Debug, Serialize, Deserialize)]
pub struct ReconcileRegions {
    column_metadatas: Vec<ColumnMetadata>,
    region_ids: HashSet<RegionId>,
}

impl ReconcileRegions {
    pub fn new(column_metadatas: Vec<ColumnMetadata>, region_ids: Vec<RegionId>) -> Self {
        Self {
            column_metadatas,
            region_ids: region_ids.into_iter().collect(),
        }
    }
}

#[async_trait]
#[typetag::serde]
impl State for ReconcileRegions {
    async fn next(
        &mut self,
        ctx: &mut ReconcileTableContext,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let table_meta = ctx.build_table_meta(&self.column_metadatas)?;
        ctx.volatile_ctx.table_meta = Some(table_meta);
        let table_id = ctx.table_id();
        let table_name = ctx.table_name();

        let primary_keys = self
            .column_metadatas
            .iter()
            .filter(|c| c.semantic_type == SemanticType::Tag)
            .map(|c| c.column_schema.name.clone())
            .collect::<HashSet<_>>();
        let column_defs = self
            .column_metadatas
            .iter()
            .map(|c| {
                let column_def = try_as_column_def(
                    &c.column_schema,
                    primary_keys.contains(&c.column_schema.name),
                )
                .context(ConvertColumnDefSnafu {
                    column: &c.column_schema.name,
                })?;

                Ok((c.column_id, column_def))
            })
            .collect::<Result<Vec<_>>>()?;

        // Sends sync column metadatas to datanode.
        // Safety: The physical table route is set in `ReconciliationStart` state.
        let region_routes = &ctx
            .persistent_ctx
            .physical_table_route
            .as_ref()
            .unwrap()
            .region_routes;
        let region_distribution = region_distribution(region_routes);
        let leaders = find_leaders(region_routes)
            .into_iter()
            .map(|p| (p.id, p))
            .collect::<HashMap<_, _>>();
        let region_rpc = ctx.region_rpc.clone();
        let mut sync_column_tsks = Vec::with_capacity(self.region_ids.len());
        for (datanode_id, region_role_set) in region_distribution {
            if region_role_set.leader_regions.is_empty() {
                continue;
            }
            // Safety: It contains all leaders in the region routes.
            let peer = leaders.get(&datanode_id).unwrap();
            for region_id in region_role_set.leader_regions {
                let region_id = RegionId::new(ctx.persistent_ctx.table_id, region_id);
                if self.region_ids.contains(&region_id) {
                    let request = make_alter_region_request(region_id, &column_defs);
                    let peer = peer.clone();
                    let region_rpc = region_rpc.clone();

                    sync_column_tsks.push(async move {
                        region_rpc
                            .handle_region(&peer, request)
                            .await
                            .map_err(add_peer_context_if_needed(peer))
                    });
                }
            }
        }

        let mut results = future::join_all(sync_column_tsks)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        // Ensures all the column metadatas are the same.
        let column_metadatas =
            extract_column_metadatas(&mut results, TABLE_COLUMN_METADATA_EXTENSION_KEY)?.context(
                UnexpectedSnafu {
                    err_msg: format!(
                        "The table column metadata schemas from datanodes are not the same, table: {}, table_id: {}",
                        table_name,
                        table_id
                    ),
                },
            )?;

        // Checks all column metadatas are consistent, and updates the table info if needed.
        if column_metadatas != self.column_metadatas {
            info!(
                "Datanode column metadatas are not consistent with metasrv, updating metasrv's column metadatas, table: {}, table_id: {}",
                table_name, table_id
            );
            // Safety: fetched in the above.
            let table_info_value = ctx.persistent_ctx.table_info_value.clone().unwrap();
            return Ok((
                Box::new(UpdateTableInfo::new(table_info_value, column_metadatas)),
                Status::executing(true),
            ));
        }

        Ok((Box::new(ReconciliationEnd), Status::executing(false)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Makes an alter region request to sync columns.
fn make_alter_region_request(
    region_id: RegionId,
    column_defs: &[(ColumnId, ColumnDef)],
) -> RegionRequest {
    let kind = alter_request::Kind::SyncColumns(to_region_sync_columns(column_defs));

    let alter_request = AlterRequest {
        region_id: region_id.as_u64(),
        schema_version: 0,
        kind: Some(kind),
    };

    RegionRequest {
        header: Some(RegionRequestHeader {
            tracing_context: TracingContext::from_current_span().to_w3c(),
            ..Default::default()
        }),
        body: Some(Body::Alter(alter_request)),
    }
}

fn to_region_sync_columns(column_defs: &[(ColumnId, ColumnDef)]) -> SyncColumns {
    let region_column_defs = column_defs
        .iter()
        .map(|(column_id, column_def)| RegionColumnDef {
            column_id: *column_id,
            column_def: Some(column_def.clone()),
        })
        .collect::<Vec<_>>();

    SyncColumns {
        column_defs: region_column_defs,
    }
}
