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

use std::vec;

use api::v1::alter_expr::Kind;
use api::v1::region::{
    alter_request, region_request, AddColumn, AddColumns, AlterRequest, DropColumn, DropColumns,
    RegionColumnDef,
};
use api::v1::{AlterExpr, RenameTable};
use async_trait::async_trait;
use common_grpc_expr::alter_expr_to_request;
use common_procedure::error::{FromJsonSnafu, Result as ProcedureResult, ToJsonSnafu};
use common_procedure::{
    Context as ProcedureContext, Error as ProcedureError, LockKey, Procedure, Status,
};
use common_telemetry::{debug, info};
use futures::future;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::RegionId;
use strum::AsRefStr;
use table::engine::TableReference;
use table::metadata::{RawTableInfo, TableId, TableInfo};
use table::requests::AlterKind;

use crate::cache_invalidator::Context;
use crate::ddl::utils::handle_operate_region_error;
use crate::ddl::DdlContext;
use crate::error::{
    self, ConvertAlterTableRequestSnafu, InvalidProtoMsgSnafu, Result, TableRouteNotFoundSnafu,
};
use crate::ident::TableIdent;
use crate::key::table_info::TableInfoValue;
use crate::key::table_name::TableNameKey;
use crate::key::table_route::TableRouteValue;
use crate::metrics;
use crate::rpc::ddl::AlterTableTask;
use crate::rpc::router::{find_leader_regions, find_leaders};
use crate::table_name::TableName;

pub struct AlterTableProcedure {
    context: DdlContext,
    data: AlterTableData,
}

impl AlterTableProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::AlterTable";

    pub fn new(
        cluster_id: u64,
        task: AlterTableTask,
        table_info_value: TableInfoValue,
        context: DdlContext,
    ) -> Self {
        Self {
            context,
            data: AlterTableData::new(task, table_info_value, cluster_id),
        }
    }

    pub fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data = serde_json::from_str(json).context(FromJsonSnafu)?;

        Ok(AlterTableProcedure { context, data })
    }

    // Checks whether the table exists.
    async fn on_prepare(&mut self) -> Result<Status> {
        let alter_expr = &self.alter_expr();
        let catalog = &alter_expr.catalog_name;
        let schema = &alter_expr.schema_name;

        let manager = &self.context.table_metadata_manager;

        if let Kind::RenameTable(RenameTable { new_table_name }) = self.alter_kind()? {
            let new_table_name_key = TableNameKey::new(catalog, schema, new_table_name);

            let exist = manager
                .table_name_manager()
                .exists(new_table_name_key)
                .await?;

            ensure!(
                !exist,
                error::TableAlreadyExistsSnafu {
                    table_name: TableName::from(new_table_name_key).to_string(),
                }
            )
        }

        let table_name_key = TableNameKey::new(catalog, schema, &alter_expr.table_name);

        let exist = manager.table_name_manager().exists(table_name_key).await?;

        ensure!(
            exist,
            error::TableNotFoundSnafu {
                table_name: TableName::from(table_name_key).to_string()
            }
        );

        self.data.state = AlterTableState::UpdateMetadata;

        Ok(Status::executing(true))
    }

    fn alter_expr(&self) -> &AlterExpr {
        &self.data.task.alter_table
    }

    fn alter_kind(&self) -> Result<&Kind> {
        self.alter_expr()
            .kind
            .as_ref()
            .context(InvalidProtoMsgSnafu {
                err_msg: "'kind' is absent",
            })
    }

    pub fn create_alter_region_request(&self, region_id: RegionId) -> Result<AlterRequest> {
        let table_info = self.data.table_info();

        let kind =
            match self.alter_kind()? {
                Kind::AddColumns(x) => {
                    let mut next_column_id = table_info.meta.next_column_id;

                    let add_columns =
                        x.add_columns
                            .iter()
                            .map(|add_column| {
                                let column_def = add_column.column_def.as_ref().context(
                                    InvalidProtoMsgSnafu {
                                        err_msg: "'column_def' is absent",
                                    },
                                )?;

                                let column_id = next_column_id;
                                next_column_id += 1;

                                let column_def = RegionColumnDef {
                                    column_def: Some(column_def.clone()),
                                    column_id,
                                };

                                Ok(AddColumn {
                                    column_def: Some(column_def),
                                    location: add_column.location.clone(),
                                })
                            })
                            .collect::<Result<Vec<_>>>()?;

                    alter_request::Kind::AddColumns(AddColumns { add_columns })
                }
                Kind::DropColumns(x) => {
                    let drop_columns = x
                        .drop_columns
                        .iter()
                        .map(|x| DropColumn {
                            name: x.name.clone(),
                        })
                        .collect::<Vec<_>>();

                    alter_request::Kind::DropColumns(DropColumns { drop_columns })
                }
                Kind::RenameTable(_) => unreachable!(),
            };

        Ok(AlterRequest {
            region_id: region_id.as_u64(),
            schema_version: table_info.ident.version,
            kind: Some(kind),
        })
    }

    pub async fn submit_alter_region_requests(&self) -> Result<Status> {
        let table_id = self.data.table_id();
        let table_ref = self.data.table_ref();

        let TableRouteValue { region_routes, .. } = self
            .context
            .table_metadata_manager
            .table_route_manager()
            .get(table_id)
            .await?
            .with_context(|| TableRouteNotFoundSnafu {
                table_name: table_ref.to_string(),
            })?;

        let leaders = find_leaders(&region_routes);
        let mut alter_region_tasks = Vec::with_capacity(leaders.len());

        for datanode in leaders {
            let datanode_manager = self.context.datanode_manager.clone();

            let regions = find_leader_regions(&region_routes, &datanode);

            alter_region_tasks.push(async move {
                for region in regions {
                    let region_id = RegionId::new(table_id, region);
                    let request = self.create_alter_region_request(region_id)?;
                    debug!("Submitting {request:?} to {datanode}");

                    let requester = datanode_manager.datanode(&datanode).await;
                    if let Err(e) = requester.handle(region_request::Body::Alter(request)).await {
                        return Err(handle_operate_region_error(datanode)(e));
                    }
                }
                Ok(())
            });
        }

        future::join_all(alter_region_tasks)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        Ok(Status::Done)
    }

    /// Update table metadata for rename table operation.
    async fn on_update_metadata_for_rename(&self, new_table_name: String) -> Result<()> {
        let table_metadata_manager = &self.context.table_metadata_manager;

        let current_table_info_value = self.data.table_info_value.clone();

        table_metadata_manager
            .rename_table(current_table_info_value, new_table_name)
            .await?;

        Ok(())
    }

    async fn on_update_metadata_for_alter(&self, new_table_info: RawTableInfo) -> Result<()> {
        let table_metadata_manager = &self.context.table_metadata_manager;
        let current_table_info_value = self.data.table_info_value.clone();

        table_metadata_manager
            .update_table_info(current_table_info_value, new_table_info)
            .await?;

        Ok(())
    }

    fn build_new_table_info(&self) -> Result<TableInfo> {
        // Builds new_meta
        let table_info = TableInfo::try_from(self.data.table_info().clone())
            .context(error::ConvertRawTableInfoSnafu)?;

        let table_ref = self.data.table_ref();

        let request = alter_expr_to_request(self.data.table_id(), self.alter_expr().clone())
            .context(ConvertAlterTableRequestSnafu)?;

        let new_meta = table_info
            .meta
            .builder_with_alter_kind(table_ref.table, &request.alter_kind)
            .context(error::TableSnafu)?
            .build()
            .with_context(|_| error::BuildTableMetaSnafu {
                table_name: table_ref.table,
            })?;

        let mut new_info = table_info.clone();
        new_info.ident.version = table_info.ident.version + 1;
        new_info.meta = new_meta;

        if let AlterKind::RenameTable { new_table_name } = &request.alter_kind {
            new_info.name = new_table_name.to_string();
        }

        Ok(new_info)
    }

    /// Update table metadata.
    async fn on_update_metadata(&mut self) -> Result<Status> {
        let table_id = self.data.table_id();
        let table_ref = self.data.table_ref();
        let new_info = self.build_new_table_info()?;

        debug!(
            "starting update table: {} metadata, new table info {:?}",
            table_ref.to_string(),
            new_info
        );

        if let Kind::RenameTable(RenameTable { new_table_name }) = self.alter_kind()? {
            self.on_update_metadata_for_rename(new_table_name.to_string())
                .await?;
        } else {
            self.on_update_metadata_for_alter(new_info.into()).await?;
        }

        info!("Updated table metadata for table {table_id}");

        self.data.state = AlterTableState::InvalidateTableCache;
        Ok(Status::executing(true))
    }

    /// Broadcasts the invalidating table cache instructions.
    async fn on_broadcast(&mut self) -> Result<Status> {
        let table_ref = self.data.table_ref();

        let table_ident = TableIdent {
            catalog: table_ref.catalog.to_string(),
            schema: table_ref.schema.to_string(),
            table: table_ref.table.to_string(),
            table_id: self.data.table_id(),
            engine: self.data.table_info().meta.engine.to_string(),
        };

        self.context
            .cache_invalidator
            .invalidate_table(
                &Context {
                    subject: Some("Invalidate table cache by alter table procedure".to_string()),
                },
                table_ident,
            )
            .await?;

        let alter_kind = self.alter_kind()?;
        if matches!(alter_kind, Kind::RenameTable { .. }) {
            Ok(Status::Done)
        } else {
            self.data.state = AlterTableState::SubmitAlterRegionRequests;

            Ok(Status::executing(true))
        }
    }

    fn lock_key_inner(&self) -> Vec<String> {
        let table_ref = self.data.table_ref();
        let table_key = common_catalog::format_full_table_name(
            table_ref.catalog,
            table_ref.schema,
            table_ref.table,
        );
        let mut lock_key = vec![table_key];

        if let Ok(Kind::RenameTable(RenameTable { new_table_name })) = self.alter_kind() {
            lock_key.push(common_catalog::format_full_table_name(
                table_ref.catalog,
                table_ref.schema,
                new_table_name,
            ))
        }

        lock_key
    }
}

#[async_trait]
impl Procedure for AlterTableProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let error_handler = |e| {
            if matches!(e, error::Error::RetryLater { .. }) {
                ProcedureError::retry_later(e)
            } else {
                ProcedureError::external(e)
            }
        };

        let state = &self.data.state;

        let _timer = common_telemetry::timer!(
            metrics::METRIC_META_PROCEDURE_ALTER_TABLE,
            &[("step", state.as_ref().to_string())]
        );

        match state {
            AlterTableState::Prepare => self.on_prepare().await,
            AlterTableState::UpdateMetadata => self.on_update_metadata().await,
            AlterTableState::InvalidateTableCache => self.on_broadcast().await,
            AlterTableState::SubmitAlterRegionRequests => self.submit_alter_region_requests().await,
        }
        .map_err(error_handler)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let key = self.lock_key_inner();

        LockKey::new(key)
    }
}

#[derive(Debug, Serialize, Deserialize, AsRefStr)]
enum AlterTableState {
    /// Prepares to alter the table
    Prepare,
    /// Updates table metadata.
    UpdateMetadata,
    /// Broadcasts the invalidating table cache instruction.
    InvalidateTableCache,
    SubmitAlterRegionRequests,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AlterTableData {
    state: AlterTableState,
    task: AlterTableTask,
    table_info_value: TableInfoValue,
    cluster_id: u64,
}

impl AlterTableData {
    pub fn new(task: AlterTableTask, table_info_value: TableInfoValue, cluster_id: u64) -> Self {
        Self {
            state: AlterTableState::Prepare,
            task,
            table_info_value,
            cluster_id,
        }
    }

    fn table_ref(&self) -> TableReference {
        self.task.table_ref()
    }

    fn table_id(&self) -> TableId {
        self.table_info().ident.table_id
    }

    fn table_info(&self) -> &RawTableInfo {
        &self.table_info_value.table_info
    }
}
