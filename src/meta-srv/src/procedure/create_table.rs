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

use async_trait::async_trait;
use catalog::helper::TableGlobalKey;
use client::Database;
use common_meta::rpc::ddl::CreateTableTask;
use common_meta::rpc::router::TableRoute;
use common_meta::table_name::TableName;
use common_procedure::error::{FromJsonSnafu, Result as ProcedureResult, ToJsonSnafu};
use common_procedure::{Context as ProcedureContext, LockKey, Procedure, Status};
use common_telemetry::warn;
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use table::engine::TableReference;
use table::metadata::TableId;

use crate::ddl::{DdlContext, ProcedureStatus};
use crate::error::{self, Result};
use crate::metasrv::SelectorContext;
use crate::service::router::{fill_table_routes, handle_create_table_metadata, table_route_key};
use crate::table_routes::{get_table_global_value, get_table_route_value};

// TODO(weny): removes in following PRs.
#[allow(unused)]
pub struct CreateTableProcedure {
    context: DdlContext,
    creator: TableCreator,
}

#[derive(Debug, Clone)]
pub struct CreateTableProcedureStatus {
    pub table_id: TableId,
}

impl CreateTableProcedureStatus {
    pub fn with_table_id(table_id: TableId) -> Self {
        Self { table_id }
    }
}

// TODO(weny): removes in following PRs.
#[allow(dead_code)]
impl CreateTableProcedure {
    pub(crate) const TYPE_NAME: &'static str = "metasrv-procedure::CreateTable";

    pub(crate) fn new(cluster_id: u64, task: CreateTableTask, context: DdlContext) -> Self {
        Self {
            context,
            creator: TableCreator::new(cluster_id, task),
        }
    }

    pub(crate) fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data = serde_json::from_str(json).context(FromJsonSnafu)?;
        Ok(CreateTableProcedure {
            context,
            creator: TableCreator { data },
        })
    }

    fn global_table_key(&self) -> TableGlobalKey {
        let table_ref = self.creator.data.table_ref();

        TableGlobalKey {
            catalog_name: table_ref.catalog.to_uppercase(),
            schema_name: table_ref.schema.to_string(),
            table_name: table_ref.table.to_string(),
        }
    }

    fn table_name(&self) -> TableName {
        self.creator.data.task.table_name()
    }

    /// Checks whether the table exists.
    async fn on_prepare(&mut self) -> Result<Status> {
        if (get_table_global_value(&self.context.kv_store, &self.global_table_key()).await?)
            .is_some()
        {
            ensure!(
                self.creator.data.task.create_table.create_if_not_exists,
                error::TableAlreadyExistsSnafu {
                    table_name: self.creator.data.table_ref().to_string(),
                }
            );

            return Ok(Status::Done);
        }

        self.creator.data.state = CreateTableState::CreateMetadata;

        Ok(Status::executing(true))
    }

    async fn on_create_metadata(&mut self) -> Result<Status> {
        let kv_store = &self.context.kv_store;
        let key = &self.global_table_key();

        match get_table_global_value(kv_store, key).await {
            Ok(Some(table_global_value)) => {
                // The metasrv crashed after metadata was created immediately.
                // Recovers table_route from kv.
                let table_id = table_global_value.table_id() as u64;
                let table_route_value =
                    get_table_route_value(kv_store, &table_route_key(table_id, key)).await?;

                let (peers, mut table_routes) =
                    fill_table_routes(vec![(table_global_value, table_route_value)])?;
                // TODO: assert table_routes.len()==0
                let table_route = TableRoute::try_from_raw(&peers, table_routes.pop().unwrap())
                    .context(error::ConvertProtoDataSnafu)?;

                self.creator.data.state = CreateTableState::DatanodeCreateTable(table_route);
            }
            Ok(None) => {
                let full_table_name = self.table_name();

                let TableName {
                    catalog_name,
                    schema_name,
                    table_name,
                } = full_table_name.clone();

                let ctx = SelectorContext::from_ctx(
                    catalog_name,
                    schema_name,
                    table_name,
                    self.context.selector_ctx.clone(),
                );

                let table_route = handle_create_table_metadata(
                    self.creator.data.cluster_id,
                    full_table_name.clone(),
                    self.creator.data.task.partitions.clone(),
                    self.creator.data.task.table_info.clone(),
                    ctx,
                    self.context.selector.clone(),
                    self.context.table_id_sequence.clone(),
                )
                .await?;

                self.creator.data.state = CreateTableState::DatanodeCreateTable(table_route);
            }
            Err(err) => return Err(err),
        }

        Ok(Status::executing(true))
    }

    async fn on_datanode_create_table(&mut self, table_route: TableRoute) -> Result<Status> {
        //FIXME: if datanode returns `Not Leader` error.

        let table_name = self.table_name();
        let clients = self.context.datanode_clients.clone();
        let leaders = table_route.find_leaders();
        let mut joins = Vec::with_capacity(leaders.len());

        for datanode in table_route.find_leaders() {
            let client = clients.get_client(&datanode).await;
            let client = Database::new(&table_name.catalog_name, &table_name.schema_name, client);

            let regions = table_route.find_leader_regions(&datanode);
            let mut create_expr_for_region = self.creator.data.task.create_table.clone();
            create_expr_for_region.region_numbers = regions;
            create_expr_for_region.create_if_not_exists = true;

            joins.push(common_runtime::spawn_bg(async move {
                client
                    .create(create_expr_for_region)
                    .await
                    .context(error::RequestDatanodeSnafu { peer: datanode })
            }));
        }

        let _ = join_all(joins)
            .await
            .into_iter()
            .map(|result| {
                result.map_err(|err| {
                    error::RetryLaterSnafu {
                        reason: format!(
                            "Failed to execute create table on datanode, source: {}",
                            err
                        ),
                    }
                    .build()
                })
            })
            .collect::<Result<Vec<_>>>()?;

        if let Some(notifier) = self.context.notifier.take() {
            if let Err(status) = notifier.send(ProcedureStatus::CreateTable(
                CreateTableProcedureStatus::with_table_id(table_route.table.id as u32),
            )) {
                warn!("Failed to notify upper: {:?}", status);
            }
        }

        Ok(Status::Done)
    }
}

#[async_trait]
impl Procedure for CreateTableProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        todo!()
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.creator.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let table_ref = &self.creator.data.table_ref();
        let key = common_catalog::format_full_table_name(
            table_ref.catalog,
            table_ref.schema,
            table_ref.table,
        );

        LockKey::single(key)
    }
}

pub struct TableCreator {
    data: CreateTableData,
}

impl TableCreator {
    pub fn new(cluster_id: u64, task: CreateTableTask) -> Self {
        Self {
            data: CreateTableData {
                state: CreateTableState::Prepare,
                cluster_id,
                task,
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum CreateTableState {
    /// Prepares to create the table
    Prepare,
    /// Creates metadata
    CreateMetadata,
    /// Datanode creates the table
    DatanodeCreateTable(TableRoute),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateTableData {
    state: CreateTableState,
    task: CreateTableTask,
    cluster_id: u64,
}

impl CreateTableData {
    fn table_ref(&self) -> TableReference<'_> {
        self.task.table_ref()
    }
}
