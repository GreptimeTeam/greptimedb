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

use api::v1::meta::TableRouteValue;
use async_trait::async_trait;
use catalog::helper::TableGlobalKey;
use client::Database;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_meta::key::TableRouteKey;
use common_meta::kv_backend::txn::{Compare, CompareOp, Txn, TxnOp};
use common_meta::rpc::ddl::CreateTableTask;
use common_meta::rpc::router::TableRoute;
use common_meta::table_name::TableName;
use common_procedure::error::{FromJsonSnafu, Result as ProcedureResult, ToJsonSnafu};
use common_procedure::{Context as ProcedureContext, LockKey, Procedure, Status};
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use table::engine::TableReference;
use table::metadata::TableId;

use super::utils::{handle_request_datanode_error, handle_retry_error};
use crate::ddl::DdlContext;
use crate::error::{self, Result};
use crate::service::router::create_table_global_value;
use crate::table_routes::get_table_global_value;

pub struct CreateTableProcedure {
    context: DdlContext,
    creator: TableCreator,
}

impl CreateTableProcedure {
    pub(crate) const TYPE_NAME: &'static str = "metasrv-procedure::CreateTable";

    pub(crate) fn new(
        cluster_id: u64,
        task: CreateTableTask,
        table_route: TableRoute,
        context: DdlContext,
    ) -> Self {
        Self {
            context,
            creator: TableCreator::new(cluster_id, task, table_route),
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
            catalog_name: table_ref.catalog.to_string(),
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

        self.creator.data.state = CreateTableState::DatanodeCreateTable;

        Ok(Status::executing(true))
    }

    /// registers the `TableRouteValue`,`TableGlobalValue`
    async fn register_metadata(&self) -> Result<()> {
        let _timer = common_telemetry::timer!(
            crate::metrics::METRIC_META_CREATE_TABLE_PROCEDURE_CREATE_META
        );
        let table_name = self.table_name();

        let table_id = self.creator.data.table_route.table.id as TableId;

        let table_route_key = TableRouteKey::with_table_name(table_id, &table_name.clone().into())
            .to_string()
            .into_bytes();

        let table_global_key = TableGlobalKey {
            catalog_name: table_name.catalog_name.clone(),
            schema_name: table_name.schema_name.clone(),
            table_name: table_name.table_name.clone(),
        }
        .to_string()
        .into_bytes();

        let (peers, table_route) = self
            .creator
            .data
            .table_route
            .clone()
            .try_into_raw()
            .context(error::ConvertProtoDataSnafu)?;

        let table_route_value = TableRouteValue {
            peers,
            table_route: Some(table_route),
        };

        let table_global_value = create_table_global_value(
            &table_route_value,
            self.creator.data.task.table_info.clone(),
        )?
        .as_bytes()
        .context(error::InvalidCatalogValueSnafu)?;

        let txn = Txn::new()
            .when(vec![
                Compare::with_not_exist_value(table_route_key.clone(), CompareOp::Equal),
                Compare::with_not_exist_value(table_global_key.clone(), CompareOp::Equal),
            ])
            .and_then(vec![
                TxnOp::Put(table_route_key, table_route_value.into()),
                TxnOp::Put(table_global_key, table_global_value),
            ]);

        let resp = self.context.kv_store.txn(txn).await?;

        ensure!(
            resp.succeeded,
            error::TxnSnafu {
                msg: "table_route_key or table_global_key exists"
            }
        );

        Ok(())
    }

    async fn on_create_metadata(&mut self) -> Result<Status> {
        let kv_store = &self.context.kv_store;
        let key = &self.global_table_key();

        match get_table_global_value(kv_store, key).await? {
            Some(table_global_value) => {
                // The metasrv crashed after metadata was created immediately.
                // Recovers table_route from kv.
                let table_id = table_global_value.table_id() as u64;

                let expected = self.creator.data.table_route.table.id;
                // If there is something like:
                // Create table A, Create table A(from another Fe, Somehow, Failed), Renames table A to B, Create table A(Recovered).
                // We must ensure the table_id isn't changed.
                ensure!(
                    table_id == expected,
                    error::TableIdChangedSnafu {
                        expected,
                        found: table_id
                    }
                );
            }
            None => {
                // registers metadata
                self.register_metadata().await?;
            }
        }

        Ok(Status::Done)
    }

    async fn on_datanode_create_table(&mut self) -> Result<Status> {
        let _timer = common_telemetry::timer!(
            crate::metrics::METRIC_META_CREATE_TABLE_PROCEDURE_CREATE_TABLE
        );
        let table_route = &self.creator.data.table_route;
        let table_name = self.table_name();
        let clients = self.context.datanode_clients.clone();
        let leaders = table_route.find_leaders();
        let mut joins = Vec::with_capacity(leaders.len());

        for datanode in leaders {
            let client = clients.get_client(&datanode).await;
            let client = Database::new(&table_name.catalog_name, &table_name.schema_name, client);

            let regions = table_route.find_leader_regions(&datanode);
            let mut create_expr_for_region = self.creator.data.task.create_table.clone();
            create_expr_for_region.region_numbers = regions;
            create_expr_for_region.table_id = Some(api::v1::TableId {
                id: table_route.table.id as u32,
            });

            joins.push(common_runtime::spawn_bg(async move {
                if let Err(err) = client.create(create_expr_for_region).await {
                    if err.status_code() != StatusCode::TableAlreadyExists {
                        return Err(handle_request_datanode_error(datanode)(err));
                    }
                }
                Ok(())
            }));
        }

        let _ = join_all(joins)
            .await
            .into_iter()
            .map(|e| e.context(error::JoinSnafu))
            .collect::<Result<Vec<_>>>()?;

        self.creator.data.state = CreateTableState::CreateMetadata;

        Ok(Status::executing(true))
    }
}

#[async_trait]
impl Procedure for CreateTableProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        match self.creator.data.state {
            CreateTableState::Prepare => self.on_prepare().await,
            CreateTableState::DatanodeCreateTable => self.on_datanode_create_table().await,
            CreateTableState::CreateMetadata => self.on_create_metadata().await,
        }
        .map_err(handle_retry_error)
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
    pub fn new(cluster_id: u64, task: CreateTableTask, table_route: TableRoute) -> Self {
        Self {
            data: CreateTableData {
                state: CreateTableState::Prepare,
                cluster_id,
                task,
                table_route,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum CreateTableState {
    /// Prepares to create the table
    Prepare,
    /// Datanode creates the table
    DatanodeCreateTable,
    /// Creates metadata
    CreateMetadata,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateTableData {
    state: CreateTableState,
    task: CreateTableTask,
    table_route: TableRoute,
    cluster_id: u64,
}

impl CreateTableData {
    fn table_ref(&self) -> TableReference<'_> {
        self.task.table_ref()
    }
}
