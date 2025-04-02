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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use api::v1::flow::FlowResponse;
use common_error::ext::BoxedError;
use common_meta::ddl::create_flow::FlowType;
use common_meta::key::flow::FlowMetadataManagerRef;
use common_meta::key::table_info::TableInfoManager;
use common_meta::key::TableMetadataManagerRef;
use common_telemetry::info;
use common_telemetry::tracing::warn;
use query::QueryEngineRef;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::RegionId;
use table::metadata::TableId;
use tokio::sync::{oneshot, RwLock};

use super::frontend_client::FrontendClient;
use crate::adapter::{CreateFlowArgs, FlowId, TableName};
use crate::batching_mode::task::BatchingTask;
use crate::batching_mode::time_window::{find_time_window_expr, TimeWindowExpr};
use crate::batching_mode::utils::sql_to_df_plan;
use crate::error::{ExternalSnafu, FlowAlreadyExistSnafu, UnexpectedSnafu};
use crate::Error;

/// Batching mode Engine, responsible for driving all the batching mode tasks
///
/// TODO(discord9): determine how to configure refresh rate
pub struct BatchingEngine {
    tasks: RwLock<BTreeMap<FlowId, BatchingTask>>,
    shutdown_txs: RwLock<BTreeMap<FlowId, oneshot::Sender<()>>>,
    frontend_client: Arc<FrontendClient>,
    flow_metadata_manager: FlowMetadataManagerRef,
    table_meta: TableMetadataManagerRef,
    query_engine: QueryEngineRef,
}

impl BatchingEngine {
    pub fn new(
        frontend_client: Arc<FrontendClient>,
        query_engine: QueryEngineRef,
        flow_metadata_manager: FlowMetadataManagerRef,
        table_meta: TableMetadataManagerRef,
    ) -> Self {
        Self {
            tasks: Default::default(),
            shutdown_txs: Default::default(),
            frontend_client,
            flow_metadata_manager,
            table_meta,
            query_engine,
        }
    }

    pub async fn handle_inserts(
        &self,
        request: api::v1::region::InsertRequests,
    ) -> Result<FlowResponse, Error> {
        let table_info_mgr = self.table_meta.table_info_manager();
        let mut group_by_table_name: HashMap<TableName, Vec<api::v1::Rows>> = HashMap::new();
        for r in request.requests {
            let tid = RegionId::from(r.region_id).table_id();
            let name = get_table_name(table_info_mgr, &tid).await?;
            let entry = group_by_table_name.entry(name).or_default();
            if let Some(rows) = r.rows {
                entry.push(rows);
            }
        }

        // TODO(discord9): not use one lock for all tasks
        for (_flow_id, task) in self.tasks.read().await.iter() {
            let src_table_names = &task.source_table_names;

            for src_table_name in src_table_names {
                if let Some(entry) = group_by_table_name.get(src_table_name) {
                    let Some(expr) = &task.time_window_expr else {
                        continue;
                    };
                    let involved_time_windows = expr.handle_rows(entry.clone()).await?;
                    let mut state = task.state.write().await;
                    state
                        .dirty_time_windows
                        .add_lower_bounds(involved_time_windows.into_iter());
                }
            }
        }

        Ok(Default::default())
    }
}

async fn get_table_name(
    table_info: &TableInfoManager,
    table_id: &TableId,
) -> Result<TableName, Error> {
    table_info
        .get(*table_id)
        .await
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?
        .with_context(|| UnexpectedSnafu {
            reason: format!("Table id = {:?}, couldn't found table name", table_id),
        })
        .map(|name| name.table_name())
        .map(|name| [name.catalog_name, name.schema_name, name.table_name])
}

impl BatchingEngine {
    pub async fn create_flow(&self, args: CreateFlowArgs) -> Result<Option<FlowId>, Error> {
        let CreateFlowArgs {
            flow_id,
            sink_table_name,
            source_table_ids,
            create_if_not_exists,
            or_replace,
            expire_after,
            comment: _,
            sql,
            flow_options,
            query_ctx,
        } = args;

        // or replace logic
        {
            let is_exist = self.tasks.read().await.contains_key(&flow_id);
            match (create_if_not_exists, or_replace, is_exist) {
                // if replace, ignore that old flow exists
                (_, true, true) => {
                    info!("Replacing flow with id={}", flow_id);
                }
                (false, false, true) => FlowAlreadyExistSnafu { id: flow_id }.fail()?,
                // already exists, and not replace, return None
                (true, false, true) => {
                    info!("Flow with id={} already exists, do nothing", flow_id);
                    return Ok(None);
                }

                // continue as normal
                (_, _, false) => (),
            }
        }

        let flow_type = flow_options.get(FlowType::FLOW_TYPE_KEY);

        ensure!(
            matches!(flow_type, Some(ty) if ty == "batching"),
            UnexpectedSnafu {
                reason: format!("Flow type is not batching nor None, got {flow_type:?}")
            }
        );

        let Some(query_ctx) = query_ctx else {
            UnexpectedSnafu {
                reason: "Query context is None".to_string(),
            }
            .fail()?
        };
        let query_ctx = Arc::new(query_ctx);
        let mut source_table_names = Vec::new();
        for src_id in source_table_ids {
            let table_name = get_table_name(self.table_meta.table_info_manager(), &src_id).await?;
            source_table_names.push(table_name);
        }

        let (tx, rx) = oneshot::channel();

        let plan = sql_to_df_plan(query_ctx.clone(), self.query_engine.clone(), &sql, true).await?;
        let (column_name, time_window_expr, _, df_schema) = find_time_window_expr(
            &plan,
            self.query_engine.engine_state().catalog_manager().clone(),
            query_ctx.clone(),
        )
        .await?;

        let phy_expr = time_window_expr
            .map(|expr| {
                TimeWindowExpr::from_expr(
                    &expr,
                    &column_name,
                    &df_schema,
                    &self.query_engine.engine_state().session_state(),
                )
            })
            .transpose()?;

        info!("Flow id={}, found time window expr={:?}", flow_id, phy_expr);

        let task = BatchingTask::new(
            flow_id,
            &sql,
            plan,
            phy_expr,
            expire_after,
            sink_table_name,
            source_table_names,
            query_ctx,
            self.table_meta.clone(),
            rx,
        );

        let task_inner = task.clone();
        let engine = self.query_engine.clone();
        let frontend = self.frontend_client.clone();

        // check execute once first to detect any error early
        task.check_execute(&engine, &frontend).await?;

        // TODO(discord9): also save handle & use time wheel or what for better
        let _handle = common_runtime::spawn_global(async move {
            task_inner.start_executing_loop(engine, frontend).await;
        });

        // only replace here not earlier because we want the old one intact if something went wrong before this line
        let replaced_old_task_opt = self.tasks.write().await.insert(flow_id, task);
        drop(replaced_old_task_opt);

        self.shutdown_txs.write().await.insert(flow_id, tx);

        Ok(Some(flow_id))
    }

    pub async fn remove_flow(&self, flow_id: FlowId) -> Result<(), Error> {
        if self.tasks.write().await.remove(&flow_id).is_none() {
            warn!("Flow {flow_id} not found in tasks")
        }
        let Some(tx) = self.shutdown_txs.write().await.remove(&flow_id) else {
            UnexpectedSnafu {
                reason: format!("Can't found shutdown tx for flow {flow_id}"),
            }
            .fail()?
        };
        if tx.send(()).is_err() {
            warn!("Fail to shutdown flow {flow_id} due to receiver already dropped, maybe flow {flow_id} is already dropped?")
        }
        Ok(())
    }

    pub async fn flush_flow(&self, flow_id: FlowId) -> Result<(), Error> {
        let task = self.tasks.read().await.get(&flow_id).cloned();
        let task = task.with_context(|| UnexpectedSnafu {
            reason: format!("Can't found task for flow {flow_id}"),
        })?;

        task.gen_exec_once(&self.query_engine, &self.frontend_client)
            .await?;
        Ok(())
    }

    /// Determine if the batching mode flow task exists with given flow id
    pub async fn flow_exist(&self, flow_id: FlowId) -> bool {
        self.tasks.read().await.contains_key(&flow_id)
    }
}
