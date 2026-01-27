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

//! Batching mode engine

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use api::v1::flow::DirtyWindowRequests;
use catalog::CatalogManagerRef;
use common_error::ext::BoxedError;
use common_meta::ddl::create_flow::FlowType;
use common_meta::key::TableMetadataManagerRef;
use common_meta::key::flow::FlowMetadataManagerRef;
use common_meta::key::table_info::{TableInfoManager, TableInfoValue};
use common_runtime::JoinHandle;
use common_telemetry::tracing::warn;
use common_telemetry::{debug, info};
use common_time::TimeToLive;
use datafusion_common::tree_node::{TreeNodeRecursion, TreeNodeVisitor};
use datafusion_expr::LogicalPlan;
use datatypes::prelude::ConcreteDataType;
use query::QueryEngineRef;
use session::context::QueryContext;
use snafu::{OptionExt, ResultExt, ensure};
use sql::parsers::utils::is_tql;
use store_api::metric_engine_consts::is_metric_engine_internal_column;
use store_api::storage::{RegionId, TableId};
use table::table_reference::TableReference;
use tokio::sync::{RwLock, oneshot};

use crate::batching_mode::BatchingModeOptions;
use crate::batching_mode::frontend_client::FrontendClient;
use crate::batching_mode::task::{BatchingTask, TaskArgs};
use crate::batching_mode::time_window::{TimeWindowExpr, find_time_window_expr};
use crate::batching_mode::utils::sql_to_df_plan;
use crate::engine::FlowEngine;
use crate::error::{
    CreateFlowSnafu, DatafusionSnafu, ExternalSnafu, FlowAlreadyExistSnafu, FlowNotFoundSnafu,
    InvalidQuerySnafu, TableNotFoundMetaSnafu, UnexpectedSnafu, UnsupportedSnafu,
};
use crate::metrics::METRIC_FLOW_BATCHING_ENGINE_BULK_MARK_TIME_WINDOW;
use crate::{CreateFlowArgs, Error, FlowId, TableName};

/// Batching mode Engine, responsible for driving all the batching mode tasks
///
/// TODO(discord9): determine how to configure refresh rate
pub struct BatchingEngine {
    tasks: RwLock<BTreeMap<FlowId, BatchingTask>>,
    shutdown_txs: RwLock<BTreeMap<FlowId, oneshot::Sender<()>>>,
    /// frontend client for insert request
    pub(crate) frontend_client: Arc<FrontendClient>,
    flow_metadata_manager: FlowMetadataManagerRef,
    table_meta: TableMetadataManagerRef,
    catalog_manager: CatalogManagerRef,
    query_engine: QueryEngineRef,
    /// Batching mode options for control how batching mode query works
    ///
    pub(crate) batch_opts: Arc<BatchingModeOptions>,
}

impl BatchingEngine {
    pub fn new(
        frontend_client: Arc<FrontendClient>,
        query_engine: QueryEngineRef,
        flow_metadata_manager: FlowMetadataManagerRef,
        table_meta: TableMetadataManagerRef,
        catalog_manager: CatalogManagerRef,
        batch_opts: BatchingModeOptions,
    ) -> Self {
        Self {
            tasks: Default::default(),
            shutdown_txs: Default::default(),
            frontend_client,
            flow_metadata_manager,
            table_meta,
            catalog_manager,
            query_engine,
            batch_opts: Arc::new(batch_opts),
        }
    }

    pub async fn handle_mark_dirty_time_window(
        &self,
        reqs: DirtyWindowRequests,
    ) -> Result<(), Error> {
        let table_info_mgr = self.table_meta.table_info_manager();

        let mut group_by_table_id: HashMap<u32, Vec<_>> = HashMap::new();
        for r in reqs.requests {
            let tid = TableId::from(r.table_id);
            let entry = group_by_table_id.entry(tid).or_default();
            entry.extend(r.timestamps);
        }
        let tids = group_by_table_id.keys().cloned().collect::<Vec<TableId>>();
        let table_infos =
            table_info_mgr
                .batch_get(&tids)
                .await
                .with_context(|_| TableNotFoundMetaSnafu {
                    msg: format!("Failed to get table info for table ids: {:?}", tids),
                })?;

        let group_by_table_name = group_by_table_id
            .into_iter()
            .filter_map(|(id, timestamps)| {
                let table_name = table_infos.get(&id).map(|info| info.table_name());
                let Some(table_name) = table_name else {
                    warn!("Failed to get table infos for table id: {:?}", id);
                    return None;
                };
                let table_name = [
                    table_name.catalog_name,
                    table_name.schema_name,
                    table_name.table_name,
                ];
                let schema = &table_infos.get(&id).unwrap().table_info.meta.schema;
                let time_index_unit = schema.column_schemas()[schema.timestamp_index().unwrap()]
                    .data_type
                    .as_timestamp()
                    .unwrap()
                    .unit();
                Some((table_name, (timestamps, time_index_unit)))
            })
            .collect::<HashMap<_, _>>();

        let group_by_table_name = Arc::new(group_by_table_name);

        let mut handles = Vec::new();
        let tasks = self.tasks.read().await;

        for (_flow_id, task) in tasks.iter() {
            let src_table_names = &task.config.source_table_names;

            if src_table_names
                .iter()
                .all(|name| !group_by_table_name.contains_key(name))
            {
                continue;
            }

            let group_by_table_name = group_by_table_name.clone();
            let task = task.clone();

            let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                let src_table_names = &task.config.source_table_names;
                let mut all_dirty_windows = HashSet::new();
                let mut is_dirty = false;
                for src_table_name in src_table_names {
                    if let Some((timestamps, unit)) = group_by_table_name.get(src_table_name) {
                        let Some(expr) = &task.config.time_window_expr else {
                            is_dirty = true;
                            continue;
                        };
                        for timestamp in timestamps {
                            let align_start = expr
                                .eval(common_time::Timestamp::new(*timestamp, *unit))?
                                .0
                                .context(UnexpectedSnafu {
                                    reason: "Failed to eval start value",
                                })?;
                            all_dirty_windows.insert(align_start);
                        }
                    }
                }
                let mut state = task.state.write().unwrap();
                if is_dirty {
                    state.dirty_time_windows.set_dirty();
                }
                let flow_id_label = task.config.flow_id.to_string();
                for timestamp in all_dirty_windows {
                    state.dirty_time_windows.add_window(timestamp, None);
                }

                METRIC_FLOW_BATCHING_ENGINE_BULK_MARK_TIME_WINDOW
                    .with_label_values(&[&flow_id_label])
                    .set(state.dirty_time_windows.len() as f64);
                Ok(())
            });
            handles.push(handle);
        }
        drop(tasks);
        for handle in handles {
            match handle.await {
                Err(e) => {
                    warn!("Failed to handle inserts: {e}");
                }
                Ok(Ok(())) => (),
                Ok(Err(e)) => {
                    warn!("Failed to handle inserts: {e}");
                }
            }
        }

        Ok(())
    }

    pub async fn handle_inserts_inner(
        &self,
        request: api::v1::region::InsertRequests,
    ) -> Result<(), Error> {
        let table_info_mgr = self.table_meta.table_info_manager();
        let mut group_by_table_id: HashMap<TableId, Vec<api::v1::Rows>> = HashMap::new();

        for r in request.requests {
            let tid = RegionId::from(r.region_id).table_id();
            let entry = group_by_table_id.entry(tid).or_default();
            if let Some(rows) = r.rows {
                entry.push(rows);
            }
        }

        let tids = group_by_table_id.keys().cloned().collect::<Vec<TableId>>();
        let table_infos =
            table_info_mgr
                .batch_get(&tids)
                .await
                .with_context(|_| TableNotFoundMetaSnafu {
                    msg: format!("Failed to get table info for table ids: {:?}", tids),
                })?;

        let missing_tids = tids
            .iter()
            .filter(|id| !table_infos.contains_key(id))
            .collect::<Vec<_>>();
        if !missing_tids.is_empty() {
            warn!(
                "Failed to get all the table info for table ids, expected table ids: {:?}, those table doesn't exist: {:?}",
                tids, missing_tids
            );
        }

        let group_by_table_name = group_by_table_id
            .into_iter()
            .filter_map(|(id, rows)| {
                let table_name = table_infos.get(&id).map(|info| info.table_name());
                let Some(table_name) = table_name else {
                    warn!("Failed to get table infos for table id: {:?}", id);
                    return None;
                };
                let table_name = [
                    table_name.catalog_name,
                    table_name.schema_name,
                    table_name.table_name,
                ];
                Some((table_name, rows))
            })
            .collect::<HashMap<_, _>>();

        let group_by_table_name = Arc::new(group_by_table_name);

        let mut handles = Vec::new();
        let tasks = self.tasks.read().await;
        for (_flow_id, task) in tasks.iter() {
            let src_table_names = &task.config.source_table_names;

            if src_table_names
                .iter()
                .all(|name| !group_by_table_name.contains_key(name))
            {
                continue;
            }

            let group_by_table_name = group_by_table_name.clone();
            let task = task.clone();

            let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                let src_table_names = &task.config.source_table_names;

                let mut is_dirty = false;

                for src_table_name in src_table_names {
                    if let Some(entry) = group_by_table_name.get(src_table_name) {
                        let Some(expr) = &task.config.time_window_expr else {
                            is_dirty = true;
                            continue;
                        };
                        let involved_time_windows = expr.handle_rows(entry.clone()).await?;
                        let mut state = task.state.write().unwrap();
                        state
                            .dirty_time_windows
                            .add_lower_bounds(involved_time_windows.into_iter());
                    }
                }
                if is_dirty {
                    task.state.write().unwrap().dirty_time_windows.set_dirty();
                }

                Ok(())
            });
            handles.push(handle);
        }

        for handle in handles {
            match handle.await {
                Err(e) => {
                    warn!("Failed to handle inserts: {e}");
                }
                Ok(Ok(())) => (),
                Ok(Err(e)) => {
                    warn!("Failed to handle inserts: {e}");
                }
            }
        }
        drop(tasks);

        Ok(())
    }
}

async fn get_table_name(
    table_info: &TableInfoManager,
    table_id: &TableId,
) -> Result<TableName, Error> {
    get_table_info(table_info, table_id).await.map(|info| {
        let name = info.table_name();
        [name.catalog_name, name.schema_name, name.table_name]
    })
}

async fn get_table_info(
    table_info: &TableInfoManager,
    table_id: &TableId,
) -> Result<TableInfoValue, Error> {
    table_info
        .get(*table_id)
        .await
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?
        .with_context(|| UnexpectedSnafu {
            reason: format!("Table id = {:?}, couldn't found table name", table_id),
        })
        .map(|info| info.into_inner())
}

impl BatchingEngine {
    pub async fn create_flow_inner(&self, args: CreateFlowArgs) -> Result<Option<FlowId>, Error> {
        let CreateFlowArgs {
            flow_id,
            sink_table_name,
            source_table_ids,
            create_if_not_exists,
            or_replace,
            expire_after,
            eval_interval,
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

        let query_ctx = query_ctx.context({
            UnexpectedSnafu {
                reason: "Query context is None".to_string(),
            }
        })?;
        let query_ctx = Arc::new(query_ctx);
        let is_tql = is_tql(query_ctx.sql_dialect(), &sql)
            .map_err(BoxedError::new)
            .context(CreateFlowSnafu { sql: &sql })?;

        // optionally set a eval interval for the flow
        if eval_interval.is_none() && is_tql {
            InvalidQuerySnafu {
                reason: "TQL query requires EVAL INTERVAL to be set".to_string(),
            }
            .fail()?;
        }

        let flow_type = flow_options.get(FlowType::FLOW_TYPE_KEY);

        ensure!(
            match flow_type {
                None => true,
                Some(ty) if ty == FlowType::BATCHING => true,
                _ => false,
            },
            UnexpectedSnafu {
                reason: format!("Flow type is not batching nor None, got {flow_type:?}")
            }
        );

        let mut source_table_names = Vec::with_capacity(2);
        for src_id in source_table_ids {
            // also check table option to see if ttl!=instant
            let table_name = get_table_name(self.table_meta.table_info_manager(), &src_id).await?;
            let table_info = get_table_info(self.table_meta.table_info_manager(), &src_id).await?;
            ensure!(
                table_info.table_info.meta.options.ttl != Some(TimeToLive::Instant),
                UnsupportedSnafu {
                    reason: format!(
                        "Source table `{}`(id={}) has instant TTL, Instant TTL is not supported under batching mode. Consider using a TTL longer than flush interval",
                        table_name.join("."),
                        src_id
                    ),
                }
            );

            source_table_names.push(table_name);
        }

        let (tx, rx) = oneshot::channel();

        let plan = sql_to_df_plan(query_ctx.clone(), self.query_engine.clone(), &sql, true).await?;

        if is_tql {
            self.check_is_tql_table(&plan, &query_ctx).await?;
        }

        let phy_expr = if !is_tql {
            let (column_name, time_window_expr, _, df_schema) = find_time_window_expr(
                &plan,
                self.query_engine.engine_state().catalog_manager().clone(),
                query_ctx.clone(),
            )
            .await?;
            time_window_expr
                .map(|expr| {
                    TimeWindowExpr::from_expr(
                        &expr,
                        &column_name,
                        &df_schema,
                        &self.query_engine.engine_state().session_state(),
                    )
                })
                .transpose()?
        } else {
            // tql control by `EVAL INTERVAL`, no need to find time window expr
            None
        };

        debug!(
            "Flow id={}, found time window expr={}",
            flow_id,
            phy_expr
                .as_ref()
                .map(|phy_expr| phy_expr.to_string())
                .unwrap_or("None".to_string())
        );

        let task_args = TaskArgs {
            flow_id,
            query: &sql,
            plan,
            time_window_expr: phy_expr,
            expire_after,
            sink_table_name,
            source_table_names,
            query_ctx,
            catalog_manager: self.catalog_manager.clone(),
            shutdown_rx: rx,
            batch_opts: self.batch_opts.clone(),
            flow_eval_interval: eval_interval.map(|secs| Duration::from_secs(secs as u64)),
        };

        let task = BatchingTask::try_new(task_args)?;

        let task_inner = task.clone();
        let engine = self.query_engine.clone();
        let frontend = self.frontend_client.clone();

        // check execute once first to detect any error early
        task.check_or_create_sink_table(&engine, &frontend).await?;

        // TODO(discord9): use time wheel or what for better
        let handle = common_runtime::spawn_global(async move {
            task_inner.start_executing_loop(engine, frontend).await;
        });
        task.state.write().unwrap().task_handle = Some(handle);

        // only replace here not earlier because we want the old one intact if something went wrong before this line
        let replaced_old_task_opt = self.tasks.write().await.insert(flow_id, task);
        drop(replaced_old_task_opt);

        self.shutdown_txs.write().await.insert(flow_id, tx);

        Ok(Some(flow_id))
    }

    async fn check_is_tql_table(
        &self,
        query: &LogicalPlan,
        query_ctx: &QueryContext,
    ) -> Result<(), Error> {
        struct CollectTableRef {
            table_refs: HashSet<datafusion_common::TableReference>,
        }

        impl TreeNodeVisitor<'_> for CollectTableRef {
            type Node = LogicalPlan;
            fn f_down(
                &mut self,
                node: &Self::Node,
            ) -> datafusion_common::Result<TreeNodeRecursion> {
                if let LogicalPlan::TableScan(scan) = node {
                    self.table_refs.insert(scan.table_name.clone());
                }
                Ok(TreeNodeRecursion::Continue)
            }
        }
        let mut table_refs = CollectTableRef {
            table_refs: HashSet::new(),
        };
        query
            .visit_with_subqueries(&mut table_refs)
            .context(DatafusionSnafu {
                context: "Checking if all source tables are TQL tables",
            })?;

        let default_catalog = query_ctx.current_catalog();
        let default_schema = query_ctx.current_schema();
        let default_schema = &default_schema;

        for table_ref in table_refs.table_refs {
            let table_ref = match &table_ref {
                datafusion_common::TableReference::Bare { table } => {
                    TableReference::full(default_catalog, default_schema, table)
                }
                datafusion_common::TableReference::Partial { schema, table } => {
                    TableReference::full(default_catalog, schema, table)
                }
                datafusion_common::TableReference::Full {
                    catalog,
                    schema,
                    table,
                } => TableReference::full(catalog, schema, table),
            };

            let table_id = self
                .table_meta
                .table_name_manager()
                .get(table_ref.into())
                .await
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?
                .with_context(|| UnexpectedSnafu {
                    reason: format!("Failed to get table id for table: {}", table_ref),
                })?
                .table_id();
            let table_info =
                get_table_info(self.table_meta.table_info_manager(), &table_id).await?;
            // first check if it's only one f64 value column
            let value_cols = table_info
                .table_info
                .meta
                .schema
                .column_schemas()
                .iter()
                .filter(|col| col.data_type == ConcreteDataType::float64_datatype())
                .collect::<Vec<_>>();
            ensure!(
                value_cols.len() == 1,
                InvalidQuerySnafu {
                    reason: format!(
                        "TQL query only supports one f64 value column, table `{}`(id={}) has {} f64 value columns, columns are: {:?}",
                        table_ref,
                        table_id,
                        value_cols.len(),
                        value_cols
                    ),
                }
            );
            // TODO(discord9): do need to check rest columns is string and is tag column?
            let pk_idxs = table_info
                .table_info
                .meta
                .primary_key_indices
                .iter()
                .collect::<HashSet<_>>();

            for (idx, col) in table_info
                .table_info
                .meta
                .schema
                .column_schemas()
                .iter()
                .enumerate()
            {
                if is_metric_engine_internal_column(&col.name) {
                    continue;
                }
                // three cases:
                // 1. val column
                // 2. timestamp column
                // 3. tag column (string)

                let is_pk: bool = pk_idxs.contains(&&idx);

                ensure!(
                    col.data_type == ConcreteDataType::float64_datatype()
                        || col.data_type.is_timestamp()
                        || (col.data_type == ConcreteDataType::string_datatype() && is_pk),
                    InvalidQuerySnafu {
                        reason: format!(
                            "TQL query only supports f64 value column, timestamp column and string tag columns, table `{}`(id={}) has column `{}` with type {:?} which is not supported",
                            table_ref, table_id, col.name, col.data_type
                        ),
                    }
                );
            }
        }
        Ok(())
    }

    pub async fn remove_flow_inner(&self, flow_id: FlowId) -> Result<(), Error> {
        if self.tasks.write().await.remove(&flow_id).is_none() {
            warn!("Flow {flow_id} not found in tasks");
            FlowNotFoundSnafu { id: flow_id }.fail()?;
        }
        let Some(tx) = self.shutdown_txs.write().await.remove(&flow_id) else {
            UnexpectedSnafu {
                reason: format!("Can't found shutdown tx for flow {flow_id}"),
            }
            .fail()?
        };
        if tx.send(()).is_err() {
            warn!(
                "Fail to shutdown flow {flow_id} due to receiver already dropped, maybe flow {flow_id} is already dropped?"
            )
        }
        Ok(())
    }

    /// Only flush the dirty windows of the flow task with given flow id, by running the query on it.
    /// As flush the whole time range is usually prohibitively expensive.
    pub async fn flush_flow_inner(&self, flow_id: FlowId) -> Result<usize, Error> {
        debug!("Try flush flow {flow_id}");
        // need to wait a bit to ensure previous mirror insert is handled
        // this is only useful for the case when we are flushing the flow right after inserting data into it
        // TODO(discord9): find a better way to ensure the data is ready, maybe inform flownode from frontend?
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let task = self.tasks.read().await.get(&flow_id).cloned();
        let task = task.with_context(|| FlowNotFoundSnafu { id: flow_id })?;

        let time_window_size = task
            .config
            .time_window_expr
            .as_ref()
            .and_then(|expr| *expr.time_window_size());

        let cur_dirty_window_cnt = time_window_size.map(|time_window_size| {
            task.state
                .read()
                .unwrap()
                .dirty_time_windows
                .effective_count(&time_window_size)
        });

        let res = task
            .gen_exec_once(
                &self.query_engine,
                &self.frontend_client,
                cur_dirty_window_cnt,
            )
            .await?;

        let affected_rows = res.map(|(r, _)| r).unwrap_or_default() as usize;
        debug!(
            "Successfully flush flow {flow_id}, affected rows={}",
            affected_rows
        );
        Ok(affected_rows)
    }

    /// Determine if the batching mode flow task exists with given flow id
    pub async fn flow_exist_inner(&self, flow_id: FlowId) -> bool {
        self.tasks.read().await.contains_key(&flow_id)
    }
}

impl FlowEngine for BatchingEngine {
    async fn create_flow(&self, args: CreateFlowArgs) -> Result<Option<FlowId>, Error> {
        self.create_flow_inner(args).await
    }
    async fn remove_flow(&self, flow_id: FlowId) -> Result<(), Error> {
        self.remove_flow_inner(flow_id).await
    }
    async fn flush_flow(&self, flow_id: FlowId) -> Result<usize, Error> {
        self.flush_flow_inner(flow_id).await
    }
    async fn flow_exist(&self, flow_id: FlowId) -> Result<bool, Error> {
        Ok(self.flow_exist_inner(flow_id).await)
    }
    async fn list_flows(&self) -> Result<impl IntoIterator<Item = FlowId>, Error> {
        Ok(self.tasks.read().await.keys().cloned().collect::<Vec<_>>())
    }
    async fn handle_flow_inserts(
        &self,
        request: api::v1::region::InsertRequests,
    ) -> Result<(), Error> {
        self.handle_inserts_inner(request).await
    }
    async fn handle_mark_window_dirty(
        &self,
        req: api::v1::flow::DirtyWindowRequests,
    ) -> Result<(), Error> {
        self.handle_mark_dirty_time_window(req).await
    }
}
