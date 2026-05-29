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
use common_meta::key::flow::flow_state::FlowStat;
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
use crate::engine::{FlowEngine, FlowStatProvider};
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
    runtime: RwLock<FlowRuntimeRegistry>,
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

#[derive(Default)]
struct FlowRuntimeRegistry {
    tasks: BTreeMap<FlowId, BatchingTask>,
    shutdown_txs: BTreeMap<FlowId, oneshot::Sender<()>>,
}

impl FlowRuntimeRegistry {
    fn insert(
        &mut self,
        flow_id: FlowId,
        task: BatchingTask,
        shutdown_tx: oneshot::Sender<()>,
    ) -> (Option<BatchingTask>, Option<oneshot::Sender<()>>) {
        (
            self.tasks.insert(flow_id, task),
            self.shutdown_txs.insert(flow_id, shutdown_tx),
        )
    }

    fn remove(&mut self, flow_id: FlowId) -> Option<(BatchingTask, Option<oneshot::Sender<()>>)> {
        let task = self.tasks.remove(&flow_id)?;
        let shutdown_tx = self.shutdown_txs.remove(&flow_id);
        Some((task, shutdown_tx))
    }

    fn remove_if_current(
        &mut self,
        flow_id: FlowId,
        task: &BatchingTask,
    ) -> (Option<BatchingTask>, Option<oneshot::Sender<()>>) {
        if self
            .tasks
            .get(&flow_id)
            .is_some_and(|current| Arc::ptr_eq(&current.state, &task.state))
        {
            let Some((removed_task, removed_shutdown_tx)) = self.remove(flow_id) else {
                return (None, None);
            };
            (Some(removed_task), removed_shutdown_tx)
        } else {
            (None, None)
        }
    }
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
            runtime: Default::default(),
            frontend_client,
            flow_metadata_manager,
            table_meta,
            catalog_manager,
            query_engine,
            batch_opts: Arc::new(batch_opts),
        }
    }

    /// Returns last execution timestamps (millisecond) for all batching flows.
    pub async fn get_last_exec_time_map(&self) -> BTreeMap<FlowId, i64> {
        let runtime = self.runtime.read().await;
        runtime
            .tasks
            .iter()
            .filter_map(|(flow_id, task)| {
                task.last_execution_time_millis()
                    .map(|timestamp| (*flow_id, timestamp))
            })
            .collect()
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

        let tasks = self
            .runtime
            .read()
            .await
            .tasks
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut handles = Vec::new();

        for task in tasks {
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

        let tasks = self
            .runtime
            .read()
            .await
            .tasks
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut handles = Vec::new();
        for task in tasks {
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
        Ok(())
    }
}

impl FlowStatProvider for BatchingEngine {
    async fn flow_stat(&self) -> FlowStat {
        FlowStat {
            state_size: BTreeMap::new(),
            last_exec_time_map: self
                .get_last_exec_time_map()
                .await
                .into_iter()
                .map(|(flow_id, timestamp)| (flow_id as u32, timestamp))
                .collect(),
        }
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
            let is_exist = self.runtime.read().await.tasks.contains_key(&flow_id);
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

        let (start_tx, start_rx) = oneshot::channel();

        // TODO(discord9): use time wheel or what for better
        let handle = common_runtime::spawn_global(async move {
            if start_rx.await.is_ok() {
                task_inner.start_executing_loop(engine, frontend).await;
            }
        });
        task.state.write().unwrap().task_handle = Some(handle);
        let task_for_rollback = task.clone();

        // Only replace here, not earlier, because we want the old one intact if
        // something went wrong before this line. Keep the task and shutdown
        // sender in one registry lock so create/remove can't observe one
        // without the other.
        let (replaced_old_task_opt, replaced_old_shutdown_tx) = {
            let mut runtime = self.runtime.write().await;

            let is_exist = runtime.tasks.contains_key(&flow_id);
            match (create_if_not_exists, or_replace, is_exist) {
                (_, true, true) => {
                    info!(
                        "Replacing flow with id={} after final registry check",
                        flow_id
                    );
                }
                (false, false, true) => {
                    abort_flow_task(flow_id, Some(task), "unregistered");
                    return FlowAlreadyExistSnafu { id: flow_id }.fail();
                }
                (true, false, true) => {
                    info!(
                        "Flow with id={} already exists at final registry check, do nothing",
                        flow_id
                    );
                    abort_flow_task(flow_id, Some(task), "unregistered");
                    return Ok(None);
                }
                (_, _, false) => (),
            }

            runtime.insert(flow_id, task, tx)
        };

        notify_flow_shutdown(flow_id, replaced_old_shutdown_tx, "replaced");
        abort_flow_task(flow_id, replaced_old_task_opt, "replaced");
        if start_tx.send(()).is_err() {
            self.rollback_flow_runtime_if_current(flow_id, &task_for_rollback)
                .await;
            UnexpectedSnafu {
                reason: format!("Failed to start flow {flow_id} due to task already dropped"),
            }
            .fail()?;
        }

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
        let (task, shutdown_tx) = {
            let mut runtime = self.runtime.write().await;
            let Some((task, shutdown_tx)) = runtime.remove(flow_id) else {
                warn!("Flow {flow_id} not found in tasks");
                FlowNotFoundSnafu { id: flow_id }.fail()?
            };
            (task, shutdown_tx)
        };

        let had_shutdown_tx = notify_flow_shutdown(flow_id, shutdown_tx, "removed");
        abort_flow_task(flow_id, Some(task), "removed");

        if !had_shutdown_tx {
            UnexpectedSnafu {
                reason: format!("Can't found shutdown tx for flow {flow_id}"),
            }
            .fail()?
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
        let task = self.runtime.read().await.tasks.get(&flow_id).cloned();
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

        let affected_rows = res.map(|(r, _)| r).unwrap_or_default();
        debug!(
            "Successfully flush flow {flow_id}, affected rows={}",
            affected_rows
        );
        Ok(affected_rows)
    }

    /// Determine if the batching mode flow task exists with given flow id
    pub async fn flow_exist_inner(&self, flow_id: FlowId) -> bool {
        self.runtime.read().await.tasks.contains_key(&flow_id)
    }

    async fn rollback_flow_runtime_if_current(&self, flow_id: FlowId, task: &BatchingTask) {
        let (removed_task, removed_shutdown_tx) = {
            let mut runtime = self.runtime.write().await;
            runtime.remove_if_current(flow_id, task)
        };

        notify_flow_shutdown(flow_id, removed_shutdown_tx, "rolled back");
        abort_flow_task(flow_id, removed_task, "rolled back");
    }
}

fn notify_flow_shutdown(flow_id: FlowId, tx: Option<oneshot::Sender<()>>, action: &str) -> bool {
    let Some(tx) = tx else {
        return false;
    };

    if tx.send(()).is_err() {
        warn!(
            "Fail to shutdown {action} flow {flow_id} due to receiver already dropped, maybe flow {flow_id} is already dropped?"
        );
    }

    true
}

fn abort_flow_task(flow_id: FlowId, task: Option<BatchingTask>, action: &str) -> bool {
    let Some(task) = task else {
        return false;
    };

    if let Some(handle) = task.state.write().unwrap().task_handle.take() {
        handle.abort();
        debug!("Aborted {action} flow task {flow_id}");
        return true;
    }

    false
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
        Ok(self
            .runtime
            .read()
            .await
            .tasks
            .keys()
            .cloned()
            .collect::<Vec<_>>())
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

#[cfg(test)]
mod tests {
    use catalog::memory::new_memory_catalog_manager;
    use common_meta::key::TableMetadataManager;
    use common_meta::key::flow::FlowMetadataManager;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use query::options::QueryOptions;
    use session::context::QueryContext;

    use super::*;
    use crate::test_utils::create_test_query_engine;

    struct DropNotify(Option<oneshot::Sender<()>>);

    impl Drop for DropNotify {
        fn drop(&mut self) {
            if let Some(tx) = self.0.take() {
                let _ = tx.send(());
            }
        }
    }

    async fn new_test_engine() -> BatchingEngine {
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let table_meta = Arc::new(TableMetadataManager::new(kv_backend.clone()));
        table_meta.init().await.unwrap();
        let flow_meta = Arc::new(FlowMetadataManager::new(kv_backend));
        let catalog_manager = new_memory_catalog_manager().unwrap();
        let query_engine = create_test_query_engine();
        let (frontend_client, _handler) =
            FrontendClient::from_empty_grpc_handler(QueryOptions::default());

        BatchingEngine::new(
            Arc::new(frontend_client),
            query_engine,
            flow_meta,
            table_meta,
            catalog_manager,
            BatchingModeOptions::default(),
        )
    }

    async fn new_test_task(flow_id: FlowId) -> (BatchingTask, oneshot::Sender<()>) {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        let plan = sql_to_df_plan(
            ctx.clone(),
            query_engine.clone(),
            "SELECT number, ts FROM numbers_with_ts",
            true,
        )
        .await
        .unwrap();
        let (tx, rx) = oneshot::channel();

        let task = BatchingTask::try_new(TaskArgs {
            flow_id,
            query: "SELECT number, ts FROM numbers_with_ts",
            plan,
            time_window_expr: None,
            expire_after: None,
            sink_table_name: [
                "greptime".to_string(),
                "public".to_string(),
                "sink".to_string(),
            ],
            source_table_names: vec![[
                "greptime".to_string(),
                "public".to_string(),
                "numbers_with_ts".to_string(),
            ]],
            query_ctx: ctx,
            catalog_manager: query_engine.engine_state().catalog_manager().clone(),
            shutdown_rx: rx,
            batch_opts: Arc::new(BatchingModeOptions::default()),
            flow_eval_interval: None,
        })
        .unwrap();

        (task, tx)
    }

    async fn install_abort_observed_handle(task: &BatchingTask) -> oneshot::Receiver<()> {
        let (drop_tx, drop_rx) = oneshot::channel();
        let (entered_tx, entered_rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            let _guard = DropNotify(Some(drop_tx));
            let _ = entered_tx.send(());
            std::future::pending::<()>().await;
        });
        task.state.write().unwrap().task_handle = Some(handle);
        tokio::time::timeout(Duration::from_secs(1), entered_rx)
            .await
            .expect("test task handle should start")
            .expect("test task handle should report start");
        drop_rx
    }

    #[tokio::test]
    async fn test_notify_flow_shutdown_sends_signal() {
        let (tx, rx) = oneshot::channel();

        assert!(notify_flow_shutdown(42, Some(tx), "test"));

        rx.await.expect("replaced flow should receive shutdown");
    }

    #[test]
    fn test_notify_flow_shutdown_accepts_missing_sender() {
        assert!(!notify_flow_shutdown(42, None, "test"));
    }

    #[tokio::test]
    async fn test_abort_flow_task_aborts_handle() {
        let (task, _shutdown_tx) = new_test_task(42).await;
        let drop_rx = install_abort_observed_handle(&task).await;

        assert!(abort_flow_task(42, Some(task), "test"));

        tokio::time::timeout(Duration::from_secs(1), drop_rx)
            .await
            .expect("aborted task should be dropped")
            .expect("drop notifier should fire");
    }

    #[tokio::test]
    async fn test_remove_flow_inner_aborts_registered_task() {
        let engine = new_test_engine().await;
        let (task, shutdown_tx) = new_test_task(42).await;
        let drop_rx = install_abort_observed_handle(&task).await;

        engine.runtime.write().await.insert(42, task, shutdown_tx);

        engine.remove_flow_inner(42).await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), drop_rx)
            .await
            .expect("removed task should be dropped")
            .expect("drop notifier should fire");
        assert!(!engine.flow_exist_inner(42).await);
        assert!(!engine.runtime.read().await.shutdown_txs.contains_key(&42));
    }

    #[tokio::test]
    async fn test_or_replace_flow_runtime_replaces_old_handles_and_keeps_new_task() {
        let engine = new_test_engine().await;
        let (old_task, old_shutdown_tx) = new_test_task(42).await;
        let old_task_identity = old_task.clone();
        let old_drop_rx = install_abort_observed_handle(&old_task).await;
        let (new_task, new_shutdown_tx) = new_test_task(42).await;
        let new_task_identity = new_task.clone();

        engine
            .runtime
            .write()
            .await
            .insert(42, old_task, old_shutdown_tx);
        let (replaced_old_task, replaced_old_shutdown_tx) =
            engine
                .runtime
                .write()
                .await
                .insert(42, new_task, new_shutdown_tx);

        let replaced_old_task = replaced_old_task.expect("old task should be returned");
        assert!(Arc::ptr_eq(
            &replaced_old_task.state,
            &old_task_identity.state
        ));
        assert!(notify_flow_shutdown(
            42,
            replaced_old_shutdown_tx,
            "replaced"
        ));
        old_task_identity
            .state
            .write()
            .unwrap()
            .shutdown_rx
            .try_recv()
            .expect("old shutdown receiver should receive signal");
        assert!(abort_flow_task(42, Some(replaced_old_task), "replaced"));

        tokio::time::timeout(Duration::from_secs(1), old_drop_rx)
            .await
            .expect("replaced task should be dropped")
            .expect("drop notifier should fire");

        let runtime = engine.runtime.read().await;
        assert_eq!(1, runtime.tasks.len());
        assert_eq!(1, runtime.shutdown_txs.len());
        let registered_task = runtime.tasks.get(&42).expect("new task should remain");
        assert!(Arc::ptr_eq(
            &registered_task.state,
            &new_task_identity.state
        ));
        assert!(runtime.shutdown_txs.contains_key(&42));
        assert!(matches!(
            new_task_identity
                .state
                .write()
                .unwrap()
                .shutdown_rx
                .try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn test_rollback_flow_runtime_if_current_removes_matching_task_only() {
        let engine = new_test_engine().await;
        let (old_task, _old_shutdown_tx) = new_test_task(42).await;
        let (current_task, current_shutdown_tx) = new_test_task(42).await;
        let current_task_identity = current_task.clone();

        engine
            .runtime
            .write()
            .await
            .insert(42, current_task, current_shutdown_tx);

        engine.rollback_flow_runtime_if_current(42, &old_task).await;

        let registered_task = engine.runtime.read().await.tasks.get(&42).cloned().unwrap();
        assert!(Arc::ptr_eq(
            &registered_task.state,
            &current_task_identity.state
        ));
        assert!(engine.runtime.read().await.shutdown_txs.contains_key(&42));

        engine
            .rollback_flow_runtime_if_current(42, &current_task_identity)
            .await;
        assert!(!engine.flow_exist_inner(42).await);
        assert!(!engine.runtime.read().await.shutdown_txs.contains_key(&42));
    }
}
