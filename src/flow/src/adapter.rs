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

//! for getting data from source and sending results to sink
//! and communicating with other parts of the database
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::Arc;

use common_meta::key::table_info::TableInfoManager;
use common_meta::key::table_name::TableNameManager;
use hydroflow::scheduled::graph::Hydroflow;
use minstant::Anchor;
use prost::bytes::buf;
use query::QueryEngine;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionId;
use table::metadata::TableId;
use tokio::sync::broadcast;
use tokio::task::LocalSet;

use crate::adapter::error::{EvalSnafu, TableNotFoundMetaSnafu, TableNotFoundSnafu};
use crate::compute::{Context, DataflowState, ErrCollector};
use crate::expr::error::InternalSnafu;
use crate::expr::GlobalId;
use crate::plan::{Plan, TypedPlan};
use crate::repr::{self, DiffRow, RelationType, Row, BOARDCAST_CAP};
use crate::transform::sql_to_flow_plan;

pub(crate) mod error;

use error::Error;

// TODO: refactor common types for flow to a separate module
pub type TaskId = u64;
pub type TableName = Vec<String>;

/// FlowNodeManager manages the state of all tasks in the flow node, which should be run on the same thread
///
/// The choice of timestamp is just using current system timestamp for now
pub struct FlowNodeManager<'subgraph> {
    /// The state of all tasks in the flow node
    /// This is also the only field that's not `Send` in the struct
    pub task_states: BTreeMap<TaskId, ActiveDataflowState<'subgraph>>,

    // TODO: catalog/tableinfo manager for query schema and translate sql to plan
    query_engine: Arc<dyn QueryEngine>,
    srv_map: TableIdNameMapper,
    /// contains mapping from table name to global id, and table schema
    worker_context: FlowWorkerContext,
    tick_manager: FlowTickManager,
}

#[test]
fn check() {
    fn is_send<T: Send>() {}
    is_send::<FlowWorkerContext>();
}

/// mapping of table name <-> table id should be query from tableinfo manager
pub struct TableIdNameMapper {
    /// for query `TableId -> TableName` mapping
    table_info_manager: TableInfoManager,
}

impl TableIdNameMapper {
    /// query metasrv about the table name and table id
    pub async fn get_table_name(&self, table_id: &TableId) -> Result<TableName, Error> {
        self.table_info_manager
            .get(*table_id)
            .await
            .with_context(|_| TableNotFoundMetaSnafu {
                msg: format!("TableId = {:?}, couldn't found table name", table_id),
            })
            .map(|name| name.unwrap().table_name())
            .map(|name| vec![name.catalog_name, name.schema_name, name.table_name])
    }
}

/// ActiveDataflowState is a wrapper around `Hydroflow` and `DataflowState`
pub(crate) struct ActiveDataflowState<'subgraph> {
    df: Hydroflow<'subgraph>,
    state: DataflowState,
    err_collector: ErrCollector,
}

impl Default for ActiveDataflowState<'_> {
    fn default() -> Self {
        ActiveDataflowState {
            df: Hydroflow::new(),
            state: DataflowState::default(),
            err_collector: ErrCollector::default(),
        }
    }
}

impl<'subgraph> ActiveDataflowState<'subgraph> {
    /// Create a new render context, assigned with given global id
    pub fn new_ctx<'ctx>(&'ctx mut self, global_id: GlobalId) -> Context<'ctx, 'subgraph>
    where
        'subgraph: 'ctx,
    {
        Context {
            id: global_id,
            df: &mut self.df,
            compute_state: &mut self.state,
            err_collector: self.err_collector.clone(),
            input_collection: Default::default(),
            local_scope: Default::default(),
        }
    }

    pub fn set_current_ts(&mut self, ts: repr::Timestamp) {
        self.state.set_current_ts(ts);
    }

    /// Run all available subgraph
    ///
    /// return true if any subgraph actually executed
    pub fn run_available(&mut self) -> bool {
        self.state.run_available_with_schedule(&mut self.df)
    }
}

pub enum DiffRequest {
    Insert(Vec<Row>),
    Delete(Vec<Row>),
}

/// iterate through the diff row and from from continuous diff row with same diff type
pub fn diff_row_to_request(rows: Vec<DiffRow>) -> Vec<DiffRequest> {
    let mut reqs = Vec::new();
    for (row, _t, diff) in rows {
        let last = reqs.last_mut();
        match (last, diff) {
            (Some(DiffRequest::Insert(rows)), 1) => {
                rows.push(row);
            }
            (Some(DiffRequest::Insert(_)), -1) => reqs.push(DiffRequest::Delete(vec![row])),
            (Some(DiffRequest::Delete(rows)), -1) => {
                rows.push(row);
            }
            (Some(DiffRequest::Delete(_)), 1) => reqs.push(DiffRequest::Insert(vec![row])),
            _ => (),
        }
    }
    reqs
}

impl<'s> FlowNodeManager<'s> {
    /// blocking run the dataflow's grpc service & execution in a `LocalSet`
    ///
    /// the idomic way to run the dataflow
    /// is spawn a new thread, then create a flow node manager, and run the dataflow
    /// using this method
    pub fn run_dataflow(self, rt: tokio::runtime::Runtime, local_set: LocalSet) {
        local_set.block_on(&rt, async move {
            // TODO(discord9): might place grpc service on another thread?
            let zelf = self;
            todo!("main loop");
        });
    }

    /// Run all available subgraph in the flow node
    /// This will try to run all dataflow in this node
    /// TODO(discord9): add flag for subgraph that have input since last run
    pub fn run_available(&mut self) {
        let now = self.tick_manager.tick();
        for (task_id, task_state) in self.task_states.iter_mut() {
            task_state.set_current_ts(now);
            task_state.run_available();

            // if there is work done, check for new data in the sink
            while task_state.run_available() {
                let sink_table_name = self.worker_context.task_to_sink.get(task_id).unwrap();
                let sink_buf = self
                    .worker_context
                    .sink_buffer
                    .get_mut(sink_table_name)
                    .unwrap();
                let sink_recv = self
                    .worker_context
                    .sink_receiver
                    .get_mut(sink_table_name)
                    .unwrap();
                // TODO(discord9): handle lagging eror
                while let Ok(row) = sink_recv.1.try_recv() {
                    sink_buf.push_back(row);
                }
            }
        }
    }

    /// Take everything in sink buffer and construct write request which should be send to the frontend
    pub fn take_sink_request_per_table(&mut self) -> Vec<(TableName, Vec<DiffRow>)> {
        std::mem::take(&mut self.worker_context.sink_buffer)
            .into_iter()
            .map(|(name, buf)| (name, buf.into_iter().collect()))
            .collect()
    }

    /// send write request to related source sender
    pub async fn handle_write_request(
        &mut self,
        region_id: RegionId,
        rows: Vec<DiffRow>,
    ) -> Result<(), Error> {
        let table_id = region_id.table_id();
        self.worker_context.send(table_id, rows)
    }

    /// Return task id if a new task is created, otherwise return None
    ///
    /// steps to create task:
    /// 1. parse query into typed plan(and optional parse expire_when expr)
    /// 2. render source/sink with output table id and used input table id
    ///
    /// TODO(discord9): use greptime-proto type to create task instead
    #[allow(clippy::too_many_arguments)]
    pub async fn create_task(
        &mut self,
        task_id: TaskId,
        sink_table_id: TableId,
        source_table_ids: &[TableId],
        create_if_not_exist: bool,
        expire_when: Option<String>,
        comment: Option<String>,
        sql: String,
        task_options: HashMap<String, String>,
    ) -> Result<Option<TaskId>, Error> {
        if create_if_not_exist {
            // check if the task already exists
            if self.task_states.contains_key(&task_id) {
                return Ok(None);
            }
        }
        // assign global id to source and sink table
        for source in source_table_ids
            .iter()
            .chain(std::iter::once(&sink_table_id))
        {
            self.worker_context
                .assign_global_id_to_table(&self.srv_map, *source)
                .await;
        }

        // construct a active dataflow state with it
        let flow_plan =
            sql_to_flow_plan(&mut self.worker_context, &self.query_engine, &sql).await?;

        // TODO(discord9): parse `expire_when`

        self.create_ctx_and_render(task_id, flow_plan, sink_table_id, source_table_ids)?;
        Ok(Some(task_id))
    }

    /// create a render context, render the plan, and connect source/sink to the rendered dataflow
    ///
    /// return the output table's assigned global id
    fn create_ctx_and_render(
        &mut self,
        task_id: TaskId,
        plan: TypedPlan,
        sink_table_id: TableId,
        source_table_ids: &[TableId],
    ) -> Result<(), Error> {
        let mut cur_task_state = ActiveDataflowState::<'s>::default();

        {
            let sink_global_id = self
                .worker_context
                .table_repr
                .get_by_table_id(&sink_table_id)
                .with_context(|| TableNotFoundSnafu {
                    name: sink_table_id.to_string(),
                })?
                .1;
            let mut ctx = cur_task_state.new_ctx(sink_global_id);
            // rendering source now that we have the context
            for source in source_table_ids {
                let source = self
                    .worker_context
                    .table_repr
                    .get_by_table_id(source)
                    .with_context(|| TableNotFoundSnafu {
                        name: source.to_string(),
                    })?
                    .1;
                let source_sender = self.worker_context.get_source_by_global_id(&source)?;
                let recv = source_sender.subscribe();
                let bundle = ctx.render_source(recv)?;
                ctx.insert_global(source, bundle);
            }

            let rendered_dataflow = ctx.render_plan(plan.plan)?;
            let sink_sender = self.worker_context.get_sink_by_global_id(&sink_global_id)?;
            ctx.render_sink(rendered_dataflow, sink_sender);
        }

        self.task_states.insert(task_id, cur_task_state);
        Ok(())
    }
}

/// A context that holds the information of the dataflow
#[derive(Default)]
pub struct FlowWorkerContext {
    /// mapping from source table to tasks, useful for schedule which task to run when a source table is updated
    pub source_to_tasks: BTreeMap<TableId, BTreeSet<TaskId>>,
    /// mapping from task to sink table, useful for sending data back to the client when a task is done running
    pub task_to_sink: BTreeMap<TaskId, TableName>,
    /// broadcast sender for source table, any incoming write request will be sent to the source table's corresponding sender
    ///
    /// Note that we are getting insert requests with table id, so we should use table id as the key
    pub source_sender: BTreeMap<TableId, broadcast::Sender<DiffRow>>,
    /// broadcast receiver for sink table, there should only be one receiver, and it will receive all the data from the sink table
    ///
    /// and send it back to the client, since we are mocking the sink table as a client, we should use table name as the key
    /// note that the sink receiver should only have one, and we are using broadcast as mpsc channel here
    pub sink_receiver:
        BTreeMap<TableName, (broadcast::Sender<DiffRow>, broadcast::Receiver<DiffRow>)>,
    /// store sink buffer for each sink table, used for sending data back to the frontend
    pub sink_buffer: BTreeMap<TableName, VecDeque<DiffRow>>,
    /// the schema of the table
    pub schema: HashMap<GlobalId, RelationType>,
    /// All the tables that have been registered in the worker
    pub table_repr: TriMap,
}

impl FlowWorkerContext {
    pub fn send(&mut self, table_id: TableId, rows: Vec<DiffRow>) -> Result<(), Error> {
        let sender = self
            .source_sender
            .get(&table_id)
            .with_context(|| TableNotFoundSnafu {
                name: table_id.to_string(),
            })?;
        for row in rows {
            sender
                .send(row)
                .map_err(|err| {
                    InternalSnafu {
                        reason: format!(
                            "Failed to send row to table_id = {:?}, error = {:?}",
                            table_id, err
                        ),
                    }
                    .build()
                })
                .with_context(|_| EvalSnafu)?;
        }

        Ok(())
    }
}

impl FlowWorkerContext {
    /// mapping source table to task, and sink table to task in worker context
    ///
    /// also add their corrseponding broadcast sender/receiver
    fn register_task_src_sink(
        &mut self,
        task_id: TaskId,
        source_table_ids: &[TableId],
        sink_table_id: TableId,
    ) {
        for source_table_id in source_table_ids {
            self.add_source_sender(*source_table_id);
            self.source_to_tasks
                .entry(*source_table_id)
                .or_default()
                .insert(task_id);
        }

        let sink_table_name = self.table_repr.get_by_table_id(&sink_table_id).unwrap().0;
        self.task_to_sink.insert(task_id, sink_table_name);
    }

    pub fn add_source_sender(&mut self, table_id: TableId) {
        self.source_sender
            .entry(table_id)
            .or_insert_with(|| broadcast::channel(BOARDCAST_CAP).0);
    }

    pub fn add_sink_receiver(&mut self, table_name: TableName) {
        self.sink_receiver
            .entry(table_name)
            .or_insert_with(|| broadcast::channel(BOARDCAST_CAP));
    }

    pub fn get_source_by_global_id(
        &self,
        id: &GlobalId,
    ) -> Result<&broadcast::Sender<DiffRow>, Error> {
        let table_id = self
            .table_repr
            .get_by_global_id(id)
            .with_context(|| TableNotFoundSnafu {
                name: format!("{:?}", id),
            })?
            .1;
        self.source_sender
            .get(&table_id)
            .with_context(|| TableNotFoundSnafu {
                name: table_id.to_string(),
            })
    }

    pub fn get_sink_by_global_id(
        &self,
        id: &GlobalId,
    ) -> Result<broadcast::Sender<DiffRow>, Error> {
        let table_name = self
            .table_repr
            .get_by_global_id(id)
            .with_context(|| TableNotFoundSnafu {
                name: format!("{:?}", id),
            })?
            .0;
        self.sink_receiver
            .get(&table_name)
            .map(|(s, r)| s.clone())
            .with_context(|| TableNotFoundSnafu {
                name: table_name.join("."),
            })
    }
}

impl FlowWorkerContext {
    /// Retrieves a GlobalId and table schema representing a table previously registered by calling the [register_table] function.
    ///
    /// Returns an error if no table has been registered with the provided names
    pub fn table(&self, name: &Vec<String>) -> Result<(GlobalId, RelationType), Error> {
        let id = self
            .table_repr
            .get_by_name(name)
            .map(|(_tid, gid)| gid)
            .with_context(|| TableNotFoundSnafu {
                name: name.join("."),
            })?;
        let schema = self
            .schema
            .get(&id)
            .cloned()
            .with_context(|| TableNotFoundSnafu {
                name: name.join("."),
            })?;
        Ok((id, schema))
    }

    /// Assign a global id to a table, if already assigned, return the existing global id
    ///
    /// NOTE: this will not actually render the table into collection refered as GlobalId
    /// merely creating a mapping from table id to global id
    pub async fn assign_global_id_to_table(
        &mut self,
        srv_map: &TableIdNameMapper,
        table_id: TableId,
    ) -> GlobalId {
        if let Some((_name, gid)) = self.table_repr.get_by_table_id(&table_id) {
            gid
        } else {
            let global_id = self.new_global_id();
            let table_name = srv_map.get_table_name(&table_id).await.unwrap();
            self.table_repr.insert(table_name, table_id, global_id);
            global_id
        }
    }

    /// Get a new global id
    pub fn new_global_id(&self) -> GlobalId {
        GlobalId::User(self.table_repr.global_id_to_name_id.len() as u64)
    }
}

/// A tri-directional map that maps table name, table id, and global id
#[derive(Default)]
pub struct TriMap {
    name_to_global_id: HashMap<TableName, GlobalId>,
    id_to_global_id: HashMap<TableId, GlobalId>,
    global_id_to_name_id: BTreeMap<GlobalId, (TableName, TableId)>,
}

impl TriMap {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn insert(&mut self, name: TableName, id: TableId, global_id: GlobalId) {
        self.name_to_global_id.insert(name.clone(), global_id);
        self.id_to_global_id.insert(id, global_id);
        self.global_id_to_name_id.insert(global_id, (name, id));
    }

    pub fn get_by_name(&self, name: &TableName) -> Option<(TableId, GlobalId)> {
        self.name_to_global_id.get(name).and_then(|global_id| {
            self.global_id_to_name_id
                .get(global_id)
                .map(|(_name, id)| (*id, *global_id))
        })
    }

    pub fn get_by_table_id(&self, id: &TableId) -> Option<(TableName, GlobalId)> {
        self.id_to_global_id.get(id).and_then(|global_id| {
            self.global_id_to_name_id
                .get(global_id)
                .map(|(name, _id)| (name.clone(), *global_id))
        })
    }

    pub fn get_by_global_id(&self, global_id: &GlobalId) -> Option<(TableName, TableId)> {
        self.global_id_to_name_id
            .get(global_id)
            .map(|(name, id)| (name.clone(), *id))
    }
}

/// FlowTickManager is a manager for flow tick
pub struct FlowTickManager {
    anchor: Anchor,
}

impl FlowTickManager {
    pub fn new() -> Self {
        FlowTickManager {
            anchor: Anchor::new(),
        }
    }

    /// Return the current timestamp in milliseconds
    pub fn tick(&self) -> repr::Timestamp {
        (minstant::Instant::now().as_unix_nanos(&self.anchor) / 1_000_000) as repr::Timestamp
    }
}
