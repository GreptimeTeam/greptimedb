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
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use common_meta::key::table_info::TableInfoManager;
use common_meta::key::table_name::TableNameManager;
use hydroflow::scheduled::graph::Hydroflow;
use query::QueryEngine;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use snafu::OptionExt;
use table::metadata::TableId;
use tokio::sync::broadcast;
use tokio::task::LocalSet;

use crate::adapter::error::TableNotFoundSnafu;
use crate::compute::{Context, DataflowState, ErrCollector};
use crate::expr::GlobalId;
use crate::plan::{Plan, TypedPlan};
use crate::repr::{DiffRow, RelationType};
use crate::transform::sql_to_flow_plan;

pub(crate) mod error;

use error::Error;

// TODO: refactor common types for flow to a separate module
pub type TaskId = u64;
pub type TableName = Vec<String>;

/// broadcast channel capacity, set to a arbitrary value
pub const BOARDCAST_CAP: usize = 1024;

/// FlowNodeManager manages the state of all tasks in the flow node, which should be run on the same thread
///
/// The choice of timestamp is just using current system timestamp for now
pub struct FlowNodeManager<'subgraph> {
    pub task_states: BTreeMap<TaskId, ActiveDataflowState<'subgraph>>,
    pub local_set: LocalSet,

    // TODO: catalog/tableinfo manager for query schema and translate sql to plan
    query_engine: Arc<dyn QueryEngine>,
    /// contains mapping from table name to global id, and table schema
    worker_context: FlowWorkerContext,
}

/// mapping of table name <-> table id should be query from tableinfo manager
struct TableNameIdMapping {
    /// for query `TableId -> TableName` mapping
    table_info_manager: TableInfoManager,
    /// for query `TableName -> TableId` mapping
    table_name_manager: TableNameManager,
    // a in memory cache, will be invalid if necessary
}

impl TableNameIdMapping {
    pub async fn get_table_id(&self, table_name: TableName) -> Result<TableId, Error> {
        todo!()
    }

    pub async fn get_table_name(&self, table_id: TableId) -> Result<TableName, Error> {
        todo!()
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
}

impl<'s> FlowNodeManager<'s> {
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
        source_table_ids: SmallVec<[TableId; 2]>,
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

        // construct a active dataflow state with it
        let flow_plan =
            sql_to_flow_plan(&mut self.worker_context, &self.query_engine, &sql).await?;

        // TODO(discord9): parse `expire_when`

        let _sink_gid =
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
        source_table_ids: SmallVec<[TableId; 2]>,
    ) -> Result<GlobalId, Error> {
        let mut cur_task_state = ActiveDataflowState::<'s>::default();
        // 1. render sources
        let source_global_ids = source_table_ids
            .iter()
            .map(|id| self.worker_context.assign_global_id_to_table(*id))
            .collect::<Vec<_>>();

        let sink_global_id = self.worker_context.assign_global_id_to_table(sink_table_id);

        {
            let mut ctx = cur_task_state.new_ctx(sink_global_id);
            // rendering source now that we have the context
            for source in source_global_ids {
                let source_sender = self.worker_context.get_source_by_global_id(&source)?;
                let recv = source_sender.subscribe();
                let bundle = ctx.render_source(recv)?;
                ctx.insert_global(source, bundle);
            }

            let rendered_dataflow = ctx.render_plan(plan.plan)?;
            let sink_sender = self.worker_context.get_sink_by_global_id(&sink_global_id)?;
            ctx.render_sink(rendered_dataflow, sink_sender);
        }

        // what is wrong with lifetime? ctx is short live than cur_task_state
        self.task_states.insert(task_id, cur_task_state);
        Ok(sink_global_id)
    }
}

/// A context that holds the information of the dataflow
#[derive(Default)]
pub struct FlowWorkerContext {
    /// broadcast sender for source table, any incoming write request will be sent to the source table's corresponding sender
    ///
    /// Note that we are getting insert requests with table id, so we should use table id as the key
    pub source_sender: BTreeMap<TableId, broadcast::Sender<DiffRow>>,
    /// broadcast receiver for sink table, there should only be one receiver, and it will receive all the data from the sink table
    ///
    /// and send it back to the client, since we are mocking the sink table as a client, we should use table name as the key
    pub sink_receiver:
        BTreeMap<TableName, (broadcast::Sender<DiffRow>, broadcast::Receiver<DiffRow>)>,
    /// `id` refer to any source table in the dataflow, and `name` is the name of the table
    /// which is a `Vec<String>` in substrait
    pub id_to_name: HashMap<GlobalId, Vec<String>>,
    /// see `id_to_name`
    pub name_to_id: HashMap<Vec<String>, GlobalId>,
    /// the schema of the table
    pub schema: HashMap<GlobalId, RelationType>,
}

impl FlowWorkerContext {
    pub fn add_source_sender(&mut self, table_id: TableId) {
        self.source_sender
            .insert(table_id, broadcast::channel(BOARDCAST_CAP).0);
    }
    pub fn get_source_by_global_id(
        &self,
        id: &GlobalId,
    ) -> Result<&broadcast::Sender<DiffRow>, Error> {
        let table_id = self.get_table_name_id(id)?.1;
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
        let table_name = self.get_table_name_id(id)?.0;
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
            .name_to_id
            .get(name)
            .copied()
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

    pub fn table_from_table_id(&self, id: &GlobalId) -> Result<(GlobalId, RelationType), Error> {
        todo!()
    }

    /// Assign a global id to a table, if already assigned, return the existing global id
    ///
    /// NOTE: this will not actually render the table into collection refered as GlobalId
    /// merely creating a mapping from table id to global id
    pub fn assign_global_id_to_table(&self, table_id: TableId) -> GlobalId {
        todo!()
    }

    /// get table name by global id
    pub fn get_table_name_id(&self, id: &GlobalId) -> Result<(TableName, TableId), Error> {
        todo!()
    }

    /// Get a new global id
    pub fn new_global_id(&self) -> GlobalId {
        todo!()
    }
}
