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

//! Node context, prone to change with every incoming requests

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use common_telemetry::trace;
use session::context::QueryContext;
use snafu::{OptionExt, ResultExt};
use table::metadata::TableId;
use tokio::sync::{broadcast, mpsc, RwLock};

use crate::adapter::{FlowId, TableName, TableSource};
use crate::error::{Error, EvalSnafu, TableNotFoundSnafu};
use crate::expr::error::InternalSnafu;
use crate::expr::{Batch, GlobalId};
use crate::metrics::METRIC_FLOW_INPUT_BUF_SIZE;
use crate::repr::{DiffRow, RelationDesc, BATCH_SIZE, BROADCAST_CAP, SEND_BUF_CAP};

/// A context that holds the information of the dataflow
#[derive(Default, Debug)]
pub struct FlownodeContext {
    /// mapping from source table to tasks, useful for schedule which task to run when a source table is updated
    pub source_to_tasks: BTreeMap<TableId, BTreeSet<FlowId>>,
    /// mapping from task to sink table, useful for sending data back to the client when a task is done running
    pub flow_to_sink: BTreeMap<FlowId, TableName>,
    pub sink_to_flow: BTreeMap<TableName, FlowId>,
    /// broadcast sender for source table, any incoming write request will be sent to the source table's corresponding sender
    ///
    /// Note that we are getting insert requests with table id, so we should use table id as the key
    pub source_sender: BTreeMap<TableId, SourceSender>,
    /// broadcast receiver for sink table, there should only be one receiver, and it will receive all the data from the sink table
    ///
    /// and send it back to the client, since we are mocking the sink table as a client, we should use table name as the key
    /// note that the sink receiver should only have one, and we are using broadcast as mpsc channel here
    pub sink_receiver:
        BTreeMap<TableName, (mpsc::UnboundedSender<Batch>, mpsc::UnboundedReceiver<Batch>)>,
    /// the schema of the table, query from metasrv or inferred from TypedPlan
    pub schema: HashMap<GlobalId, RelationDesc>,
    /// All the tables that have been registered in the worker
    pub table_repr: IdToNameMap,
    pub query_context: Option<Arc<QueryContext>>,
}

/// a simple broadcast sender with backpressure, bounded capacity and blocking on send when send buf is full
/// note that it wouldn't evict old data, so it's possible to block forever if the receiver is slow
///
/// receiver still use tokio broadcast channel, since only sender side need to know
/// backpressure and adjust dataflow running duration to avoid blocking
#[derive(Debug)]
pub struct SourceSender {
    // TODO(discord9): make it all Vec<DiffRow>?
    sender: broadcast::Sender<Batch>,
    send_buf_tx: mpsc::Sender<Batch>,
    send_buf_rx: RwLock<mpsc::Receiver<Batch>>,
    send_buf_row_cnt: AtomicUsize,
}

impl Default for SourceSender {
    fn default() -> Self {
        // TODO(discord9): the capacity is arbitrary, we can adjust it later, might also want to limit the max number of rows in send buf
        let (send_buf_tx, send_buf_rx) = mpsc::channel(SEND_BUF_CAP);
        Self {
            // TODO(discord9): found a better way then increase this to prevent lagging and hence missing input data
            sender: broadcast::Sender::new(SEND_BUF_CAP),
            send_buf_tx,
            send_buf_rx: RwLock::new(send_buf_rx),
            send_buf_row_cnt: AtomicUsize::new(0),
        }
    }
}

impl SourceSender {
    /// max number of iterations to try flush send buf
    const MAX_ITERATIONS: usize = 16;
    pub fn get_receiver(&self) -> broadcast::Receiver<Batch> {
        self.sender.subscribe()
    }

    /// send as many as possible rows from send buf
    /// until send buf is empty or broadchannel is full
    pub async fn try_flush(&self) -> Result<usize, Error> {
        let mut row_cnt = 0;
        loop {
            let mut send_buf = self.send_buf_rx.write().await;
            // if inner sender channel is empty or send buf is empty, there
            // is nothing to do for now, just break
            if self.sender.len() >= BROADCAST_CAP || send_buf.is_empty() {
                break;
            }
            // TODO(discord9): send rows instead so it's just moving a point
            if let Some(batch) = send_buf.recv().await {
                let len = batch.row_count();
                self.send_buf_row_cnt.fetch_sub(len, Ordering::SeqCst);
                row_cnt += len;
                self.sender
                    .send(batch)
                    .map_err(|err| {
                        InternalSnafu {
                            reason: format!("Failed to send row, error = {:?}", err),
                        }
                        .build()
                    })
                    .with_context(|_| EvalSnafu)?;
            }
        }
        if row_cnt > 0 {
            trace!("Source Flushed {} rows", row_cnt);
            METRIC_FLOW_INPUT_BUF_SIZE.sub(row_cnt as _);
            trace!(
                "Remaining Source Send buf.len() = {}",
                METRIC_FLOW_INPUT_BUF_SIZE.get()
            );
        }

        Ok(row_cnt)
    }

    /// return number of rows it actual send(including what's in the buffer)
    pub async fn send_rows(&self, rows: Vec<DiffRow>) -> Result<usize, Error> {
        METRIC_FLOW_INPUT_BUF_SIZE.add(rows.len() as _);
        while self.send_buf_row_cnt.load(Ordering::SeqCst) >= BATCH_SIZE * 4 {
            tokio::task::yield_now().await;
        }
        // row count metrics is approx so relaxed order is ok
        self.send_buf_row_cnt
            .fetch_add(rows.len(), Ordering::SeqCst);
        let batch = Batch::try_from_rows(rows.into_iter().map(|(row, _, _)| row).collect())
            .context(EvalSnafu)?;
        common_telemetry::trace!("Send one batch to worker with {} rows", batch.row_count());
        self.send_buf_tx.send(batch).await.map_err(|e| {
            crate::error::InternalSnafu {
                reason: format!("Failed to send row, error = {:?}", e),
            }
            .build()
        })?;

        Ok(0)
    }
}

impl FlownodeContext {
    /// return number of rows it actual send(including what's in the buffer)
    ///
    /// TODO(discord9): make this concurrent
    pub async fn send(&self, table_id: TableId, rows: Vec<DiffRow>) -> Result<usize, Error> {
        let sender = self
            .source_sender
            .get(&table_id)
            .with_context(|| TableNotFoundSnafu {
                name: table_id.to_string(),
            })?;
        sender.send_rows(rows).await
    }

    /// flush all sender's buf
    ///
    /// return numbers being sent
    pub async fn flush_all_sender(&self) -> Result<usize, Error> {
        let mut sum = 0;
        for sender in self.source_sender.values() {
            sender.try_flush().await.inspect(|x| sum += x)?;
        }
        Ok(sum)
    }
}

impl FlownodeContext {
    /// mapping source table to task, and sink table to task in worker context
    ///
    /// also add their corresponding broadcast sender/receiver
    pub fn register_task_src_sink(
        &mut self,
        task_id: FlowId,
        source_table_ids: &[TableId],
        sink_table_name: TableName,
    ) {
        for source_table_id in source_table_ids {
            self.add_source_sender_if_not_exist(*source_table_id);
            self.source_to_tasks
                .entry(*source_table_id)
                .or_default()
                .insert(task_id);
        }

        self.add_sink_receiver(sink_table_name.clone());
        self.flow_to_sink.insert(task_id, sink_table_name.clone());
        self.sink_to_flow.insert(sink_table_name, task_id);
    }

    /// remove flow from worker context
    pub fn remove_flow(&mut self, task_id: FlowId) {
        if let Some(sink_table_name) = self.flow_to_sink.remove(&task_id) {
            self.sink_to_flow.remove(&sink_table_name);
        }
        for (source_table_id, tasks) in self.source_to_tasks.iter_mut() {
            tasks.remove(&task_id);
            if tasks.is_empty() {
                self.source_sender.remove(source_table_id);
            }
        }
    }

    /// try add source sender, if already exist, do nothing
    pub fn add_source_sender_if_not_exist(&mut self, table_id: TableId) {
        let _sender = self.source_sender.entry(table_id).or_default();
    }

    pub fn add_sink_receiver(&mut self, table_name: TableName) {
        self.sink_receiver
            .entry(table_name)
            .or_insert_with(mpsc::unbounded_channel);
    }

    pub fn get_source_by_global_id(&self, id: &GlobalId) -> Result<&SourceSender, Error> {
        let table_id = self
            .table_repr
            .get_by_global_id(id)
            .with_context(|| TableNotFoundSnafu {
                name: format!("Global Id = {:?}", id),
            })?
            .1
            .with_context(|| TableNotFoundSnafu {
                name: format!("Table Id = {:?}", id),
            })?;
        self.source_sender
            .get(&table_id)
            .with_context(|| TableNotFoundSnafu {
                name: table_id.to_string(),
            })
    }

    pub fn get_sink_by_global_id(
        &self,
        id: &GlobalId,
    ) -> Result<mpsc::UnboundedSender<Batch>, Error> {
        let table_name = self
            .table_repr
            .get_by_global_id(id)
            .with_context(|| TableNotFoundSnafu {
                name: format!("{:?}", id),
            })?
            .0
            .with_context(|| TableNotFoundSnafu {
                name: format!("Global Id = {:?}", id),
            })?;
        self.sink_receiver
            .get(&table_name)
            .map(|(s, _r)| s.clone())
            .with_context(|| TableNotFoundSnafu {
                name: table_name.join("."),
            })
    }
}

impl FlownodeContext {
    /// Retrieves a GlobalId and table schema representing a table previously registered by calling the [register_table] function.
    ///
    /// Returns an error if no table has been registered with the provided names
    pub fn table(&self, name: &TableName) -> Result<(GlobalId, RelationDesc), Error> {
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
    /// require at least one of `table_name` or `table_id` to be `Some`
    ///
    /// and will try to fetch the schema from table info manager(if table exist now)
    ///
    /// NOTE: this will not actually render the table into collection referred as GlobalId
    /// merely creating a mapping from table id to global id
    pub async fn assign_global_id_to_table(
        &mut self,
        srv_map: &TableSource,
        mut table_name: Option<TableName>,
        table_id: Option<TableId>,
    ) -> Result<GlobalId, Error> {
        // if we can find by table name/id. not assign it
        if let Some(gid) = table_name
            .as_ref()
            .and_then(|table_name| self.table_repr.get_by_name(table_name))
            .map(|(_, gid)| gid)
            .or_else(|| {
                table_id
                    .and_then(|id| self.table_repr.get_by_table_id(&id))
                    .map(|(_, gid)| gid)
            })
        {
            Ok(gid)
        } else {
            let global_id = self.new_global_id();

            if let Some(table_id) = table_id {
                let (known_table_name, schema) = srv_map.get_table_name_schema(&table_id).await?;
                table_name = table_name.or(Some(known_table_name));
                self.schema.insert(global_id, schema);
            } // if we don't have table id, it means database havn't assign one yet or we don't need it

            self.table_repr.insert(table_name, table_id, global_id);
            Ok(global_id)
        }
    }

    /// Assign a schema to a table
    ///
    pub fn assign_table_schema(
        &mut self,
        table_name: &TableName,
        schema: RelationDesc,
    ) -> Result<(), Error> {
        let gid = self
            .table_repr
            .get_by_name(table_name)
            .map(|(_, gid)| gid)
            .context(TableNotFoundSnafu {
                name: format!("Table not found: {:?} in flownode cache", table_name),
            })?;

        self.schema.insert(gid, schema);
        Ok(())
    }

    /// Get a new global id
    pub fn new_global_id(&self) -> GlobalId {
        GlobalId::User(self.table_repr.global_id_to_name_id.len() as u64)
    }
}

/// A tri-directional map that maps table name, table id, and global id
#[derive(Default, Debug)]
pub struct IdToNameMap {
    name_to_global_id: HashMap<TableName, GlobalId>,
    id_to_global_id: HashMap<TableId, GlobalId>,
    global_id_to_name_id: BTreeMap<GlobalId, (Option<TableName>, Option<TableId>)>,
}

impl IdToNameMap {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn insert(&mut self, name: Option<TableName>, id: Option<TableId>, global_id: GlobalId) {
        name.clone()
            .and_then(|name| self.name_to_global_id.insert(name.clone(), global_id));
        id.and_then(|id| self.id_to_global_id.insert(id, global_id));
        self.global_id_to_name_id.insert(global_id, (name, id));
    }

    pub fn get_by_name(&self, name: &TableName) -> Option<(Option<TableId>, GlobalId)> {
        self.name_to_global_id.get(name).map(|global_id| {
            let (_name, id) = self.global_id_to_name_id.get(global_id).unwrap();
            (*id, *global_id)
        })
    }

    pub fn get_by_table_id(&self, id: &TableId) -> Option<(Option<TableName>, GlobalId)> {
        self.id_to_global_id.get(id).map(|global_id| {
            let (name, _id) = self.global_id_to_name_id.get(global_id).unwrap();
            (name.clone(), *global_id)
        })
    }

    pub fn get_by_global_id(
        &self,
        global_id: &GlobalId,
    ) -> Option<(Option<TableName>, Option<TableId>)> {
        self.global_id_to_name_id.get(global_id).cloned()
    }
}
