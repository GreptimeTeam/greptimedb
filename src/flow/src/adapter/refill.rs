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

//! This module contains the refill flow task, which is used to refill flow with given table id and a time range.

use std::collections::BTreeSet;
use std::sync::Arc;

use catalog::CatalogManagerRef;
use common_error::ext::BoxedError;
use common_meta::key::flow::FlowMetadataManagerRef;
use common_recordbatch::{RecordBatch, RecordBatches, SendableRecordBatchStream};
use common_runtime::JoinHandle;
use common_telemetry::error;
use datatypes::value::Value;
use futures::StreamExt;
use query::parser::QueryLanguageParser;
use session::context::QueryContextBuilder;
use snafu::{ensure, OptionExt, ResultExt};
use table::metadata::TableId;

use crate::adapter::table_source::ManagedTableSource;
use crate::adapter::{FlowId, FlowStreamingEngineRef, StreamingEngine};
use crate::error::{FlowNotFoundSnafu, JoinTaskSnafu, UnexpectedSnafu};
use crate::expr::error::ExternalSnafu;
use crate::expr::utils::find_plan_time_window_expr_lower_bound;
use crate::repr::RelationDesc;
use crate::server::get_all_flow_ids;
use crate::{Error, FrontendInvoker};

impl StreamingEngine {
    /// Create and start refill flow tasks in background
    pub async fn create_and_start_refill_flow_tasks(
        self: &FlowStreamingEngineRef,
        flow_metadata_manager: &FlowMetadataManagerRef,
        catalog_manager: &CatalogManagerRef,
    ) -> Result<(), Error> {
        let tasks = self
            .create_refill_flow_tasks(flow_metadata_manager, catalog_manager)
            .await?;
        self.starting_refill_flows(tasks).await?;
        Ok(())
    }

    /// Create a series of tasks to refill flow
    pub async fn create_refill_flow_tasks(
        &self,
        flow_metadata_manager: &FlowMetadataManagerRef,
        catalog_manager: &CatalogManagerRef,
    ) -> Result<Vec<RefillTask>, Error> {
        let nodeid = self.node_id.map(|c| c as u64);

        let flow_ids = get_all_flow_ids(flow_metadata_manager, catalog_manager, nodeid).await?;
        let mut refill_tasks = Vec::new();
        'flow_id_loop: for flow_id in flow_ids {
            let info = flow_metadata_manager
                .flow_info_manager()
                .get(flow_id)
                .await
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?
                .context(FlowNotFoundSnafu { id: flow_id })?;

            // TODO(discord9): also check flow is already running
            for src_table in info.source_table_ids() {
                // check if source table still exists
                if !self.table_info_source.check_table_exist(src_table).await? {
                    error!(
                        "Source table id = {:?} not found while refill flow_id={}, consider re-create the flow if necessary",
                        src_table, flow_id
                    );
                    continue 'flow_id_loop;
                }
            }

            let expire_after = info.expire_after();
            // TODO(discord9): better way to get last point
            let now = self.tick_manager.tick();
            let plan = self
                .node_context
                .read()
                .await
                .get_flow_plan(&FlowId::from(flow_id))
                .context(FlowNotFoundSnafu { id: flow_id })?;
            let time_range = if let Some(expire_after) = expire_after {
                let low_bound = common_time::Timestamp::new_millisecond(now - expire_after);
                let real_low_bound = find_plan_time_window_expr_lower_bound(&plan, low_bound)?;
                real_low_bound.map(|l| (l, common_time::Timestamp::new_millisecond(now)))
            } else {
                None
            };

            common_telemetry::debug!(
                "Time range for refill flow_id={} is {:?}",
                flow_id,
                time_range
            );

            for src_table in info.source_table_ids() {
                let time_index_col = self
                    .table_info_source
                    .get_time_index_column_from_table_id(*src_table)
                    .await?
                    .1;
                let time_index_name = time_index_col.name;
                let task = RefillTask::create(
                    flow_id as u64,
                    *src_table,
                    time_range,
                    &time_index_name,
                    &self.table_info_source,
                )
                .await?;
                refill_tasks.push(task);
            }
        }
        Ok(refill_tasks)
    }

    /// Starting to refill flows, if any error occurs, will rebuild the flow and retry
    pub(crate) async fn starting_refill_flows(
        self: &FlowStreamingEngineRef,
        tasks: Vec<RefillTask>,
    ) -> Result<(), Error> {
        // TODO(discord9): add a back pressure mechanism
        let frontend_invoker =
            self.frontend_invoker
                .read()
                .await
                .clone()
                .context(UnexpectedSnafu {
                    reason: "frontend invoker is not set",
                })?;

        for mut task in tasks {
            task.start_running(self.clone(), &frontend_invoker).await?;
            // TODO(discord9): save refill tasks to a map and check if it's finished when necessary
            // i.e. when system table need query it's state
            self.refill_tasks
                .write()
                .await
                .insert(task.data.flow_id, task);
        }
        Ok(())
    }
}

/// Task to refill flow with given table id and a time range
pub struct RefillTask {
    data: TaskData,
    state: TaskState<()>,
}

#[derive(Clone)]
struct TaskData {
    flow_id: FlowId,
    table_id: TableId,
    table_schema: RelationDesc,
}

impl TaskData {
    /// validate that incoming batch's schema is the same as table schema(by comparing types&names)
    fn validate_schema(table_schema: &RelationDesc, rb: &RecordBatch) -> Result<(), Error> {
        let rb_schema = &rb.schema;
        ensure!(
            rb_schema.column_schemas().len() == table_schema.len()?,
            UnexpectedSnafu {
                reason: format!(
                    "RecordBatch schema length does not match table schema length, {}!={}",
                    rb_schema.column_schemas().len(),
                    table_schema.len()?
                )
            }
        );
        for (i, rb_col) in rb_schema.column_schemas().iter().enumerate() {
            let (rb_name, rb_ty) = (rb_col.name.as_str(), &rb_col.data_type);
            let (table_name, table_ty) = (
                table_schema.names[i].as_ref(),
                &table_schema.typ().column_types[i].scalar_type,
            );
            ensure!(
                Some(rb_name) == table_name.map(|c| c.as_str()),
                UnexpectedSnafu {
                    reason: format!(
                        "Mismatch in column names: expected {:?}, found {}",
                        table_name, rb_name
                    )
                }
            );

            ensure!(
                rb_ty == table_ty,
                UnexpectedSnafu {
                    reason: format!(
                        "Mismatch in column types for {}: expected {:?}, found {:?}",
                        rb_name, table_ty, rb_ty
                    )
                }
            );
        }
        Ok(())
    }
}

/// Refill task state
enum TaskState<T> {
    /// Task is not started
    Prepared { sql: String },
    /// Task is running
    Running {
        handle: JoinHandle<Result<T, Error>>,
    },
    /// Task is finished
    Finished { res: Result<T, Error> },
}

impl<T> TaskState<T> {
    fn new(sql: String) -> Self {
        Self::Prepared { sql }
    }
}

mod test_send {
    use std::collections::BTreeMap;

    use tokio::sync::RwLock;

    use super::*;
    fn is_send<T: Send + Sync>() {}
    fn foo() {
        is_send::<TaskState<()>>();
        is_send::<RefillTask>();
        is_send::<BTreeMap<FlowId, RefillTask>>();
        is_send::<RwLock<BTreeMap<FlowId, RefillTask>>>();
    }
}

impl TaskState<()> {
    /// check if task is finished
    async fn is_finished(&mut self) -> Result<bool, Error> {
        match self {
            Self::Finished { .. } => Ok(true),
            Self::Running { handle } => Ok(if handle.is_finished() {
                *self = Self::Finished {
                    res: handle.await.context(JoinTaskSnafu)?,
                };
                true
            } else {
                false
            }),
            _ => Ok(false),
        }
    }

    fn start_running(
        &mut self,
        task_data: &TaskData,
        manager: FlowStreamingEngineRef,
        mut output_stream: SendableRecordBatchStream,
    ) -> Result<(), Error> {
        let data = (*task_data).clone();
        let handle: JoinHandle<Result<(), Error>> = common_runtime::spawn_global(async move {
            while let Some(rb) = output_stream.next().await {
                let rb = match rb {
                    Ok(rb) => rb,
                    Err(err) => Err(BoxedError::new(err)).context(ExternalSnafu)?,
                };
                TaskData::validate_schema(&data.table_schema, &rb)?;

                // send rb into flow node
                manager
                    .node_context
                    .read()
                    .await
                    .send_rb(data.table_id, rb)
                    .await?;
            }
            common_telemetry::info!(
                "Refill successful for source table_id={}, flow_id={}",
                data.table_id,
                data.flow_id
            );
            Ok(())
        });
        *self = Self::Running { handle };

        Ok(())
    }
}

/// Query stream of RefillTask, simply wrap RecordBatches and RecordBatchStream and check output is not `AffectedRows`
enum QueryStream {
    Batches { batches: RecordBatches },
    Stream { stream: SendableRecordBatchStream },
}

impl TryFrom<common_query::Output> for QueryStream {
    type Error = Error;
    fn try_from(value: common_query::Output) -> Result<Self, Self::Error> {
        match value.data {
            common_query::OutputData::Stream(stream) => Ok(QueryStream::Stream { stream }),
            common_query::OutputData::RecordBatches(batches) => {
                Ok(QueryStream::Batches { batches })
            }
            _ => UnexpectedSnafu {
                reason: format!("Unexpected output data type: {:?}", value.data),
            }
            .fail(),
        }
    }
}

impl QueryStream {
    fn try_into_stream(self) -> Result<SendableRecordBatchStream, Error> {
        match self {
            Self::Batches { batches } => Ok(batches.as_stream()),
            Self::Stream { stream } => Ok(stream),
        }
    }
}

impl RefillTask {
    /// Query with "select * from table WHERE time >= range_start and time < range_end"
    pub async fn create(
        flow_id: FlowId,
        table_id: TableId,
        time_range: Option<(common_time::Timestamp, common_time::Timestamp)>,
        time_col_name: &str,
        table_src: &ManagedTableSource,
    ) -> Result<RefillTask, Error> {
        let (table_name, table_schema) = table_src.get_table_name_schema(&table_id).await?;
        let all_col_names: BTreeSet<_> = table_schema
            .relation_desc
            .iter_names()
            .flatten()
            .map(|s| s.as_str())
            .collect();

        if !all_col_names.contains(time_col_name) {
            UnexpectedSnafu {
                reason: format!(
                    "Can't find column {} in table {} while refill flow",
                    time_col_name,
                    table_name.join(".")
                ),
            }
            .fail()?;
        }

        let sql = if let Some(time_range) = time_range {
            format!(
                "select * from {0} where {1} >= {2} and {1} < {3}",
                table_name.join("."),
                time_col_name,
                Value::from(time_range.0),
                Value::from(time_range.1),
            )
        } else {
            format!("select * from {0}", table_name.join("."))
        };

        Ok(RefillTask {
            data: TaskData {
                flow_id,
                table_id,
                table_schema: table_schema.relation_desc,
            },
            state: TaskState::new(sql),
        })
    }

    /// Start running the task in background, non-blocking
    pub async fn start_running(
        &mut self,
        manager: FlowStreamingEngineRef,
        invoker: &FrontendInvoker,
    ) -> Result<(), Error> {
        let TaskState::Prepared { sql } = &mut self.state else {
            UnexpectedSnafu {
                reason: "task is not prepared",
            }
            .fail()?
        };

        // we don't need information from query context in this query so a default query context is enough
        let query_ctx = Arc::new(
            QueryContextBuilder::default()
                .current_catalog("greptime".to_string())
                .current_schema("public".to_string())
                .build(),
        );

        let stmt_exec = invoker.statement_executor();

        let stmt = QueryLanguageParser::parse_sql(sql, &query_ctx)
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;
        let plan = stmt_exec
            .plan(&stmt, query_ctx.clone())
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;

        let output_data = stmt_exec
            .exec_plan(plan, query_ctx)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;

        let output_stream = QueryStream::try_from(output_data)?;
        let output_stream = output_stream.try_into_stream()?;

        self.state
            .start_running(&self.data, manager, output_stream)?;
        Ok(())
    }

    pub async fn is_finished(&mut self) -> Result<bool, Error> {
        self.state.is_finished().await
    }
}
