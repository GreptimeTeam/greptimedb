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

use std::collections::BTreeSet;

use common_error::ext::BoxedError;
use common_recordbatch::{RecordBatch, RecordBatches, SendableRecordBatchStream};
use common_runtime::JoinHandle;
use datatypes::value::Value;
use futures::StreamExt;
use query::parser::QueryLanguageParser;
use session::context::QueryContext;
use snafu::{OptionExt, ResultExt};
use table::metadata::TableId;

use super::FlowId;
use crate::adapter::table_source::TableSource;
use crate::adapter::{CreateFlowArgs, FlowWorkerManagerRef};
use crate::error::{FlowNotFoundSnafu, UnexpectedSnafu};
use crate::expr::error::ExternalSnafu;
use crate::repr::RelationDesc;
use crate::{Error, FlownodeBuilder, FrontendInvoker};

impl FlownodeBuilder {
    /// Create a series of tasks to refill flow
    async fn create_refill_flow_tasks(
        &self,
        manager: &FlowWorkerManagerRef,
    ) -> Result<Vec<RefillTask>, Error> {
        let nodeid = manager.node_id.map(|c| c as u64);

        let frontend_invoker =
            manager
                .frontend_invoker
                .read()
                .await
                .clone()
                .context(UnexpectedSnafu {
                    reason: "frontend invoker is not set",
                })?;

        let flow_ids = self.get_all_flow_ids(nodeid).await?;
        let mut refill_tasks = Vec::new();
        for flow_id in flow_ids {
            let info = self
                .flow_metadata_manager()
                .flow_info_manager()
                .get(flow_id)
                .await
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?
                .context(FlowNotFoundSnafu { id: flow_id })?;
            let expire_after = info.expire_after();
            // TODO(discord9): better way to get last point
            let now = manager.tick_manager.tick();
            let time_range = expire_after.map(|e| {
                (
                    common_time::Timestamp::new_millisecond(now - e),
                    common_time::Timestamp::new_millisecond(now),
                )
            });
            for src_table in info.source_table_ids() {
                let time_index_col = manager
                    .table_info_source
                    .get_time_index_column_from_table_id(src_table)
                    .await?;
                let time_index_name = time_index_col.name;
                let task = RefillTask::create(
                    &frontend_invoker,
                    flow_id as u64,
                    *src_table,
                    time_range,
                    &time_index_name,
                    &manager.table_info_source,
                )
                .await?;
                refill_tasks.push(task);
            }
        }
        Ok(refill_tasks)
    }

    /// Starting to refill flows, if any error occurs, will rebuild the flow and retry
    pub(crate) async fn starting_refill_flows(
        &self,
        manager: &FlowWorkerManagerRef,
    ) -> Result<Vec<JoinHandle<Result<(), Error>>>, Error> {
        let tasks = self.create_refill_flow_tasks(manager).await?;
        // TODO(discord9): add a back pressure mechanism
        let mut handles = Vec::new();
        for mut task in tasks {
            let flow_metadata_manager = self.flow_metadata_manager();
            let manager = manager.clone();
            let handle: JoinHandle<Result<(), Error>> = common_runtime::spawn_global(async move {
                // if failed to refill, will rebuild the flow without refill
                match task.handle_refill_inserts(manager.clone()).await {
                    Ok(()) => {
                        common_telemetry::info!(
                            "Successfully refill flow: flow_id={}",
                            task.flow_id
                        );
                    }
                    Err(err) => {
                        common_telemetry::error!(err; "Failed to refill flow(id={}), will rebuild the flow with clean state", task.flow_id);

                        let flow_id = task.flow_id;
                        manager.remove_flow(flow_id).await?;
                        let info = flow_metadata_manager
                            .flow_info_manager()
                            .get(flow_id as u32)
                            .await
                            .map_err(BoxedError::new)
                            .context(ExternalSnafu)?
                            .context(FlowNotFoundSnafu { id: flow_id })?;
                        let args = CreateFlowArgs::from_flow_info(flow_id, info, true, true);
                        manager.create_flow(args).await?;
                    }
                }
                Ok(())
            });
            handles.push(handle);
        }
        Ok(handles)
    }
}

/// Task to refill flow with given table id and a time range
pub struct RefillTask {
    flow_id: FlowId,
    table_id: TableId,
    table_schema: RelationDesc,
    output_stream: SendableRecordBatchStream,
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
        invoker: &FrontendInvoker,
        flow_id: FlowId,
        table_id: TableId,
        time_range: Option<(common_time::Timestamp, common_time::Timestamp)>,
        time_col_name: &str,
        table_src: &TableSource,
    ) -> Result<RefillTask, Error> {
        let (table_name, table_schema) = table_src.get_table_name_schema(&table_id).await?;
        let all_col_names: BTreeSet<_> = table_schema
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

        // we don't need information from query context in this query so a default query context is enough
        let query_ctx = QueryContext::arc();

        let stmt = QueryLanguageParser::parse_sql(&sql, &query_ctx)
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;

        let stmt_exec = invoker.statement_executor();

        let output_data = stmt_exec
            .execute_stmt(stmt, query_ctx)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;

        let output_stream = QueryStream::try_from(output_data)?;
        let output_stream = output_stream.try_into_stream()?;

        Ok(RefillTask {
            flow_id,
            table_id,
            table_schema,
            output_stream,
        })
    }

    /// handle refill insert requests
    ///
    /// TODO(discord9): add a back pressure mechanism
    pub async fn handle_refill_inserts(
        &mut self,
        manager: FlowWorkerManagerRef,
    ) -> Result<(), Error> {
        while let Some(rb) = self.output_stream.next().await {
            let rb = match rb {
                Ok(rb) => rb,
                Err(err) => Err(BoxedError::new(err)).context(ExternalSnafu)?,
            };
            self.validate_schema(&rb)?;

            // send rb into flow node
            manager
                .node_context
                .read()
                .await
                .send_rb(self.table_id, rb)
                .await?;
        }
        Ok(())
    }

    /// validate that incoming batch's schema is the same as table schema(by comparing types&names)
    fn validate_schema(&self, rb: &RecordBatch) -> Result<(), Error> {
        let rb_schema = &rb.schema;
        let table_schema = &self.table_schema;
        if rb_schema.column_schemas().len() != table_schema.len()? {
            UnexpectedSnafu {
                reason: "rb schema len != table schema len",
            }
            .fail()?;
        }
        for (i, rb_col) in rb_schema.column_schemas().iter().enumerate() {
            let (rb_name, rb_ty) = (rb_col.name.as_str(), &rb_col.data_type);
            let (table_name, table_ty) = (
                table_schema.names[i].as_ref(),
                &table_schema.typ().column_types[i].scalar_type,
            );
            if Some(rb_name) != table_name.map(|c| c.as_str()) {
                UnexpectedSnafu {
                    reason: format!(
                        "incoming batch's schema name {} != expected table schema name {:?}",
                        rb_name, table_name
                    ),
                }
                .fail()?;
            }

            if rb_ty != table_ty {
                UnexpectedSnafu {
                    reason: format!(
                        "incoming batch's schema type {:?} != expected table schema type {:?}",
                        rb_ty, table_ty
                    ),
                }
                .fail()?;
            }
        }
        Ok(())
    }
}
