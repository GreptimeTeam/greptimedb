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

use std::sync::{Arc, Weak};

use api::v1::meta::{ProcedureMeta, ProcedureStatus};
use arrow_schema::SchemaRef as ArrowSchemaRef;
use common_catalog::consts::INFORMATION_SCHEMA_PROCEDURE_INFO_TABLE_ID;
use common_config::Mode;
use common_error::ext::BoxedError;
use common_meta::ddl::{ExecutorContext, ProcedureExecutor};
use common_meta::rpc::procedure;
use common_procedure::{ProcedureInfo, ProcedureState};
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{RecordBatch, SendableRecordBatchStream};
use common_time::timestamp::Timestamp;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter as DfRecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream as DfPartitionStream;
use datafusion::physical_plan::SendableRecordBatchStream as DfSendableRecordBatchStream;
use datatypes::prelude::{ConcreteDataType, ScalarVectorBuilder, VectorRef};
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::timestamp::TimestampMillisecond;
use datatypes::value::Value;
use datatypes::vectors::{StringVectorBuilder, TimestampMillisecondVectorBuilder};
use snafu::ResultExt;
use store_api::storage::{ScanRequest, TableId};

use super::PROCEDURE_INFO;
use crate::error::{
    ConvertProtoDataSnafu, CreateRecordBatchSnafu, GetProcedureClientSnafu, InternalSnafu,
    ListProceduresSnafu, ProcedureIdNotFoundSnafu, Result,
};
use crate::system_schema::information_schema::{InformationTable, Predicates};
use crate::system_schema::utils;
use crate::CatalogManager;

const PROCEDURE_ID: &str = "procedure_id";
const PROCEDURE_TYPE: &str = "procedure_type";
const START_TIME: &str = "start_time";
const END_TIME: &str = "end_time";
const STATUS: &str = "status";
const LOCK_KEYS: &str = "lock_keys";

const INIT_CAPACITY: usize = 42;

/// The `PROCEDURE_INFO` table provides information about the current procedure information of the cluster.
///
/// - `procedure_id`: the unique identifier of the procedure.
/// - `procedure_name`: the name of the procedure.
/// - `start_time`: the starting execution time of the procedure.
/// - `end_time`: the ending execution time of the procedure.
/// - `status`: the status of the procedure.
/// - `lock_keys`: the lock keys of the procedure.
///
pub(super) struct InformationSchemaProcedureInfo {
    schema: SchemaRef,
    catalog_manager: Weak<dyn CatalogManager>,
}

impl InformationSchemaProcedureInfo {
    pub(super) fn new(catalog_manager: Weak<dyn CatalogManager>) -> Self {
        Self {
            schema: Self::schema(),
            catalog_manager,
        }
    }

    pub(crate) fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            ColumnSchema::new(PROCEDURE_ID, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(PROCEDURE_TYPE, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(
                START_TIME,
                ConcreteDataType::timestamp_millisecond_datatype(),
                true,
            ),
            ColumnSchema::new(
                END_TIME,
                ConcreteDataType::timestamp_millisecond_datatype(),
                true,
            ),
            ColumnSchema::new(STATUS, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(LOCK_KEYS, ConcreteDataType::string_datatype(), true),
        ]))
    }

    fn builder(&self) -> InformationSchemaProcedureInfoBuilder {
        InformationSchemaProcedureInfoBuilder::new(
            self.schema.clone(),
            self.catalog_manager.clone(),
        )
    }
}

impl InformationTable for InformationSchemaProcedureInfo {
    fn table_id(&self) -> TableId {
        INFORMATION_SCHEMA_PROCEDURE_INFO_TABLE_ID
    }

    fn table_name(&self) -> &'static str {
        PROCEDURE_INFO
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn to_stream(&self, request: ScanRequest) -> Result<SendableRecordBatchStream> {
        let schema = self.schema.arrow_schema().clone();
        let mut builder = self.builder();
        let stream = Box::pin(DfRecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move {
                builder
                    .make_procedure_info(Some(request))
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ));
        Ok(Box::pin(
            RecordBatchStreamAdapter::try_new(stream)
                .map_err(BoxedError::new)
                .context(InternalSnafu)?,
        ))
    }
}

struct InformationSchemaProcedureInfoBuilder {
    schema: SchemaRef,
    catalog_manager: Weak<dyn CatalogManager>,

    procedure_ids: StringVectorBuilder,
    procedure_types: StringVectorBuilder,
    start_times: TimestampMillisecondVectorBuilder,
    end_times: TimestampMillisecondVectorBuilder,
    statuses: StringVectorBuilder,
    lock_keys: StringVectorBuilder,
}

impl InformationSchemaProcedureInfoBuilder {
    fn new(schema: SchemaRef, catalog_manager: Weak<dyn CatalogManager>) -> Self {
        Self {
            schema,
            catalog_manager,
            procedure_ids: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            procedure_types: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            start_times: TimestampMillisecondVectorBuilder::with_capacity(INIT_CAPACITY),
            end_times: TimestampMillisecondVectorBuilder::with_capacity(INIT_CAPACITY),
            statuses: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            lock_keys: StringVectorBuilder::with_capacity(INIT_CAPACITY),
        }
    }

    /// Construct the `information_schema.procedure_info` virtual table
    async fn make_procedure_info(&mut self, request: Option<ScanRequest>) -> Result<RecordBatch> {
        let predicates = Predicates::from_scan_request(&request);
        let mode = utils::running_mode(&self.catalog_manager)?.unwrap_or(Mode::Standalone);
        match mode {
            Mode::Standalone => {
                if let Some(procedure_manager) = utils::procedure_manager(&self.catalog_manager)? {
                    let procedures = procedure_manager
                        .list_procedures()
                        .await
                        .map_err(BoxedError::new)
                        .context(ListProceduresSnafu)?;
                    for procedure in procedures {
                        let (state, _) = procedure::procedure_state_to_pb_state(&procedure.state);
                        self.add_procedure(&predicates, procedure, state.as_str_name());
                    }
                } else {
                    return GetProcedureClientSnafu { mode: "standalone" }.fail();
                }
            }
            Mode::Distributed => {
                if let Some(meta_client) = utils::meta_client(&self.catalog_manager)? {
                    let procedures = meta_client
                        .list_procedures(&ExecutorContext::default())
                        .await
                        .map_err(BoxedError::new)
                        .context(ListProceduresSnafu)?;
                    for procedure in procedures.procedures {
                        self.add_procedure_info(&predicates, procedure)?;
                    }
                } else {
                    return GetProcedureClientSnafu {
                        mode: "distributed",
                    }
                    .fail();
                }
            }
        };

        self.finish()
    }

    fn add_procedure(
        &mut self,
        predicates: &Predicates,
        procedure_info: ProcedureInfo,
        status: &str,
    ) {
        let ProcedureInfo {
            id,
            type_name,
            start_time_ms,
            end_time_ms,
            lock_keys,
            ..
        } = procedure_info;
        let pid = id.to_string();
        let start_time = TimestampMillisecond(Timestamp::new_millisecond(start_time_ms));
        let end_time = TimestampMillisecond(Timestamp::new_millisecond(end_time_ms));
        let lock_keys = lock_keys.join(",");

        let row = [
            (PROCEDURE_ID, &Value::from(pid.clone())),
            (PROCEDURE_TYPE, &Value::from(type_name.clone())),
            (START_TIME, &Value::from(start_time)),
            (END_TIME, &Value::from(end_time)),
            (STATUS, &Value::from(status.to_string())),
            (LOCK_KEYS, &Value::from(lock_keys.clone())),
        ];
        if !predicates.eval(&row) {
            return;
        }
        self.procedure_ids.push(Some(&pid));
        self.procedure_types.push(Some(&type_name));
        self.start_times.push(Some(start_time));
        self.end_times.push(Some(end_time));
        self.statuses.push(Some(status));
        self.lock_keys.push(Some(&lock_keys));
    }

    fn add_procedure_info(
        &mut self,
        predicates: &Predicates,
        procedure: ProcedureMeta,
    ) -> Result<()> {
        let pid = match procedure.id {
            Some(pid) => pid,
            None => return ProcedureIdNotFoundSnafu {}.fail(),
        };
        let pid = procedure::pb_pid_to_pid(&pid)
            .map_err(BoxedError::new)
            .context(ConvertProtoDataSnafu)?;
        let status = ProcedureStatus::try_from(procedure.status)
            .map(|v| v.as_str_name())
            .unwrap_or("Unknown");
        let procedure_info = ProcedureInfo {
            id: pid,
            type_name: procedure.type_name,
            start_time_ms: procedure.start_time_ms,
            end_time_ms: procedure.end_time_ms,
            state: ProcedureState::Running,
            lock_keys: procedure.lock_keys,
        };
        self.add_procedure(predicates, procedure_info, status);
        Ok(())
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let columns: Vec<VectorRef> = vec![
            Arc::new(self.procedure_ids.finish()),
            Arc::new(self.procedure_types.finish()),
            Arc::new(self.start_times.finish()),
            Arc::new(self.end_times.finish()),
            Arc::new(self.statuses.finish()),
            Arc::new(self.lock_keys.finish()),
        ];
        RecordBatch::new(self.schema.clone(), columns).context(CreateRecordBatchSnafu)
    }
}

impl DfPartitionStream for InformationSchemaProcedureInfo {
    fn schema(&self) -> &ArrowSchemaRef {
        self.schema.arrow_schema()
    }

    fn execute(&self, _: Arc<TaskContext>) -> DfSendableRecordBatchStream {
        let schema = self.schema.arrow_schema().clone();
        let mut builder = self.builder();
        Box::pin(DfRecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move {
                builder
                    .make_procedure_info(None)
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ))
    }
}
