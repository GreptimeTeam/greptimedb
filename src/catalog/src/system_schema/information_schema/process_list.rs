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

use std::sync::Arc;

use common_catalog::consts::INFORMATION_SCHEMA_PROCESS_LIST_TABLE_ID;
use common_error::ext::BoxedError;
use common_meta::key::process_list::{Process, ProcessManager};
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{RecordBatch, SendableRecordBatchStream};
use common_time::util::current_time_millis;
use common_time::{Duration, Timestamp};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter as DfRecordBatchStreamAdapter;
use datatypes::prelude::ConcreteDataType;
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::value::Value;
use datatypes::vectors::{
    DurationMillisecondVectorBuilder, StringVectorBuilder, TimestampMillisecondVectorBuilder,
    UInt64VectorBuilder, VectorRef,
};
use futures::StreamExt;
use snafu::ResultExt;
use store_api::storage::{ScanRequest, TableId};

use super::{InformationTable, Predicates};
use crate::error::{self, InternalSnafu};

pub struct InformationSchemaProcessList {
    schema: SchemaRef,
    process_manager: Arc<ProcessManager>,
}

impl InformationSchemaProcessList {
    #[allow(dead_code)]
    pub fn new(process_manager: Arc<ProcessManager>) -> Self {
        Self {
            schema: Self::schema(),
            process_manager,
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(
            vec![
                ("id", ConcreteDataType::uint64_datatype(), false),
                ("database", ConcreteDataType::string_datatype(), false),
                ("query", ConcreteDataType::string_datatype(), false),
                (
                    "start_timestamp_ms",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                (
                    "elapsed_time",
                    ConcreteDataType::duration_millisecond_datatype(),
                    false,
                ),
            ]
            .into_iter()
            .map(|(name, ty, nullable)| ColumnSchema::new(name, ty, nullable))
            .collect(),
        ))
    }
}

impl InformationTable for InformationSchemaProcessList {
    fn table_id(&self) -> TableId {
        INFORMATION_SCHEMA_PROCESS_LIST_TABLE_ID
    }

    fn table_name(&self) -> &'static str {
        "process_list"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn to_stream(
        &self,
        request: store_api::storage::ScanRequest,
    ) -> error::Result<SendableRecordBatchStream> {
        let schema = self.schema.arrow_schema().clone();
        let process_manager = self.process_manager.clone();
        let stream = Box::pin(DfRecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move {
                make_process_list(process_manager, request)
                    .await
                    .map(RecordBatch::into_df_record_batch)
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))
            }),
        ));

        Ok(Box::pin(
            RecordBatchStreamAdapter::try_new(stream)
                .map_err(BoxedError::new)
                .context(InternalSnafu)?,
        ))
    }
}

async fn make_process_list(
    process_manager: Arc<ProcessManager>,
    request: ScanRequest,
) -> error::Result<RecordBatch> {
    let predicates = Predicates::from_scan_request(&Some(request));
    let current_time = current_time_millis();
    let mut stream = Box::pin(process_manager.list_all_processes().unwrap().into_stream());

    let mut rows = Vec::new();
    while let Some(process) = stream.next().await.transpose().unwrap() {
        let row = process_to_row(process, current_time);
        if predicates.eval(&row) {
            rows.push(row);
        }
    }
    Ok(rows_to_record_batch(rows).unwrap())
}

fn process_to_row(process: Process, current_time: i64) -> Vec<(&'static str, Value)> {
    vec![
        ("id", Value::UInt64(process.query_id())),
        (
            "database",
            Value::String(process.database().to_string().into()),
        ),
        (
            "query",
            Value::String(process.query_string().to_string().into()),
        ),
        (
            "start_timestamp_ms",
            Value::Timestamp(Timestamp::new_millisecond(
                process.query_start_timestamp_ms(),
            )),
        ),
        (
            "elapsed_time",
            Value::Duration(Duration::new_millisecond(
                current_time - process.query_start_timestamp_ms(),
            )),
        ),
    ]
}

fn rows_to_record_batch(rows: Vec<Vec<(&'static str, Value)>>) -> error::Result<RecordBatch> {
    let mut id_builder = UInt64VectorBuilder::with_capacity(rows.len());
    let mut database_builder = StringVectorBuilder::with_capacity(rows.len());
    let mut query_builder = StringVectorBuilder::with_capacity(rows.len());
    let mut start_time_builder = TimestampMillisecondVectorBuilder::with_capacity(rows.len());
    let mut elapsed_time_builder = DurationMillisecondVectorBuilder::with_capacity(rows.len());

    for row in rows {
        id_builder.push(row[0].1.as_u64());
        database_builder.push(row[1].1.as_string().as_deref());
        query_builder.push(row[2].1.as_string().as_deref());
        start_time_builder.push(row[3].1.as_timestamp().map(|t| t.value().into()));
        elapsed_time_builder.push(row[4].1.as_duration().map(|d| d.value().into()));
    }

    let columns: Vec<VectorRef> = vec![
        Arc::new(id_builder.finish()),
        Arc::new(database_builder.finish()),
        Arc::new(query_builder.finish()),
        Arc::new(start_time_builder.finish()),
        Arc::new(elapsed_time_builder.finish()),
    ];

    Ok(RecordBatch::new(InformationSchemaProcessList::schema(), columns).unwrap())
}
