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

use crate::error::{self, InternalSnafu};
use crate::information_schema::Predicates;
use crate::system_schema::information_schema::InformationTable;

/// Column names of `information_schema.process_list`
const ID: &str = "id";
const DATABASE: &str = "database";
const QUERY: &str = "query";
const START_TIMESTAMP: &str = "start_timestamp";
const ELAPSED_TIME: &str = "elapsed_time";

/// `information_schema.process_list` table implementation that tracks running
/// queries in current cluster.
pub struct InformationSchemaProcessList {
    schema: SchemaRef,
    process_manager: Arc<ProcessManager>,
}

impl InformationSchemaProcessList {
    pub fn new(process_manager: Arc<ProcessManager>) -> Self {
        Self {
            schema: Self::schema(),
            process_manager,
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            ColumnSchema::new(ID, ConcreteDataType::uint64_datatype(), false),
            ColumnSchema::new(DATABASE, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(QUERY, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(
                START_TIMESTAMP,
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            ColumnSchema::new(
                ELAPSED_TIME,
                ConcreteDataType::duration_millisecond_datatype(),
                false,
            ),
        ]))
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

    fn to_stream(&self, request: ScanRequest) -> error::Result<SendableRecordBatchStream> {
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

/// Build running process list.
async fn make_process_list(
    process_manager: Arc<ProcessManager>,
    request: ScanRequest,
) -> error::Result<RecordBatch> {
    let predicates = Predicates::from_scan_request(&Some(request));
    let current_time = current_time_millis();
    let mut stream = Box::pin(
        process_manager
            .list_all_processes()
            .context(error::ListProcessSnafu)?
            .into_stream(),
    );

    let mut id_builder = UInt64VectorBuilder::with_capacity(8);
    let mut database_builder = StringVectorBuilder::with_capacity(8);
    let mut query_builder = StringVectorBuilder::with_capacity(8);
    let mut start_time_builder = TimestampMillisecondVectorBuilder::with_capacity(8);
    let mut elapsed_time_builder = DurationMillisecondVectorBuilder::with_capacity(8);

    let mut current_row = Vec::with_capacity(5);
    while let Some(process) = stream
        .next()
        .await
        .transpose()
        .context(error::ListProcessSnafu)?
    {
        process_to_row(process, current_time, &mut current_row);
        if predicates.eval(&current_row) {
            id_builder.push(current_row[0].1.as_u64());
            database_builder.push(current_row[1].1.as_string().as_deref());
            query_builder.push(current_row[2].1.as_string().as_deref());
            start_time_builder.push(current_row[3].1.as_timestamp().map(|t| t.value().into()));
            elapsed_time_builder.push(current_row[4].1.as_duration().map(|d| d.value().into()));
        }
    }

    let columns: Vec<VectorRef> = vec![
        Arc::new(id_builder.finish()),
        Arc::new(database_builder.finish()),
        Arc::new(query_builder.finish()),
        Arc::new(start_time_builder.finish()),
        Arc::new(elapsed_time_builder.finish()),
    ];

    RecordBatch::new(InformationSchemaProcessList::schema(), columns)
        .context(error::CreateRecordBatchSnafu)
}

// Convert [Process] structs to rows.
fn process_to_row(
    process: Process,
    current_time_ms: i64,
    current_row: &mut Vec<(&'static str, Value)>,
) {
    current_row.clear();
    current_row.push((ID, Value::UInt64(process.query_id())));
    current_row.push((
        DATABASE,
        Value::String(process.database().to_string().into()),
    ));
    current_row.push((
        QUERY,
        Value::String(process.query_string().to_string().into()),
    ));

    current_row.push((
        START_TIMESTAMP,
        Value::Timestamp(Timestamp::new_millisecond(
            process.query_start_timestamp_ms(),
        )),
    ));
    current_row.push((
        ELAPSED_TIME,
        Value::Duration(Duration::new_millisecond(
            current_time_ms - process.query_start_timestamp_ms(),
        )),
    ));
}
