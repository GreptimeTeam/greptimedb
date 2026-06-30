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

use common_catalog::consts::INFORMATION_SCHEMA_FLOW_STATISTICS_TABLE_ID;
use common_error::ext::BoxedError;
use common_meta::key::FlowId;
use common_meta::key::flow::FlowMetadataManager;
use common_meta::key::flow::flow_state::FlowStat;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{DfSendableRecordBatchStream, RecordBatch, SendableRecordBatchStream};
use common_time::util::current_time_millis;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter as DfRecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream as DfPartitionStream;
use datatypes::prelude::ConcreteDataType as CDT;
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::timestamp::TimestampMillisecond;
use datatypes::value::Value;
use datatypes::vectors::{
    Int64VectorBuilder, StringVectorBuilder, TimestampMillisecondVectorBuilder,
    UInt32VectorBuilder, UInt64VectorBuilder, VectorRef,
};
use futures::TryStreamExt;
use snafu::{OptionExt, ResultExt};
use store_api::storage::{ScanRequest, TableId};

use crate::CatalogManager;
use crate::error::{CreateRecordBatchSnafu, InternalSnafu, ListFlowsSnafu, Result};
use crate::information_schema::{FLOW_STATISTICS, Predicates};
use crate::system_schema::information_schema::InformationTable;
use crate::system_schema::utils;

const INIT_CAPACITY: usize = 42;

// rows of information_schema.flow_statistics
pub const FLOW_ID: &str = "flow_id";
pub const FLOW_NAME: &str = "flow_name";
pub const START_TIME: &str = "start_time";
pub const LAST_EXECUTION_TIME: &str = "last_execution_time";
pub const UPTIME_SECONDS: &str = "uptime_seconds";
pub const STATE_SIZE: &str = "state_size";
pub const PROCESSED_ROWS: &str = "processed_rows";
pub const LAST_ERRORS: &str = "last_errors";

/// The `information_schema.flow_statistics` provides runtime statistics about flows.
#[derive(Debug)]
pub(super) struct InformationSchemaFlowStatistics {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,
    flow_metadata_manager: Arc<FlowMetadataManager>,
}

impl InformationSchemaFlowStatistics {
    pub(super) fn new(
        catalog_name: String,
        catalog_manager: Weak<dyn CatalogManager>,
        flow_metadata_manager: Arc<FlowMetadataManager>,
    ) -> Self {
        Self {
            schema: Self::schema(),
            catalog_name,
            catalog_manager,
            flow_metadata_manager,
        }
    }

    pub(crate) fn schema() -> SchemaRef {
        Arc::new(Schema::new(
            vec![
                (FLOW_ID, CDT::uint32_datatype(), false),
                (FLOW_NAME, CDT::string_datatype(), false),
                (START_TIME, CDT::timestamp_millisecond_datatype(), true),
                (
                    LAST_EXECUTION_TIME,
                    CDT::timestamp_millisecond_datatype(),
                    true,
                ),
                (UPTIME_SECONDS, CDT::int64_datatype(), true),
                (STATE_SIZE, CDT::uint64_datatype(), true),
                (PROCESSED_ROWS, CDT::uint64_datatype(), true),
                (LAST_ERRORS, CDT::string_datatype(), true),
            ]
            .into_iter()
            .map(|(name, ty, nullable)| ColumnSchema::new(name, ty, nullable))
            .collect(),
        ))
    }

    fn builder(&self) -> InformationSchemaFlowStatisticsBuilder {
        InformationSchemaFlowStatisticsBuilder::new(
            self.schema.clone(),
            self.catalog_name.clone(),
            self.catalog_manager.clone(),
            &self.flow_metadata_manager,
        )
    }
}

impl InformationTable for InformationSchemaFlowStatistics {
    fn table_id(&self) -> TableId {
        INFORMATION_SCHEMA_FLOW_STATISTICS_TABLE_ID
    }

    fn table_name(&self) -> &'static str {
        FLOW_STATISTICS
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
                    .make_flow_statistics(Some(request))
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(|err| datafusion::error::DataFusionError::External(Box::new(err)))
            }),
        ));
        Ok(Box::pin(
            RecordBatchStreamAdapter::try_new(stream)
                .map_err(BoxedError::new)
                .context(InternalSnafu)?,
        ))
    }
}

/// Builds the `information_schema.flow_statistics` table row by row.
struct InformationSchemaFlowStatisticsBuilder {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,
    flow_metadata_manager: Arc<FlowMetadataManager>,

    flow_ids: UInt32VectorBuilder,
    flow_names: StringVectorBuilder,
    start_times: TimestampMillisecondVectorBuilder,
    last_execution_times: TimestampMillisecondVectorBuilder,
    uptime_seconds: Int64VectorBuilder,
    state_sizes: UInt64VectorBuilder,
    processed_rows: UInt64VectorBuilder,
    last_errors: StringVectorBuilder,
}

impl InformationSchemaFlowStatisticsBuilder {
    fn new(
        schema: SchemaRef,
        catalog_name: String,
        catalog_manager: Weak<dyn CatalogManager>,
        flow_metadata_manager: &Arc<FlowMetadataManager>,
    ) -> Self {
        Self {
            schema,
            catalog_name,
            catalog_manager,
            flow_metadata_manager: flow_metadata_manager.clone(),

            flow_ids: UInt32VectorBuilder::with_capacity(INIT_CAPACITY),
            flow_names: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            start_times: TimestampMillisecondVectorBuilder::with_capacity(INIT_CAPACITY),
            last_execution_times: TimestampMillisecondVectorBuilder::with_capacity(INIT_CAPACITY),
            uptime_seconds: Int64VectorBuilder::with_capacity(INIT_CAPACITY),
            state_sizes: UInt64VectorBuilder::with_capacity(INIT_CAPACITY),
            processed_rows: UInt64VectorBuilder::with_capacity(INIT_CAPACITY),
            last_errors: StringVectorBuilder::with_capacity(INIT_CAPACITY),
        }
    }

    /// Construct the `information_schema.flow_statistics` virtual table.
    async fn make_flow_statistics(&mut self, request: Option<ScanRequest>) -> Result<RecordBatch> {
        let catalog_name = self.catalog_name.clone();
        let predicates = Predicates::from_scan_request(&request);

        let flow_info_manager = self.flow_metadata_manager.clone();

        let mut stream = flow_info_manager
            .flow_name_manager()
            .flow_names(&catalog_name)
            .await;

        let flow_stat = {
            let information_extension = utils::information_extension(&self.catalog_manager)?;
            information_extension.flow_stats().await?
        };

        while let Some((flow_name, flow_id)) = stream
            .try_next()
            .await
            .map_err(BoxedError::new)
            .context(ListFlowsSnafu {
                catalog: &catalog_name,
            })?
        {
            self.add_flow_statistic(&predicates, flow_id.flow_id(), &flow_name, &flow_stat);
        }

        self.finish()
    }

    fn add_flow_statistic(
        &mut self,
        predicates: &Predicates,
        flow_id: FlowId,
        flow_name: &str,
        flow_stat: &Option<FlowStat>,
    ) {
        let row = [
            (FLOW_ID, &Value::from(flow_id)),
            (FLOW_NAME, &Value::from(flow_name.to_string())),
        ];
        if !predicates.eval(&row) {
            return;
        }

        let start_time = flow_stat
            .as_ref()
            .and_then(|stat| stat.start_time_map.get(&flow_id).copied());

        self.flow_ids.push(Some(flow_id));
        self.flow_names.push(Some(flow_name));
        self.start_times
            .push(start_time.map(TimestampMillisecond::new));
        self.last_execution_times
            .push(flow_stat.as_ref().and_then(|stat| {
                stat.last_exec_time_map
                    .get(&flow_id)
                    .map(|v| TimestampMillisecond::new(*v))
            }));
        self.uptime_seconds
            .push(start_time.map(|start| (current_time_millis() - start) / 1000));
        self.state_sizes.push(
            flow_stat
                .as_ref()
                .and_then(|stat| stat.state_size.get(&flow_id).map(|v| *v as u64)),
        );
        self.processed_rows.push(
            flow_stat
                .as_ref()
                .and_then(|stat| stat.processed_rows_map.get(&flow_id).copied()),
        );
        self.last_errors.push(
            flow_stat
                .as_ref()
                .and_then(|stat| stat.error_map.get(&flow_id))
                .filter(|errors| !errors.is_empty())
                .map(|errors| errors.join(", "))
                .as_deref(),
        );
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let columns: Vec<VectorRef> = vec![
            Arc::new(self.flow_ids.finish()),
            Arc::new(self.flow_names.finish()),
            Arc::new(self.start_times.finish()),
            Arc::new(self.last_execution_times.finish()),
            Arc::new(self.uptime_seconds.finish()),
            Arc::new(self.state_sizes.finish()),
            Arc::new(self.processed_rows.finish()),
            Arc::new(self.last_errors.finish()),
        ];
        RecordBatch::new(self.schema.clone(), columns).context(CreateRecordBatchSnafu)
    }
}

impl DfPartitionStream for InformationSchemaFlowStatistics {
    fn schema(&self) -> &arrow_schema::SchemaRef {
        self.schema.arrow_schema()
    }

    fn execute(&self, _: Arc<TaskContext>) -> DfSendableRecordBatchStream {
        let schema: Arc<arrow_schema::Schema> = self.schema.arrow_schema().clone();
        let mut builder = self.builder();
        Box::pin(DfRecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move {
                builder
                    .make_flow_statistics(None)
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ))
    }
}
