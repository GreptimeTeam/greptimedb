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

use common_catalog::consts::INFORMATION_SCHEMA_FLOW_TABLE_ID;
use common_error::ext::BoxedError;
use common_meta::key::flow::flow_info::FlowInfoValue;
use common_meta::key::flow::FlowMetadataManager;
use common_meta::key::FlowId;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{DfSendableRecordBatchStream, RecordBatch, SendableRecordBatchStream};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter as DfRecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream as DfPartitionStream;
use datatypes::prelude::ConcreteDataType as CDT;
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::value::Value;
use datatypes::vectors::{Int64VectorBuilder, StringVectorBuilder, UInt32VectorBuilder, VectorRef};
use futures::TryStreamExt;
use snafu::{OptionExt, ResultExt};
use store_api::storage::{ScanRequest, TableId};

use crate::error::{
    CreateRecordBatchSnafu, FlowInfoNotFoundSnafu, InternalSnafu, JsonSnafu, ListFlowsSnafu, Result,
};
use crate::information_schema::{InformationTable, Predicates, FLOWS};

const INIT_CAPACITY: usize = 42;

// rows of information_schema.flows
// pk is (flow_name, flow_id, catalog_name)
pub const FLOW_NAME: &str = "flow_name";
pub const FLOW_ID: &str = "flow_id";
pub const CATALOG_NAME: &str = "catalog_name";
pub const RAW_SQL: &str = "raw_sql";
pub const COMMENT: &str = "comment";
pub const EXPIRE_AFTER: &str = "expire_after";
pub const SOURCE_TABLE_IDS: &str = "source_table_ids";
pub const SINK_TABLE_NAME: &str = "sink_table_name";
pub const FLOWNODE_IDS: &str = "flownode_ids";
pub const OPTIONS: &str = "options";

pub(super) struct InformationSchemaFlows {
    schema: SchemaRef,
    catalog_name: String,
    flow_metadata_manager: Arc<FlowMetadataManager>,
}

impl InformationSchemaFlows {
    pub(super) fn new(
        catalog_name: String,
        flow_metadata_manager: Arc<FlowMetadataManager>,
    ) -> Self {
        Self {
            schema: Self::schema(),
            catalog_name,
            flow_metadata_manager,
        }
    }

    /// for complex fields, it will be serialized to json string for now
    /// TODO(discord9): use a better way to store complex fields like json type
    pub(crate) fn schema() -> SchemaRef {
        Arc::new(Schema::new(
            vec![
                (FLOW_NAME, CDT::string_datatype(), false),
                (FLOW_ID, CDT::uint32_datatype(), false),
                (CATALOG_NAME, CDT::string_datatype(), false),
                (RAW_SQL, CDT::string_datatype(), false),
                (COMMENT, CDT::string_datatype(), true),
                (EXPIRE_AFTER, CDT::int64_datatype(), true),
                (SOURCE_TABLE_IDS, CDT::string_datatype(), true),
                (SINK_TABLE_NAME, CDT::string_datatype(), false),
                (FLOWNODE_IDS, CDT::string_datatype(), true),
                (OPTIONS, CDT::string_datatype(), true),
            ]
            .into_iter()
            .map(|(name, ty, nullable)| ColumnSchema::new(name, ty, nullable))
            .collect(),
        ))
    }

    fn builder(&self) -> InformationSchemaFlowsBuilder {
        InformationSchemaFlowsBuilder::new(
            self.schema.clone(),
            self.catalog_name.clone(),
            &self.flow_metadata_manager,
        )
    }
}

impl InformationTable for InformationSchemaFlows {
    fn table_id(&self) -> TableId {
        INFORMATION_SCHEMA_FLOW_TABLE_ID
    }

    fn table_name(&self) -> &'static str {
        FLOWS
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
                    .make_views(Some(request))
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(|err| {
                        datafusion::error::DataFusionError::External(format!("{err:?}").into())
                    })
            }),
        ));
        Ok(Box::pin(
            RecordBatchStreamAdapter::try_new(stream)
                .map_err(BoxedError::new)
                .context(InternalSnafu)?,
        ))
    }
}

/// Builds the `information_schema.VIEWS` table row by row
///
/// columns are based on [`FlowInfoValue`]
struct InformationSchemaFlowsBuilder {
    schema: SchemaRef,
    catalog_name: String,
    flow_metadata_manager: Arc<FlowMetadataManager>,

    flow_names: StringVectorBuilder,
    flow_ids: UInt32VectorBuilder,
    catalog_names: StringVectorBuilder,
    raw_sqls: StringVectorBuilder,
    comments: StringVectorBuilder,
    expire_afters: Int64VectorBuilder,
    source_table_id_groups: StringVectorBuilder,
    sink_table_names: StringVectorBuilder,
    flownode_id_groups: StringVectorBuilder,
    option_groups: StringVectorBuilder,
}

impl InformationSchemaFlowsBuilder {
    fn new(
        schema: SchemaRef,
        catalog_name: String,
        flow_metadata_manager: &Arc<FlowMetadataManager>,
    ) -> Self {
        Self {
            schema,
            catalog_name,
            flow_metadata_manager: flow_metadata_manager.clone(),

            flow_names: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            flow_ids: UInt32VectorBuilder::with_capacity(INIT_CAPACITY),
            catalog_names: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            raw_sqls: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            comments: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            expire_afters: Int64VectorBuilder::with_capacity(INIT_CAPACITY),
            source_table_id_groups: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            sink_table_names: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            flownode_id_groups: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            option_groups: StringVectorBuilder::with_capacity(INIT_CAPACITY),
        }
    }

    /// Construct the `information_schema.views` virtual table
    async fn make_views(&mut self, request: Option<ScanRequest>) -> Result<RecordBatch> {
        let catalog_name = self.catalog_name.clone();
        let predicates = Predicates::from_scan_request(&request);

        let flow_info_manager = self.flow_metadata_manager.clone();

        // TODO(discord9): use `AsyncIterator` once it's stable-ish
        let mut stream = flow_info_manager
            .flow_name_manager()
            .flow_names(&catalog_name)
            .await;

        while let Some((flow_name, flow_id)) = stream
            .try_next()
            .await
            .map_err(BoxedError::new)
            .context(ListFlowsSnafu {
                catalog: catalog_name.to_string(),
            })?
        {
            let flow_info = flow_info_manager
                .flow_info_manager()
                .get(flow_id.flow_id())
                .await
                .map_err(BoxedError::new)
                .context(InternalSnafu)?
                .context(FlowInfoNotFoundSnafu {
                    catalog_name: catalog_name.to_string(),
                    flow_name: flow_name.to_string(),
                })?;
            self.add_view(&predicates, flow_id.flow_id(), flow_info)?;
        }

        self.finish()
    }

    fn add_view(
        &mut self,
        predicates: &Predicates,
        flow_id: FlowId,
        flow_info: FlowInfoValue,
    ) -> Result<()> {
        let row = [
            (FLOW_NAME, &Value::from(flow_info.flow_name().to_string())),
            (FLOW_ID, &Value::from(flow_id)),
            (
                CATALOG_NAME,
                &Value::from(flow_info.catalog_name().to_string()),
            ),
        ];
        if !predicates.eval(&row) {
            return Ok(());
        }
        self.flow_names.push(Some(flow_info.flow_name()));
        self.flow_ids.push(Some(flow_id));
        self.catalog_names.push(Some(flow_info.catalog_name()));
        self.raw_sqls.push(Some(flow_info.raw_sql()));
        self.comments.push(Some(flow_info.comment()));
        self.expire_afters.push(flow_info.expire_after());
        self.source_table_id_groups.push(Some(
            &serde_json::to_string(flow_info.source_table_ids())
                .map_err(|raw| JsonSnafu { raw }.build())?,
        ));
        self.sink_table_names
            .push(Some(&flow_info.sink_table_name().to_string()));
        self.flownode_id_groups.push(Some(
            &serde_json::to_string(flow_info.flownode_ids())
                .map_err(|raw| JsonSnafu { raw }.build())?,
        ));
        self.option_groups.push(Some(
            &serde_json::to_string(flow_info.options()).map_err(|raw| JsonSnafu { raw }.build())?,
        ));

        Ok(())
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let columns: Vec<VectorRef> = vec![
            Arc::new(self.flow_names.finish()),
            Arc::new(self.flow_ids.finish()),
            Arc::new(self.catalog_names.finish()),
            Arc::new(self.raw_sqls.finish()),
            Arc::new(self.comments.finish()),
            Arc::new(self.expire_afters.finish()),
            Arc::new(self.source_table_id_groups.finish()),
            Arc::new(self.sink_table_names.finish()),
            Arc::new(self.flownode_id_groups.finish()),
            Arc::new(self.option_groups.finish()),
        ];
        RecordBatch::new(self.schema.clone(), columns).context(CreateRecordBatchSnafu)
    }
}

impl DfPartitionStream for InformationSchemaFlows {
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
                    .make_views(None)
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ))
    }
}
