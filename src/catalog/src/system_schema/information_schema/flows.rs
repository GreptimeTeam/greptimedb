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

use common_catalog::consts::INFORMATION_SCHEMA_FLOW_TABLE_ID;
use common_error::ext::BoxedError;
use common_meta::key::FlowId;
use common_meta::key::flow::FlowMetadataManager;
use common_meta::key::flow::flow_info::FlowInfoValue;
use common_meta::key::flow::flow_state::FlowStat;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{DfSendableRecordBatchStream, RecordBatch, SendableRecordBatchStream};
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
use sql::ast::Ident;
use sql::dialect::GreptimeDbDialect;
use sql::parser::ParserContext;
use sql::statements::create::{CreateFlow, SqlOrTql};
use sql::statements::statement::Statement;
use store_api::storage::{ScanRequest, TableId};

use crate::CatalogManager;
use crate::error::{
    CreateRecordBatchSnafu, FlowInfoNotFoundSnafu, InternalSnafu, JsonSnafu, ListFlowsSnafu,
    Result, UpgradeWeakCatalogManagerRefSnafu,
};
use crate::information_schema::{FLOWS, Predicates};
use crate::system_schema::information_schema::InformationTable;
use crate::system_schema::utils;

const INIT_CAPACITY: usize = 42;

// rows of information_schema.flows
// pk is (flow_name, flow_id, table_catalog)
pub const FLOW_NAME: &str = "flow_name";
pub const FLOW_ID: &str = "flow_id";
pub const STATE_SIZE: &str = "state_size";
pub const TABLE_CATALOG: &str = "table_catalog";
pub const FLOW_DEFINITION: &str = "flow_definition";
pub const COMMENT: &str = "comment";
pub const EXPIRE_AFTER: &str = "expire_after";
pub const SOURCE_TABLE_IDS: &str = "source_table_ids";
pub const SINK_TABLE_NAME: &str = "sink_table_name";
pub const FLOWNODE_IDS: &str = "flownode_ids";
pub const OPTIONS: &str = "options";
pub const CREATED_TIME: &str = "created_time";
pub const UPDATED_TIME: &str = "updated_time";
pub const LAST_EXECUTION_TIME: &str = "last_execution_time";
pub const SOURCE_TABLE_NAMES: &str = "source_table_names";

/// The `information_schema.flows` to provides information about flows in databases.
#[derive(Debug)]
pub(super) struct InformationSchemaFlows {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,
    flow_metadata_manager: Arc<FlowMetadataManager>,
}

impl InformationSchemaFlows {
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

    /// for complex fields(including [`SOURCE_TABLE_IDS`], [`FLOWNODE_IDS`] and [`OPTIONS`]), it will be serialized to json string for now
    /// TODO(discord9): use a better way to store complex fields like json type
    pub(crate) fn schema() -> SchemaRef {
        Arc::new(Schema::new(
            vec![
                (FLOW_NAME, CDT::string_datatype(), false),
                (FLOW_ID, CDT::uint32_datatype(), false),
                (STATE_SIZE, CDT::uint64_datatype(), true),
                (TABLE_CATALOG, CDT::string_datatype(), false),
                (FLOW_DEFINITION, CDT::string_datatype(), false),
                (COMMENT, CDT::string_datatype(), true),
                (EXPIRE_AFTER, CDT::int64_datatype(), true),
                (SOURCE_TABLE_IDS, CDT::string_datatype(), true),
                (SINK_TABLE_NAME, CDT::string_datatype(), false),
                (FLOWNODE_IDS, CDT::string_datatype(), true),
                (OPTIONS, CDT::string_datatype(), true),
                (CREATED_TIME, CDT::timestamp_millisecond_datatype(), false),
                (UPDATED_TIME, CDT::timestamp_millisecond_datatype(), false),
                (
                    LAST_EXECUTION_TIME,
                    CDT::timestamp_millisecond_datatype(),
                    true,
                ),
                (SOURCE_TABLE_NAMES, CDT::string_datatype(), true),
            ]
            .into_iter()
            .map(|(name, ty, nullable)| ColumnSchema::new(name, ty, nullable))
            .collect(),
        ))
    }

    /// Generates the CREATE FLOW statement for the flow_definition column
    pub(crate) fn generate_show_create_flow(flow_info: &FlowInfoValue) -> Result<String> {
        let mut parser_ctx = ParserContext::new(&GreptimeDbDialect {}, flow_info.raw_sql())
            .map_err(BoxedError::new)
            .context(InternalSnafu)?;

        let query = parser_ctx
            .parse_statement()
            .map_err(BoxedError::new)
            .context(InternalSnafu)?;

        let raw_query = match &query {
            Statement::Tql(_) => flow_info.raw_sql().clone(),
            _ => query.to_string(),
        };

        let query = Box::new(
            SqlOrTql::try_from_statement(query, &raw_query)
                .map_err(BoxedError::new)
                .context(InternalSnafu)?,
        );

        let comment = if flow_info.comment().is_empty() {
            None
        } else {
            Some(flow_info.comment().clone())
        };

        let stmt = CreateFlow {
            flow_name: sql::ast::ObjectName::from(vec![Ident::new(flow_info.flow_name())]),
            sink_table_name: sql::ast::ObjectName::from(vec![
                Ident::new(&flow_info.sink_table_name().schema_name),
                Ident::new(&flow_info.sink_table_name().table_name),
            ]),
            or_replace: false,
            if_not_exists: true,
            expire_after: flow_info.expire_after(),
            eval_interval: flow_info.eval_interval(),
            comment,
            query,
        };

        Ok(stmt.to_string())
    }

    fn builder(&self) -> InformationSchemaFlowsBuilder {
        InformationSchemaFlowsBuilder::new(
            self.schema.clone(),
            self.catalog_name.clone(),
            self.catalog_manager.clone(),
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
                    .make_flows(Some(request))
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

/// Builds the `information_schema.FLOWS` table row by row
///
/// columns are based on [`FlowInfoValue`]
struct InformationSchemaFlowsBuilder {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,
    flow_metadata_manager: Arc<FlowMetadataManager>,

    flow_names: StringVectorBuilder,
    flow_ids: UInt32VectorBuilder,
    state_sizes: UInt64VectorBuilder,
    table_catalogs: StringVectorBuilder,
    raw_sqls: StringVectorBuilder,
    comments: StringVectorBuilder,
    expire_afters: Int64VectorBuilder,
    source_table_id_groups: StringVectorBuilder,
    sink_table_names: StringVectorBuilder,
    flownode_id_groups: StringVectorBuilder,
    option_groups: StringVectorBuilder,
    created_time: TimestampMillisecondVectorBuilder,
    updated_time: TimestampMillisecondVectorBuilder,
    last_execution_time: TimestampMillisecondVectorBuilder,
    source_table_names: StringVectorBuilder,
}

impl InformationSchemaFlowsBuilder {
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

            flow_names: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            flow_ids: UInt32VectorBuilder::with_capacity(INIT_CAPACITY),
            state_sizes: UInt64VectorBuilder::with_capacity(INIT_CAPACITY),
            table_catalogs: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            raw_sqls: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            comments: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            expire_afters: Int64VectorBuilder::with_capacity(INIT_CAPACITY),
            source_table_id_groups: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            sink_table_names: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            flownode_id_groups: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            option_groups: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            created_time: TimestampMillisecondVectorBuilder::with_capacity(INIT_CAPACITY),
            updated_time: TimestampMillisecondVectorBuilder::with_capacity(INIT_CAPACITY),
            last_execution_time: TimestampMillisecondVectorBuilder::with_capacity(INIT_CAPACITY),
            source_table_names: StringVectorBuilder::with_capacity(INIT_CAPACITY),
        }
    }

    /// Construct the `information_schema.flows` virtual table
    async fn make_flows(&mut self, request: Option<ScanRequest>) -> Result<RecordBatch> {
        let catalog_name = self.catalog_name.clone();
        let predicates = Predicates::from_scan_request(&request);

        let flow_info_manager = self.flow_metadata_manager.clone();

        // TODO(discord9): use `AsyncIterator` once it's stable-ish
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
            let flow_info = flow_info_manager
                .flow_info_manager()
                .get(flow_id.flow_id())
                .await
                .map_err(BoxedError::new)
                .context(InternalSnafu)?
                .with_context(|| FlowInfoNotFoundSnafu {
                    catalog_name: catalog_name.clone(),
                    flow_name: flow_name.clone(),
                })?;
            self.add_flow(&predicates, flow_id.flow_id(), flow_info, &flow_stat)
                .await?;
        }

        self.finish()
    }

    async fn add_flow(
        &mut self,
        predicates: &Predicates,
        flow_id: FlowId,
        flow_info: FlowInfoValue,
        flow_stat: &Option<FlowStat>,
    ) -> Result<()> {
        let row = [
            (FLOW_NAME, &Value::from(flow_info.flow_name().clone())),
            (FLOW_ID, &Value::from(flow_id)),
            (
                TABLE_CATALOG,
                &Value::from(flow_info.catalog_name().clone()),
            ),
        ];
        if !predicates.eval(&row) {
            return Ok(());
        }
        self.flow_names.push(Some(flow_info.flow_name()));
        self.flow_ids.push(Some(flow_id));
        self.state_sizes.push(
            flow_stat
                .as_ref()
                .and_then(|state| state.state_size.get(&flow_id).map(|v| *v as u64)),
        );
        self.table_catalogs.push(Some(flow_info.catalog_name()));
        self.raw_sqls
            .push(Some(&InformationSchemaFlows::generate_show_create_flow(
                &flow_info,
            )?));
        self.comments.push(Some(flow_info.comment()));
        self.expire_afters.push(flow_info.expire_after());
        self.source_table_id_groups.push(Some(
            &serde_json::to_string(flow_info.source_table_ids()).context(JsonSnafu {
                input: format!("{:?}", flow_info.source_table_ids()),
            })?,
        ));
        self.sink_table_names
            .push(Some(&flow_info.sink_table_name().to_string()));
        self.flownode_id_groups.push(Some(
            &serde_json::to_string(flow_info.flownode_ids()).context({
                JsonSnafu {
                    input: format!("{:?}", flow_info.flownode_ids()),
                }
            })?,
        ));
        self.option_groups
            .push(Some(&serde_json::to_string(flow_info.options()).context(
                JsonSnafu {
                    input: format!("{:?}", flow_info.options()),
                },
            )?));
        self.created_time
            .push(Some(flow_info.created_time().timestamp_millis().into()));
        self.updated_time
            .push(Some(flow_info.updated_time().timestamp_millis().into()));
        self.last_execution_time
            .push(flow_stat.as_ref().and_then(|state| {
                state
                    .last_exec_time_map
                    .get(&flow_id)
                    .map(|v| TimestampMillisecond::new(*v))
            }));

        let mut source_table_names = vec![];
        let catalog_manager = self
            .catalog_manager
            .upgrade()
            .context(UpgradeWeakCatalogManagerRefSnafu)?;
        for table_id in flow_info.source_table_ids() {
            if let Some(table_info) = catalog_manager.table_info_by_id(*table_id).await? {
                source_table_names.push(table_info.full_table_name());
            }
        }

        let source_table_names = source_table_names.join(",");
        self.source_table_names.push(Some(&source_table_names));

        Ok(())
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let columns: Vec<VectorRef> = vec![
            Arc::new(self.flow_names.finish()),
            Arc::new(self.flow_ids.finish()),
            Arc::new(self.state_sizes.finish()),
            Arc::new(self.table_catalogs.finish()),
            Arc::new(self.raw_sqls.finish()),
            Arc::new(self.comments.finish()),
            Arc::new(self.expire_afters.finish()),
            Arc::new(self.source_table_id_groups.finish()),
            Arc::new(self.sink_table_names.finish()),
            Arc::new(self.flownode_id_groups.finish()),
            Arc::new(self.option_groups.finish()),
            Arc::new(self.created_time.finish()),
            Arc::new(self.updated_time.finish()),
            Arc::new(self.last_execution_time.finish()),
            Arc::new(self.source_table_names.finish()),
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
                    .make_flows(None)
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ))
    }
}
