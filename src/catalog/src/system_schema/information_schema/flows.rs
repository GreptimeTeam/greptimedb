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
use common_meta::ddl::create_flow::{FlowType, effective_eval_schedule_from_flow_info};
use common_meta::key::FlowId;
use common_meta::key::flow::FlowMetadataManager;
use common_meta::key::flow::flow_info::FlowInfoValue;
use common_meta::key::flow::flow_state::FlowStat;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{DfSendableRecordBatchStream, RecordBatch, SendableRecordBatchStream};
use datafusion::execution::TaskContext;
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
use futures::{StreamExt, TryStreamExt};
use snafu::{OptionExt, ResultExt};
use sql::ast::Ident;
use sql::dialect::GreptimeDbDialect;
use sql::parser::ParserContext;
use sql::statements::create::{CreateFlow, SqlOrTql};
use sql::statements::statement::Statement;
use store_api::storage::{ScanRequest, TableId};

use crate::CatalogManager;
use crate::batch_scan::BatchScanner;
use crate::error::{
    CreateRecordBatchSnafu, InternalSnafu, JsonSnafu, ListFlowsSnafu, Result,
    UpgradeWeakCatalogManagerRefSnafu,
};
use crate::information_schema::{FLOWS, Predicates};
use crate::kvbackend::KvBackendCatalogManager;
use crate::system_schema::SystemTable;
use crate::system_schema::information_schema::loaders::{self, FlowInfoLoader};
use crate::system_schema::information_schema::{InformationSchemaTableScope, InformationTable};
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
pub const FLOWNODE_ADDRS: &str = "flownode_addrs";

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

    /// for complex fields(including [`SOURCE_TABLE_IDS`], [`FLOWNODE_IDS`], [`OPTIONS`] and
    /// [`FLOWNODE_ADDRS`]), it will be serialized to json string for now
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
                (FLOWNODE_ADDRS, CDT::string_datatype(), true),
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
            eval_offset: effective_eval_schedule_from_flow_info(flow_info)
                .map_err(BoxedError::new)
                .context(InternalSnafu)?
                .map(|schedule| schedule.anchor_secs)
                .filter(|anchor_secs| *anchor_secs != 0),
            comment,
            flow_options: sql::statements::OptionMap::from_filtered_string_map(
                flow_info.options(),
                &[FlowType::FLOW_TYPE_KEY],
            ),
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
    fn scope(&self) -> InformationSchemaTableScope {
        InformationSchemaTableScope::Catalog
    }
}

impl SystemTable for InformationSchemaFlows {
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
        let stream = utils::record_batch_stream(&self.schema, self.builder().scan(Some(request)));
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
    flownode_addr_groups: StringVectorBuilder,
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
            flownode_addr_groups: StringVectorBuilder::with_capacity(INIT_CAPACITY),
        }
    }

    /// Construct the `information_schema.flows` virtual table
    fn scan(
        mut self,
        request: Option<ScanRequest>,
    ) -> impl futures::Stream<Item = Result<RecordBatch>> {
        async_stream::try_stream!({
            let catalog_name = self.catalog_name.clone();
            let predicates = Predicates::from_scan_request(&request);

            let flow_info_manager = self.flow_metadata_manager.clone();

            // TODO(discord9): use `AsyncIterator` once it's stable-ish
            let stream = flow_info_manager
                .flow_name_manager()
                .flow_names(&catalog_name)
                .await;

            let flow_stat = {
                let information_extension = utils::information_extension(&self.catalog_manager)?;
                information_extension.flow_stats().await?
            };

            let catalog_manager = self
                .catalog_manager
                .upgrade()
                .context(UpgradeWeakCatalogManagerRefSnafu)?;
            let cache = catalog_manager
                .as_any()
                .downcast_ref::<KvBackendCatalogManager>()
                .and_then(|manager| manager.flow_info_cache());
            let names = stream
                .map_ok(|(name, id)| (name, id.flow_id()))
                .map(|entry| {
                    entry.map_err(BoxedError::new).context(ListFlowsSnafu {
                        catalog: &catalog_name,
                    })
                });
            let loader = FlowInfoLoader::new(
                cache.as_deref(),
                flow_info_manager.flow_info_manager(),
                &catalog_name,
            );
            let batches = BatchScanner::METADATA.scan(names, |entries| loader.load(entries));
            let mut batches = std::pin::pin!(batches);
            while let Some(batch) = batches.try_next().await? {
                let flows: Vec<_> = batch
                    .into_iter()
                    .filter(|(id, info)| {
                        predicates.eval(&[
                            (FLOW_NAME, &Value::from(info.flow_name().clone())),
                            (FLOW_ID, &Value::from(*id)),
                            (TABLE_CATALOG, &Value::from(info.catalog_name().clone())),
                        ])
                    })
                    .collect();
                let names =
                    loaders::load_source_table_names(catalog_manager.as_ref(), &flows).await?;
                for (id, info) in flows {
                    let source_names = info
                        .source_table_ids()
                        .iter()
                        .filter_map(|id| names.get(id).map(String::as_str))
                        .collect::<Vec<_>>()
                        .join(",");
                    self.add_flow(id, &info, &flow_stat, &source_names).await?;
                }
                yield self.finish()?;
            }
        })
    }

    async fn add_flow(
        &mut self,
        flow_id: FlowId,
        flow_info: &FlowInfoValue,
        flow_stat: &Option<FlowStat>,
        source_names: &str,
    ) -> Result<()> {
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
                flow_info,
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
        let flownode_addrs = self
            .flow_metadata_manager
            .flownode_addrs(flow_id)
            .await
            .map_err(BoxedError::new)
            .context(InternalSnafu)?;
        if flownode_addrs.is_empty() {
            self.flownode_addr_groups.push(None);
        } else {
            let flownode_addrs_json =
                serde_json::to_string(&flownode_addrs).with_context(|_| JsonSnafu {
                    input: format!("{:?}", flownode_addrs),
                })?;
            self.flownode_addr_groups.push(Some(&flownode_addrs_json));
        }

        self.source_table_names.push(Some(source_names));

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
            Arc::new(self.flownode_addr_groups.finish()),
        ];
        RecordBatch::new(self.schema.clone(), columns).context(CreateRecordBatchSnafu)
    }
}

impl DfPartitionStream for InformationSchemaFlows {
    fn schema(&self) -> &arrow_schema::SchemaRef {
        self.schema.arrow_schema()
    }

    fn execute(&self, _: Arc<TaskContext>) -> DfSendableRecordBatchStream {
        utils::record_batch_stream(&self.schema, self.builder().scan(None))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use common_meta::key::flow::flow_info::{FlowMissedTickPolicy, FlowScheduleConfig, FlowStatus};
    use sql::parser::ParseOptions;
    use table::table_name::TableName;

    use super::*;

    fn flow_info_for_show_create(
        raw_sql: &str,
        eval_interval_secs: Option<i64>,
        anchor_secs: i64,
        options: HashMap<String, String>,
    ) -> FlowInfoValue {
        let created_time = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        FlowInfoValue {
            source_table_ids: vec![],
            all_source_table_names: vec![],
            unresolved_source_table_names: vec![],
            sink_table_name: TableName::new("greptime", "public", "sink"),
            flownode_ids: BTreeMap::new(),
            catalog_name: "greptime".to_string(),
            query_context: None,
            flow_name: "my_flow".to_string(),
            raw_sql: raw_sql.to_string(),
            expire_after: None,
            eval_interval_secs,
            comment: String::new(),
            options,
            status: FlowStatus::Active,
            created_time,
            updated_time: created_time,
            eval_schedule: eval_interval_secs.map(|interval| FlowScheduleConfig {
                anchor_secs,
                start_secs: anchor_secs + interval,
                missed_tick_policy: FlowMissedTickPolicy::BoundedCatchUp,
                catchup_max_runs: 3,
                catchup_max_lag_secs: 300,
            }),
        }
    }

    #[tokio::test]
    async fn test_flow_scan_preserves_source_order_and_duplicates() {
        use crate::information_schema::NoopInformationExtension;
        use crate::kvbackend::KvBackendCatalogManagerBuilder;
        use common_meta::cache::LayeredCacheRegistryBuilder;
        use common_meta::ddl::test_util::create_table::test_create_table_task;
        use common_meta::key::table_route::TableRouteValue;
        use common_meta::kv_backend::memory::MemoryKvBackend;

        let backend = Arc::new(MemoryKvBackend::default());
        let registry = cache::with_default_composite_cache_registry(
            LayeredCacheRegistryBuilder::default()
                .add_cache_registry(cache::build_fundamental_cache_registry(backend.clone())),
        )
        .unwrap()
        .build();
        let catalog = KvBackendCatalogManagerBuilder::new(
            Arc::new(NoopInformationExtension),
            backend.clone(),
            Arc::new(registry),
        )
        .build();
        for (id, name) in [(1024, "first"), (1025, "second")] {
            let mut info = test_create_table_task(name, id).table_info;
            info.catalog_name = "greptime".into();
            info.schema_name = "public".into();
            catalog
                .table_metadata_manager_ref()
                .create_table_metadata(info, TableRouteValue::physical(vec![]), Default::default())
                .await
                .unwrap();
        }

        let flows = FlowMetadataManager::new(backend);
        let mut info = flow_info_for_show_create("SELECT 1", None, 0, HashMap::new());
        info.source_table_ids = vec![1025, 9999, 1024, 1025];
        flows.create_flow_metadata(1, info, vec![]).await.unwrap();

        let loader = FlowInfoLoader::new(None, flows.flow_info_manager(), "greptime");
        let entries = vec![("my_flow".into(), 1), ("my_flow".into(), 1)];
        let loaded = loader.load(entries).await.unwrap();
        assert_eq!(loaded.len(), 2);
        assert!(Arc::ptr_eq(&loaded[0].1, &loaded[1].1));

        let table = catalog
            .table("greptime", "information_schema", FLOWS, None)
            .await
            .unwrap()
            .unwrap();
        let batches: Vec<_> = table
            .scan_to_stream(ScanRequest::default())
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        let names = batches[0]
            .column_by_name(SOURCE_TABLE_NAMES)
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(
            names.value(0),
            "greptime.public.second,greptime.public.first,greptime.public.second"
        );
        assert!(catalog.flow_info_cache().unwrap().contains_key(&1));
    }

    #[test]
    fn test_generate_show_create_flow_with_eval_offset() {
        // `raw_sql` stores only the query part (the `AS` clause).
        let raw_sql = "SELECT max(c1) FROM public.src";
        let flow_info = flow_info_for_show_create(raw_sql, Some(3600), 120, HashMap::new());
        let sql = InformationSchemaFlows::generate_show_create_flow(&flow_info).unwrap();
        assert!(
            sql.contains("EVAL OFFSET '120 s'"),
            "EVAL OFFSET must be emitted, got:\n{sql}"
        );
        assert!(
            !sql.contains("__greptime_internal_eval_offset_secs"),
            "internal key must be absent, got:\n{sql}"
        );

        let stmts = ParserContext::create_with_dialect(
            &sql,
            &GreptimeDbDialect {},
            ParseOptions::default(),
        )
        .unwrap();
        let Statement::CreateFlow(reparsed) = &stmts[0] else {
            panic!("unexpected stmt: {:?}", stmts[0]);
        };
        assert_eq!(reparsed.eval_interval, Some(3600));
        assert_eq!(reparsed.eval_offset, Some(120));
    }

    #[test]
    fn test_generate_show_create_flow_omits_zero_offset() {
        let raw_sql = "SELECT max(c1) FROM public.src";
        let flow_info = flow_info_for_show_create(raw_sql, Some(3600), 0, HashMap::new());
        let sql = InformationSchemaFlows::generate_show_create_flow(&flow_info).unwrap();
        assert!(
            !sql.contains("EVAL OFFSET"),
            "zero offset must be omitted, got:\n{sql}"
        );
        let stmts = ParserContext::create_with_dialect(
            &sql,
            &GreptimeDbDialect {},
            ParseOptions::default(),
        )
        .unwrap();
        let Statement::CreateFlow(reparsed) = &stmts[0] else {
            panic!("unexpected stmt: {:?}", stmts[0]);
        };
        assert_eq!(reparsed.eval_offset, None);
    }
}
