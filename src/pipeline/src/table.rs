use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use api::v1::greptime_request::Request;
use api::v1::value::ValueData;
use api::v1::{
    ColumnDataType, ColumnDef, ColumnSchema as PbColumnSchema, Row, RowInsertRequest,
    RowInsertRequests, Rows, SemanticType,
};
use common_error::ext::{BoxedError, ErrorExt, PlainError};
use common_error::status_code::StatusCode;
use common_query::OutputData;
use common_recordbatch::util as record_util;
use common_telemetry::info;
use common_time::util;
use datafusion::datasource::DefaultTableSource;
use datafusion::logical_expr::{and, col, lit};
use datafusion_common::TableReference;
use datafusion_expr::LogicalPlanBuilder;
use datatypes::prelude::ScalarVector;
use datatypes::vectors::{StringVector, Vector};
use pipeline::transform::GreptimeTransformer;
use pipeline::{parse, Content, Pipeline};
use query::plan::LogicalPlan;
use query::QueryEngineRef;
use servers::query_handler::grpc::GrpcQueryHandlerRef;
use session::context::{QueryContextBuilder, QueryContextRef};
use snafu::{ensure, OptionExt, ResultExt};
use table::metadata::TableInfo;
use table::table::adapter::DfTableProviderAdapter;
use table::TableRef;

use crate::error::{
    BuildDfLogicalPlanSnafu, CastTypeSnafu, CollectRecordsSnafu, ExecuteInternalStatementSnafu,
    InsertPipelineSnafu, ParsePipelineSnafu, PipelineNotFoundSnafu, Result,
};

pub type PipelineTableRef<E> = Arc<PipelineTable<E>>;

pub const PIPELINE_TABLE_NAME: &str = "pipelines";
pub struct PipelineTable<E: ErrorExt + Send + Sync + 'static> {
    grpc_handler: GrpcQueryHandlerRef<E>,
    table: TableRef,
    query_engine: QueryEngineRef,
    pipelines: RwLock<HashMap<String, Pipeline<GreptimeTransformer>>>,
}

impl<E: ErrorExt + Send + Sync + 'static> PipelineTable<E> {
    pub fn new(
        table: TableRef,
        grpc_handler: GrpcQueryHandlerRef<E>,
        query_engine: QueryEngineRef,
    ) -> Self {
        Self {
            grpc_handler,
            table,
            query_engine,
            pipelines: RwLock::new(HashMap::default()),
        }
    }

    pub fn build_pipeline_schema() -> (String, Vec<String>, Vec<ColumnDef>) {
        let pipeline_name = "name";
        let schema_name = "schema";
        let content_type = "content_type";
        let pipeline_content = "pipeline";
        let created_at = "created_at";
        let updated_at = "updated_at";

        (
            created_at.to_string(),
            vec![
                schema_name.to_string(),
                pipeline_name.to_string(),
                content_type.to_string(),
            ],
            vec![
                ColumnDef {
                    name: pipeline_name.to_string(),
                    data_type: ColumnDataType::String as i32,
                    is_nullable: false,
                    default_constraint: vec![],
                    semantic_type: SemanticType::Tag as i32,
                    comment: "".to_string(),
                    datatype_extension: None,
                },
                ColumnDef {
                    name: schema_name.to_string(),
                    data_type: ColumnDataType::String as i32,
                    is_nullable: false,
                    default_constraint: vec![],
                    semantic_type: SemanticType::Tag as i32,
                    comment: "".to_string(),
                    datatype_extension: None,
                },
                ColumnDef {
                    name: content_type.to_string(),
                    data_type: ColumnDataType::String as i32,
                    is_nullable: false,
                    default_constraint: vec![],
                    semantic_type: SemanticType::Tag as i32,
                    comment: "".to_string(),
                    datatype_extension: None,
                },
                ColumnDef {
                    name: pipeline_content.to_string(),
                    data_type: ColumnDataType::String as i32,
                    is_nullable: false,
                    default_constraint: vec![],
                    semantic_type: SemanticType::Field as i32,
                    comment: "".to_string(),
                    datatype_extension: None,
                },
                ColumnDef {
                    name: created_at.to_string(),
                    data_type: ColumnDataType::TimestampMillisecond as i32,
                    is_nullable: false,
                    default_constraint: vec![],
                    semantic_type: SemanticType::Timestamp as i32,
                    comment: "".to_string(),
                    datatype_extension: None,
                },
                ColumnDef {
                    name: updated_at.to_string(),
                    data_type: ColumnDataType::TimestampMillisecond as i32,
                    is_nullable: false,
                    default_constraint: vec![],
                    semantic_type: SemanticType::Field as i32,
                    comment: "".to_string(),
                    datatype_extension: None,
                },
            ],
        )
    }

    fn build_insert_column_schemas() -> Vec<PbColumnSchema> {
        vec![
            PbColumnSchema {
                column_name: "name".to_string(),
                datatype: ColumnDataType::String.into(),
                semantic_type: SemanticType::Tag.into(),
                ..Default::default()
            },
            PbColumnSchema {
                column_name: "schema".to_string(),
                datatype: ColumnDataType::String.into(),
                semantic_type: SemanticType::Tag.into(),
                ..Default::default()
            },
            PbColumnSchema {
                column_name: "content_type".to_string(),
                datatype: ColumnDataType::String.into(),
                semantic_type: SemanticType::Tag.into(),
                ..Default::default()
            },
            PbColumnSchema {
                column_name: "pipeline".to_string(),
                datatype: ColumnDataType::String.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            PbColumnSchema {
                column_name: "created_at".to_string(),
                datatype: ColumnDataType::TimestampMillisecond.into(),
                semantic_type: SemanticType::Timestamp.into(),
                ..Default::default()
            },
            PbColumnSchema {
                column_name: "updated_at".to_string(),
                datatype: ColumnDataType::TimestampMillisecond.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
        ]
    }

    fn query_ctx(table_info: &TableInfo) -> QueryContextRef {
        QueryContextBuilder::default()
            .current_catalog(table_info.catalog_name.to_string())
            .current_schema(table_info.schema_name.to_string())
            .build()
            .into()
    }

    pub fn compile_pipeline(pipeline: &str) -> Result<Pipeline<GreptimeTransformer>> {
        let yaml_content = Content::Yaml(pipeline.into());
        parse::<GreptimeTransformer>(&yaml_content)
            .map_err(|e| BoxedError::new(PlainError::new(e, StatusCode::InvalidArguments)))
            .context(ParsePipelineSnafu)
    }

    fn generate_pipeline_cache_key(schema: &str, name: &str) -> String {
        format!("{}.{}", schema, name)
    }

    fn get_compiled_pipeline_from_cache(
        &self,
        schema: &str,
        name: &str,
    ) -> Option<Pipeline<GreptimeTransformer>> {
        self.pipelines
            .read()
            .unwrap()
            .get(&Self::generate_pipeline_cache_key(schema, name))
            .cloned()
    }

    async fn insert_pipeline_to_pipeline_table(
        &self,
        schema: &str,
        name: &str,
        content_type: &str,
        pipeline: &str,
    ) -> Result<()> {
        let now = util::current_time_millis();

        let table_info = self.table.table_info();

        let insert = RowInsertRequest {
            table_name: PIPELINE_TABLE_NAME.to_string(),
            rows: Some(Rows {
                schema: Self::build_insert_column_schemas(),
                rows: vec![Row {
                    values: vec![
                        ValueData::StringValue(name.to_string()).into(),
                        ValueData::StringValue(schema.to_string()).into(),
                        ValueData::StringValue(content_type.to_string()).into(),
                        ValueData::StringValue(pipeline.to_string()).into(),
                        ValueData::TimestampMillisecondValue(now).into(),
                        ValueData::TimestampMillisecondValue(now).into(),
                    ],
                }],
            }),
        };

        let requests = RowInsertRequests {
            inserts: vec![insert],
        };

        let output = self
            .grpc_handler
            .do_query(Request::RowInserts(requests), Self::query_ctx(&table_info))
            .await
            .map_err(BoxedError::new)
            .context(InsertPipelineSnafu { name })?;

        info!(
            "Inserted pipeline: {} into {} table: {}, output: {:?}.",
            name,
            PIPELINE_TABLE_NAME,
            table_info.full_table_name(),
            output
        );

        Ok(())
    }

    pub async fn get_pipeline(
        &self,
        schema: &str,
        name: &str,
    ) -> Result<Pipeline<GreptimeTransformer>> {
        if let Some(pipeline) = self.get_compiled_pipeline_from_cache(schema, name) {
            return Ok(pipeline);
        }

        let pipeline = self.find_pipeline_by_name(schema, name).await?;
        let compiled_pipeline = Self::compile_pipeline(&pipeline)?;
        self.pipelines.write().unwrap().insert(
            Self::generate_pipeline_cache_key(schema, name),
            compiled_pipeline.clone(),
        );
        Ok(compiled_pipeline)
    }

    pub async fn insert_and_compile(
        &self,
        schema: &str,
        name: &str,
        content_type: &str,
        pipeline: &str,
    ) -> Result<Pipeline<GreptimeTransformer>> {
        let compiled_pipeline = Self::compile_pipeline(pipeline)?;

        self.insert_pipeline_to_pipeline_table(schema, name, content_type, pipeline)
            .await?;

        self.pipelines.write().unwrap().insert(
            Self::generate_pipeline_cache_key(schema, name),
            compiled_pipeline.clone(),
        );

        Ok(compiled_pipeline)
    }

    async fn find_pipeline_by_name(&self, schema: &str, name: &str) -> Result<String> {
        let table_info = self.table.table_info();

        let table_name = TableReference::full(
            table_info.catalog_name.clone(),
            table_info.schema_name.clone(),
            table_info.name.clone(),
        );

        let table_provider = Arc::new(DfTableProviderAdapter::new(self.table.clone()));
        let table_source = Arc::new(DefaultTableSource::new(table_provider));

        let plan = LogicalPlanBuilder::scan(table_name, table_source, None)
            .context(BuildDfLogicalPlanSnafu)?
            .filter(and(
                col("schema").eq(lit(schema)),
                col("name").eq(lit(name)),
            ))
            .context(BuildDfLogicalPlanSnafu)?
            .project(vec![col("pipeline")])
            .context(BuildDfLogicalPlanSnafu)?
            .build()
            .context(BuildDfLogicalPlanSnafu)?;

        let output = self
            .query_engine
            .execute(LogicalPlan::DfPlan(plan), Self::query_ctx(&table_info))
            .await
            .context(ExecuteInternalStatementSnafu)?;
        let stream = match output.data {
            OutputData::Stream(stream) => stream,
            OutputData::RecordBatches(record_batches) => record_batches.as_stream(),
            _ => unreachable!(),
        };

        let records = record_util::collect(stream)
            .await
            .context(CollectRecordsSnafu)?;

        ensure!(!records.is_empty(), PipelineNotFoundSnafu { name });

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].num_columns(), 1);

        let script_column = records[0].column(0);
        let script_column = script_column
            .as_any()
            .downcast_ref::<StringVector>()
            .with_context(|| CastTypeSnafu {
                msg: format!(
                    "can't downcast {:?} array into string vector",
                    script_column.data_type()
                ),
            })?;

        assert_eq!(script_column.len(), 1);

        // Safety: asserted above
        Ok(script_column.get_data(0).unwrap().to_string())
    }
}
