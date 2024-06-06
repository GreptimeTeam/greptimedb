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

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use api::v1::value::ValueData;
use api::v1::{
    ColumnDataType, ColumnDef, ColumnSchema as PbColumnSchema, Row, RowInsertRequest,
    RowInsertRequests, Rows, SemanticType,
};
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
use operator::insert::InserterRef;
use operator::statement::StatementExecutorRef;
use query::plan::LogicalPlan;
use query::QueryEngineRef;
use session::context::{QueryContextBuilder, QueryContextRef};
use snafu::{ensure, OptionExt, ResultExt};
use table::metadata::TableInfo;
use table::table::adapter::DfTableProviderAdapter;
use table::TableRef;

use crate::error::{
    BuildDfLogicalPlanSnafu, CastTypeSnafu, CollectRecordsSnafu, CompilePipelineSnafu,
    ExecuteInternalStatementSnafu, InsertPipelineSnafu, PipelineNotFoundSnafu, Result,
};
use crate::etl::transform::GreptimeTransformer;
use crate::etl::{parse, Content, Pipeline};

pub type PipelineTableRef = Arc<PipelineTable>;

pub const PIPELINE_TABLE_NAME: &str = "pipelines";

pub const PIPELINE_TABLE_PIPELINE_NAME_COLUMN_NAME: &str = "name";
pub const PIPELINE_TABLE_PIPELINE_SCHEMA_COLUMN_NAME: &str = "schema";
pub const PIPELINE_TABLE_PIPELINE_CONTENT_TYPE_COLUMN_NAME: &str = "content_type";
pub const PIPELINE_TABLE_PIPELINE_CONTENT_COLUMN_NAME: &str = "pipeline";
pub const PIPELINE_TABLE_CREATED_AT_COLUMN_NAME: &str = "created_at";

/// PipelineTable is a table that stores the pipeline schema and content.
/// Every catalog has its own pipeline table.
pub struct PipelineTable {
    inserter: InserterRef,
    statement_executor: StatementExecutorRef,
    table: TableRef,
    query_engine: QueryEngineRef,
    pipelines: RwLock<HashMap<String, Pipeline<GreptimeTransformer>>>,
}

impl PipelineTable {
    /// Create a new PipelineTable.
    pub fn new(
        inserter: InserterRef,
        statement_executor: StatementExecutorRef,
        table: TableRef,
        query_engine: QueryEngineRef,
    ) -> Self {
        Self {
            inserter,
            statement_executor,
            table,
            query_engine,
            pipelines: RwLock::new(HashMap::default()),
        }
    }

    /// Build the schema for the pipeline table.
    pub fn build_pipeline_schema() -> (String, Vec<String>, Vec<ColumnDef>) {
        (
            PIPELINE_TABLE_CREATED_AT_COLUMN_NAME.to_string(),
            vec![
                PIPELINE_TABLE_PIPELINE_SCHEMA_COLUMN_NAME.to_string(),
                PIPELINE_TABLE_PIPELINE_NAME_COLUMN_NAME.to_string(),
                PIPELINE_TABLE_PIPELINE_CONTENT_TYPE_COLUMN_NAME.to_string(),
            ],
            vec![
                ColumnDef {
                    name: PIPELINE_TABLE_PIPELINE_NAME_COLUMN_NAME.to_string(),
                    data_type: ColumnDataType::String as i32,
                    is_nullable: false,
                    default_constraint: vec![],
                    semantic_type: SemanticType::Tag as i32,
                    comment: "".to_string(),
                    datatype_extension: None,
                },
                ColumnDef {
                    name: PIPELINE_TABLE_PIPELINE_SCHEMA_COLUMN_NAME.to_string(),
                    data_type: ColumnDataType::String as i32,
                    is_nullable: false,
                    default_constraint: vec![],
                    semantic_type: SemanticType::Tag as i32,
                    comment: "".to_string(),
                    datatype_extension: None,
                },
                ColumnDef {
                    name: PIPELINE_TABLE_PIPELINE_CONTENT_TYPE_COLUMN_NAME.to_string(),
                    data_type: ColumnDataType::String as i32,
                    is_nullable: false,
                    default_constraint: vec![],
                    semantic_type: SemanticType::Tag as i32,
                    comment: "".to_string(),
                    datatype_extension: None,
                },
                ColumnDef {
                    name: PIPELINE_TABLE_PIPELINE_CONTENT_COLUMN_NAME.to_string(),
                    data_type: ColumnDataType::String as i32,
                    is_nullable: false,
                    default_constraint: vec![],
                    semantic_type: SemanticType::Field as i32,
                    comment: "".to_string(),
                    datatype_extension: None,
                },
                ColumnDef {
                    name: PIPELINE_TABLE_CREATED_AT_COLUMN_NAME.to_string(),
                    data_type: ColumnDataType::TimestampMillisecond as i32,
                    is_nullable: false,
                    default_constraint: vec![],
                    semantic_type: SemanticType::Timestamp as i32,
                    comment: "".to_string(),
                    datatype_extension: None,
                },
            ],
        )
    }

    /// Build the column schemas for inserting a row into the pipeline table.
    fn build_insert_column_schemas() -> Vec<PbColumnSchema> {
        vec![
            PbColumnSchema {
                column_name: PIPELINE_TABLE_PIPELINE_NAME_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::String.into(),
                semantic_type: SemanticType::Tag.into(),
                ..Default::default()
            },
            PbColumnSchema {
                column_name: PIPELINE_TABLE_PIPELINE_SCHEMA_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::String.into(),
                semantic_type: SemanticType::Tag.into(),
                ..Default::default()
            },
            PbColumnSchema {
                column_name: PIPELINE_TABLE_PIPELINE_CONTENT_TYPE_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::String.into(),
                semantic_type: SemanticType::Tag.into(),
                ..Default::default()
            },
            PbColumnSchema {
                column_name: PIPELINE_TABLE_PIPELINE_CONTENT_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::String.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            PbColumnSchema {
                column_name: PIPELINE_TABLE_CREATED_AT_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::TimestampMillisecond.into(),
                semantic_type: SemanticType::Timestamp.into(),
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

    /// Compile a pipeline from a string.
    pub fn compile_pipeline(pipeline: &str) -> Result<Pipeline<GreptimeTransformer>> {
        let yaml_content = Content::Yaml(pipeline.into());
        parse::<GreptimeTransformer>(&yaml_content)
            .map_err(|e| CompilePipelineSnafu { reason: e }.build())
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

    /// Insert a pipeline into the pipeline table.
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
                    ],
                }],
            }),
        };

        let requests = RowInsertRequests {
            inserts: vec![insert],
        };

        let output = self
            .inserter
            .handle_row_inserts(
                requests,
                Self::query_ctx(&table_info),
                &self.statement_executor,
            )
            .await
            .context(InsertPipelineSnafu)?;

        info!(
            "Inserted pipeline: {} into {} table: {}, output: {:?}.",
            name,
            PIPELINE_TABLE_NAME,
            table_info.full_table_name(),
            output
        );

        Ok(())
    }

    /// Get a pipeline by name.
    /// If the pipeline is not in the cache, it will be get from table and compiled and inserted into the cache.
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

    /// Insert a pipeline into the pipeline table and compile it.
    /// The compiled pipeline will be inserted into the cache.
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
                col(PIPELINE_TABLE_PIPELINE_SCHEMA_COLUMN_NAME).eq(lit(schema)),
                col(PIPELINE_TABLE_PIPELINE_NAME_COLUMN_NAME).eq(lit(name)),
            ))
            .context(BuildDfLogicalPlanSnafu)?
            .project(vec![col(PIPELINE_TABLE_PIPELINE_CONTENT_COLUMN_NAME)])
            .context(BuildDfLogicalPlanSnafu)?
            .sort(vec![
                col(PIPELINE_TABLE_CREATED_AT_COLUMN_NAME),
                lit("DESC"),
            ])
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
        // assume schema + name is unique for now
        ensure!(
            records.len() == 1 && records[0].num_columns() == 1,
            PipelineNotFoundSnafu { name }
        );

        let pipeline_column = records[0].column(0);
        let pipeline_column = pipeline_column
            .as_any()
            .downcast_ref::<StringVector>()
            .with_context(|| CastTypeSnafu {
                msg: format!(
                    "can't downcast {:?} array into string vector",
                    pipeline_column.data_type()
                ),
            })?;

        ensure!(pipeline_column.len() == 1, PipelineNotFoundSnafu { name });

        // Safety: asserted above
        Ok(pipeline_column.get_data(0).unwrap().to_string())
    }
}
