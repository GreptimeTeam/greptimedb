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
use std::time::Duration;

use api::v1::value::ValueData;
use api::v1::{
    ColumnDataType, ColumnDef, ColumnSchema as PbColumnSchema, Row, RowInsertRequest,
    RowInsertRequests, Rows, SemanticType,
};
use common_query::OutputData;
use common_recordbatch::util as record_util;
use common_telemetry::{debug, info};
use common_time::timestamp::{TimeUnit, Timestamp};
use datafusion::logical_expr::col;
use datafusion_common::{TableReference, ToDFSchema};
use datafusion_expr::{DmlStatement, LogicalPlan};
use datatypes::prelude::ScalarVector;
use datatypes::timestamp::TimestampNanosecond;
use datatypes::vectors::{StringVector, TimestampNanosecondVector, Vector};
use moka::sync::Cache;
use operator::insert::InserterRef;
use operator::statement::StatementExecutorRef;
use query::dataframe::DataFrame;
use query::QueryEngineRef;
use session::context::{QueryContextBuilder, QueryContextRef};
use snafu::{ensure, OptionExt, ResultExt};
use table::metadata::TableInfo;
use table::TableRef;

use crate::error::{
    BuildDfLogicalPlanSnafu, CastTypeSnafu, CollectRecordsSnafu, CompilePipelineSnafu,
    DataFrameSnafu, ExecuteInternalStatementSnafu, InsertPipelineSnafu,
    InvalidPipelineVersionSnafu, PipelineNotFoundSnafu, Result,
};
use crate::etl::transform::GreptimeTransformer;
use crate::etl::{parse, Content, Pipeline};
use crate::manager::{PipelineInfo, PipelineVersion};
use crate::util::{generate_pipeline_cache_key, prepare_dataframe_conditions};

pub(crate) const PIPELINE_TABLE_NAME: &str = "pipelines";
pub(crate) const PIPELINE_TABLE_PIPELINE_NAME_COLUMN_NAME: &str = "name";
pub(crate) const PIPELINE_TABLE_PIPELINE_SCHEMA_COLUMN_NAME: &str = "schema";
const PIPELINE_TABLE_PIPELINE_CONTENT_TYPE_COLUMN_NAME: &str = "content_type";
const PIPELINE_TABLE_PIPELINE_CONTENT_COLUMN_NAME: &str = "pipeline";
pub(crate) const PIPELINE_TABLE_CREATED_AT_COLUMN_NAME: &str = "created_at";

/// Pipeline table cache size.
const PIPELINES_CACHE_SIZE: u64 = 10000;
/// Pipeline table cache time to live.
const PIPELINES_CACHE_TTL: Duration = Duration::from_secs(10);

/// PipelineTable is a table that stores the pipeline schema and content.
/// Every catalog has its own pipeline table.
pub struct PipelineTable {
    inserter: InserterRef,
    statement_executor: StatementExecutorRef,
    table: TableRef,
    query_engine: QueryEngineRef,
    pipelines: Cache<String, Arc<Pipeline<GreptimeTransformer>>>,
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
            pipelines: Cache::builder()
                .max_capacity(PIPELINES_CACHE_SIZE)
                .time_to_live(PIPELINES_CACHE_TTL)
                .build(),
        }
    }

    /// Build the schema for the pipeline table.
    /// Returns the (time index, primary keys, column) definitions.
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
                    options: None,
                },
                ColumnDef {
                    name: PIPELINE_TABLE_PIPELINE_SCHEMA_COLUMN_NAME.to_string(),
                    data_type: ColumnDataType::String as i32,
                    is_nullable: false,
                    default_constraint: vec![],
                    semantic_type: SemanticType::Tag as i32,
                    comment: "".to_string(),
                    datatype_extension: None,
                    options: None,
                },
                ColumnDef {
                    name: PIPELINE_TABLE_PIPELINE_CONTENT_TYPE_COLUMN_NAME.to_string(),
                    data_type: ColumnDataType::String as i32,
                    is_nullable: false,
                    default_constraint: vec![],
                    semantic_type: SemanticType::Tag as i32,
                    comment: "".to_string(),
                    datatype_extension: None,
                    options: None,
                },
                ColumnDef {
                    name: PIPELINE_TABLE_PIPELINE_CONTENT_COLUMN_NAME.to_string(),
                    data_type: ColumnDataType::String as i32,
                    is_nullable: false,
                    default_constraint: vec![],
                    semantic_type: SemanticType::Field as i32,
                    comment: "".to_string(),
                    datatype_extension: None,
                    options: None,
                },
                ColumnDef {
                    name: PIPELINE_TABLE_CREATED_AT_COLUMN_NAME.to_string(),
                    data_type: ColumnDataType::TimestampNanosecond as i32,
                    is_nullable: false,
                    default_constraint: vec![],
                    semantic_type: SemanticType::Timestamp as i32,
                    comment: "".to_string(),
                    datatype_extension: None,
                    options: None,
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
                datatype: ColumnDataType::TimestampNanosecond.into(),
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

    /// Insert a pipeline into the pipeline table.
    async fn insert_pipeline_to_pipeline_table(
        &self,
        schema: &str,
        name: &str,
        content_type: &str,
        pipeline: &str,
    ) -> Result<Timestamp> {
        let now = Timestamp::current_time(TimeUnit::Nanosecond);

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
                        ValueData::TimestampNanosecondValue(now.value()).into(),
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
            "Insert pipeline success, name: {:?}, table: {:?}, output: {:?}",
            name,
            table_info.full_table_name(),
            output
        );

        Ok(now)
    }

    /// Get a pipeline by name.
    /// If the pipeline is not in the cache, it will be get from table and compiled and inserted into the cache.
    pub async fn get_pipeline(
        &self,
        schema: &str,
        name: &str,
        version: PipelineVersion,
    ) -> Result<Arc<Pipeline<GreptimeTransformer>>> {
        if let Some(pipeline) = self
            .pipelines
            .get(&generate_pipeline_cache_key(schema, name, version))
        {
            return Ok(pipeline);
        }

        let pipeline = self
            .find_pipeline(schema, name, version)
            .await?
            .context(PipelineNotFoundSnafu { name, version })?;
        let compiled_pipeline = Arc::new(Self::compile_pipeline(&pipeline.0)?);

        self.pipelines.insert(
            generate_pipeline_cache_key(schema, name, version),
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
    ) -> Result<PipelineInfo> {
        let compiled_pipeline = Arc::new(Self::compile_pipeline(pipeline)?);
        // we will use the version in the future
        let version = self
            .insert_pipeline_to_pipeline_table(schema, name, content_type, pipeline)
            .await?;

        {
            self.pipelines.insert(
                generate_pipeline_cache_key(schema, name, None),
                compiled_pipeline.clone(),
            );
            self.pipelines.insert(
                generate_pipeline_cache_key(schema, name, Some(TimestampNanosecond(version))),
                compiled_pipeline.clone(),
            );
        }

        Ok((version, compiled_pipeline))
    }

    pub async fn delete_pipeline(
        &self,
        schema: &str,
        name: &str,
        version: PipelineVersion,
    ) -> Result<Option<()>> {
        // 0. version is ensured at the http api level not None
        ensure!(
            version.is_some(),
            InvalidPipelineVersionSnafu { version: "None" }
        );

        // 1. check pipeline exist in catalog
        let pipeline = self.find_pipeline(schema, name, version).await?;
        if pipeline.is_none() {
            return Ok(None);
        }

        // 2. prepare dataframe
        let dataframe = self
            .query_engine
            .read_table(self.table.clone())
            .context(DataFrameSnafu)?;
        let DataFrame::DataFusion(dataframe) = dataframe;

        let dataframe = dataframe
            .filter(prepare_dataframe_conditions(schema, name, version))
            .context(BuildDfLogicalPlanSnafu)?;

        // 3. prepare dml stmt
        let table_info = self.table.table_info();
        let table_name = TableReference::full(
            table_info.catalog_name.clone(),
            table_info.schema_name.clone(),
            table_info.name.clone(),
        );

        let df_schema = Arc::new(
            table_info
                .meta
                .schema
                .arrow_schema()
                .clone()
                .to_dfschema()
                .context(BuildDfLogicalPlanSnafu)?,
        );

        // create dml stmt
        let stmt = DmlStatement::new(
            table_name,
            df_schema,
            datafusion_expr::WriteOp::Delete,
            Arc::new(dataframe.into_parts().1),
        );

        let plan = LogicalPlan::Dml(stmt);

        // 4. execute dml stmt
        let output = self
            .query_engine
            .execute(plan, Self::query_ctx(&table_info))
            .await
            .context(ExecuteInternalStatementSnafu)?;

        info!(
            "Delete pipeline success, name: {:?}, version: {:?}, table: {:?}, output: {:?}",
            name,
            version,
            table_info.full_table_name(),
            output
        );

        // remove cache with version and latest
        self.pipelines
            .remove(&generate_pipeline_cache_key(schema, name, version));
        self.pipelines
            .remove(&generate_pipeline_cache_key(schema, name, None));

        Ok(Some(()))
    }

    async fn find_pipeline(
        &self,
        schema: &str,
        name: &str,
        version: PipelineVersion,
    ) -> Result<Option<(String, TimestampNanosecond)>> {
        // 1. prepare dataframe
        let dataframe = self
            .query_engine
            .read_table(self.table.clone())
            .context(DataFrameSnafu)?;
        let DataFrame::DataFusion(dataframe) = dataframe;

        let dataframe = dataframe
            .filter(prepare_dataframe_conditions(schema, name, version))
            .context(BuildDfLogicalPlanSnafu)?
            .select_columns(&[
                PIPELINE_TABLE_PIPELINE_CONTENT_COLUMN_NAME,
                PIPELINE_TABLE_CREATED_AT_COLUMN_NAME,
            ])
            .context(BuildDfLogicalPlanSnafu)?
            .sort(vec![
                col(PIPELINE_TABLE_CREATED_AT_COLUMN_NAME).sort(false, true)
            ])
            .context(BuildDfLogicalPlanSnafu)?
            .limit(0, Some(1))
            .context(BuildDfLogicalPlanSnafu)?;

        let plan = dataframe.into_parts().1;

        let table_info = self.table.table_info();

        debug!("find_pipeline_by_name: plan: {:?}", plan);

        // 2. execute plan
        let output = self
            .query_engine
            .execute(plan, Self::query_ctx(&table_info))
            .await
            .context(ExecuteInternalStatementSnafu)?;
        let stream = match output.data {
            OutputData::Stream(stream) => stream,
            OutputData::RecordBatches(record_batches) => record_batches.as_stream(),
            _ => unreachable!(),
        };

        // 3. construct result
        let records = record_util::collect(stream)
            .await
            .context(CollectRecordsSnafu)?;

        if records.is_empty() {
            return Ok(None);
        }

        // limit 1
        ensure!(
            records.len() == 1 && records[0].num_columns() == 2,
            PipelineNotFoundSnafu { name, version }
        );

        let pipeline_content_column = records[0].column(0);
        let pipeline_content = pipeline_content_column
            .as_any()
            .downcast_ref::<StringVector>()
            .with_context(|| CastTypeSnafu {
                msg: format!(
                    "can't downcast {:?} array into string vector",
                    pipeline_content_column.data_type()
                ),
            })?;

        let pipeline_created_at_column = records[0].column(1);
        let pipeline_created_at = pipeline_created_at_column
            .as_any()
            .downcast_ref::<TimestampNanosecondVector>()
            .with_context(|| CastTypeSnafu {
                msg: format!(
                    "can't downcast {:?} array into scalar vector",
                    pipeline_created_at_column.data_type()
                ),
            })?;

        debug!(
            "find_pipeline_by_name: pipeline_content: {:?}, pipeline_created_at: {:?}",
            pipeline_content, pipeline_created_at
        );

        ensure!(
            pipeline_content.len() == 1,
            PipelineNotFoundSnafu { name, version }
        );

        // Safety: asserted above
        Ok(Some((
            pipeline_content.get_data(0).unwrap().to_string(),
            pipeline_created_at.get_data(0).unwrap(),
        )))
    }
}
