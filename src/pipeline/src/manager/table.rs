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

use api::v1::value::ValueData;
use api::v1::{
    ColumnDataType, ColumnDef, ColumnSchema as PbColumnSchema, Row, RowInsertRequest,
    RowInsertRequests, Rows, SemanticType,
};
use common_query::OutputData;
use common_recordbatch::util as record_util;
use common_telemetry::{debug, info};
use common_time::timestamp::{TimeUnit, Timestamp};
use datafusion::datasource::DefaultTableSource;
use datafusion::logical_expr::col;
use datafusion_common::TableReference;
use datafusion_expr::{DmlStatement, LogicalPlan};
use datatypes::prelude::ScalarVector;
use datatypes::timestamp::TimestampNanosecond;
use datatypes::vectors::{StringVector, TimestampNanosecondVector, Vector};
use itertools::Itertools;
use operator::insert::InserterRef;
use operator::statement::StatementExecutorRef;
use query::dataframe::DataFrame;
use query::QueryEngineRef;
use session::context::{QueryContextBuilder, QueryContextRef};
use snafu::{ensure, OptionExt, ResultExt};
use table::metadata::TableInfo;
use table::table::adapter::DfTableProviderAdapter;
use table::TableRef;

use crate::error::{
    BuildDfLogicalPlanSnafu, CastTypeSnafu, CollectRecordsSnafu, DataFrameSnafu, Error,
    ExecuteInternalStatementSnafu, InsertPipelineSnafu, InvalidPipelineVersionSnafu,
    MultiPipelineWithDiffSchemaSnafu, PipelineNotFoundSnafu, RecordBatchLenNotMatchSnafu, Result,
};
use crate::etl::{parse, Content, Pipeline};
use crate::manager::pipeline_cache::PipelineCache;
use crate::manager::{PipelineInfo, PipelineVersion};
use crate::metrics::METRIC_PIPELINE_TABLE_FIND_COUNT;
use crate::util::prepare_dataframe_conditions;

pub(crate) const PIPELINE_TABLE_NAME: &str = "pipelines";
pub(crate) const PIPELINE_TABLE_PIPELINE_NAME_COLUMN_NAME: &str = "name";
const PIPELINE_TABLE_PIPELINE_SCHEMA_COLUMN_NAME: &str = "schema";
const PIPELINE_TABLE_PIPELINE_CONTENT_TYPE_COLUMN_NAME: &str = "content_type";
const PIPELINE_TABLE_PIPELINE_CONTENT_COLUMN_NAME: &str = "pipeline";
pub(crate) const PIPELINE_TABLE_CREATED_AT_COLUMN_NAME: &str = "created_at";
pub(crate) const EMPTY_SCHEMA_NAME: &str = "";

/// PipelineTable is a table that stores the pipeline schema and content.
/// Every catalog has its own pipeline table.
pub struct PipelineTable {
    inserter: InserterRef,
    statement_executor: StatementExecutorRef,
    table: TableRef,
    query_engine: QueryEngineRef,
    cache: PipelineCache,
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
            cache: PipelineCache::new(),
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
    pub fn compile_pipeline(pipeline: &str) -> Result<Pipeline> {
        let yaml_content = Content::Yaml(pipeline);
        parse(&yaml_content)
    }

    /// Insert a pipeline into the pipeline table.
    async fn insert_pipeline_to_pipeline_table(
        &self,
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
                        ValueData::StringValue(EMPTY_SCHEMA_NAME.to_string()).into(),
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
                false,
                false,
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
    ) -> Result<Arc<Pipeline>> {
        if let Some(pipeline) = self.cache.get_pipeline_cache(schema, name, version)? {
            return Ok(pipeline);
        }

        let pipeline = self.get_pipeline_str(schema, name, version).await?;
        let compiled_pipeline = Arc::new(Self::compile_pipeline(&pipeline.0)?);

        self.cache
            .insert_pipeline_cache(schema, name, version, compiled_pipeline.clone(), false);
        Ok(compiled_pipeline)
    }

    /// Get a original pipeline by name.
    /// If the pipeline is not in the cache, it will be get from table and compiled and inserted into the cache.
    pub async fn get_pipeline_str(
        &self,
        schema: &str,
        name: &str,
        version: PipelineVersion,
    ) -> Result<(String, TimestampNanosecond)> {
        if let Some(pipeline) = self.cache.get_pipeline_str_cache(schema, name, version)? {
            return Ok(pipeline);
        }

        let mut pipeline_vec;
        match self.find_pipeline(name, version).await {
            Ok(p) => {
                METRIC_PIPELINE_TABLE_FIND_COUNT
                    .with_label_values(&["true"])
                    .inc();
                pipeline_vec = p;
            }
            Err(e) => {
                match e {
                    Error::CollectRecords { .. } => {
                        // if collect records failed, it means the pipeline table is temporary invalid
                        // we should use failover cache
                        METRIC_PIPELINE_TABLE_FIND_COUNT
                            .with_label_values(&["false"])
                            .inc();
                        return self
                            .cache
                            .get_failover_cache(schema, name, version)?
                            .ok_or(PipelineNotFoundSnafu { name, version }.build());
                    }
                    _ => {
                        // if other error, we should return it
                        return Err(e);
                    }
                }
            }
        };
        ensure!(
            !pipeline_vec.is_empty(),
            PipelineNotFoundSnafu { name, version }
        );

        // if the result is exact one, use it
        if pipeline_vec.len() == 1 {
            let (pipeline_content, found_schema, version) = pipeline_vec.remove(0);
            let p = (pipeline_content, version);
            self.cache.insert_pipeline_str_cache(
                &found_schema,
                name,
                Some(version),
                p.clone(),
                false,
            );
            return Ok(p);
        }

        // check if there's empty schema pipeline
        // if there isn't, check current schema
        let pipeline = pipeline_vec
            .iter()
            .find(|v| v.1 == EMPTY_SCHEMA_NAME)
            .or_else(|| pipeline_vec.iter().find(|v| v.1 == schema));

        // multiple pipeline with no empty or current schema
        // throw an error
        let (pipeline_content, found_schema, version) =
            pipeline.context(MultiPipelineWithDiffSchemaSnafu {
                schemas: pipeline_vec.iter().map(|v| v.1.clone()).join(","),
            })?;

        let v = *version;
        let p = (pipeline_content.clone(), v);
        self.cache
            .insert_pipeline_str_cache(found_schema, name, Some(v), p.clone(), false);
        Ok(p)
    }

    /// Insert a pipeline into the pipeline table and compile it.
    /// The compiled pipeline will be inserted into the cache.
    /// Newly created pipelines will be saved under empty schema.
    pub async fn insert_and_compile(
        &self,
        name: &str,
        content_type: &str,
        pipeline: &str,
    ) -> Result<PipelineInfo> {
        let compiled_pipeline = Arc::new(Self::compile_pipeline(pipeline)?);
        // we will use the version in the future
        let version = self
            .insert_pipeline_to_pipeline_table(name, content_type, pipeline)
            .await?;

        {
            self.cache.insert_pipeline_cache(
                EMPTY_SCHEMA_NAME,
                name,
                Some(TimestampNanosecond(version)),
                compiled_pipeline.clone(),
                true,
            );

            self.cache.insert_pipeline_str_cache(
                EMPTY_SCHEMA_NAME,
                name,
                Some(TimestampNanosecond(version)),
                (pipeline.to_owned(), TimestampNanosecond(version)),
                true,
            );
        }

        Ok((version, compiled_pipeline))
    }

    pub async fn delete_pipeline(
        &self,
        name: &str,
        version: PipelineVersion,
    ) -> Result<Option<()>> {
        // 0. version is ensured at the http api level not None
        ensure!(
            version.is_some(),
            InvalidPipelineVersionSnafu { version: "None" }
        );

        // 1. check pipeline exist in catalog
        let pipeline = self.find_pipeline(name, version).await?;
        if pipeline.is_empty() {
            return Ok(None);
        }

        // 2. prepare dataframe
        let dataframe = self
            .query_engine
            .read_table(self.table.clone())
            .context(DataFrameSnafu)?;
        let DataFrame::DataFusion(dataframe) = dataframe;

        let dataframe = dataframe
            .filter(prepare_dataframe_conditions(name, version))
            .context(BuildDfLogicalPlanSnafu)?;

        // 3. prepare dml stmt
        let table_info = self.table.table_info();
        let table_name = TableReference::full(
            table_info.catalog_name.clone(),
            table_info.schema_name.clone(),
            table_info.name.clone(),
        );

        let table_provider = Arc::new(DfTableProviderAdapter::new(self.table.clone()));
        let table_source = Arc::new(DefaultTableSource::new(table_provider));

        // create dml stmt
        let stmt = DmlStatement::new(
            table_name,
            table_source,
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
        self.cache.remove_cache(name, version);

        Ok(Some(()))
    }

    // find all pipelines with name and version
    // cloud be multiple with different schema
    // return format: (pipeline content, schema, created_at)
    async fn find_pipeline(
        &self,
        name: &str,
        version: PipelineVersion,
    ) -> Result<Vec<(String, String, TimestampNanosecond)>> {
        // 1. prepare dataframe
        let dataframe = self
            .query_engine
            .read_table(self.table.clone())
            .context(DataFrameSnafu)?;
        let DataFrame::DataFusion(dataframe) = dataframe;

        // select all pipelines with name and version
        let dataframe = dataframe
            .filter(prepare_dataframe_conditions(name, version))
            .context(BuildDfLogicalPlanSnafu)?
            .select_columns(&[
                PIPELINE_TABLE_PIPELINE_CONTENT_COLUMN_NAME,
                PIPELINE_TABLE_PIPELINE_SCHEMA_COLUMN_NAME,
                PIPELINE_TABLE_CREATED_AT_COLUMN_NAME,
            ])
            .context(BuildDfLogicalPlanSnafu)?
            .sort(vec![
                col(PIPELINE_TABLE_CREATED_AT_COLUMN_NAME).sort(false, true)
            ])
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
            return Ok(vec![]);
        }

        ensure!(
            !records.is_empty() && records.iter().all(|r| r.num_columns() == 3),
            PipelineNotFoundSnafu { name, version }
        );

        let mut re = Vec::with_capacity(records.len());
        for r in records {
            let pipeline_content_column = r.column(0);
            let pipeline_content = pipeline_content_column
                .as_any()
                .downcast_ref::<StringVector>()
                .with_context(|| CastTypeSnafu {
                    msg: format!(
                        "can't downcast {:?} array into string vector",
                        pipeline_content_column.data_type()
                    ),
                })?;

            let pipeline_schema_column = r.column(1);
            let pipeline_schema = pipeline_schema_column
                .as_any()
                .downcast_ref::<StringVector>()
                .with_context(|| CastTypeSnafu {
                    msg: format!(
                        "can't downcast {:?} array into string vector",
                        pipeline_schema_column.data_type()
                    ),
                })?;

            let pipeline_created_at_column = r.column(2);
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
                "find_pipeline_by_name: pipeline_content: {:?}, pipeline_schema: {:?}, pipeline_created_at: {:?}",
                pipeline_content, pipeline_schema, pipeline_created_at
            );

            ensure!(
                pipeline_content.len() == pipeline_schema.len()
                    && pipeline_schema.len() == pipeline_created_at.len(),
                RecordBatchLenNotMatchSnafu
            );

            let len = pipeline_content.len();
            for i in 0..len {
                re.push((
                    pipeline_content.get_data(i).unwrap().to_string(),
                    pipeline_schema.get_data(i).unwrap().to_string(),
                    pipeline_created_at.get_data(i).unwrap(),
                ));
            }
        }

        Ok(re)
    }
}
