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

//! Scripts table
use std::sync::Arc;

use api::v1::greptime_request::Request;
use api::v1::value::ValueData;
use api::v1::{
    ColumnDataType, ColumnDef, ColumnSchema as PbColumnSchema, Row, RowInsertRequest,
    RowInsertRequests, Rows, SemanticType,
};
use common_error::ext::{BoxedError, ErrorExt};
use common_query::OutputData;
use common_recordbatch::util as record_util;
use common_telemetry::logging;
use common_time::util;
use datafusion::datasource::DefaultTableSource;
use datafusion::logical_expr::{and, col, lit};
use datafusion_common::TableReference;
use datafusion_expr::LogicalPlanBuilder;
use datatypes::prelude::ScalarVector;
use datatypes::vectors::{StringVector, UInt64Vector, Vector};
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
    InsertScriptSnafu, Result, ScriptNotFoundSnafu,
};

pub const SCRIPTS_TABLE_NAME: &str = "scripts";

pub type ScriptsTableRef<E> = Arc<ScriptsTable<E>>;

/// The scripts table that keeps the script content etc.
pub struct ScriptsTable<E: ErrorExt + Send + Sync + 'static> {
    table: TableRef,
    grpc_handler: GrpcQueryHandlerRef<E>,
    query_engine: QueryEngineRef,
}

impl<E: ErrorExt + Send + Sync + 'static> ScriptsTable<E> {
    /// Create a new `[ScriptsTable]` based on the table.
    pub fn new(
        table: TableRef,
        grpc_handler: GrpcQueryHandlerRef<E>,
        query_engine: QueryEngineRef,
    ) -> Self {
        Self {
            table,
            grpc_handler,
            query_engine,
        }
    }

    pub async fn insert(&self, schema: &str, name: &str, script: &str, version: u64) -> Result<()> {
        let now = util::current_time_millis();

        let table_info = self.table.table_info();

        let insert = RowInsertRequest {
            table_name: SCRIPTS_TABLE_NAME.to_string(),
            rows: Some(Rows {
                schema: build_insert_column_schemas(),
                rows: vec![Row {
                    values: vec![
                        ValueData::StringValue(schema.to_string()).into(),
                        ValueData::StringValue(name.to_string()).into(),
                        // TODO(dennis): we only supports python right now.
                        ValueData::StringValue("python".to_string()).into(),
                        ValueData::StringValue(script.to_string()).into(),
                        // Timestamp in key part is intentionally left to 0
                        ValueData::TimestampMillisecondValue(0).into(),
                        ValueData::TimestampMillisecondValue(now).into(),
                        ValueData::U64Value(version).into(),
                    ],
                }],
            }),
        };

        let requests = RowInsertRequests {
            inserts: vec![insert],
        };

        let output = self
            .grpc_handler
            .do_query(Request::RowInserts(requests), query_ctx(&table_info))
            .await
            .map_err(BoxedError::new)
            .context(InsertScriptSnafu { name })?;

        logging::info!(
            "Inserted script: {} into scripts table: {}, output: {:?}.",
            name,
            table_info.full_table_name(),
            output
        );

        Ok(())
    }

    pub async fn find_script_and_version_by_name(
        &self,
        schema: &str,
        name: &str,
    ) -> Result<(String, u64)> {
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
            .project(vec![col("script"), col("version")])
            .context(BuildDfLogicalPlanSnafu)?
            .build()
            .context(BuildDfLogicalPlanSnafu)?;

        let output = self
            .query_engine
            .execute(LogicalPlan::DfPlan(plan), query_ctx(&table_info))
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

        ensure!(!records.is_empty(), ScriptNotFoundSnafu { name });

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].num_columns(), 2);

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

        let ver_column = records[0].column(1);
        let ver_column = ver_column
            .as_any()
            .downcast_ref::<UInt64Vector>()
            .with_context(|| CastTypeSnafu {
                msg: format!(
                    "can't downcast {:?} array into uint64 vector",
                    ver_column.data_type()
                ),
            })?;
        assert_eq!(ver_column.len(), 1);

        // Safety: asserted above
        Ok((
            script_column.get_data(0).unwrap().to_string(),
            ver_column.get_data(0).unwrap(),
        ))
    }
}

/// Build the inserted column schemas
fn build_insert_column_schemas() -> Vec<PbColumnSchema> {
    vec![
        // The schema that script belongs to.
        PbColumnSchema {
            column_name: "schema".to_string(),
            datatype: ColumnDataType::String.into(),
            semantic_type: SemanticType::Tag.into(),
            ..Default::default()
        },
        PbColumnSchema {
            column_name: "name".to_string(),
            datatype: ColumnDataType::String.into(),
            semantic_type: SemanticType::Tag.into(),
            ..Default::default()
        },
        PbColumnSchema {
            column_name: "engine".to_string(),
            datatype: ColumnDataType::String.into(),
            semantic_type: SemanticType::Tag.into(),
            ..Default::default()
        },
        PbColumnSchema {
            column_name: "script".to_string(),
            datatype: ColumnDataType::String.into(),
            semantic_type: SemanticType::Field.into(),
            ..Default::default()
        },
        PbColumnSchema {
            column_name: "greptime_timestamp".to_string(),
            datatype: ColumnDataType::TimestampMillisecond.into(),
            semantic_type: SemanticType::Timestamp.into(),
            ..Default::default()
        },
        PbColumnSchema {
            column_name: "gmt_modified".to_string(),
            datatype: ColumnDataType::TimestampMillisecond.into(),
            semantic_type: SemanticType::Field.into(),
            ..Default::default()
        },
        PbColumnSchema {
            column_name: "version".to_string(),
            datatype: ColumnDataType::Uint64.into(),
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
}

/// Builds scripts schema, returns (time index, primary keys, column defs)
pub fn build_scripts_schema() -> (String, Vec<String>, Vec<ColumnDef>) {
    let cols = build_insert_column_schemas();

    let time_index = cols
        .iter()
        .find_map(|c| {
            (c.semantic_type == (SemanticType::Timestamp as i32)).then(|| c.column_name.clone())
        })
        .unwrap(); // Safety: the column always exists

    let primary_keys = cols
        .iter()
        .filter(|c| (c.semantic_type == (SemanticType::Tag as i32)))
        .map(|c| c.column_name.clone())
        .collect();

    let column_defs = cols
        .into_iter()
        .map(|c| ColumnDef {
            name: c.column_name,
            data_type: c.datatype,
            is_nullable: false,
            default_constraint: vec![],
            semantic_type: c.semantic_type,
            comment: "".to_string(),
            datatype_extension: None,
        })
        .collect();

    (time_index, primary_keys, column_defs)
}
