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

//! Stateless DataFusion execution for streaming flows.

use std::sync::Arc;

use api::helper::vectors_to_rows;
use api::v1::greptime_request::Request;
use api::v1::{RowInsertRequest, RowInsertRequests, Rows};
use common_error::ext::BoxedError;
use common_query::OutputData;
use common_recordbatch::{RecordBatch, RecordBatches};
use datafusion::catalog::MemTable;
use datafusion::datasource::{TableProvider, provider_as_source};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DFSchema, TableReference};
use datafusion_expr::LogicalPlan;
use datatypes::schema::SchemaRef;
use query::QueryEngine;
use session::context::QueryContextRef;
use snafu::{ResultExt, ensure};
use table::metadata::TableId;

use crate::TableName;
use crate::adapter::util::column_schemas_to_proto;
use crate::batching_mode::frontend_client::FrontendClient;
use crate::error::{DatafusionSnafu, Error, ExternalSnafu, InvalidQuerySnafu, UnexpectedSnafu};
use crate::repr::DiffRow;

/// The validated, immutable part of one streaming flow.
#[derive(Clone)]
pub(crate) struct StatelessFlow {
    pub(crate) source_table_id: TableId,
    pub(crate) source_table_name: TableName,
    pub(crate) source_schema: SchemaRef,
    pub(crate) sink_table_name: TableName,
    pub(crate) plan: LogicalPlan,
    pub(crate) query_ctx: QueryContextRef,
}

/// Per-request input provider. It owns no catalog or storage state.
#[cfg(test)]
fn test_source_plan(table_name: TableReference, provider: Arc<dyn TableProvider>) -> LogicalPlan {
    datafusion_expr::LogicalPlanBuilder::scan(table_name, provider_as_source(provider), None)
        .unwrap()
        .filter(datafusion_expr::col("number").gt(datafusion_expr::lit(1)))
        .unwrap()
        .project(vec![datafusion_expr::col("number")])
        .unwrap()
        .build()
        .unwrap()
}

fn input_provider(batch: &RecordBatch) -> Result<Arc<dyn TableProvider>, Error> {
    let arrow_batch = batch.df_record_batch().clone();
    let provider = MemTable::try_new(arrow_batch.schema(), vec![vec![arrow_batch]]).context(
        DatafusionSnafu {
            context: "Failed to create transient flow input provider",
        },
    )?;
    Ok(Arc::new(provider))
}

fn replace_source(
    plan: LogicalPlan,
    source_name: &TableReference,
    provider: Arc<dyn TableProvider>,
) -> Result<LogicalPlan, Error> {
    let mut scan_count = 0;
    let mut replaced_scan_count = 0;
    let plan = plan
        .transform_up_with_subqueries(|node| match node {
            LogicalPlan::TableScan(mut scan) => {
                scan_count += 1;
                if scan.table_name.resolved_eq(source_name) {
                    replaced_scan_count += 1;
                    scan.source = provider_as_source(provider.clone());
                    let schema = scan.source.schema();
                    scan.projected_schema = if let Some(projection) = &scan.projection {
                        Arc::new(DFSchema::new_with_metadata(
                            projection
                                .iter()
                                .map(|index| {
                                    (
                                        Some(scan.table_name.clone()),
                                        Arc::new(schema.field(*index).clone()),
                                    )
                                })
                                .collect(),
                            schema.metadata().clone(),
                        )?)
                    } else {
                        Arc::new(DFSchema::try_from_qualified_schema(
                            scan.table_name.clone(),
                            &schema,
                        )?)
                    };
                    Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
                } else {
                    Ok(Transformed::no(LogicalPlan::TableScan(scan)))
                }
            }
            LogicalPlan::Join(_) | LogicalPlan::Aggregate(_) => {
                Err(datafusion::error::DataFusionError::Plan(
                    "Streaming flow supports only a single projection/filter source".into(),
                ))
            }
            _ => Ok(Transformed::no(node)),
        })
        .context(DatafusionSnafu {
            context: "Failed to substitute transient flow input provider",
        })?
        .data;
    ensure!(
        scan_count == 1 && replaced_scan_count == 1,
        InvalidQuerySnafu {
            reason: format!(
                "Expected one source scan matching {:?}, found {scan_count} scans and {replaced_scan_count} matches",
                source_name
            )
        }
    );
    Ok(plan)
}

/// Validates the deliberately small stateless streaming SQL subset.
pub(crate) fn validate_plan(plan: &LogicalPlan) -> Result<(), Error> {
    let mut scans = 0;
    plan.apply(|node| {
        match node {
            LogicalPlan::TableScan(_) => scans += 1,
            LogicalPlan::Projection(_) | LogicalPlan::Filter(_) => {}
            _ => {
                return Err(datafusion::error::DataFusionError::Plan(
                    "Streaming flow supports only projection and filter over one source scan"
                        .into(),
                ));
            }
        }
        Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
    })
    .context(DatafusionSnafu {
        context: "Failed to validate streaming flow plan",
    })?;
    ensure!(
        scans == 1,
        InvalidQuerySnafu {
            reason: format!("Expected one source scan, found {scans}")
        }
    );
    Ok(())
}

/// Executes one mirror write using only the supplied batch and writes its output.
pub(crate) async fn execute(
    flow: &StatelessFlow,
    rows: &[DiffRow],
    batch_datatypes: &[datatypes::data_type::ConcreteDataType],
    query_engine: &Arc<dyn QueryEngine>,
    frontend_client: &Arc<FrontendClient>,
) -> Result<usize, Error> {
    let values = rows.iter().map(|(row, _, _)| row.clone()).collect();
    let batch = crate::expr::Batch::try_from_rows_with_types(values, batch_datatypes)
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?;
    let batch = RecordBatch::new(flow.source_schema.clone(), batch.batch().to_vec())
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?;
    let provider = input_provider(&batch)?;
    let source_ref = TableReference::full(
        flow.source_table_name[0].clone(),
        flow.source_table_name[1].clone(),
        flow.source_table_name[2].clone(),
    );
    let plan = replace_source(flow.plan.clone(), &source_ref, provider)?;
    let output = query_engine
        .execute(plan, flow.query_ctx.clone())
        .await
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?;
    let batches = match output.data {
        OutputData::RecordBatches(batches) => batches,
        OutputData::Stream(stream) => RecordBatches::try_collect(stream)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?,
        OutputData::AffectedRows(_) => {
            return UnexpectedSnafu {
                reason: "Streaming flow query returned affected rows",
            }
            .fail();
        }
    };

    let proto_schema = column_schemas_to_proto(batches.schema().column_schemas().to_vec(), &[])?;
    let mut output_rows = Vec::new();
    for batch in batches {
        let vectors = datatypes::vectors::Helper::try_into_vectors(batch.columns())
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;
        output_rows.extend(vectors_to_rows(vectors.iter(), batch.num_rows()));
    }
    if output_rows.is_empty() {
        return Ok(0);
    }
    let output_row_count = output_rows.len();
    let request = Request::RowInserts(RowInsertRequests {
        inserts: vec![RowInsertRequest {
            table_name: flow.sink_table_name[2].clone(),
            rows: Some(Rows {
                schema: proto_schema,
                rows: output_rows,
            }),
        }],
    });
    let mut peer = None;
    frontend_client
        .handle(
            request,
            &flow.sink_table_name[0],
            &flow.sink_table_name[1],
            &mut peer,
        )
        .await
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?;
    Ok(output_row_count)
}

#[cfg(test)]
mod tests {
    use datafusion::catalog::MemTable;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::Int32Vector;
    use session::context::QueryContext;

    use super::*;

    #[test]
    fn validation_rejects_plan_without_source_scan() {
        let plan = LogicalPlan::EmptyRelation(datafusion_expr::logical_plan::EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });
        assert!(validate_plan(&plan).is_err());
    }

    fn provider(schema: &SchemaRef, value: i32) -> Arc<dyn TableProvider> {
        let batch = RecordBatch::new(
            schema.clone(),
            vec![Arc::new(Int32Vector::from_slice([value])) as datatypes::prelude::VectorRef],
        )
        .unwrap();
        let arrow = batch.df_record_batch().clone();
        Arc::new(MemTable::try_new(arrow.schema(), vec![vec![arrow]]).unwrap())
    }

    #[tokio::test]
    async fn finite_projection_filter_does_not_retain_previous_batch() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "number",
            ConcreteDataType::int32_datatype(),
            false,
        )]));
        let initial_provider = provider(&schema, 0);
        let plan = test_source_plan(TableReference::bare("source"), initial_provider);
        assert!(validate_plan(&plan).is_ok());

        let engine = crate::test_utils::create_test_query_engine();
        let run = |input: Arc<dyn TableProvider>| {
            let engine = engine.clone();
            let plan = plan.clone();
            async move {
                let plan = replace_source(plan, &TableReference::bare("source"), input).unwrap();
                let output = engine.execute(plan, QueryContext::arc()).await.unwrap();
                match output.data {
                    OutputData::Stream(stream) => RecordBatches::try_collect(stream)
                        .await
                        .unwrap()
                        .iter()
                        .map(|batch| batch.num_rows())
                        .sum(),
                    OutputData::RecordBatches(batches) => {
                        batches.iter().map(|batch| batch.num_rows()).sum()
                    }
                    OutputData::AffectedRows(_) => 0,
                }
            }
        };

        assert_eq!(run(provider(&schema, 1)).await, 0);
        assert_eq!(run(provider(&schema, 2)).await, 1);
    }
}
