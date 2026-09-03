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

use std::collections::HashSet;
use std::sync::Arc;

use api::helper::{to_grpc_value, vectors_to_rows};
use api::v1::greptime_request::Request;
use api::v1::{RowInsertRequest, RowInsertRequests, Rows};
use common_error::ext::BoxedError;
use common_query::OutputData;
use common_recordbatch::{RecordBatch, RecordBatches, map_dictionary_to_values_data_type};
use common_time::Timestamp;
use datafusion::catalog::MemTable;
use datafusion::datasource::{TableProvider, provider_as_source};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, DFSchema, TableReference};
use datafusion_expr::logical_plan::{Distinct, Projection};
use datafusion_expr::{Expr, LogicalPlan};
use datatypes::schema::{ColumnSchema, SchemaRef};
use datatypes::value::Value;
use query::QueryEngine;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt, ensure};
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
    pub(crate) source_schema_version: u32,
    pub(crate) sink_table_name: TableName,
    pub(crate) sink_schema: Vec<ColumnSchema>,
    pub(crate) sink_primary_keys: Vec<String>,
    /// The exact trailing columns resolved when the flow was created.
    pub(crate) auto_columns: Vec<ColumnSchema>,
    pub(crate) plan: LogicalPlan,
    pub(crate) query_ctx: QueryContextRef,
    pub(crate) create_args: crate::CreateFlowArgs,
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

/// Adds the source timestamp to every supported plan node that has to carry it through a
/// filter or projection. The expression is appended only after the visible expressions, so the
/// sink contract remains positional.
pub(crate) fn rewrite_source_timestamp(
    plan: LogicalPlan,
    source_name: &TableReference,
    source_timestamp_name: &str,
) -> Result<LogicalPlan, Error> {
    let mut names = HashSet::new();
    plan.apply(|node| {
        names.extend(
            node.schema()
                .fields()
                .iter()
                .map(|field| field.name().clone()),
        );
        Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
    })
    .context(DatafusionSnafu {
        context: "Failed to inspect streaming flow plan schema",
    })?;
    let mut hidden_name = "__flow_source_timestamp".to_string();
    let mut suffix = 0;
    while !names.insert(hidden_name.clone()) {
        suffix += 1;
        hidden_name = format!("__flow_source_timestamp_{suffix}");
    }
    let visible_count = plan.schema().fields().len();
    let source_timestamp = Column::from_name(source_timestamp_name);
    let mut plan = plan
        .transform_up_with_subqueries(|node| match node {
            LogicalPlan::TableScan(mut scan) => {
                if scan.table_name.resolved_eq(source_name)
                    && let Some(projection) = &mut scan.projection
                {
                    let timestamp_index = scan
                        .source
                        .schema()
                        .index_of(source_timestamp_name)
                        .map_err(|_| {
                            datafusion::error::DataFusionError::Plan(
                                "Source timestamp is absent from source scan".into(),
                            )
                        })?;
                    if !projection.contains(&timestamp_index) {
                        projection.push(timestamp_index);
                        let schema = scan.source.schema();
                        scan.projected_schema = Arc::new(DFSchema::new_with_metadata(
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
                        )?);
                    }
                }
                Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
            }
            LogicalPlan::Projection(mut projection) => {
                let hidden_expr = if projection
                    .input
                    .schema()
                    .fields()
                    .iter()
                    .any(|field| field.name() == &hidden_name)
                {
                    Expr::Column(Column::from_name(hidden_name.clone()))
                } else {
                    Expr::Column(source_timestamp.clone())
                };
                projection.expr.push(hidden_expr.alias(hidden_name.clone()));
                let projection = Projection::try_new(projection.expr, projection.input)?;
                Ok(Transformed::yes(LogicalPlan::Projection(projection)))
            }
            _ => Ok(Transformed::no(node)),
        })
        .context(DatafusionSnafu {
            context: "Failed to add source timestamp to streaming flow plan",
        })?
        .data;

    // A plan ending at a scan or filter has no projection at which to give the carried value its
    // hidden name. Add one only in that case; a projection below a filter already carries it.
    if !plan
        .schema()
        .fields()
        .iter()
        .any(|field| field.name() == &hidden_name)
    {
        let expressions = plan
            .schema()
            .fields()
            .iter()
            .take(visible_count)
            .map(|field| Expr::Column(Column::from_name(field.name())))
            .chain(std::iter::once(
                Expr::Column(source_timestamp).alias(hidden_name),
            ))
            .collect::<Vec<_>>();
        plan = Projection::try_new(expressions, Arc::new(plan))
            .map(LogicalPlan::Projection)
            .context(DatafusionSnafu {
                context: "Failed to finalize source timestamp in streaming flow plan",
            })?;
    }
    Ok(plan)
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
            // DISTINCT is evaluated against this request's transient input only. DISTINCT ON
            // has ordering/selection semantics beyond the supported stateless subset.
            LogicalPlan::Distinct(Distinct::All(_)) => {}
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

/// Rejects execution when the source metadata changed after the flow plan was retained.
/// Replanning is intentionally not attempted: the flow must be recreated or recovered so its
/// plan and source schema are captured together.
fn validate_source_schema_version(
    retained_version: u32,
    current_version: u32,
) -> Result<(), Error> {
    ensure!(
        retained_version == current_version,
        InvalidQuerySnafu {
            reason: format!(
                "Source schema version changed from {retained_version} to {current_version}; recreate or recover the flow before writing"
            )
        }
    );
    Ok(())
}

fn synthesize_auto_values(columns: &[ColumnSchema], now: Timestamp) -> Result<Vec<Value>, Error> {
    columns
        .iter()
        .map(|column| {
            let timestamp_type = column.data_type.as_timestamp().context(InvalidQuerySnafu {
                reason: format!("Auto sink column {} is not a timestamp", column.name),
            })?;
            let value = if column.name == crate::adapter::AUTO_CREATED_UPDATE_AT_TS_COL {
                now.convert_to(timestamp_type.unit())
                    .context(InvalidQuerySnafu {
                        reason: "Current timestamp cannot be represented in sink timestamp unit",
                    })?
            } else if column.name == crate::adapter::AUTO_CREATED_PLACEHOLDER_TS_COL {
                Timestamp::new(0, timestamp_type.unit())
            } else {
                return InvalidQuerySnafu {
                    reason: format!("Unsupported auto sink column {}", column.name),
                }
                .fail();
            };
            Ok(Value::Timestamp(value))
        })
        .collect()
}

/// Executes one mirror write using only the supplied batch and writes its output.
pub(crate) async fn execute(
    flow: &StatelessFlow,
    rows: &[DiffRow],
    batch_datatypes: &[datatypes::data_type::ConcreteDataType],
    query_engine: &Arc<dyn QueryEngine>,
    frontend_client: &Arc<FrontendClient>,
    current_source_schema_version: u32,
) -> Result<usize, Error> {
    validate_source_schema_version(flow.source_schema_version, current_source_schema_version)?;
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

    let output_schema = batches
        .schema()
        .column_schemas()
        .iter()
        .cloned()
        .map(|mut column| {
            column.data_type = map_dictionary_to_values_data_type(&column.data_type);
            column
        })
        .collect::<Vec<_>>();
    crate::adapter::validate_sink_layout_with_suffix(
        &output_schema,
        &flow.sink_schema,
        &flow.auto_columns,
    )?;
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

    // Auto columns are deliberately synthesized here rather than in the query plan. This keeps
    // one current timestamp for the whole request and preserves the sink's timestamp precision.
    let auto_values = synthesize_auto_values(&flow.auto_columns, Timestamp::current_millis())?
        .into_iter()
        .map(to_grpc_value)
        .collect::<Vec<_>>();
    for row in &mut output_rows {
        row.values.extend(auto_values.iter().cloned());
    }

    let output_row_count = output_rows.len();
    let proto_schema = column_schemas_to_proto(flow.sink_schema.clone(), &flow.sink_primary_keys)?;
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
        .handle_insert_once(
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
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{Int32Vector, TimestampMillisecondVector};
    use session::context::QueryContext;

    use super::*;

    #[test]
    fn validation_accepts_distinct_over_one_source() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "number",
            ConcreteDataType::int32_datatype(),
            false,
        )]));
        let plan = LogicalPlanBuilder::scan(
            TableReference::bare("source"),
            provider_as_source(provider(&schema, 1)),
            None,
        )
        .unwrap()
        .project(vec![datafusion_expr::col("number")])
        .unwrap()
        .distinct()
        .unwrap()
        .build()
        .unwrap();
        assert!(validate_plan(&plan).is_ok());
    }

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

    #[test]
    fn source_schema_version_must_match_retained_plan() {
        assert!(validate_source_schema_version(7, 7).is_ok());

        let error = validate_source_schema_version(7, 8).unwrap_err();
        assert!(matches!(error, Error::InvalidQuery { reason, .. } if
            reason.contains("Source schema version changed from 7 to 8")
                && reason.contains("recreate or recover")
        ));
    }

    #[test]
    fn auto_values_use_the_sink_timestamp_units() {
        let update_at = ColumnSchema::new(
            crate::adapter::AUTO_CREATED_UPDATE_AT_TS_COL,
            datatypes::data_type::ConcreteDataType::timestamp_second_datatype(),
            true,
        );
        let placeholder = ColumnSchema::new(
            crate::adapter::AUTO_CREATED_PLACEHOLDER_TS_COL,
            datatypes::data_type::ConcreteDataType::timestamp_nanosecond_datatype(),
            true,
        );
        let values = synthesize_auto_values(
            &[update_at],
            Timestamp::new(1_234, common_time::timestamp::TimeUnit::Millisecond),
        )
        .unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(
            values[0].as_timestamp().unwrap().unit(),
            common_time::timestamp::TimeUnit::Second
        );
        assert!(values[0].as_timestamp().unwrap().value() > 0);

        let values = synthesize_auto_values(
            &[
                ColumnSchema::new(
                    crate::adapter::AUTO_CREATED_UPDATE_AT_TS_COL,
                    datatypes::data_type::ConcreteDataType::timestamp_microsecond_datatype(),
                    true,
                ),
                placeholder,
            ],
            Timestamp::new(1_234, common_time::timestamp::TimeUnit::Millisecond),
        )
        .unwrap();
        assert_eq!(values[1].as_timestamp().unwrap().value(), 0);
        assert_eq!(
            values[1].as_timestamp().unwrap().unit(),
            common_time::timestamp::TimeUnit::Nanosecond
        );
    }

    #[test]
    fn auto_values_reject_arbitrary_columns() {
        let column = ColumnSchema::new(
            "other",
            datatypes::data_type::ConcreteDataType::timestamp_millisecond_datatype(),
            true,
        );
        assert!(synthesize_auto_values(&[column], Timestamp::current_millis()).is_err());
    }

    #[test]
    fn rewrite_source_timestamp_appends_collision_free_hidden_output() {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("number", ConcreteDataType::int32_datatype(), false),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new(
                "__flow_source_timestamp",
                ConcreteDataType::int32_datatype(),
                false,
            ),
        ]));
        let batch = RecordBatch::new(
            schema.clone(),
            vec![
                Arc::new(Int32Vector::from_slice([2])) as datatypes::prelude::VectorRef,
                Arc::new(TimestampMillisecondVector::from_slice([42]))
                    as datatypes::prelude::VectorRef,
                Arc::new(Int32Vector::from_slice([7])) as datatypes::prelude::VectorRef,
            ],
        )
        .unwrap();
        let plan = datafusion_expr::LogicalPlanBuilder::scan(
            TableReference::bare("source"),
            provider_as_source(input_provider(&batch).unwrap()),
            None,
        )
        .unwrap()
        .filter(datafusion_expr::col("number").gt(datafusion_expr::lit(1)))
        .unwrap()
        .project(vec![datafusion_expr::col("number")])
        .unwrap()
        .build()
        .unwrap();
        let rewritten =
            rewrite_source_timestamp(plan, &TableReference::bare("source"), "ts").unwrap();
        assert_eq!(rewritten.schema().fields().len(), 2);
        assert!(
            rewritten
                .schema()
                .field(1)
                .name()
                .starts_with("__flow_source_timestamp")
        );
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

    #[tokio::test]
    async fn finite_distinct_collapses_duplicates_per_request() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "number",
            ConcreteDataType::int32_datatype(),
            false,
        )]));
        let input = |values: &[i32]| {
            let batch = RecordBatch::new(
                schema.clone(),
                vec![Arc::new(Int32Vector::from_slice(values)) as datatypes::prelude::VectorRef],
            )
            .unwrap();
            input_provider(&batch).unwrap()
        };
        let plan = LogicalPlanBuilder::scan(
            TableReference::bare("source"),
            provider_as_source(input(&[1])),
            None,
        )
        .unwrap()
        .project(vec![datafusion_expr::col("number")])
        .unwrap()
        .distinct()
        .unwrap()
        .build()
        .unwrap();
        assert!(validate_plan(&plan).is_ok());

        let engine = crate::test_utils::create_test_query_engine();
        let run = |provider: Arc<dyn TableProvider>| {
            let engine = engine.clone();
            let plan = plan.clone();
            async move {
                let plan = replace_source(plan, &TableReference::bare("source"), provider).unwrap();
                let output = engine.execute(plan, QueryContext::arc()).await.unwrap();
                let batches = match output.data {
                    OutputData::Stream(stream) => RecordBatches::try_collect(stream).await.unwrap(),
                    OutputData::RecordBatches(batches) => batches,
                    OutputData::AffectedRows(_) => panic!("unexpected affected rows"),
                };
                batches.iter().map(|batch| batch.num_rows()).sum::<usize>()
            }
        };

        assert_eq!(run(input(&[1, 1, 2])).await, 2);
        assert_eq!(run(input(&[1, 1])).await, 1);
    }
}
