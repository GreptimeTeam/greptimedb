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

use common_error::ext::BoxedError;
use common_function::function::FunctionContext;
use common_function::function_registry::FUNCTION_REGISTRY;
use common_query::error::RegisterUdfSnafu;
use common_query::logical_plan::SubstraitPlanDecoder;
use datafusion::catalog::CatalogProviderList;
use datafusion::common::DataFusionError;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::execution::registry::SerializerRegistry;
use datafusion::execution::{FunctionRegistry, SessionStateBuilder};
use datafusion::logical_expr::LogicalPlan;
use datafusion_expr::UserDefinedLogicalNode;
use greptime_proto::substrait_extension::MergeScan as PbMergeScan;
use promql::functions::{
    AbsentOverTime, AvgOverTime, Changes, CountOverTime, Delta, Deriv, DoubleExponentialSmoothing,
    IDelta, Increase, LastOverTime, MaxOverTime, MinOverTime, PredictLinear, PresentOverTime,
    QuantileOverTime, Rate, Resets, Round, StddevOverTime, StdvarOverTime, SumOverTime,
    quantile_udaf,
};
use prost::Message;
use session::context::QueryContextRef;
use snafu::ResultExt;
use substrait::extension_serializer::ExtensionSerializer;
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};

use crate::dist_plan::MergeScanLogicalPlan;
use crate::range_select::plan::RangeSelect;
use crate::range_select::serializer::{self, RANGE_SELECT_PARTIAL_NODE_NAME};

/// Extended [`substrait::extension_serializer::ExtensionSerializer`] but supports [`MergeScanLogicalPlan`] serialization.
#[derive(Debug)]
pub struct DefaultSerializer;

impl SerializerRegistry for DefaultSerializer {
    fn serialize_logical_plan(&self, node: &dyn UserDefinedLogicalNode) -> Result<Vec<u8>> {
        if node.name() == MergeScanLogicalPlan::name() {
            let merge_scan = node
                .as_any()
                .downcast_ref::<MergeScanLogicalPlan>()
                .expect("Failed to downcast to MergeScanLogicalPlan");

            let input = merge_scan.input();
            let is_placeholder = merge_scan.is_placeholder();
            let input = DFLogicalSubstraitConvertor
                .encode(input, DefaultSerializer)
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .to_vec();

            Ok(PbMergeScan {
                is_placeholder,
                input,
            }
            .encode_to_vec())
        } else if let Some(range_select) = node.as_any().downcast_ref::<RangeSelect>() {
            serializer::serialize(range_select)
        } else {
            ExtensionSerializer.serialize_logical_plan(node)
        }
    }

    fn deserialize_logical_plan(
        &self,
        name: &str,
        bytes: &[u8],
    ) -> Result<Arc<dyn UserDefinedLogicalNode>> {
        if name == MergeScanLogicalPlan::name() {
            // TODO(dennis): missing `session_state` to decode the logical plan in `MergeScanLogicalPlan`,
            // so we only save the unoptimized logical plan for view currently.
            Err(DataFusionError::Substrait(format!(
                "Unsupported plan node: {name}"
            )))
        } else if name == RANGE_SELECT_PARTIAL_NODE_NAME {
            serializer::deserialize(bytes)
        } else {
            ExtensionSerializer.deserialize_logical_plan(name, bytes)
        }
    }
}

/// The datafusion `[LogicalPlan]` decoder.
pub struct DefaultPlanDecoder {
    session_state: SessionState,
    query_ctx: QueryContextRef,
}

impl DefaultPlanDecoder {
    pub fn new(
        session_state: SessionState,
        query_ctx: &QueryContextRef,
    ) -> crate::error::Result<Self> {
        Ok(Self {
            session_state,
            query_ctx: query_ctx.clone(),
        })
    }
}

#[async_trait::async_trait]
impl SubstraitPlanDecoder for DefaultPlanDecoder {
    async fn decode(
        &self,
        message: bytes::Bytes,
        catalog_list: Arc<dyn CatalogProviderList>,
        optimize: bool,
    ) -> common_query::error::Result<LogicalPlan> {
        // The session_state already has the `DefaultSerialzier` as `SerializerRegistry`.
        let mut session_state = SessionStateBuilder::new_from_existing(self.session_state.clone())
            .with_catalog_list(catalog_list)
            .build();
        // Substrait decoder will look up the UDFs in SessionState, so we need to register them
        // Note: the query context must be passed to set the timezone
        // We MUST register the UDFs after we build the session state, otherwise the UDFs will be lost
        // if they have the same name as the default UDFs or their alias.
        // e.g. The default UDF `to_char()` has an alias `date_format()`, if we register a UDF with the name `date_format()`
        // before we build the session state, the UDF will be lost.
        for func in FUNCTION_REGISTRY.scalar_functions() {
            let udf = func.provide(FunctionContext {
                query_ctx: self.query_ctx.clone(),
                state: Default::default(),
            });
            session_state
                .register_udf(Arc::new(udf))
                .context(RegisterUdfSnafu { name: func.name() })?;
        }

        for func in FUNCTION_REGISTRY.aggregate_functions() {
            let name = func.name().to_string();
            session_state
                .register_udaf(Arc::new(func))
                .context(RegisterUdfSnafu { name })?;
        }

        let _ = session_state.register_udaf(quantile_udaf());

        let _ = session_state.register_udf(Arc::new(IDelta::<false>::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(IDelta::<true>::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(Rate::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(Increase::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(Delta::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(Resets::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(Changes::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(Deriv::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(Round::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(AvgOverTime::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(MinOverTime::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(MaxOverTime::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(SumOverTime::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(CountOverTime::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(LastOverTime::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(AbsentOverTime::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(PresentOverTime::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(StddevOverTime::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(StdvarOverTime::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(QuantileOverTime::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(PredictLinear::scalar_udf()));
        let double_exponential_smoothing_udf =
            DoubleExponentialSmoothing::scalar_udf().with_aliases(["prom_holt_winters"]);
        let _ = session_state.register_udf(Arc::new(double_exponential_smoothing_udf));

        let logical_plan = DFLogicalSubstraitConvertor
            .decode(message, session_state)
            .await
            .map_err(BoxedError::new)
            .context(common_query::error::DecodePlanSnafu)?;

        if optimize {
            self.session_state
                .optimize(&logical_plan)
                .map_err(Into::into)
        } else {
            Ok(logical_plan)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use datafusion::catalog::TableProvider;
    use datafusion::common::{Column, TableReference};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::datasource::{MemTable, provider_as_source};
    use datafusion::logical_expr::{LogicalPlanBuilder, LogicalTableSource, col, lit};
    use datafusion::physical_plan::{ExecutionPlan, collect};
    use datafusion::prelude::SessionContext;
    use datafusion_expr::{Expr, Extension, LogicalPlan};
    use datafusion_physical_expr::create_physical_expr;
    use datatypes::arrow::array::{Int64Array, StringDictionaryBuilder, TimestampSecondArray};
    use datatypes::arrow::datatypes::{DataType, Field, Int8Type, Schema, SchemaRef};
    use datatypes::arrow::record_batch::RecordBatch;
    use prost::Message;
    use session::context::QueryContext;
    use substrait::substrait_proto_df::proto::plan_rel;
    use substrait::substrait_proto_df::proto::rel::RelType;

    use super::*;
    use crate::QueryEngineFactory;
    use crate::dummy_catalog::DummyCatalogList;
    use crate::optimizer::test_util::mock_table_provider;
    use crate::options::QueryOptions;
    use crate::range_select::plan::RangeFn;

    fn mock_plan(schema: SchemaRef) -> LogicalPlan {
        let table_source = LogicalTableSource::new(schema);
        let projection = None;
        let builder =
            LogicalPlanBuilder::scan("devices", Arc::new(table_source), projection).unwrap();

        builder
            .filter(col("k0").eq(lit("hello")))
            .unwrap()
            .build()
            .unwrap()
    }

    fn range_select_partial_fixture() -> (RangeSelect, Arc<dyn TableProvider>, RecordBatch) {
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Second, None),
                    false,
                ),
                Field::new("value", DataType::Int64, true),
                Field::new(
                    "host",
                    DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                    true,
                ),
            ],
            HashMap::from([("range_select_test".to_string(), "metadata".to_string())]),
        ));
        let mut hosts = StringDictionaryBuilder::<Int8Type>::new();
        for host in ["a", "a", "a", "a"] {
            hosts.append(host).unwrap();
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampSecondArray::from(vec![0, 5, 0, 5])),
                Arc::new(Int64Array::from(vec![Some(1), Some(3), Some(2), Some(4)])),
                Arc::new(hosts.finish()),
            ],
        )
        .unwrap();
        let provider: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(schema, vec![vec![batch.clone()]]).unwrap());
        let input = LogicalPlanBuilder::scan(
            "public.measurements",
            provider_as_source(provider.clone()),
            None,
        )
        .unwrap()
        .build()
        .unwrap();
        let column = |name| {
            Expr::Column(Column::new(
                Some(TableReference::partial("public", "measurements")),
                name,
            ))
        };
        let value = || column("value");
        let time = column("timestamp");
        let by = column("host");
        let complete = RangeSelect::try_new(
            Arc::new(input),
            vec![
                RangeFn {
                    name: "sum_value".into(),
                    data_type: DataType::Int64,
                    expr: datafusion::functions_aggregate::expr_fn::sum(value()),
                    range: Duration::from_secs(10),
                    fill: None,
                    need_cast: false,
                },
                RangeFn {
                    name: "min_value".into(),
                    data_type: DataType::Int64,
                    expr: datafusion::functions_aggregate::expr_fn::min(value()),
                    range: Duration::from_secs(10),
                    fill: None,
                    need_cast: false,
                },
                RangeFn {
                    name: "avg_value".into(),
                    data_type: DataType::Float64,
                    expr: datafusion::functions_aggregate::expr_fn::avg(value()),
                    range: Duration::from_secs(10),
                    fill: None,
                    need_cast: false,
                },
            ],
            Duration::from_secs(5),
            0,
            time,
            vec![by],
            &[],
        )
        .unwrap();
        (complete, provider, batch)
    }

    fn split_range_select_partial(complete: &RangeSelect) -> (RangeSelect, RangeSelect) {
        let LogicalPlan::Extension(Extension { node: final_node }) =
            complete.try_split_for_pushdown().unwrap()
        else {
            unreachable!()
        };
        let final_node = final_node
            .as_any()
            .downcast_ref::<RangeSelect>()
            .unwrap()
            .clone();
        let LogicalPlan::Extension(Extension { node: partial_node }) = final_node.input.as_ref()
        else {
            unreachable!()
        };
        (
            partial_node
                .as_any()
                .downcast_ref::<RangeSelect>()
                .unwrap()
                .clone(),
            final_node,
        )
    }

    async fn execute_range_select_partial(
        partial: &RangeSelect,
        input_batch: RecordBatch,
    ) -> Vec<RecordBatch> {
        let LogicalPlan::Projection(projection) = partial.input.as_ref() else {
            unreachable!()
        };
        let session = SessionContext::new();
        let state = session.state();
        let input: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(
                &[vec![input_batch]],
                projection.input.schema().as_arrow().clone().into(),
                None,
            )
            .unwrap(),
        )));
        let expressions = projection
            .expr
            .iter()
            .enumerate()
            .map(|(index, expr)| {
                Ok((
                    create_physical_expr(
                        expr,
                        projection.input.schema().as_ref(),
                        state.execution_props(),
                    )?,
                    projection.schema.field(index).name().clone(),
                ))
            })
            .collect::<datafusion::error::Result<Vec<_>>>()
            .unwrap();
        let input = Arc::new(
            datafusion::physical_plan::projection::ProjectionExec::try_new(expressions, input)
                .unwrap(),
        );
        let plan = partial
            .to_execution_plan(partial.input.as_ref(), input, &state)
            .unwrap();
        collect(plan, session.task_ctx()).await.unwrap()
    }

    #[tokio::test]
    async fn range_select_partial_crosses_default_serializer_boundary() {
        let (complete, provider, batch) = range_select_partial_fixture();
        let (partial, _) = split_range_select_partial(&complete);
        let LogicalPlan::Projection(projection) = partial.input.as_ref() else {
            unreachable!()
        };
        assert!(matches!(
            &projection.expr[4],
            Expr::Alias(alias) if matches!(alias.expr.as_ref(), Expr::Cast(_))
        ));
        let metadata = partial.partial_wire_metadata().unwrap();
        assert_eq!(metadata.by_indices(), &[1]);
        assert_eq!(
            metadata
                .functions()
                .iter()
                .map(|function| function.argument_index())
                .collect::<Vec<_>>(),
            vec![2, 3, 4],
        );

        let partial_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(partial.clone()),
        });
        let bytes = DFLogicalSubstraitConvertor
            .encode(&partial_plan, DefaultSerializer)
            .unwrap();
        let substrait = substrait::substrait_proto_df::proto::Plan::decode(bytes.clone()).unwrap();
        let Some(plan_rel::RelType::Root(root)) = substrait.relations[0].rel_type.as_ref() else {
            unreachable!()
        };
        let Some(RelType::ExtensionSingle(extension)) = root
            .input
            .as_ref()
            .and_then(|input| input.rel_type.as_ref())
        else {
            unreachable!()
        };
        assert_eq!(
            extension.detail.as_ref().unwrap().type_url,
            RANGE_SELECT_PARTIAL_NODE_NAME
        );
        assert_eq!(partial.name(), RANGE_SELECT_PARTIAL_NODE_NAME);

        let factory = QueryEngineFactory::new(
            catalog::memory::new_memory_catalog_manager().unwrap(),
            None,
            None,
            None,
            None,
            false,
            QueryOptions::default(),
        );
        let decoded = factory
            .query_engine()
            .engine_context(QueryContext::arc())
            .new_plan_decoder()
            .unwrap()
            .decode(
                bytes,
                Arc::new(DummyCatalogList::with_table_provider(provider)),
                false,
            )
            .await
            .unwrap();
        let LogicalPlan::Extension(Extension { node }) = decoded else {
            unreachable!()
        };
        let decoded = node.as_any().downcast_ref::<RangeSelect>().unwrap();
        assert_eq!(decoded.name(), RANGE_SELECT_PARTIAL_NODE_NAME);
        assert_eq!(decoded.schema.as_arrow(), partial.schema.as_arrow());
        assert_eq!(decoded.schema.metadata(), partial.schema.metadata());
        assert_eq!(decoded.by_schema.as_arrow(), partial.by_schema.as_arrow());
        assert_eq!(
            decoded.partial_wire_metadata(),
            partial.partial_wire_metadata()
        );
        let LogicalPlan::Projection(decoded_projection) = decoded.input.as_ref() else {
            unreachable!()
        };
        assert!(matches!(
            &decoded_projection.expr[4],
            Expr::Alias(alias) if matches!(alias.expr.as_ref(), Expr::Cast(_))
        ));

        let original_batches = execute_range_select_partial(&partial, batch.clone()).await;
        let decoded_batches = execute_range_select_partial(decoded, batch).await;
        assert_eq!(
            original_batches.first().unwrap().schema(),
            decoded_batches.first().unwrap().schema()
        );
        assert_eq!(original_batches, decoded_batches);
    }

    #[test]
    fn range_select_partial_serializer_rejects_complete_and_final() {
        let (complete, _, _) = range_select_partial_fixture();
        let (_, final_node) = split_range_select_partial(&complete);

        assert!(DefaultSerializer.serialize_logical_plan(&complete).is_err());
        assert!(
            DefaultSerializer
                .serialize_logical_plan(&final_node)
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_serializer_decode_plan() {
        let catalog_list = catalog::memory::new_memory_catalog_manager().unwrap();
        let factory = QueryEngineFactory::new(
            catalog_list,
            None,
            None,
            None,
            None,
            false,
            QueryOptions::default(),
        );

        let engine = factory.query_engine();

        let table_provider = Arc::new(mock_table_provider(1.into()));
        let plan = mock_plan(table_provider.schema().clone());

        let bytes = DFLogicalSubstraitConvertor
            .encode(&plan, DefaultSerializer)
            .unwrap();

        let plan_decoder = engine
            .engine_context(QueryContext::arc())
            .new_plan_decoder()
            .unwrap();
        let catalog_list = Arc::new(DummyCatalogList::with_table_provider(table_provider));

        let decode_plan = plan_decoder
            .decode(bytes, catalog_list, false)
            .await
            .unwrap();

        assert_eq!(
            "Filter: devices.k0 = Utf8(\"hello\")
  TableScan: devices",
            decode_plan.to_string(),
        );
    }
}
