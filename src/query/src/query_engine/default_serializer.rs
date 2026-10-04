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

use bytes::Bytes;
use catalog::CatalogManagerRef;
use common_error::ext::BoxedError;
use common_function::function::FunctionContext;
use common_function::function_registry::FUNCTION_REGISTRY;
use common_query::error::RegisterUdfSnafu;
use common_query::logical_plan::SubstraitPlanDecoder;
use datafusion::catalog::{CatalogProviderList, TableProvider};
use datafusion::common::{DFSchema, DataFusionError, TableReference, not_impl_err, substrait_err};
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::execution::registry::SerializerRegistry;
use datafusion::execution::{FunctionRegistry, SessionStateBuilder};
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion_expr::{Expr, UserDefinedLogicalNode};
use greptime_proto::substrait_extension::MergeScan as PbMergeScan;
use promql::functions::{
    AbsentOverTime, AvgOverTime, Changes, CountOverTime, Delta, Deriv, DoubleExponentialSmoothing,
    IDelta, Increase, LastOverTime, MaxOverTime, MinOverTime, MixedRange,
    NativeHistogramAbsentOverTime, NativeHistogramAdd, NativeHistogramAggAvg,
    NativeHistogramAggSum, NativeHistogramAvg, NativeHistogramAvgOverTime, NativeHistogramChanges,
    NativeHistogramCount, NativeHistogramCountOverTime, NativeHistogramDelta,
    NativeHistogramDivScalar, NativeHistogramDrop, NativeHistogramEq, NativeHistogramFraction,
    NativeHistogramIDelta, NativeHistogramIRate, NativeHistogramIncrease,
    NativeHistogramLastOverTime, NativeHistogramMulScalar, NativeHistogramNeg,
    NativeHistogramNotEq, NativeHistogramPresentOverTime, NativeHistogramQuantile,
    NativeHistogramRate, NativeHistogramResets, NativeHistogramScalarMul, NativeHistogramStddev,
    NativeHistogramStdvar, NativeHistogramSub, NativeHistogramSum, NativeHistogramSumOverTime,
    NativeHistogramToString, PredictLinear, PresentOverTime, PromqlFloatToString, QuantileOverTime,
    Rate, Resets, Round, StddevOverTime, StdvarOverTime, SumOverTime, quantile_udaf,
};
use prost::Message;
use session::context::{QueryContext, QueryContextRef};
use snafu::ResultExt;
use substrait::df_logical_plan::consumer::{DefaultSubstraitConsumer, SubstraitConsumer};
use substrait::error::{DecodeDfPlanSnafu, DecodeRelSnafu};
use substrait::extension_serializer::ExtensionSerializer;
use substrait::substrait_proto_df::proto::{
    ExtensionLeafRel, ExtensionMultiRel, ExtensionSingleRel, Plan, Type,
};
use substrait::{DFLogicalSubstraitConvertor, Extensions, SubstraitPlan};

use crate::dist_plan::MergeScanLogicalPlan;
use crate::query_engine::QueryEngineState;

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
            // This registry has no session state; the query engine decodes `MergeScan` payloads.
            Err(DataFusionError::Substrait(format!(
                "Unsupported plan node: {name}"
            )))
        } else {
            ExtensionSerializer.deserialize_logical_plan(name, bytes)
        }
    }
}

/// Async `MergeScan` payload decoding over DataFusion's default Substrait consumer.
struct MergeScanSubstraitConsumer<'a> {
    inner: DefaultSubstraitConsumer<'a>,
    /// State of the level being decoded; payload states are rebuilt from it.
    session_state: &'a SessionState,
    /// Engine catalog for payload tables; absent for manually built session states.
    catalog_manager: Option<CatalogManagerRef>,
}

impl MergeScanSubstraitConsumer<'_> {
    fn query_ctx(&self) -> QueryContextRef {
        self.session_state
            .config()
            .get_extension()
            .unwrap_or_else(QueryContext::arc)
    }

    /// Rebuilds payload state with request defaults and re-registers Greptime function bindings.
    fn payload_state(&self, catalog_list: Arc<dyn CatalogProviderList>) -> Result<SessionState> {
        let query_ctx = self.query_ctx();
        let mut config = self.session_state.config().clone();
        {
            let catalog_options = &mut config.options_mut().catalog;
            catalog_options.default_catalog = query_ctx.current_catalog().to_string();
            catalog_options.default_schema = query_ctx.current_schema();
        }

        let mut state = SessionStateBuilder::new_from_existing(self.session_state.clone())
            .with_config(config)
            .with_catalog_list(catalog_list)
            .build();
        register_greptime_functions(&mut state, &query_ctx).map_err(DataFusionError::from)?;

        Ok(state)
    }

    /// Decodes payload tables through the engine catalog, not the region-bound request catalog.
    ///
    /// Do not fall back after an engine-catalog failure: it can bind payload tables to the request
    /// region with the wrong schema. Manually built states lack that catalog and use the request list.
    async fn decode_payload(&self, payload: Vec<u8>) -> Result<LogicalPlan> {
        let state = if let Some(catalog_manager) = &self.catalog_manager {
            let engine_catalog = Arc::new(
                catalog::table_source::dummy_catalog::DummyCatalogList::new_with_query_ctx(
                    catalog_manager.clone(),
                    self.query_ctx(),
                ),
            );
            self.payload_state(engine_catalog)?
        } else {
            self.payload_state(self.session_state.catalog_list().clone())?
        };

        decode_plan(Bytes::from(payload), state)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}

#[async_trait::async_trait]
impl SubstraitConsumer for MergeScanSubstraitConsumer<'_> {
    async fn resolve_table_ref(
        &self,
        table_ref: &TableReference,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        self.inner.resolve_table_ref(table_ref).await
    }

    fn get_extensions(&self) -> &Extensions {
        self.inner.get_extensions()
    }

    fn get_function_registry(&self) -> &impl FunctionRegistry {
        self.inner.get_function_registry()
    }

    fn push_outer_schema(&self, schema: Arc<DFSchema>) {
        self.inner.push_outer_schema(schema)
    }

    fn pop_outer_schema(&self) {
        self.inner.pop_outer_schema()
    }

    fn get_outer_schema(&self, steps_out: usize) -> Option<Arc<DFSchema>> {
        self.inner.get_outer_schema(steps_out)
    }

    fn push_lambda_parameters(
        &self,
        lambda_parameters: &[Type],
        input_schema: &DFSchema,
    ) -> Result<Vec<String>> {
        self.inner
            .push_lambda_parameters(lambda_parameters, input_schema)
    }

    fn pop_lambda_parameters(&self) {
        self.inner.pop_lambda_parameters()
    }

    fn lambda_variable(&self, steps_out: usize, field_idx: usize) -> Result<Expr> {
        self.inner.lambda_variable(steps_out, field_idx)
    }

    async fn consume_extension_leaf(&self, rel: &ExtensionLeafRel) -> Result<LogicalPlan> {
        // Unknown and detail-less leaves keep the default consumer's errors.
        let Some(detail) = rel.detail.as_ref() else {
            return self.inner.consume_extension_leaf(rel).await;
        };
        if detail.type_url != MergeScanLogicalPlan::name() {
            return self.inner.consume_extension_leaf(rel).await;
        }

        let merge_scan = PbMergeScan::decode(detail.value.as_ref()).map_err(|e| {
            DataFusionError::Substrait(format!("Failed to decode the MergeScan plan node: {e}"))
        })?;

        let input = self.decode_payload(merge_scan.input).await?;

        // `PbMergeScan` lacks `partition_cols`; decoded plans lose this optimization and may repartition.
        Ok(
            MergeScanLogicalPlan::new(input, merge_scan.is_placeholder, Default::default())
                .into_logical_plan(),
        )
    }

    // Keep child dispatch on this consumer; delegating the parent bypasses async MergeScan decoding.
    async fn consume_extension_single(&self, rel: &ExtensionSingleRel) -> Result<LogicalPlan> {
        let Some(detail) = &rel.detail else {
            return substrait_err!("Unexpected empty detail in ExtensionSingleRel");
        };
        let plan = self
            .session_state
            .serializer_registry()
            .deserialize_logical_plan(&detail.type_url, &detail.value)?;
        let Some(input_rel) = &rel.input else {
            return substrait_err!(
                "ExtensionSingleRel missing input rel, try using ExtensionLeafRel instead"
            );
        };
        let input_plan = self.consume_rel(input_rel).await?;
        let plan = plan.with_exprs_and_inputs(plan.expressions(), vec![input_plan])?;
        Ok(LogicalPlan::Extension(Extension { node: plan }))
    }

    // Multi-input extension children likewise need this consumer's dispatch.
    async fn consume_extension_multi(&self, rel: &ExtensionMultiRel) -> Result<LogicalPlan> {
        let Some(detail) = &rel.detail else {
            return substrait_err!("Unexpected empty detail in ExtensionMultiRel");
        };
        let plan = self
            .session_state
            .serializer_registry()
            .deserialize_logical_plan(&detail.type_url, &detail.value)?;
        let mut inputs = Vec::with_capacity(rel.inputs.len());
        for input in &rel.inputs {
            let input_plan = self.consume_rel(input).await?;
            inputs.push(input_plan);
        }
        let plan = plan.with_exprs_and_inputs(plan.expressions(), inputs)?;
        Ok(LogicalPlan::Extension(Extension { node: plan }))
    }
}

/// Decode with async MergeScan handling, preserving the default consumer's extension checks.
async fn decode_plan(bytes: Bytes, state: SessionState) -> substrait::error::Result<LogicalPlan> {
    let plan = Plan::decode(bytes).context(DecodeRelSnafu)?;
    let extensions = Extensions::try_from(&plan.extensions).context(DecodeDfPlanSnafu)?;
    if !extensions.type_variations.is_empty() {
        return not_impl_err!("Type variation extensions are not supported")
            .context(DecodeDfPlanSnafu);
    }

    // Payloads need the engine catalog rather than the request's region-bound catalog.
    let catalog_manager = state
        .config()
        .get_extension::<QueryEngineState>()
        .map(|engine_state| engine_state.catalog_manager().clone());
    let consumer = MergeScanSubstraitConsumer {
        inner: DefaultSubstraitConsumer::new(&extensions, &state),
        session_state: &state,
        catalog_manager,
    };

    DFLogicalSubstraitConvertor
        .decode_with_consumer(&plan, &state, &consumer)
        .await
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

/// Re-registers Greptime functions after every state build.
///
/// DataFusion aliases can rebind Greptime `date_format` to built-in `to_char`; registration must
/// happen after `SessionStateBuilder::build` on the state that decodes the plan.
fn register_greptime_functions(
    session_state: &mut SessionState,
    query_ctx: &QueryContextRef,
) -> common_query::error::Result<()> {
    for func in FUNCTION_REGISTRY.scalar_functions() {
        let udf = func.provide(FunctionContext {
            query_ctx: query_ctx.clone(),
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

    for udf in [
        NativeHistogramAbsentOverTime::scalar_udf(),
        NativeHistogramAdd::scalar_udf(),
        NativeHistogramAvg::scalar_udf(),
        NativeHistogramAvgOverTime::scalar_udf(),
        NativeHistogramChanges::scalar_udf(),
        NativeHistogramCount::scalar_udf(),
        NativeHistogramCountOverTime::scalar_udf(),
        NativeHistogramDelta::scalar_udf(),
        NativeHistogramDivScalar::scalar_udf(),
        NativeHistogramDrop::bool_false_udf(String::new(), None),
        NativeHistogramDrop::bool_true_udf(String::new(), None),
        NativeHistogramDrop::float_null_udf(String::new(), None),
        NativeHistogramEq::scalar_udf(),
        NativeHistogramFraction::scalar_udf(),
        NativeHistogramIDelta::scalar_udf(),
        NativeHistogramIRate::scalar_udf(),
        NativeHistogramIncrease::scalar_udf(),
        NativeHistogramLastOverTime::scalar_udf(),
        MixedRange::float_udf(None),
        MixedRange::histogram_udf(None),
        NativeHistogramMulScalar::scalar_udf(),
        NativeHistogramNeg::scalar_udf(),
        NativeHistogramNotEq::scalar_udf(),
        NativeHistogramPresentOverTime::scalar_udf(),
        NativeHistogramQuantile::scalar_udf(),
        NativeHistogramRate::scalar_udf(),
        NativeHistogramResets::scalar_udf(),
        NativeHistogramScalarMul::scalar_udf(),
        NativeHistogramStddev::scalar_udf(),
        NativeHistogramStdvar::scalar_udf(),
        NativeHistogramSub::scalar_udf(),
        NativeHistogramSum::scalar_udf(),
        NativeHistogramSumOverTime::scalar_udf(),
        NativeHistogramToString::scalar_udf(),
        PromqlFloatToString::scalar_udf(),
    ] {
        let _ = session_state.register_udf(Arc::new(udf));
    }

    for udaf in [
        NativeHistogramAggAvg::aggregate_udf(),
        NativeHistogramAggSum::aggregate_udf(),
    ] {
        let _ = session_state.register_udaf(Arc::new(udaf));
    }

    Ok(())
}

#[async_trait::async_trait]
impl SubstraitPlanDecoder for DefaultPlanDecoder {
    async fn decode(
        &self,
        message: bytes::Bytes,
        catalog_list: Arc<dyn CatalogProviderList>,
        optimize: bool,
    ) -> common_query::error::Result<LogicalPlan> {
        let mut session_state = SessionStateBuilder::new_from_existing(self.session_state.clone())
            .with_serializer_registry(Arc::new(DefaultSerializer))
            .with_catalog_list(catalog_list)
            .build();
        // Re-register after the build to avoid Greptime UDF alias collisions.
        register_greptime_functions(&mut session_state, &self.query_ctx)?;

        let logical_plan = decode_plan(message, session_state)
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
    use std::any::Any;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use catalog::error::{QueryAccessDeniedSnafu, Result as CatalogResult};
    use catalog::{CatalogManager, RegisterTableRequest};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, NUMBERS_TABLE_ID};
    use common_function::aggrs::aggr_wrapper::StateWrapper;
    use common_function::aggrs::aggr_wrapper::fix_order::FixStateUdafOrderingAnalyzer;
    use common_query::logical_plan::SubstraitPlanDecoderRef;
    use common_query::native_histogram::native_histogram_value_type;
    use common_time::Timezone;
    use datafusion::catalog::TableProvider;
    use datafusion::datasource::MemTable;
    use datafusion::logical_expr::Extension;
    use datafusion::optimizer::AnalyzerRule;
    use datafusion_expr::expr::{AggregateFunction, Cast, InSubquery, ScalarFunction, Sort};
    use datafusion_expr::{
        AggregateUDF, Expr, LogicalPlanBuilder, LogicalTableSource, ScalarUDF, Subquery, col, lit,
    };
    use datatypes::arrow::datatypes::{
        DataType as ArrowDataType, Field, Schema, SchemaRef, TimeUnit,
    };
    use datatypes::data_type::DataType;
    use futures::stream::BoxStream;
    use promql::extension_plan::{RangeManipulate, SeriesNormalize, UnionDistinctOn};
    use session::context::QueryContext;
    use table::TableRef;
    use table::metadata::{TableId, TableInfoRef};
    use table::table::numbers::{NUMBERS_TABLE_NAME, NumbersTable};

    use super::*;
    use crate::QueryEngineFactory;
    use crate::dummy_catalog::DummyCatalogList;
    use crate::optimizer::test_util::mock_table_provider;
    use crate::options::QueryOptions;

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

    #[tokio::test]
    async fn test_serializer_decode_native_histogram_udf() {
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
        let schema = Arc::new(Schema::new(vec![Field::new(
            "histogram",
            native_histogram_value_type().as_arrow_type(),
            true,
        )]));
        let plan = LogicalPlanBuilder::scan(
            "devices",
            Arc::new(LogicalTableSource::new(schema.clone())),
            None,
        )
        .unwrap()
        .aggregate(
            Vec::<Expr>::new(),
            vec![
                Arc::new(NativeHistogramAggSum::aggregate_udf())
                    .call(vec![col("histogram")])
                    .alias("sum"),
                Arc::new(NativeHistogramAggAvg::aggregate_udf())
                    .call(vec![col("histogram")])
                    .alias("avg"),
            ],
        )
        .unwrap()
        .project(vec![
            Expr::ScalarFunction(ScalarFunction {
                func: Arc::new(NativeHistogramCount::scalar_udf()),
                args: vec![col("sum")],
            }),
            Expr::ScalarFunction(ScalarFunction {
                func: Arc::new(NativeHistogramDrop::bool_false_udf(
                    "ignored annotation".to_string(),
                    None,
                )),
                args: vec![col("sum")],
            }),
            Expr::ScalarFunction(ScalarFunction {
                func: Arc::new(NativeHistogramDrop::bool_true_udf(
                    "ignored annotation".to_string(),
                    None,
                )),
                args: vec![col("sum")],
            }),
            Expr::ScalarFunction(ScalarFunction {
                func: Arc::new(NativeHistogramDrop::float_null_udf(
                    "ignored annotation".to_string(),
                    None,
                )),
                args: vec![col("sum")],
            }),
            Expr::ScalarFunction(ScalarFunction {
                func: Arc::new(PromqlFloatToString::scalar_udf()),
                args: vec![lit(2.0)],
            }),
        ])
        .unwrap()
        .build()
        .unwrap();
        let bytes = DFLogicalSubstraitConvertor
            .encode(&plan, DefaultSerializer)
            .unwrap();
        let table_provider = Arc::new(MemTable::try_new(schema, vec![vec![]]).unwrap());
        let plan_decoder = engine
            .engine_context(QueryContext::arc())
            .new_plan_decoder()
            .unwrap();

        let decoded = plan_decoder
            .decode(
                bytes,
                Arc::new(DummyCatalogList::with_table_provider(table_provider)),
                false,
            )
            .await
            .unwrap();

        let decoded = decoded.to_string();
        assert!(decoded.contains("prom_native_histogram_count"));
        assert!(decoded.contains("prom_native_histogram_drop_bool"));
        assert!(decoded.contains("prom_native_histogram_keep_bool"));
        assert!(decoded.contains("prom_native_histogram_drop_float"));
        assert!(decoded.contains(NativeHistogramAggSum::name()));
        assert!(decoded.contains(NativeHistogramAggAvg::name()));
        assert!(decoded.contains(PromqlFloatToString::name()));

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("float", ArrowDataType::Float64, true),
            Field::new(
                "histogram",
                native_histogram_value_type().as_arrow_type(),
                true,
            ),
        ]));
        let input = LogicalPlanBuilder::scan(
            "devices",
            Arc::new(LogicalTableSource::new(schema.clone())),
            None,
        )
        .unwrap()
        .build()
        .unwrap();
        let input = LogicalPlan::Extension(Extension {
            node: Arc::new(
                RangeManipulate::new(
                    0,
                    1000,
                    1000,
                    0,
                    1000,
                    "timestamp".to_string(),
                    vec!["float".to_string(), "histogram".to_string()],
                    input,
                )
                .unwrap(),
            ),
        });
        let plan = LogicalPlanBuilder::from(input)
            .project(vec![
                Expr::ScalarFunction(ScalarFunction {
                    func: Arc::new(MixedRange::float_udf(None)),
                    args: vec![
                        lit("last_over_time"),
                        col("timestamp_range"),
                        col("float"),
                        col("histogram"),
                    ],
                })
                .alias("mixed_float"),
                Expr::ScalarFunction(ScalarFunction {
                    func: Arc::new(MixedRange::histogram_udf(None)),
                    args: vec![
                        lit("last_over_time"),
                        col("timestamp_range"),
                        col("float"),
                        col("histogram"),
                    ],
                })
                .alias("mixed_histogram"),
            ])
            .unwrap()
            .build()
            .unwrap();
        let bytes = DFLogicalSubstraitConvertor
            .encode(&plan, DefaultSerializer)
            .unwrap();
        let table_provider = Arc::new(MemTable::try_new(schema, vec![vec![]]).unwrap());
        let decoded = plan_decoder
            .decode(
                bytes,
                Arc::new(DummyCatalogList::with_table_provider(table_provider)),
                false,
            )
            .await
            .unwrap()
            .to_string();
        assert!(decoded.contains("prom_mixed_range_float"));
        assert!(decoded.contains("prom_mixed_range_histogram"));
    }

    #[tokio::test]
    async fn test_serializer_decode_merge_scan() {
        let catalog_manager = catalog::memory::new_memory_catalog_manager().unwrap();
        // Payload tables must be registered in the engine catalog.
        catalog_manager
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: NUMBERS_TABLE_NAME.to_string(),
                table_id: NUMBERS_TABLE_ID,
                table: NumbersTable::table(NUMBERS_TABLE_ID),
            })
            .unwrap();
        let factory = QueryEngineFactory::new(
            catalog_manager,
            None,
            None,
            None,
            None,
            false,
            QueryOptions::default(),
        );
        let engine = factory.query_engine();

        let input = LogicalPlanBuilder::scan(
            NUMBERS_TABLE_NAME,
            Arc::new(LogicalTableSource::new(
                NumbersTable::schema().arrow_schema().clone(),
            )),
            None,
        )
        .unwrap()
        .build()
        .unwrap();
        let plan =
            MergeScanLogicalPlan::new(input.clone(), true, Default::default()).into_logical_plan();

        let bytes = DFLogicalSubstraitConvertor
            .encode(&plan, DefaultSerializer)
            .unwrap();
        let plan_decoder = engine
            .engine_context(QueryContext::arc())
            .new_plan_decoder()
            .unwrap();
        // Must not bind payload tables through the request catalog.
        let catalog_list = Arc::new(DummyCatalogList::with_table_provider(Arc::new(
            mock_table_provider(1.into()),
        )));

        let decoded = plan_decoder
            .decode(bytes, catalog_list, false)
            .await
            .unwrap();

        let LogicalPlan::Extension(extension) = &decoded else {
            panic!("Expect a MergeScan plan, got: {decoded}");
        };
        let merge_scan = extension
            .node
            .as_any()
            .downcast_ref::<MergeScanLogicalPlan>()
            .expect("Expect a MergeScan plan node");
        assert!(merge_scan.is_placeholder());
        assert_eq!(merge_scan.input().to_string(), input.to_string());
        // `PbMergeScan` doesn't carry `partition_cols`, so decoded nodes have an empty mapping.
        assert!(merge_scan.partition_cols().is_empty());
    }

    /// Payload table binding uses the engine catalog, not the request catalog.
    #[tokio::test]
    async fn test_serializer_decode_merge_scan_with_engine_catalog() {
        let catalog_manager = catalog::memory::new_memory_catalog_manager().unwrap();
        catalog_manager
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: NUMBERS_TABLE_NAME.to_string(),
                table_id: NUMBERS_TABLE_ID,
                table: NumbersTable::table(NUMBERS_TABLE_ID),
            })
            .unwrap();
        let factory = QueryEngineFactory::new(
            catalog_manager,
            None,
            None,
            None,
            None,
            false,
            QueryOptions::default(),
        );
        let engine = factory.query_engine();

        let input = LogicalPlanBuilder::scan(
            NUMBERS_TABLE_NAME,
            Arc::new(LogicalTableSource::new(
                NumbersTable::schema().arrow_schema().clone(),
            )),
            None,
        )
        .unwrap()
        .build()
        .unwrap();
        let plan = MergeScanLogicalPlan::new(input, false, Default::default()).into_logical_plan();
        let bytes = DFLogicalSubstraitConvertor
            .encode(&plan, DefaultSerializer)
            .unwrap();

        // This request catalog deliberately has a different schema.
        let table_provider = Arc::new(mock_table_provider(1.into()));
        let catalog_list = Arc::new(DummyCatalogList::with_table_provider(table_provider));

        let plan_decoder = engine
            .engine_context(QueryContext::arc())
            .new_plan_decoder()
            .unwrap();
        let decoded = plan_decoder
            .decode(bytes, catalog_list, false)
            .await
            .unwrap();

        let LogicalPlan::Extension(extension) = &decoded else {
            panic!("Expect a MergeScan plan, got: {decoded}");
        };
        let merge_scan = extension
            .node
            .as_any()
            .downcast_ref::<MergeScanLogicalPlan>()
            .expect("Expect a MergeScan plan node");
        let LogicalPlan::TableScan(scan) = merge_scan.input() else {
            panic!("Expect a table scan, got: {}", merge_scan.input());
        };
        assert_eq!(scan.table_name.table(), NUMBERS_TABLE_NAME);
        assert_eq!(scan.source.schema().fields().len(), 1);
        assert_eq!(scan.source.schema().field(0).name(), "number");
    }

    /// Nested `MergeScan` payloads decode recursively.
    #[tokio::test]
    async fn test_serializer_decode_nested_merge_scan() {
        let catalog_manager = catalog::memory::new_memory_catalog_manager().unwrap();
        catalog_manager
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: NUMBERS_TABLE_NAME.to_string(),
                table_id: NUMBERS_TABLE_ID,
                table: NumbersTable::table(NUMBERS_TABLE_ID),
            })
            .unwrap();
        let factory = QueryEngineFactory::new(
            catalog_manager,
            None,
            None,
            None,
            None,
            false,
            QueryOptions::default(),
        );
        let engine = factory.query_engine();

        let numbers_scan = || {
            LogicalPlanBuilder::scan(
                NUMBERS_TABLE_NAME,
                Arc::new(LogicalTableSource::new(
                    NumbersTable::schema().arrow_schema().clone(),
                )),
                None,
            )
            .unwrap()
            .build()
            .unwrap()
        };
        let inner = MergeScanLogicalPlan::new(numbers_scan(), false, Default::default())
            .into_logical_plan();
        let input = LogicalPlanBuilder::from(inner)
            .filter(col("number").lt(lit(10u32)))
            .unwrap()
            .build()
            .unwrap();
        let plan =
            MergeScanLogicalPlan::new(input.clone(), false, Default::default()).into_logical_plan();

        let bytes = DFLogicalSubstraitConvertor
            .encode(&plan, DefaultSerializer)
            .unwrap();
        let plan_decoder = engine
            .engine_context(QueryContext::arc())
            .new_plan_decoder()
            .unwrap();
        let catalog_list = Arc::new(DummyCatalogList::with_table_provider(Arc::new(
            mock_table_provider(1.into()),
        )));

        let decoded = plan_decoder
            .decode(bytes, catalog_list, false)
            .await
            .unwrap();

        assert_eq!(decoded.to_string(), plan.to_string());
        assert_eq!(decoded.to_string().matches("MergeScan [").count(), 2);
    }

    fn greptime_date_format_udf(query_ctx: &QueryContextRef) -> Arc<ScalarUDF> {
        FUNCTION_REGISTRY
            .get_function("date_format")
            .expect("`date_format` is a function of GreptimeDB")
            .provide(FunctionContext {
                query_ctx: query_ctx.clone(),
                state: Default::default(),
            })
            .into()
    }

    /// Uses a non-default timezone to distinguish the Greptime implementation.
    fn date_format_query_ctx() -> QueryContextRef {
        let query_ctx = QueryContext::with(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME);
        query_ctx.set_timezone(Timezone::from_tz_string("Asia/Shanghai").unwrap());
        Arc::new(query_ctx)
    }

    fn greptime_date_format_expr(query_ctx: &QueryContextRef, timestamp: Expr) -> Expr {
        Expr::ScalarFunction(ScalarFunction {
            func: greptime_date_format_udf(query_ctx),
            args: vec![timestamp, lit("%Y-%m-%d %H:%i:%S")],
        })
    }

    /// Includes `MergeScan` payload expressions, which are not node inputs.
    fn scalar_functions_of(plan: &LogicalPlan) -> Vec<Arc<ScalarUDF>> {
        use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};

        let mut udfs = Vec::new();
        for expr in plan.expressions() {
            expr.apply(|expr| {
                if let Expr::ScalarFunction(func) = expr {
                    udfs.push(Arc::clone(&func.func));
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .unwrap();
        }

        if let LogicalPlan::Extension(extension) = plan
            && let Some(merge_scan) = extension
                .node
                .as_any()
                .downcast_ref::<MergeScanLogicalPlan>()
        {
            udfs.extend(scalar_functions_of(merge_scan.input()));
        }

        for input in plan.inputs() {
            udfs.extend(scalar_functions_of(input));
        }

        udfs
    }

    /// Asserts the Greptime `date_format` binding rather than DataFusion's `to_char` alias.
    fn assert_only_function_is_greptime_date_format(
        plan: &LogicalPlan,
        query_ctx: &QueryContextRef,
    ) {
        let udfs = scalar_functions_of(plan);
        assert_eq!(udfs.len(), 1, "expected one function in: {plan}");

        let udf = &udfs[0];
        assert_eq!(udf.name(), "date_format", "got: {plan}");
        assert_eq!(
            udf.as_ref(),
            greptime_date_format_udf(query_ctx).as_ref(),
            "`date_format` is not bound to the implementation of GreptimeDB: {plan}"
        );
    }

    /// Rebuilding state must retain the Greptime `date_format` binding.
    #[tokio::test]
    async fn test_serializer_decode_keeps_greptime_date_format() {
        // Repeat to expose nondeterministic function-registration order.
        for _ in 0..8 {
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
            let query_ctx = date_format_query_ctx();
            let plan = LogicalPlanBuilder::scan(
                "devices",
                Arc::new(LogicalTableSource::new(table_provider.schema().clone())),
                None,
            )
            .unwrap()
            .project(vec![greptime_date_format_expr(&query_ctx, col("ts"))])
            .unwrap()
            .build()
            .unwrap();

            let bytes = DFLogicalSubstraitConvertor
                .encode(&plan, DefaultSerializer)
                .unwrap();
            let plan_decoder = engine
                .engine_context(query_ctx.clone())
                .new_plan_decoder()
                .unwrap();
            let decoded = plan_decoder
                .decode(
                    bytes,
                    Arc::new(DummyCatalogList::with_table_provider(table_provider)),
                    false,
                )
                .await
                .unwrap();

            assert_only_function_is_greptime_date_format(&decoded, &query_ctx);
        }
    }

    /// Payload-state rebuilding must retain the Greptime `date_format` binding.
    #[tokio::test]
    async fn test_serializer_decode_merge_scan_payload_keeps_greptime_date_format() {
        let catalog_manager = catalog::memory::new_memory_catalog_manager().unwrap();
        catalog_manager
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: NUMBERS_TABLE_NAME.to_string(),
                table_id: NUMBERS_TABLE_ID,
                table: NumbersTable::table(NUMBERS_TABLE_ID),
            })
            .unwrap();

        for _ in 0..8 {
            let factory = QueryEngineFactory::new(
                catalog_manager.clone(),
                None,
                None,
                None,
                None,
                false,
                QueryOptions::default(),
            );
            let engine = factory.query_engine();
            let query_ctx = date_format_query_ctx();

            let input = LogicalPlanBuilder::scan(
                NUMBERS_TABLE_NAME,
                Arc::new(LogicalTableSource::new(
                    NumbersTable::schema().arrow_schema().clone(),
                )),
                None,
            )
            .unwrap()
            .project(vec![greptime_date_format_expr(
                &query_ctx,
                Expr::Cast(Cast::new(
                    Box::new(col("number")),
                    ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
                )),
            )])
            .unwrap()
            .build()
            .unwrap();
            let plan =
                MergeScanLogicalPlan::new(input, false, Default::default()).into_logical_plan();

            let bytes = DFLogicalSubstraitConvertor
                .encode(&plan, DefaultSerializer)
                .unwrap();
            let plan_decoder = engine
                .engine_context(query_ctx.clone())
                .new_plan_decoder()
                .unwrap();
            let catalog_list = Arc::new(DummyCatalogList::with_table_provider(Arc::new(
                mock_table_provider(1.into()),
            )));

            let decoded = plan_decoder
                .decode(bytes, catalog_list, false)
                .await
                .unwrap();

            let LogicalPlan::Extension(extension) = &decoded else {
                panic!("Expect a MergeScan plan, got: {decoded}");
            };
            let merge_scan = extension
                .node
                .as_any()
                .downcast_ref::<MergeScanLogicalPlan>()
                .expect("Expect a MergeScan plan node");

            assert_only_function_is_greptime_date_format(merge_scan.input(), &query_ctx);
        }
    }

    /// Registers a `numbers` table in a memory catalog manager.
    fn numbers_catalog_manager() -> CatalogManagerRef {
        let catalog_manager = catalog::memory::new_memory_catalog_manager().unwrap();
        catalog_manager
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: NUMBERS_TABLE_NAME.to_string(),
                table_id: NUMBERS_TABLE_ID,
                table: NumbersTable::table(NUMBERS_TABLE_ID),
            })
            .unwrap();
        catalog_manager
    }

    /// Scan plan over the `numbers` schema shared by the fixtures below.
    fn numbers_scan() -> LogicalPlan {
        LogicalPlanBuilder::scan(
            NUMBERS_TABLE_NAME,
            Arc::new(LogicalTableSource::new(
                NumbersTable::schema().arrow_schema().clone(),
            )),
            None,
        )
        .unwrap()
        .build()
        .unwrap()
    }

    fn merge_scan(input: LogicalPlan, is_placeholder: bool) -> LogicalPlan {
        MergeScanLogicalPlan::new(input, is_placeholder, Default::default()).into_logical_plan()
    }

    fn encode_plan(plan: &LogicalPlan) -> Bytes {
        DFLogicalSubstraitConvertor
            .encode(plan, DefaultSerializer)
            .unwrap()
    }

    fn plan_decoder(engine: &dyn crate::QueryEngine) -> SubstraitPlanDecoderRef {
        engine
            .engine_context(QueryContext::arc())
            .new_plan_decoder()
            .unwrap()
    }

    /// Request catalog that resolves any table to a provider the payload must not use.
    fn request_catalog_list() -> Arc<dyn CatalogProviderList> {
        Arc::new(DummyCatalogList::with_table_provider(Arc::new(
            mock_table_provider(1.into()),
        )))
    }

    /// How [`InterceptingCatalogManager`] resolves payload tables.
    enum TableResolution {
        /// Fails with the frontend's cross-catalog access error.
        AccessDenied,
        /// Resolves once another task on the caller runtime signals.
        AwaitSignal {
            notify: Arc<tokio::sync::Notify>,
            entered: Arc<AtomicBool>,
        },
        /// Never resolves, holding `live` until the caller drops the decode future.
        Pending {
            live: Arc<AtomicUsize>,
            entered: Arc<AtomicBool>,
        },
    }

    /// Engine catalog manager that takes over `table` resolution while delegating the rest.
    struct InterceptingCatalogManager {
        inner: CatalogManagerRef,
        resolution: TableResolution,
    }

    #[async_trait::async_trait]
    impl CatalogManager for InterceptingCatalogManager {
        fn as_any(&self) -> &dyn Any {
            self.inner.as_any()
        }

        async fn catalog_names(&self) -> CatalogResult<Vec<String>> {
            self.inner.catalog_names().await
        }

        async fn schema_names(
            &self,
            catalog: &str,
            query_ctx: Option<&QueryContext>,
        ) -> CatalogResult<Vec<String>> {
            self.inner.schema_names(catalog, query_ctx).await
        }

        async fn table_names(
            &self,
            catalog: &str,
            schema: &str,
            query_ctx: Option<&QueryContext>,
        ) -> CatalogResult<Vec<String>> {
            self.inner.table_names(catalog, schema, query_ctx).await
        }

        async fn catalog_exists(&self, catalog: &str) -> CatalogResult<bool> {
            self.inner.catalog_exists(catalog).await
        }

        async fn schema_exists(
            &self,
            catalog: &str,
            schema: &str,
            query_ctx: Option<&QueryContext>,
        ) -> CatalogResult<bool> {
            self.inner.schema_exists(catalog, schema, query_ctx).await
        }

        async fn table_exists(
            &self,
            catalog: &str,
            schema: &str,
            table: &str,
            query_ctx: Option<&QueryContext>,
        ) -> CatalogResult<bool> {
            self.inner
                .table_exists(catalog, schema, table, query_ctx)
                .await
        }

        async fn table(
            &self,
            catalog: &str,
            schema: &str,
            table_name: &str,
            query_ctx: Option<&QueryContext>,
        ) -> CatalogResult<Option<TableRef>> {
            match &self.resolution {
                TableResolution::AccessDenied => QueryAccessDeniedSnafu {
                    catalog: catalog.to_string(),
                    schema: schema.to_string(),
                }
                .fail(),
                TableResolution::AwaitSignal { notify, entered } => {
                    entered.store(true, Ordering::SeqCst);
                    notify.notified().await;
                    self.inner
                        .table(catalog, schema, table_name, query_ctx)
                        .await
                }
                TableResolution::Pending { live, entered } => {
                    entered.store(true, Ordering::SeqCst);
                    let _guard = LiveGuard::new(live);
                    std::future::pending::<()>().await;
                    unreachable!("pending resolution never completes")
                }
            }
        }

        async fn table_info_by_id(&self, table_id: TableId) -> CatalogResult<Option<TableInfoRef>> {
            self.inner.table_info_by_id(table_id).await
        }

        async fn tables_by_ids(
            &self,
            catalog: &str,
            schema: &str,
            table_ids: &[TableId],
        ) -> CatalogResult<Vec<TableRef>> {
            self.inner.tables_by_ids(catalog, schema, table_ids).await
        }

        fn tables<'a>(
            &'a self,
            catalog: &'a str,
            schema: &'a str,
            query_ctx: Option<&'a QueryContext>,
        ) -> BoxStream<'a, CatalogResult<TableRef>> {
            self.inner.tables(catalog, schema, query_ctx)
        }
    }

    /// Counts live payload resolutions, so a dropped decode leaves no work behind.
    struct LiveGuard(Arc<AtomicUsize>);

    impl LiveGuard {
        fn new(live: &Arc<AtomicUsize>) -> Self {
            let live = live.clone();
            live.fetch_add(1, Ordering::SeqCst);
            Self(live)
        }
    }

    impl Drop for LiveGuard {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    fn engine_with_intercepting_catalog(
        resolution: TableResolution,
    ) -> Arc<dyn crate::QueryEngine> {
        let catalog_manager: CatalogManagerRef = Arc::new(InterceptingCatalogManager {
            inner: numbers_catalog_manager(),
            resolution,
        });
        QueryEngineFactory::new(
            catalog_manager,
            None,
            None,
            None,
            None,
            false,
            QueryOptions::default(),
        )
        .query_engine()
    }

    /// Decoding must not need a Tokio runtime: the public path is only `async`.
    #[test]
    fn test_serializer_decode_nested_merge_scan_without_tokio_runtime() {
        let engine = QueryEngineFactory::new(
            numbers_catalog_manager(),
            None,
            None,
            None,
            None,
            false,
            QueryOptions::default(),
        )
        .query_engine();
        let plan = merge_scan(merge_scan(numbers_scan(), false), true);
        let bytes = encode_plan(&plan);

        let decoded = futures::executor::block_on(plan_decoder(engine.as_ref()).decode(
            bytes,
            request_catalog_list(),
            false,
        ))
        .unwrap();

        assert_eq!(decoded.to_string(), plan.to_string());
        assert_eq!(decoded.to_string().matches("MergeScan [").count(), 2);
    }

    /// Payload decoding must not block a multi-thread runtime either.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_serializer_decode_deeply_nested_merge_scan_without_blocking() {
        let engine = QueryEngineFactory::new(
            numbers_catalog_manager(),
            None,
            None,
            None,
            None,
            false,
            QueryOptions::default(),
        )
        .query_engine();
        let plan = merge_scan(
            merge_scan(
                LogicalPlanBuilder::from(merge_scan(numbers_scan(), false))
                    .filter(col("number").lt(lit(10u32)))
                    .unwrap()
                    .build()
                    .unwrap(),
                false,
            ),
            false,
        );
        let bytes = encode_plan(&plan);

        let decoded = plan_decoder(engine.as_ref())
            .decode(bytes, request_catalog_list(), false)
            .await
            .unwrap();

        assert_eq!(decoded.to_string(), plan.to_string());
        assert_eq!(decoded.to_string().matches("MergeScan [").count(), 3);
    }

    /// A payload lookup parked on a caller-runtime task must keep the decode async.
    ///
    /// The engine catalog resolves only once a task spawned on this runtime signals it, so a
    /// blocking bridge would deadlock the test instead of decoding.
    #[tokio::test(flavor = "current_thread")]
    async fn test_serializer_decode_merge_scan_awaits_caller_runtime_task() {
        let notify = Arc::new(tokio::sync::Notify::new());
        let entered = Arc::new(AtomicBool::new(false));
        let engine = engine_with_intercepting_catalog(TableResolution::AwaitSignal {
            notify: notify.clone(),
            entered: entered.clone(),
        });
        let plan = merge_scan(numbers_scan(), false);
        let bytes = encode_plan(&plan);

        let decoder = plan_decoder(engine.as_ref());
        let mut decode = Box::pin(decoder.decode(bytes, request_catalog_list(), false));
        // First poll reaches the payload table lookup, which waits for the signal below.
        assert!(futures::poll!(decode.as_mut()).is_pending());
        assert!(entered.load(Ordering::SeqCst));

        let signal = tokio::spawn(async move {
            notify.notify_waiters();
        });
        tokio::task::yield_now().await;
        let decoded = decode.await.unwrap();
        signal.await.unwrap();

        assert_eq!(decoded.to_string(), plan.to_string());
    }

    /// Dropping a decode parked on the catalog cancels it; no detached job keeps running.
    #[tokio::test(flavor = "current_thread")]
    async fn test_serializer_decode_dropped_pending_payload_leaves_no_job() {
        let live = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(AtomicBool::new(false));
        let engine = engine_with_intercepting_catalog(TableResolution::Pending {
            live: live.clone(),
            entered: entered.clone(),
        });
        let bytes = encode_plan(&merge_scan(numbers_scan(), false));

        let decoder = plan_decoder(engine.as_ref());
        {
            let mut decode = Box::pin(decoder.decode(bytes, request_catalog_list(), false));
            assert!(futures::poll!(decode.as_mut()).is_pending());
            assert_eq!(live.load(Ordering::SeqCst), 1);
            assert!(entered.load(Ordering::SeqCst));
        }

        assert_eq!(
            live.load(Ordering::SeqCst),
            0,
            "dropping the decode future must cancel its payload work"
        );
    }

    /// A payload table missing from the engine catalog must not fall back to the request catalog.
    #[tokio::test]
    async fn test_serializer_decode_payload_missing_in_engine_catalog() {
        // The engine catalog deliberately has no `numbers` table.
        let catalog_manager: CatalogManagerRef =
            catalog::memory::new_memory_catalog_manager().unwrap();
        let engine = QueryEngineFactory::new(
            catalog_manager,
            None,
            None,
            None,
            None,
            false,
            QueryOptions::default(),
        )
        .query_engine();
        let bytes = encode_plan(&merge_scan(numbers_scan(), false));

        let err = plan_decoder(engine.as_ref())
            .decode(bytes, request_catalog_list(), false)
            .await
            .unwrap_err();

        let err = format!("{err:?}");
        assert!(
            err.contains("Table not found: greptime.public.numbers"),
            "unexpected error: {err}"
        );
    }

    /// A payload table denied by the engine catalog must not fall back to the request catalog.
    #[tokio::test]
    async fn test_serializer_decode_payload_access_denied_in_engine_catalog() {
        let engine = engine_with_intercepting_catalog(TableResolution::AccessDenied);
        let bytes = encode_plan(&merge_scan(numbers_scan(), false));

        let err = plan_decoder(engine.as_ref())
            .decode(bytes, request_catalog_list(), false)
            .await
            .unwrap_err();

        let err = format!("{err:?}");
        assert!(
            err.contains("Illegal access to catalog"),
            "unexpected error: {err}"
        );
    }

    /// Manually built states need ordinary extension decoding without an engine catalog.
    #[tokio::test]
    async fn test_serializer_decode_manual_state_keeps_extension_registry() {
        let state = SessionStateBuilder::new().with_default_features().build();
        let decoder = DefaultPlanDecoder::new(state, &QueryContext::arc()).unwrap();
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(SeriesNormalize::new(
                0,
                "number",
                false,
                Vec::new(),
                merge_scan(numbers_scan(), false),
            )),
        });
        let request_catalog = Arc::new(DummyCatalogList::with_table_provider(Arc::new(
            MemTable::try_new(NumbersTable::schema().arrow_schema().clone(), vec![vec![]]).unwrap(),
        )));
        let decoded = decoder
            .decode(encode_plan(&plan), request_catalog, false)
            .await
            .unwrap();

        assert_eq!(decoded.to_string(), plan.to_string());
        assert_eq!(decoded.schema().as_arrow(), plan.schema().as_arrow());
    }

    /// `MergeScan` decodes under an ordinary single-input extension parent.
    #[tokio::test]
    async fn test_serializer_decode_merge_scan_under_series_normalize() {
        let outer = merge_scan(numbers_scan(), false);
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(SeriesNormalize::new(
                0,
                "number",
                false,
                Vec::new(),
                outer.clone(),
            )),
        });
        let bytes = encode_plan(&plan);

        let decoded = plan_decoder(
            QueryEngineFactory::new(
                numbers_catalog_manager(),
                None,
                None,
                None,
                None,
                false,
                QueryOptions::default(),
            )
            .query_engine()
            .as_ref(),
        )
        .decode(bytes, request_catalog_list(), false)
        .await
        .unwrap();

        let LogicalPlan::Extension(extension) = &decoded else {
            panic!("Expect a SeriesNormalize plan, got: {decoded}");
        };
        let normalize = extension
            .node
            .as_any()
            .downcast_ref::<SeriesNormalize>()
            .expect("Expect a SeriesNormalize plan node");
        assert_eq!(normalize.inputs()[0].to_string(), outer.to_string());
    }

    /// `MergeScan` decodes under an ordinary multi-input extension parent.
    #[tokio::test]
    async fn test_serializer_decode_merge_scan_under_union_distinct_on() {
        let left = merge_scan(numbers_scan(), false);
        let right = merge_scan(numbers_scan(), true);
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(
                UnionDistinctOn::try_new(left.clone(), right.clone(), vec![], 0).unwrap(),
            ),
        });
        let bytes = encode_plan(&plan);

        let decoded = plan_decoder(
            QueryEngineFactory::new(
                numbers_catalog_manager(),
                None,
                None,
                None,
                None,
                false,
                QueryOptions::default(),
            )
            .query_engine()
            .as_ref(),
        )
        .decode(bytes, request_catalog_list(), false)
        .await
        .unwrap();

        let LogicalPlan::Extension(extension) = &decoded else {
            panic!("Expect a UnionDistinctOn plan, got: {decoded}");
        };
        let union = extension
            .node
            .as_any()
            .downcast_ref::<UnionDistinctOn>()
            .expect("Expect a UnionDistinctOn plan node");
        assert_eq!(union.inputs()[0].to_string(), left.to_string());
        assert_eq!(union.inputs()[1].to_string(), right.to_string());
    }

    /// `MergeScan` inside a subquery still decodes through the wrapper consumer.
    ///
    /// Correlated outer references cannot be covered here: the in-repo Substrait producer
    /// rejects `Expr::OuterReferenceColumn`, so no supported helper can build such a plan.
    #[tokio::test]
    async fn test_serializer_decode_merge_scan_inside_subquery() {
        let inner = merge_scan(numbers_scan(), false);
        let payload = LogicalPlanBuilder::from(numbers_scan())
            .filter(Expr::InSubquery(InSubquery::new(
                Box::new(col("number")),
                Subquery {
                    subquery: Arc::new(inner.clone()),
                    outer_ref_columns: vec![],
                    spans: Default::default(),
                },
                false,
            )))
            .unwrap()
            .build()
            .unwrap();
        let plan = merge_scan(payload.clone(), false);
        let bytes = encode_plan(&plan);

        let decoded = plan_decoder(
            QueryEngineFactory::new(
                numbers_catalog_manager(),
                None,
                None,
                None,
                None,
                false,
                QueryOptions::default(),
            )
            .query_engine()
            .as_ref(),
        )
        .decode(bytes, request_catalog_list(), false)
        .await
        .unwrap();

        assert_eq!(decoded.to_string().matches("MergeScan [").count(), 2);
        assert!(
            decoded.to_string().contains("IN (<subquery>)"),
            "expect the decoded subquery filter, got: {decoded}"
        );
    }

    /// Recursive payload decoding must restore ordered state aggregates, including their schema.
    #[tokio::test]
    async fn test_serializer_decode_payload_keeps_state_udaf_ordering() {
        let last_value = (*datafusion::functions_aggregate::first_last::last_value_udaf()).clone();
        let state_udaf = AggregateUDF::new_from_impl(StateWrapper::new(last_value).unwrap());
        let aggr_expr = Expr::AggregateFunction(AggregateFunction::new_udf(
            Arc::new(state_udaf),
            vec![col("number")],
            false,
            None,
            vec![Sort::new(col("number"), true, true)],
            None,
        ));
        let payload = LogicalPlanBuilder::from(numbers_scan())
            .aggregate(Vec::<Expr>::new(), vec![aggr_expr])
            .unwrap()
            .build()
            .unwrap();
        let bytes = encode_plan(&payload);
        let encoded = Plan::decode(bytes.clone()).unwrap();
        let extensions = Extensions::try_from(&encoded.extensions).unwrap();
        let catalog_manager = numbers_catalog_manager();
        let engine = QueryEngineFactory::new(
            catalog_manager.clone(),
            None,
            None,
            None,
            None,
            false,
            QueryOptions::default(),
        )
        .query_engine();
        let state = engine.engine_context(QueryContext::arc()).state().clone();
        let consumer = MergeScanSubstraitConsumer {
            inner: DefaultSubstraitConsumer::new(&extensions, &state),
            session_state: &state,
            catalog_manager: Some(catalog_manager),
        };
        let decoded = consumer.decode_payload(bytes.to_vec()).await.unwrap();

        let fixed_payload = FixStateUdafOrderingAnalyzer {}
            .analyze(payload.clone(), &Default::default())
            .unwrap();
        assert_ne!(
            fixed_payload.schema().as_arrow(),
            payload.schema().as_arrow()
        );
        assert_eq!(
            decoded.schema().as_arrow(),
            fixed_payload.schema().as_arrow()
        );
        let LogicalPlan::Aggregate(aggregate) = &decoded else {
            panic!("Expected an aggregate payload, got: {decoded}");
        };
        let Expr::AggregateFunction(function) = &aggregate.aggr_expr[0] else {
            panic!("Expected a state aggregate, got: {:?}", aggregate.aggr_expr);
        };
        assert!(function.func.inner().is::<StateWrapper>());
        assert_eq!(function.params.order_by.len(), 1);
        let ArrowDataType::Struct(fields) = decoded.schema().field(0).data_type() else {
            panic!("Expected the ordered aggregate state type");
        };
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[1].name(), "number");
        assert!(fields[1].is_nullable());
    }

    /// Type variation extensions keep being rejected while decoding.
    #[tokio::test]
    async fn test_serializer_decode_rejects_type_variations() {
        use substrait::substrait_proto_df::proto::extensions::SimpleExtensionDeclaration;
        use substrait::substrait_proto_df::proto::extensions::simple_extension_declaration::{
            ExtensionTypeVariation, MappingType,
        };

        let engine = QueryEngineFactory::new(
            numbers_catalog_manager(),
            None,
            None,
            None,
            None,
            false,
            QueryOptions::default(),
        )
        .query_engine();
        let encoded = encode_plan(&merge_scan(numbers_scan(), false));
        let mut substrait_plan = Plan::decode(encoded).unwrap();
        substrait_plan.extensions.push(SimpleExtensionDeclaration {
            mapping_type: Some(MappingType::ExtensionTypeVariation(
                ExtensionTypeVariation {
                    extension_urn_reference: u32::MAX,
                    type_variation_anchor: 1,
                    name: "u!variation".to_string(),
                },
            )),
        });
        let bytes = Bytes::from(substrait_plan.encode_to_vec());

        let err = plan_decoder(engine.as_ref())
            .decode(bytes, request_catalog_list(), false)
            .await
            .unwrap_err();

        let err = format!("{err:?}");
        assert!(
            err.contains("Type variation extensions are not supported"),
            "unexpected error: {err}"
        );
    }
}
