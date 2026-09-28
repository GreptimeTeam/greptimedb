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

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use catalog::CatalogManagerRef;
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
use substrait::extension_serializer::ExtensionSerializer;
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};

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
            // `DefaultSerializer` has no session state; use `MergeScanAwareSerializer` to decode.
            Err(DataFusionError::Substrait(format!(
                "Unsupported plan node: {name}"
            )))
        } else {
            ExtensionSerializer.deserialize_logical_plan(name, bytes)
        }
    }
}

/// [`ExtensionSerializer`] with the session state required to decode [`MergeScanLogicalPlan`].
struct MergeScanAwareSerializer {
    /// Request state used to decode nested `MergeScan` payloads.
    session_state: SessionState,
    /// Engine catalog for payload tables; absent for manually built session states.
    catalog_manager: Option<CatalogManagerRef>,
}

impl fmt::Debug for MergeScanAwareSerializer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // `dyn CatalogManager` is not `Debug`.
        f.debug_struct("MergeScanAwareSerializer")
            .field("session_state", &self.session_state)
            .field("has_catalog_manager", &self.catalog_manager.is_some())
            .finish()
    }
}

impl MergeScanAwareSerializer {
    fn query_ctx(&self) -> QueryContextRef {
        self.session_state
            .config()
            .get_extension()
            .unwrap_or_else(QueryContext::arc)
    }

    /// Rebuilds state for recursive payload decoding with request catalog/schema defaults.
    ///
    /// Re-register Greptime functions after each build to preserve bindings over DataFusion aliases.
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
            .with_serializer_registry(Arc::new(Self {
                session_state: self.session_state.clone(),
                catalog_manager: self.catalog_manager.clone(),
            }))
            .with_catalog_list(catalog_list)
            .build();
        register_greptime_functions(&mut state, &query_ctx).map_err(DataFusionError::from)?;

        Ok(state)
    }

    /// Decodes payload tables through the engine catalog, not the region-bound request catalog.
    ///
    /// Do not fall back after an engine-catalog failure: it can bind payload tables to the request
    /// region with the wrong schema. Manually built states lack that catalog and use the request list.
    fn decode_payload(&self, payload: Vec<u8>) -> Result<LogicalPlan> {
        if let Some(catalog_manager) = &self.catalog_manager {
            let engine_catalog = Arc::new(
                catalog::table_source::dummy_catalog::DummyCatalogList::new_with_query_ctx(
                    catalog_manager.clone(),
                    self.query_ctx(),
                ),
            );
            return decode_sub_plan(payload, self.payload_state(engine_catalog)?);
        }

        decode_sub_plan(
            payload,
            self.payload_state(self.session_state.catalog_list().clone())?,
        )
    }
}

impl SerializerRegistry for MergeScanAwareSerializer {
    fn serialize_logical_plan(&self, node: &dyn UserDefinedLogicalNode) -> Result<Vec<u8>> {
        DefaultSerializer.serialize_logical_plan(node)
    }

    fn deserialize_logical_plan(
        &self,
        name: &str,
        bytes: &[u8],
    ) -> Result<Arc<dyn UserDefinedLogicalNode>> {
        if name != MergeScanLogicalPlan::name() {
            return DefaultSerializer.deserialize_logical_plan(name, bytes);
        }

        let merge_scan = PbMergeScan::decode(bytes).map_err(|e| {
            DataFusionError::Substrait(format!("Failed to decode the MergeScan plan node: {e}"))
        })?;

        let input = self.decode_payload(merge_scan.input)?;

        // `PbMergeScan` lacks `partition_cols`; decoded plans lose this optimization and may repartition.
        Ok(Arc::new(MergeScanLogicalPlan::new(
            input,
            merge_scan.is_placeholder,
            Default::default(),
        )))
    }
}

/// Bridges synchronous serializer callbacks to asynchronous payload decoding.
///
/// Nested `MergeScan` payloads recurse through this bridge, so plain nested `block_on` is invalid.
/// Multi-thread runtimes use `block_in_place`; current-thread or absent runtimes use a fresh thread.
/// The latter assumes catalog resolution does not require the caller runtime and has no cancellation.
fn decode_sub_plan(sub_plan: Vec<u8>, session_state: SessionState) -> Result<LogicalPlan> {
    let decode = async move {
        DFLogicalSubstraitConvertor
            .decode(Bytes::from(sub_plan), session_state)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    };

    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| handle.block_on(decode))
        }
        _ => std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            runtime.block_on(decode)
        })
        .join()
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic)),
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
        let session_state = SessionStateBuilder::new_from_existing(self.session_state.clone())
            .with_catalog_list(catalog_list)
            .build();

        // Payloads need the engine catalog rather than the request's region-bound catalog.
        let catalog_manager = session_state
            .config()
            .get_extension::<QueryEngineState>()
            .map(|engine_state| engine_state.catalog_manager().clone());
        let mut session_state = SessionStateBuilder::new_from_existing(session_state.clone())
            .with_serializer_registry(Arc::new(MergeScanAwareSerializer {
                session_state,
                catalog_manager,
            }))
            .build();
        // Re-register after the build to avoid Greptime UDF alias collisions.
        register_greptime_functions(&mut session_state, &self.query_ctx)?;

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
    use catalog::RegisterTableRequest;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, NUMBERS_TABLE_ID};
    use common_query::native_histogram::native_histogram_value_type;
    use common_time::Timezone;
    use datafusion::catalog::TableProvider;
    use datafusion::datasource::MemTable;
    use datafusion::logical_expr::Extension;
    use datafusion_expr::expr::{Cast, ScalarFunction};
    use datafusion_expr::{Expr, LogicalPlanBuilder, LogicalTableSource, ScalarUDF, col, lit};
    use datatypes::arrow::datatypes::{
        DataType as ArrowDataType, Field, Schema, SchemaRef, TimeUnit,
    };
    use datatypes::data_type::DataType;
    use promql::extension_plan::RangeManipulate;
    use session::context::QueryContext;
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
}
