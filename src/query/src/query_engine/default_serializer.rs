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
            // TODO(dennis): missing `session_state` to decode the logical plan in `MergeScanLogicalPlan`,
            // so we only save the unoptimized logical plan for view currently.
            // Plan decoding paths that need to decode a `MergeScan` use [`MergeScanAwareSerializer`].
            Err(DataFusionError::Substrait(format!(
                "Unsupported plan node: {name}"
            )))
        } else {
            ExtensionSerializer.deserialize_logical_plan(name, bytes)
        }
    }
}

/// Extended [`substrait::extension_serializer::ExtensionSerializer`] that supports both
/// serialization and deserialization of [`MergeScanLogicalPlan`].
///
/// [`DefaultSerializer`] is a unit struct that is shared by every encoding path, hence it cannot
/// decode a `MergeScan` node: the plan embedded in the `MergeScan` payload is decoded with a
/// [`SessionState`] which is only known at the decoding site. This registry keeps such a session
/// state and is installed (instead of `DefaultSerializer`) into the session state built by
/// [`DefaultPlanDecoder::decode`], so that plans containing a `MergeScan` can be decoded.
struct MergeScanAwareSerializer {
    /// The session state used to decode the plans embedded in `MergeScan` payloads: the state of
    /// the request being decoded, with the functions and the catalog list given to
    /// [`DefaultPlanDecoder::decode`].
    session_state: SessionState,
    /// The catalog of the query engine, which knows every table of the cluster. It resolves the
    /// tables of a `MergeScan` payload, see [`Self::decode_payload`].
    ///
    /// It is `None` when the session state doesn't carry the engine state (a session state built
    /// by hand, e.g. in a unit test): the payload is then decoded with the catalog list of the
    /// request, which is the behavior of every payload before this field existed.
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
    /// Returns the query context of the session state, or an empty one if it doesn't carry any.
    fn query_ctx(&self) -> QueryContextRef {
        self.session_state
            .config()
            .get_extension()
            .unwrap_or_else(QueryContext::arc)
    }

    /// Returns the session state used to decode a `MergeScan` payload whose tables are resolved
    /// with `catalog_list`.
    ///
    /// The state is rebuilt with this registry, so that a `MergeScan` nested in the payload (e.g.
    /// the plan sent to a datanode that has to query another datanode) is decoded recursively.
    ///
    /// The default catalog and schema of the state are the ones of the request, because the table
    /// references of a payload may be bare (a table name as it was written in the query): they are
    /// resolved against that pair, which is how they refer to a `(catalog, schema, table)` of the
    /// catalog. Decoding the payload with the catalog list of a region aware request used to make
    /// that pair irrelevant (that list resolves every catalog and schema to the region); a catalog
    /// that resolves the tables of the cluster needs it.
    fn payload_state(&self, catalog_list: Arc<dyn CatalogProviderList>) -> SessionState {
        let query_ctx = self.query_ctx();
        let mut config = self.session_state.config().clone();
        {
            let catalog_options = &mut config.options_mut().catalog;
            catalog_options.default_catalog = query_ctx.current_catalog().to_string();
            catalog_options.default_schema = query_ctx.current_schema();
        }

        SessionStateBuilder::new_from_existing(self.session_state.clone())
            .with_config(config)
            .with_serializer_registry(Arc::new(Self {
                session_state: self.session_state.clone(),
                catalog_manager: self.catalog_manager.clone(),
            }))
            .with_catalog_list(catalog_list)
            .build()
    }

    /// Decodes `payload`, the plan embedded in a `MergeScan`.
    ///
    /// The tables of the payload are the tables that the merge scan reads, i.e. the tables of that
    /// merge scan and not the region that the enclosing plan is decoded for: they are resolved by
    /// `(catalog, schema, table)` name through the catalog of the query engine, which knows every
    /// table of the cluster.
    ///
    /// The catalog list of the request is deliberately not used for the payload: on a datanode that
    /// list is the region aware one (`NameAwareCatalogList`), which resolves *every* table name to
    /// the region of the request. A nested `MergeScan` reads another table than the enclosing plan
    /// (the build side of a nested join, e.g.), and binding it to the region of the request decodes
    /// it with the schema of the table of that region (the columns don't even match when the two
    /// tables don't share their column names). The tables of a payload are read by the datanodes
    /// that own their regions: `MergeScanExec` sends the plan of the payload to them as is, and
    /// they decode it against their own regions.
    ///
    /// When the session state carries a catalog manager (the query engine does), the payload is
    /// decoded with it and a failure is returned as is: falling back to the catalog list of the
    /// request would decode the payload with a *different* table binding semantics (on a datanode
    /// it binds every table to the region of the request), masking a real catalog, metadata or
    /// decoding error behind a wrong schema. The catalog list of the request is only used when no
    /// catalog manager is available (e.g. a hand-built session state of a unit test).
    fn decode_payload(&self, payload: Vec<u8>) -> Result<LogicalPlan> {
        if let Some(catalog_manager) = &self.catalog_manager {
            let engine_catalog = Arc::new(
                catalog::table_source::dummy_catalog::DummyCatalogList::new_with_query_ctx(
                    catalog_manager.clone(),
                    self.query_ctx(),
                ),
            );
            return decode_sub_plan(payload, self.payload_state(engine_catalog));
        }

        decode_sub_plan(
            payload,
            self.payload_state(self.session_state.catalog_list().clone()),
        )
    }
}

impl SerializerRegistry for MergeScanAwareSerializer {
    fn serialize_logical_plan(&self, node: &dyn UserDefinedLogicalNode) -> Result<Vec<u8>> {
        // Encoding doesn't need any session state, delegate to the default serializer.
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

        // `PbMergeScan` doesn't carry `partition_cols`, so the decoded node maps no partition
        // column to its aliases (an empty `AliasMapping`). `MergeScanExec` uses it to declare the
        // hash partitioning of its output and to keep a hash partitioned input of an upstream
        // operator (see `MergeScanExec::try_with_new_distribution`), so an empty mapping only
        // loses that optimization (an extra repartition may be inserted by the planner), it
        // doesn't change the results. Carrying `partition_cols` in the payload requires a proto
        // change and is left to a follow-up.
        Ok(Arc::new(MergeScanLogicalPlan::new(
            input,
            merge_scan.is_placeholder,
            Default::default(),
        )))
    }
}

/// Decodes `sub_plan`, the plan embedded in a `MergeScan` payload.
///
/// This sync/async bridge exists because the two sides of it have incompatible shapes (a PoC
/// simplification):
///
/// * `SerializerRegistry::deserialize_logical_plan` is synchronous — it is called from the
///   synchronous `consume_extension_leaf` of the datafusion fork — while decoding a substrait plan
///   is asynchronous.
/// * `consume_extension_leaf` does not treat the input of a `MergeScan` as a subtree it recurses
///   into, so a nested `MergeScan` is only decoded by recursing from here, in our own registry.
///   Each recursion level therefore re-enters this synchronous bridge *from inside the future it
///   is currently driving*, which a plain `block_on` cannot do: the nested executor aborts with
///   `cannot execute LocalPool executor from within another executor: EnterError`.
///
/// The future must thus be driven on a runtime other than the one the caller is already running
/// in. Driving it at all needs a runtime — not a bare polling loop — because resolving the tables
/// of the payload may read the metadata through the catalog (a remote read on a datanode), which
/// needs a reactor.
///
/// * On a multi-threaded runtime (the datanode production path) `block_in_place` hands the other
///   tasks of the current worker thread over to another worker, then drives the future on the
///   current runtime; the remaining workers keep serving the I/O the future awaits.
/// * Otherwise (a current-thread runtime, e.g. the default of `#[tokio::test]`, or no runtime at
///   all) there is no other worker to hand the tasks over to, so driving the future here would
///   both starve the runtime and panic as soon as this function is re-entered recursively. The
///   future is moved to a fresh thread that builds its own current-thread runtime instead. That
///   thread is unrelated to whatever the caller runs in, so a recursive call simply spawns another
///   fresh thread; the caller thread only joins and never re-enters its own executor.
///
/// Scope of that current-thread branch (best effort, not a general guarantee): the production
/// datanode runs on a multi-threaded runtime and uses the `block_in_place` branch above. The
/// current-thread branch exists for environments such as `#[tokio::test]`. It assumes the
/// catalog resolution the future awaits does *not* depend on tasks running on the caller's
/// runtime (the PoC uses in-memory / KV catalogs, for which that holds). `Send`-ness of the
/// future alone does not establish that independence, so this branch makes no safety claim for
/// arbitrary runtime setups; it also offers no cancellation of the synchronous `join`.
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

#[async_trait::async_trait]
impl SubstraitPlanDecoder for DefaultPlanDecoder {
    async fn decode(
        &self,
        message: bytes::Bytes,
        catalog_list: Arc<dyn CatalogProviderList>,
        optimize: bool,
    ) -> common_query::error::Result<LogicalPlan> {
        // The session_state already has the `DefaultSerialzier` as `SerializerRegistry`. It is
        // replaced by `MergeScanAwareSerializer` below, once this state is fully built.
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

        // Install a registry that is also able to decode `MergeScan` nodes (the plans sent to a
        // datanode may contain one) after all the functions above are registered, so that the
        // sub-plans of a `MergeScan` see the same functions.
        //
        // The registry resolves the tables of a `MergeScan` payload with the catalog of the query
        // engine, which it retrieves from the engine state the query engine leaves in the session
        // config (`QueryEngine::engine_context`). A session state that carries no engine state has
        // no catalog manager either, in which case the payloads are decoded with the catalog list
        // of the request, see `MergeScanAwareSerializer::decode_payload`.
        let catalog_manager = session_state
            .config()
            .get_extension::<QueryEngineState>()
            .map(|engine_state| engine_state.catalog_manager().clone());
        let session_state = SessionStateBuilder::new_from_existing(session_state.clone())
            .with_serializer_registry(Arc::new(MergeScanAwareSerializer {
                session_state,
                catalog_manager,
            }))
            .build();

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
    use datafusion::catalog::TableProvider;
    use datafusion::datasource::MemTable;
    use datafusion::logical_expr::Extension;
    use datafusion_expr::expr::ScalarFunction;
    use datafusion_expr::{Expr, LogicalPlanBuilder, LogicalTableSource, col, lit};
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
        // The payload scans `numbers`: it has to be resolvable through the catalog of the query
        // engine, which is what a datanode uses to decode the payload of a `MergeScan` (the
        // catalog list of the request is not a fallback for the engine catalog).
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

        // A `MergeScan` embeds its input plan into its payload, so decoding a `MergeScan` has to
        // decode that plan recursively.
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
        // The catalog list of the request is deliberately unrelated to the payload: the payload
        // is resolved through the catalog of the query engine, not through this list.
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

    /// The plan embedded in a `MergeScan` is decoded with the catalog of the query engine, which
    /// resolves its tables by `(catalog, schema, table)` name, and not with the catalog list of the
    /// request.
    ///
    /// On a datanode the catalog list of the request is the region aware one
    /// (`NameAwareCatalogList`), which resolves *every* table name to the region of the request: a
    /// payload reading another table is then decoded with the schema of the table of that region,
    /// which breaks the plan as soon as the two tables don't share their columns. This test covers
    /// that binding with a `numbers` table whose column names differ from the request region's.
    #[tokio::test]
    async fn test_serializer_decode_merge_scan_with_engine_catalog() {
        let catalog_manager = catalog::memory::new_memory_catalog_manager().unwrap();
        // The table of the payload: `numbers`, with a single `number` column.
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

        // The payload scans `numbers` by name, so the catalog that decodes it has to resolve a
        // bare table name too.
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

        // The catalog list of the request resolves every table name to a provider whose schema is
        // not the one of `numbers` (the region aware list of a datanode does the same).
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
        // The scan is the `numbers` table of the catalog of the query engine, not the table of the
        // request catalog list.
        assert_eq!(scan.table_name.table(), NUMBERS_TABLE_NAME);
        assert_eq!(scan.source.schema().fields().len(), 1);
        assert_eq!(scan.source.schema().field(0).name(), "number");
    }

    /// A `MergeScan` nested in the payload of another `MergeScan` is decoded recursively: this is
    /// the shape of a datanode querying another datanode.
    #[tokio::test]
    async fn test_serializer_decode_nested_merge_scan() {
        let catalog_manager = catalog::memory::new_memory_catalog_manager().unwrap();
        // The payload of both `MergeScan` levels scans `numbers`, which has to be resolvable
        // through the catalog of the query engine (the request catalog list is not a fallback).
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
        // The catalog list of the request is deliberately unrelated to the payload.
        let catalog_list = Arc::new(DummyCatalogList::with_table_provider(Arc::new(
            mock_table_provider(1.into()),
        )));

        let decoded = plan_decoder
            .decode(bytes, catalog_list, false)
            .await
            .unwrap();

        assert_eq!(decoded.to_string(), plan.to_string());
        // Both levels are `MergeScan` nodes.
        assert_eq!(decoded.to_string().matches("MergeScan [").count(), 2);
    }
}
