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
    use std::cmp::Ordering;
    use std::fmt;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::TableProvider;
    use datafusion::common::DFSchema;
    use datafusion::datasource::memory::MemTable;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_expr::{
        EmptyRelation, Expr, Extension, LogicalPlanBuilder, LogicalTableSource,
        UserDefinedLogicalNodeCore, col, lit,
    };
    use datatypes::arrow::datatypes::SchemaRef;
    use prost::Message;
    use session::context::QueryContext;
    use substrait::substrait_proto_df::proto::{plan_rel, rel};

    use super::*;
    use crate::QueryEngineFactory;
    use crate::dummy_catalog::DummyCatalogList;
    use crate::optimizer::test_util::mock_table_provider;
    use crate::options::QueryOptions;
    use crate::range_select::plan::{
        RangeSelect, RangeSelectPartialFixture, collect_range_select_partial_fixture,
        range_select_partial_fixture, range_select_split_fixture,
    };
    use crate::range_select::serializer::RANGE_SELECT_PARTIAL_NODE_NAME;

    const EXTENSION_EXPRESSION_PROBE_NAME: &str = "ExtensionExpressionWireContractProbe";
    const EXTENSION_EXPRESSION_PROBE_DETAIL: &[u8] = b"extension-expression-wire-contract";

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct ExtensionExpressionWireContractProbe {
        input: LogicalPlan,
        expr: Expr,
    }

    impl PartialOrd for ExtensionExpressionWireContractProbe {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            match self.input.partial_cmp(&other.input) {
                Some(Ordering::Equal) => self.expr.partial_cmp(&other.expr),
                order => order,
            }
        }
    }

    impl ExtensionExpressionWireContractProbe {
        fn new(input: LogicalPlan, expr: Expr) -> Self {
            Self { input, expr }
        }
    }

    impl UserDefinedLogicalNodeCore for ExtensionExpressionWireContractProbe {
        fn name(&self) -> &str {
            EXTENSION_EXPRESSION_PROBE_NAME
        }

        fn inputs(&self) -> Vec<&LogicalPlan> {
            vec![&self.input]
        }

        fn schema(&self) -> &datafusion_common::DFSchemaRef {
            self.input.schema()
        }

        fn expressions(&self) -> Vec<Expr> {
            vec![self.expr.clone()]
        }

        fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "{EXTENSION_EXPRESSION_PROBE_NAME}")
        }

        fn with_exprs_and_inputs(
            &self,
            exprs: Vec<Expr>,
            inputs: Vec<LogicalPlan>,
        ) -> Result<Self> {
            if exprs.len() != 1 || inputs.len() != 1 {
                return Err(DataFusionError::Plan(format!(
                    "{EXTENSION_EXPRESSION_PROBE_NAME} expects one expression and one input"
                )));
            }

            Ok(Self::new(
                inputs.into_iter().next().unwrap(),
                exprs.into_iter().next().unwrap(),
            ))
        }
    }

    #[derive(Debug)]
    struct ExtensionExpressionWireContractProbeSerializer;

    impl SerializerRegistry for ExtensionExpressionWireContractProbeSerializer {
        fn serialize_logical_plan(&self, node: &dyn UserDefinedLogicalNode) -> Result<Vec<u8>> {
            assert_eq!(node.name(), EXTENSION_EXPRESSION_PROBE_NAME);
            assert!(node.as_any().is::<ExtensionExpressionWireContractProbe>());

            // Intentionally omit expressions: this is a wire-contract probe, not a serializer.
            Ok(EXTENSION_EXPRESSION_PROBE_DETAIL.to_vec())
        }

        fn deserialize_logical_plan(
            &self,
            name: &str,
            detail: &[u8],
        ) -> Result<Arc<dyn UserDefinedLogicalNode>> {
            assert_eq!(name, EXTENSION_EXPRESSION_PROBE_NAME);
            assert_eq!(detail, EXTENSION_EXPRESSION_PROBE_DETAIL);

            // The consumer subsequently supplies the real input through
            // `with_exprs_and_inputs`; this placeholder expression is deliberately distinct.
            Ok(Arc::new(ExtensionExpressionWireContractProbe::new(
                probe_input(),
                placeholder_expr(),
            )))
        }
    }

    fn probe_input() -> LogicalPlan {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "probe_input",
            DataType::Int64,
            false,
        )]));
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::try_from(schema).unwrap()),
        })
    }

    fn original_expr() -> Expr {
        col("probe_input").alias("original_probe_expression")
    }

    fn placeholder_expr() -> Expr {
        lit(99_i64).alias("placeholder_probe_expression")
    }

    fn extension_single_rel(
        plan: &substrait::substrait_proto_df::proto::Plan,
    ) -> &substrait::substrait_proto_df::proto::ExtensionSingleRel {
        let [relation] = plan.relations.as_slice() else {
            panic!("wire-contract probe expected one Substrait relation");
        };
        let Some(plan_rel::RelType::Root(root)) = &relation.rel_type else {
            panic!("wire-contract probe expected a Substrait root relation");
        };
        let Some(input) = &root.input else {
            panic!("wire-contract probe expected a root input");
        };
        let Some(rel::RelType::ExtensionSingle(extension)) = &input.rel_type else {
            panic!("wire-contract probe expected ExtensionSingleRel");
        };

        extension
    }

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
    async fn extension_single_expression_wire_contract_roundtrip_probe() {
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(ExtensionExpressionWireContractProbe::new(
                probe_input(),
                original_expr(),
            )),
        });

        let bytes = DFLogicalSubstraitConvertor
            .encode(&plan, ExtensionExpressionWireContractProbeSerializer)
            .unwrap();
        let substrait_plan = substrait::substrait_proto_df::proto::Plan::decode(bytes.clone())
            .expect("wire-contract probe expected encoded Substrait plan");
        let extension = extension_single_rel(&substrait_plan);
        let detail = extension
            .detail
            .as_ref()
            .expect("wire-contract probe expected ExtensionSingleRel detail");
        assert_eq!(detail.type_url, EXTENSION_EXPRESSION_PROBE_NAME);
        assert_eq!(&detail.value[..], EXTENSION_EXPRESSION_PROBE_DETAIL);
        assert!(
            extension.input.is_some(),
            "wire-contract probe expected ExtensionSingleRel input"
        );

        // Match the project's default DataFusion Substrait session settings while installing the
        // test-only registry required to decode this test-only extension node.
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new())
            .with_runtime_env(Arc::new(RuntimeEnv::default()))
            .with_default_features()
            .with_serializer_registry(Arc::new(ExtensionExpressionWireContractProbeSerializer))
            .build();
        let decoded = DFLogicalSubstraitConvertor
            .decode(bytes, state)
            .await
            .unwrap();

        let LogicalPlan::Extension(extension) = decoded else {
            panic!("wire-contract probe expected decoded extension node");
        };
        let decoded = extension
            .node
            .as_any()
            .downcast_ref::<ExtensionExpressionWireContractProbe>()
            .expect("wire-contract probe expected its decoded node type");

        assert_eq!(UserDefinedLogicalNodeCore::inputs(decoded).len(), 1);
        assert_eq!(decoded.expr, placeholder_expr());
        assert_ne!(decoded.expr, original_expr());
        let Expr::Alias(alias) = &decoded.expr else {
            panic!("wire-contract probe expected an aliased placeholder expression");
        };
        assert_eq!(alias.name, "placeholder_probe_expression");
        assert_eq!(*alias.expr, lit(99_i64));
    }

    #[tokio::test]
    async fn range_select_partial_substrait_roundtrip_rebuilds_real_node() {
        for fixture in [
            RangeSelectPartialFixture::CompoundAvg,
            RangeSelectPartialFixture::Builtins,
        ] {
            let plan = range_select_partial_fixture(fixture);
            let LogicalPlan::Extension(original_extension) = &plan else {
                panic!("fixture must produce an extension");
            };
            let original = original_extension
                .node
                .as_any()
                .downcast_ref::<RangeSelect>()
                .expect("fixture must produce RangeSelect Partial");
            let bytes = DFLogicalSubstraitConvertor
                .encode(&plan, DefaultSerializer)
                .expect("Partial must serialize");
            let substrait_plan = substrait::substrait_proto_df::proto::Plan::decode(bytes.clone())
                .expect("must encode Substrait plan");
            let extension = extension_single_rel(&substrait_plan);
            let detail = extension.detail.as_ref().expect("Partial must have detail");
            assert_eq!(detail.type_url, RANGE_SELECT_PARTIAL_NODE_NAME);
            assert!(!detail.value.is_empty());
            assert!(extension.input.is_some());

            let LogicalPlan::Projection(projection) = original.input.as_ref() else {
                panic!("fixture Partial must materialize through a Projection");
            };
            let context = SessionContext::new();
            context
                .register_table(
                    "range_select_fixture",
                    Arc::new(
                        MemTable::try_new(
                            Arc::new(projection.input.schema().as_arrow().clone()),
                            vec![vec![]],
                        )
                        .expect("fixture table provider"),
                    ),
                )
                .expect("register fixture table");
            let state = SessionStateBuilder::new_from_existing(context.state())
                .with_serializer_registry(Arc::new(DefaultSerializer))
                .build();
            let decoded_plan = DFLogicalSubstraitConvertor
                .decode(bytes, state)
                .await
                .expect("Partial must deserialize");
            let LogicalPlan::Extension(decoded_extension) = &decoded_plan else {
                panic!("decoded plan must be an extension");
            };
            let decoded = decoded_extension
                .node
                .as_any()
                .downcast_ref::<RangeSelect>()
                .expect("placeholder must rebuild a real RangeSelect");
            assert_eq!(
                original.partial_wire_metadata(),
                decoded.partial_wire_metadata()
            );
            assert_eq!(original.schema, decoded.schema);
            assert_eq!(
                original.schema_before_project,
                decoded.schema_before_project
            );
            assert_eq!(
                UserDefinedLogicalNodeCore::expressions(original),
                UserDefinedLogicalNodeCore::expressions(decoded)
            );
            assert!(matches!(decoded.input.as_ref(), LogicalPlan::Projection(_)));
            assert_eq!(original.input.schema(), decoded.input.schema());

            if fixture == RangeSelectPartialFixture::CompoundAvg {
                let execution = collect_range_select_partial_fixture(&decoded_plan, fixture)
                    .await
                    .expect("decoded Partial must plan and collect");
                assert_eq!(
                    execution.physical_schema.as_ref(),
                    execution.logical_schema.as_arrow()
                );
                assert!(!execution.batches.is_empty());
                let batch = &execution.batches[0];
                assert_eq!(batch.schema(), execution.physical_schema);
                assert!(matches!(
                    batch.schema().field(0).data_type(),
                    DataType::Struct(_)
                ));
                assert!(!batch.schema().field(0).is_nullable());
                assert_eq!(
                    batch.schema().field(1).data_type(),
                    &DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, None)
                );
                assert_eq!(batch.schema().field(2).name(), "group");
            }
        }
    }

    #[test]
    fn range_select_complete_and_final_serializer_reject_while_partial_succeeds() {
        let fixture = range_select_split_fixture(RangeSelectPartialFixture::Builtins);
        for plan in [&fixture.complete, &fixture.final_plan] {
            let LogicalPlan::Extension(extension) = plan else {
                panic!("fixture stage must be an extension");
            };
            assert!(
                DefaultSerializer
                    .serialize_logical_plan(extension.node.as_ref())
                    .is_err()
            );
        }
        let LogicalPlan::Extension(partial) = &fixture.partial else {
            panic!("fixture Partial must be an extension");
        };
        assert!(
            DefaultSerializer
                .serialize_logical_plan(partial.node.as_ref())
                .is_ok()
        );
    }
}
