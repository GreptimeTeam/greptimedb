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
            Err(DataFusionError::Substrait(format!(
                "Unsupported plan node: {name}"
            )))
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
    use catalog::{CatalogManagerRef, RegisterTableRequest};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, NUMBERS_TABLE_ID};
    use datafusion::catalog::TableProvider;
    use datafusion_common::TableReference;
    use datafusion_expr::{LogicalPlanBuilder, LogicalTableSource, col, lit};
    use datatypes::arrow::datatypes::SchemaRef;
    use session::context::QueryContext;
    use substrait::substrait_proto_df::proto::Plan;
    use substrait::substrait_proto_df::proto::plan_rel::RelType as PlanRelType;
    use substrait::substrait_proto_df::proto::read_rel::ReadType;
    use substrait::substrait_proto_df::proto::rel::RelType;
    use table::table::numbers::{NUMBERS_TABLE_NAME, NumbersTable};

    use super::*;
    use crate::QueryEngineFactory;
    use crate::dummy_catalog::DummyCatalogList;
    use crate::optimizer::test_util::mock_table_provider;
    use crate::options::QueryOptions;
    use crate::parser::QueryLanguageParser;
    use crate::plan::extract_and_rewrite_full_table_names;

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

    fn test_engine_with_numbers_table() -> (crate::query_engine::QueryEngineRef, CatalogManagerRef)
    {
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
        (engine, catalog_manager)
    }

    async fn plan_sql(
        engine: &crate::query_engine::QueryEngineRef,
        query_ctx: &QueryContextRef,
        sql: &str,
    ) -> LogicalPlan {
        let statement = QueryLanguageParser::parse_sql(sql, query_ctx).unwrap();
        engine
            .planner()
            .plan(&statement, query_ctx.clone())
            .await
            .unwrap()
    }

    fn named_table_names(plan: &Plan) -> &[String] {
        let Some(PlanRelType::Root(root)) =
            plan.relations.first().and_then(|rel| rel.rel_type.as_ref())
        else {
            panic!("expected a root relation");
        };
        let Some(RelType::Project(project)) = root
            .input
            .as_ref()
            .and_then(|input| input.rel_type.as_ref())
        else {
            panic!("expected the root input to be a project relation");
        };
        let Some(RelType::Read(read)) = project
            .input
            .as_ref()
            .and_then(|input| input.rel_type.as_ref())
        else {
            panic!("expected the project input to be a read relation");
        };
        let Some(ReadType::NamedTable(table)) = read.read_type.as_ref() else {
            panic!("expected the read relation to be a named table");
        };
        &table.names
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
    async fn test_pg_get_keywords_legacy_named_table_shape() {
        let (engine, _) = test_engine_with_numbers_table();
        let query_ctx = QueryContext::arc();
        let plan = plan_sql(&engine, &query_ctx, "SELECT * FROM pg_get_keywords()").await;

        let LogicalPlan::Projection(projection) = &plan else {
            panic!("expected pg_get_keywords() to have a projection root, got {plan}");
        };
        let LogicalPlan::TableScan(scan) = projection.input.as_ref() else {
            panic!("expected the projection to wrap a TableScan, got {plan}");
        };
        let TableReference::Bare { table } = &scan.table_name else {
            panic!("expected the table function scan to have a bare table reference");
        };
        assert_eq!(table.as_ref(), "pg_get_keywords()");

        let (_, rewritten_plan) = extract_and_rewrite_full_table_names(plan, query_ctx).unwrap();
        let encoded = DFLogicalSubstraitConvertor
            .encode(&rewritten_plan, DefaultSerializer)
            .unwrap();
        let substrait_plan = Plan::decode(encoded).unwrap();
        assert_eq!(
            named_table_names(&substrait_plan),
            &["greptime", "public", "pg_get_keywords()"]
        );
    }

    #[tokio::test]
    async fn test_generic_plan_decoder_does_not_resolve_persisted_view_table_function() {
        let (engine, catalog_manager) = test_engine_with_numbers_table();
        let query_ctx = QueryContext::arc();
        let plan = plan_sql(&engine, &query_ctx, "SELECT * FROM pg_get_keywords()").await;
        let (_, rewritten_plan) =
            extract_and_rewrite_full_table_names(plan, query_ctx.clone()).unwrap();
        let encoded = DFLogicalSubstraitConvertor
            .encode(&rewritten_plan, DefaultSerializer)
            .unwrap();

        let decoder = engine.engine_context(query_ctx).new_plan_decoder().unwrap();
        assert!(
            decoder
                .decode(
                    encoded,
                    Arc::new(catalog::table_source::dummy_catalog::DummyCatalogList::new(
                        catalog_manager,
                    )),
                    false,
                )
                .await
                .is_err()
        );
    }
}
