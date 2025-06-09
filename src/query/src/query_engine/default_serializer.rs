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
use common_function::aggr::{GeoPathAccumulator, HllState, UddSketchState};
use common_function::function_registry::FUNCTION_REGISTRY;
use common_function::scalars::udf::create_udf;
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
    quantile_udaf, AbsentOverTime, AvgOverTime, Changes, CountOverTime, Delta, Deriv, IDelta,
    Increase, LastOverTime, MaxOverTime, MinOverTime, PresentOverTime, Rate, Resets, Round,
    StddevOverTime, StdvarOverTime, SumOverTime,
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
        for func in FUNCTION_REGISTRY.functions() {
            let udf = Arc::new(create_udf(
                func.clone(),
                self.query_ctx.clone(),
                Default::default(),
            ));
            session_state
                .register_udf(udf)
                .context(RegisterUdfSnafu { name: func.name() })?;
            let _ = session_state.register_udaf(Arc::new(UddSketchState::state_udf_impl()));
            let _ = session_state.register_udaf(Arc::new(UddSketchState::merge_udf_impl()));
            let _ = session_state.register_udaf(Arc::new(HllState::state_udf_impl()));
            let _ = session_state.register_udaf(Arc::new(HllState::merge_udf_impl()));
            let _ = session_state.register_udaf(Arc::new(GeoPathAccumulator::udf_impl()));
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
            // TODO(ruihang): add quantile_over_time, predict_linear, holt_winters, round
        }
        let logical_plan = DFLogicalSubstraitConvertor
            .decode(message, session_state)
            .await
            .map_err(BoxedError::new)
            .context(common_query::error::DecodePlanSnafu)?;

        if optimize {
            self.session_state
                .optimize(&logical_plan)
                .context(common_query::error::GeneralDataFusionSnafu)
        } else {
            Ok(logical_plan)
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::catalog::TableProvider;
    use datafusion_expr::{col, lit, LogicalPlanBuilder, LogicalTableSource};
    use datatypes::arrow::datatypes::SchemaRef;
    use session::context::QueryContext;

    use super::*;
    use crate::dummy_catalog::DummyCatalogList;
    use crate::optimizer::test_util::mock_table_provider;
    use crate::options::QueryOptions;
    use crate::QueryEngineFactory;

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
}
