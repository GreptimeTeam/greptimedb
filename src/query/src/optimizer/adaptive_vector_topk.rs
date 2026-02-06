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

use common_function::scalars::vector::distance::{
    VEC_COS_DISTANCE, VEC_DOT_PRODUCT, VEC_L2SQ_DISTANCE,
};
use datafusion_common::Result;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_expr::logical_plan::{FetchType, Limit, SkipType, Sort};
use datafusion_expr::{Expr, LogicalPlan};
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};
use store_api::storage::VectorDistanceMetric;
use tokio::task_local;

use crate::vector_search::plan::AdaptiveVectorTopKLogicalPlan;

#[derive(Debug)]
pub struct AdaptiveVectorTopKRule;

task_local! {
    static SKIP_REWRITE: bool;
}

pub async fn with_adaptive_topk_disabled<F, T>(fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    SKIP_REWRITE.scope(true, fut).await
}

impl OptimizerRule for AdaptiveVectorTopKRule {
    fn name(&self) -> &str {
        "AdaptiveVectorTopKRule"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let skip_rewrite = SKIP_REWRITE.try_with(|flag| *flag).unwrap_or(false);
        if skip_rewrite {
            return Ok(Transformed::no(plan));
        }
        plan.transform_down(&mut |plan| Self::rewrite_limit_sort(plan))
    }
}

impl AdaptiveVectorTopKRule {
    fn rewrite_limit_sort(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Limit(limit) = &plan else {
            return Ok(Transformed::no(plan));
        };
        let LogicalPlan::Sort(sort) = limit.input.as_ref() else {
            return Ok(Transformed::no(plan));
        };
        if !Self::is_vector_sort(sort) {
            return Ok(Transformed::no(plan));
        }

        let (fetch, skip) = match Self::extract_limit_info(limit) {
            Some(info) => info,
            None => return Ok(Transformed::no(plan)),
        };

        let new_plan = AdaptiveVectorTopKLogicalPlan::new(
            sort.input.clone(),
            sort.expr.clone(),
            Some(fetch),
            skip,
        )
        .into_logical_plan();
        Ok(Transformed::yes(new_plan))
    }

    fn is_vector_sort(sort: &Sort) -> bool {
        let Some(sort_expr) = sort.expr.first() else {
            return false;
        };
        let Some(metric) = Self::distance_metric(&sort_expr.expr) else {
            return false;
        };
        let expected_asc = metric != VectorDistanceMetric::InnerProduct;
        sort_expr.asc == expected_asc
    }

    fn distance_metric(expr: &Expr) -> Option<VectorDistanceMetric> {
        let Expr::ScalarFunction(func) = expr else {
            return None;
        };
        match func.name().to_lowercase().as_str() {
            VEC_L2SQ_DISTANCE => Some(VectorDistanceMetric::L2sq),
            VEC_COS_DISTANCE => Some(VectorDistanceMetric::Cosine),
            VEC_DOT_PRODUCT => Some(VectorDistanceMetric::InnerProduct),
            _ => None,
        }
    }

    fn extract_limit_info(limit: &Limit) -> Option<(usize, usize)> {
        let fetch = match limit.get_fetch_type().ok()? {
            FetchType::Literal(fetch) => fetch?,
            FetchType::UnsupportedExpr => return None,
        };
        let skip = match limit.get_skip_type().ok()? {
            SkipType::Literal(skip) => skip,
            SkipType::UnsupportedExpr => return None,
        };
        Some((fetch, skip))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::SemanticType;
    use common_function::function::Function;
    use common_function::scalars::udf::create_udf;
    use common_function::scalars::vector::distance::VEC_L2SQ_DISTANCE;
    use datafusion::datasource::DefaultTableSource;
    use datafusion_common::{DataFusionError, Result, ScalarValue};
    use datafusion_expr::expr::ScalarFunction;
    use datafusion_expr::{Expr, LogicalPlanBuilder, Signature, Volatility, col, lit};
    use datafusion_optimizer::{OptimizerContext, OptimizerRule};
    use datatypes::schema::ColumnSchema;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::ConcreteDataType;

    use crate::dummy_catalog::DummyTableProvider;
    use crate::optimizer::adaptive_vector_topk::AdaptiveVectorTopKRule;
    use crate::optimizer::scan_hint::ScanHintRule;
    use crate::optimizer::test_util::MetaRegionEngine;

    struct TestVectorFunction {
        name: &'static str,
        signature: Signature,
    }

    impl TestVectorFunction {
        fn new(name: &'static str) -> Self {
            Self {
                name,
                signature: Signature::any(2, Volatility::Immutable),
            }
        }
    }

    impl std::fmt::Display for TestVectorFunction {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.name)
        }
    }

    impl Function for TestVectorFunction {
        fn name(&self) -> &str {
            self.name
        }

        fn return_type(
            &self,
            _input_types: &[datatypes::arrow::datatypes::DataType],
        ) -> Result<datatypes::arrow::datatypes::DataType> {
            Ok(datatypes::arrow::datatypes::DataType::Float32)
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn invoke_with_args(
            &self,
            _args: datafusion_expr::ScalarFunctionArgs,
        ) -> Result<datafusion_expr::ColumnarValue> {
            Err(DataFusionError::Execution(
                "test udf should not be invoked".to_string(),
            ))
        }
    }

    fn vec_distance_expr(function_name: &'static str) -> Expr {
        let udf = create_udf(Arc::new(TestVectorFunction::new(function_name)));
        Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(udf),
            vec![
                col("v"),
                lit(ScalarValue::Utf8(Some("[1.0, 2.0]".to_string()))),
            ],
        ))
    }

    fn build_dummy_provider(column_id: u32) -> Arc<DummyTableProvider> {
        let mut builder = RegionMetadataBuilder::new(0.into());
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("k0", ConcreteDataType::string_datatype(), true),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("v", ConcreteDataType::vector_datatype(2), false),
                semantic_type: SemanticType::Field,
                column_id,
            })
            .primary_key(vec![1]);
        let metadata = Arc::new(builder.build().unwrap());
        let engine = Arc::new(MetaRegionEngine::with_metadata(metadata.clone()));
        Arc::new(DummyTableProvider::new(0.into(), engine, metadata))
    }

    #[test]
    fn adaptive_vector_topk_rewrites_limit_sort() {
        let provider = build_dummy_provider(10);
        let source = DefaultTableSource::new(provider);

        let plan = LogicalPlanBuilder::scan_with_filters("t", Arc::new(source), None, vec![])
            .unwrap()
            .sort(vec![vec_distance_expr(VEC_L2SQ_DISTANCE).sort(true, false)])
            .unwrap()
            .limit(0, Some(5))
            .unwrap()
            .build()
            .unwrap();

        let context = OptimizerContext::new();
        let plan = ScanHintRule.rewrite(plan, &context).unwrap().data;
        let result = AdaptiveVectorTopKRule.rewrite(plan, &context).unwrap().data;
        let plan_str = result.display().to_string();
        assert!(plan_str.contains("AdaptiveVectorTopK"));
    }
}
