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

//! Analyzer rule for vector search (KNN queries).
//!
//! This rule extracts vector distance function calls from the query and
//! creates a hint for the table scan to use vector index for KNN search.
//!
//! The pattern recognized is:
//! ```sql
//! SELECT * FROM table
//! ORDER BY vec_l2sq_distance(vec_column, '[1.0, 2.0, ...]')
//! LIMIT k;
//! ```

use datafusion::datasource::DefaultTableSource;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::logical_plan::FetchType;
use datafusion_expr::{Expr, LogicalPlan, SortExpr};
use datafusion_optimizer::AnalyzerRule;
use datafusion_sql::TableReference;
use datatypes::types::parse_string_to_vector_type_value;
use store_api::storage::{VectorDistanceMetric, VectorSearchRequest};
use table::table::adapter::DfTableProviderAdapter;

use crate::dummy_catalog::DummyTableProvider;

/// Vector distance function names.
const VEC_L2SQ_DISTANCE: &str = "vec_l2sq_distance";
const VEC_COS_DISTANCE: &str = "vec_cos_distance";
const VEC_DOT_PRODUCT: &str = "vec_dot_product";

/// This rule detects KNN vector search patterns and sets hints on the table scan.
///
/// Pattern: `ORDER BY vec_distance(column, query_vector) LIMIT k`
#[derive(Debug)]
pub struct VectorSearchRule;

impl AnalyzerRule for VectorSearchRule {
    fn name(&self) -> &str {
        "VectorSearchRule"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        let result = Self::optimize(plan)?;
        Ok(result.data)
    }
}

impl VectorSearchRule {
    fn optimize(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        let mut visitor = VectorSearchVisitor::default();
        let _ = plan.visit(&mut visitor)?;

        if visitor.need_rewrite() {
            plan.transform_down(&|plan| Self::set_hints(plan, &visitor))
        } else {
            Ok(Transformed::no(plan))
        }
    }

    fn set_hints(
        plan: LogicalPlan,
        visitor: &VectorSearchVisitor,
    ) -> Result<Transformed<LogicalPlan>> {
        match &plan {
            LogicalPlan::TableScan(table_scan) => {
                if let Some(source) = table_scan
                    .source
                    .as_any()
                    .downcast_ref::<DefaultTableSource>()
                {
                    let table_name = &table_scan.table_name;

                    if let Some(dummy_provider) = source
                        .table_provider
                        .as_any()
                        .downcast_ref::<DummyTableProvider>()
                        && let Some(request) =
                            visitor.build_request_from_dummy(dummy_provider, table_name)
                    {
                        dummy_provider.with_vector_search_hint(request);
                        return Ok(Transformed::yes(plan));
                    }

                    if let Some(adapter) = source
                        .table_provider
                        .as_any()
                        .downcast_ref::<DfTableProviderAdapter>()
                        && let Some(request) =
                            visitor.build_request_from_adapter(adapter, table_name)
                    {
                        adapter.with_vector_search_hint(request);
                        return Ok(Transformed::yes(plan));
                    }
                }
                Ok(Transformed::no(plan))
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}

/// Visitor to collect vector search hints from the logical plan.
#[derive(Default)]
struct VectorSearchVisitor {
    /// The vector distance function info extracted from ORDER BY.
    distance_func: Option<VectorDistanceInfo>,
    /// The LIMIT value (k for KNN).
    limit: Option<usize>,
}

/// Information about a vector distance function call.
struct VectorDistanceInfo {
    /// Table reference (if column was qualified, e.g., "t2" in "t2.vec_col").
    table_reference: Option<TableReference>,
    /// Column name of the vector column.
    column_name: String,
    /// Query vector to search for.
    query_vector: Vec<f32>,
    /// Distance metric.
    metric: VectorDistanceMetric,
}

impl TreeNodeVisitor<'_> for VectorSearchVisitor {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
        if let LogicalPlan::Limit(limit) = node
            && let Ok(FetchType::Literal(Some(fetch))) = limit.get_fetch_type()
        {
            self.limit = Some(fetch);
        }

        if let LogicalPlan::Sort(sort) = node
            // TODO: consider supporting vector search with additional ORDER BY expressions.
            && sort.expr.len() == 1
        {
            let sort_expr: &SortExpr = &sort.expr[0];
            if let Some(info) = Self::extract_distance_info(&sort_expr.expr) {
                let expected_asc = info.metric != VectorDistanceMetric::InnerProduct;
                if sort_expr.asc == expected_asc {
                    self.distance_func = Some(info);
                }
            }
        }

        Ok(TreeNodeRecursion::Continue)
    }
}

impl VectorSearchVisitor {
    fn need_rewrite(&self) -> bool {
        self.distance_func.is_some() && self.limit.is_some()
    }

    fn build_request_from_dummy(
        &self,
        provider: &DummyTableProvider,
        table_name: &TableReference,
    ) -> Option<VectorSearchRequest> {
        let info = self.distance_func.as_ref()?;
        let k = self.limit?;

        if let Some(ref hint_table) = info.table_reference
            && table_name != hint_table
        {
            return None;
        }

        let metadata = provider.region_metadata();
        let column = metadata.column_by_name(&info.column_name)?;

        Some(VectorSearchRequest {
            column_id: column.column_id,
            query_vector: info.query_vector.clone(),
            k,
            metric: info.metric,
        })
    }

    fn build_request_from_adapter(
        &self,
        adapter: &DfTableProviderAdapter,
        table_name: &TableReference,
    ) -> Option<VectorSearchRequest> {
        let info = self.distance_func.as_ref()?;
        let k = self.limit?;

        if let Some(ref hint_table) = info.table_reference
            && table_name != hint_table
        {
            return None;
        }

        let table = adapter.table();
        let table_info = table.table_info();
        let schema = &table_info.meta.schema;

        let column_index = schema.column_index_by_name(&info.column_name)?;
        let column_id = *table_info.meta.column_ids.get(column_index)?;

        Some(VectorSearchRequest {
            column_id,
            query_vector: info.query_vector.clone(),
            k,
            metric: info.metric,
        })
    }

    fn extract_distance_info(expr: &Expr) -> Option<VectorDistanceInfo> {
        if let Expr::ScalarFunction(func) = expr {
            let func_name = func.name().to_lowercase();
            let metric = match func_name.as_str() {
                VEC_L2SQ_DISTANCE => VectorDistanceMetric::L2sq,
                VEC_COS_DISTANCE => VectorDistanceMetric::Cosine,
                VEC_DOT_PRODUCT => VectorDistanceMetric::InnerProduct,
                _ => return None,
            };

            if func.args.len() != 2 {
                return None;
            }

            let (table_reference, column_name) = match &func.args[0] {
                Expr::Column(col) => (col.relation.clone(), col.name.clone()),
                _ => return None,
            };

            let query_vector = Self::extract_query_vector(&func.args[1])?;

            Some(VectorDistanceInfo {
                table_reference,
                column_name,
                query_vector,
                metric,
            })
        } else {
            None
        }
    }

    // TODO: support parameterized query vectors (non-literal expressions).
    fn extract_query_vector(expr: &Expr) -> Option<Vec<f32>> {
        match expr {
            Expr::Literal(scalar, _) => match scalar {
                ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                    Self::parse_json_vector(s)
                }
                ScalarValue::Binary(Some(bytes)) | ScalarValue::LargeBinary(Some(bytes)) => {
                    Self::parse_binary_vector(bytes)
                }
                _ => None,
            },
            _ => None,
        }
    }

    fn parse_json_vector(s: &str) -> Option<Vec<f32>> {
        let trimmed = s.trim();
        if !trimmed.starts_with('[') || !trimmed.ends_with(']') {
            return None;
        }
        parse_string_to_vector_type_value(trimmed, None)
            .ok()
            .and_then(|bytes| if bytes.is_empty() { None } else { Some(bytes) })
            .and_then(|bytes| Self::parse_binary_vector(&bytes))
    }

    fn parse_binary_vector(bytes: &[u8]) -> Option<Vec<f32>> {
        if !bytes.len().is_multiple_of(4) {
            return None;
        }
        Some(
            bytes
                .chunks(4)
                .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::SemanticType;
    use common_function::function::Function;
    use common_function::scalars::udf::create_udf;
    use common_recordbatch::{RecordBatch, RecordBatchStream, SendableRecordBatchStream};
    use datafusion::datasource::DefaultTableSource;
    use datafusion::logical_expr::ColumnarValue;
    use datafusion_common::{DataFusionError, ScalarValue};
    use datafusion_expr::expr::ScalarFunction;
    use datafusion_expr::{LogicalPlanBuilder, Signature, Volatility, col, lit};
    use datafusion_optimizer::{OptimizerContext, OptimizerRule};
    use datatypes::schema::{ColumnSchema, SchemaBuilder, SchemaRef};
    use futures::Stream;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::ConcreteDataType;
    use table::metadata::{FilterPushDownType, TableInfoBuilder, TableMeta, TableType};
    use table::table::adapter::DfTableProviderAdapter;
    use table::{Table, TableRef};

    use super::*;
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
        ) -> Result<ColumnarValue> {
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

    fn build_vector_table() -> TableRef {
        let schema = {
            let columns = vec![
                ColumnSchema::new("k0", ConcreteDataType::string_datatype(), true),
                ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
                ColumnSchema::new("v", ConcreteDataType::vector_datatype(2), false),
            ];
            Arc::new(
                SchemaBuilder::try_from_columns(columns)
                    .unwrap()
                    .build()
                    .unwrap(),
            )
        };

        struct TestDataSource {
            schema: SchemaRef,
        }

        impl TestDataSource {
            fn new(schema: SchemaRef) -> Self {
                Self { schema }
            }
        }

        struct EmptyStream {
            schema: SchemaRef,
        }

        impl RecordBatchStream for EmptyStream {
            fn schema(&self) -> SchemaRef {
                self.schema.clone()
            }

            fn output_ordering(&self) -> Option<&[common_recordbatch::OrderOption]> {
                None
            }

            fn metrics(&self) -> Option<common_recordbatch::adapter::RecordBatchMetrics> {
                None
            }
        }

        impl Stream for EmptyStream {
            type Item = common_recordbatch::error::Result<RecordBatch>;

            fn poll_next(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Option<Self::Item>> {
                std::task::Poll::Ready(None)
            }
        }

        impl store_api::data_source::DataSource for TestDataSource {
            fn get_stream(
                &self,
                request: store_api::storage::ScanRequest,
            ) -> Result<SendableRecordBatchStream, common_error::ext::BoxedError> {
                let projected_schema = match &request.projection {
                    Some(projection) => Arc::new(self.schema.try_project(projection).unwrap()),
                    None => self.schema.clone(),
                };
                Ok(Box::pin(EmptyStream {
                    schema: projected_schema,
                }))
            }
        }

        let table_meta = TableMeta {
            schema: schema.clone(),
            primary_key_indices: vec![0],
            value_indices: vec![2],
            engine: "test_engine".to_string(),
            next_column_id: 4,
            options: Default::default(),
            created_on: Default::default(),
            updated_on: Default::default(),
            partition_key_indices: vec![0],
            column_ids: vec![1, 2, 3],
        };

        let table_info = TableInfoBuilder::default()
            .table_id(1)
            .name("t".to_string())
            .catalog_name(common_catalog::consts::DEFAULT_CATALOG_NAME)
            .schema_name(common_catalog::consts::DEFAULT_SCHEMA_NAME)
            .table_version(0)
            .table_type(TableType::Base)
            .meta(table_meta)
            .build()
            .unwrap();

        let data_source = Arc::new(TestDataSource::new(schema));
        Arc::new(Table::new(
            Arc::new(table_info),
            FilterPushDownType::Unsupported,
            data_source,
        ))
    }

    #[test]
    fn test_parse_json_vector() {
        assert_eq!(
            VectorSearchVisitor::parse_json_vector("[1.0, 2.0, 3.0]"),
            Some(vec![1.0, 2.0, 3.0])
        );
        assert_eq!(
            VectorSearchVisitor::parse_json_vector("[1.5, -2.3, 0.0]"),
            Some(vec![1.5, -2.3, 0.0])
        );
        assert_eq!(VectorSearchVisitor::parse_json_vector("invalid"), None);
        assert_eq!(VectorSearchVisitor::parse_json_vector("[]"), None);
        assert_eq!(VectorSearchVisitor::parse_json_vector(""), None);
        assert_eq!(VectorSearchVisitor::parse_json_vector("["), None);
        assert_eq!(VectorSearchVisitor::parse_json_vector("[1.0, abc]"), None);
    }

    #[test]
    fn test_parse_binary_vector() {
        let v1: f32 = 1.0;
        let v2: f32 = 2.0;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&v1.to_le_bytes());
        bytes.extend_from_slice(&v2.to_le_bytes());

        let result = VectorSearchVisitor::parse_binary_vector(&bytes);
        assert_eq!(result, Some(vec![1.0, 2.0]));

        let result = VectorSearchVisitor::parse_binary_vector(&[1, 2, 3]);
        assert_eq!(result, None);
    }

    #[test]
    fn test_dummy_provider_vector_hint() {
        let dummy_provider = build_dummy_provider(10);
        let table_source = Arc::new(DefaultTableSource::new(dummy_provider.clone()));
        let expr = vec_distance_expr(VEC_L2SQ_DISTANCE);
        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .sort(vec![expr.sort(true, false)])
            .unwrap()
            .limit(0, Some(5))
            .unwrap()
            .build()
            .unwrap();

        let config = ConfigOptions::default();
        let _ = VectorSearchRule.analyze(plan, &config).unwrap();

        let hint = dummy_provider.get_vector_search_hint().unwrap();
        assert_eq!(hint.column_id, 10);
        assert_eq!(hint.k, 5);
        assert_eq!(hint.metric, VectorDistanceMetric::L2sq);
        assert_eq!(hint.query_vector, vec![1.0, 2.0]);
    }

    #[test]
    fn test_scan_hint_pipeline_keeps_vector_search_hint() {
        let dummy_provider = build_dummy_provider(10);
        let table_source = Arc::new(DefaultTableSource::new(dummy_provider.clone()));
        let expr = vec_distance_expr(VEC_L2SQ_DISTANCE);
        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .sort(vec![expr.sort(true, false)])
            .unwrap()
            .limit(0, Some(5))
            .unwrap()
            .build()
            .unwrap();

        let config = ConfigOptions::default();
        let plan = VectorSearchRule.analyze(plan, &config).unwrap();
        let context = OptimizerContext::default();
        let _ = ScanHintRule.rewrite(plan, &context).unwrap();

        let scan_req = dummy_provider.scan_request();
        assert!(scan_req.vector_search.is_some());
    }

    #[test]
    fn test_inner_product_sort_direction() {
        let dummy_provider = build_dummy_provider(10);
        let table_source = Arc::new(DefaultTableSource::new(dummy_provider.clone()));
        let expr = vec_distance_expr(VEC_DOT_PRODUCT);
        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source.clone(), None, vec![])
            .unwrap()
            .sort(vec![expr.clone().sort(true, false)])
            .unwrap()
            .limit(0, Some(3))
            .unwrap()
            .build()
            .unwrap();

        let config = ConfigOptions::default();
        let _ = VectorSearchRule.analyze(plan, &config).unwrap();
        assert!(dummy_provider.get_vector_search_hint().is_none());

        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .sort(vec![expr.sort(false, false)])
            .unwrap()
            .limit(0, Some(3))
            .unwrap()
            .build()
            .unwrap();
        let _ = VectorSearchRule.analyze(plan, &config).unwrap();
        let hint = dummy_provider.get_vector_search_hint().unwrap();
        assert_eq!(hint.metric, VectorDistanceMetric::InnerProduct);
    }

    #[test]
    fn test_adapter_overfetch_with_filter() {
        let table = build_vector_table();
        let adapter = Arc::new(DfTableProviderAdapter::new(table));
        let table_source = Arc::new(DefaultTableSource::new(adapter.clone()));
        let expr = vec_distance_expr(VEC_L2SQ_DISTANCE);
        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .filter(col("k0").eq(lit("hello")))
            .unwrap()
            .sort(vec![expr.sort(true, false)])
            .unwrap()
            .limit(0, Some(7))
            .unwrap()
            .build()
            .unwrap();

        let config = ConfigOptions::default();
        let _ = VectorSearchRule.analyze(plan, &config).unwrap();

        let hint = adapter.get_vector_search_hint().unwrap();
        assert_eq!(hint.column_id, 3);
        assert_eq!(hint.k, 7);
        assert_eq!(hint.metric, VectorDistanceMetric::L2sq);
    }

    #[test]
    fn test_no_limit_clause() {
        let dummy_provider = build_dummy_provider(10);
        let table_source = Arc::new(DefaultTableSource::new(dummy_provider.clone()));
        let expr = vec_distance_expr(VEC_L2SQ_DISTANCE);
        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .sort(vec![expr.sort(true, false)])
            .unwrap()
            .build()
            .unwrap();

        let config = ConfigOptions::default();
        let _ = VectorSearchRule.analyze(plan, &config).unwrap();

        assert!(dummy_provider.get_vector_search_hint().is_none());
    }

    #[test]
    fn test_multiple_order_by_expressions() {
        let dummy_provider = build_dummy_provider(10);
        let table_source = Arc::new(DefaultTableSource::new(dummy_provider.clone()));
        let expr = vec_distance_expr(VEC_L2SQ_DISTANCE);
        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .sort(vec![expr.sort(true, false), col("k0").sort(true, false)])
            .unwrap()
            .limit(0, Some(5))
            .unwrap()
            .build()
            .unwrap();

        let config = ConfigOptions::default();
        let _ = VectorSearchRule.analyze(plan, &config).unwrap();

        assert!(dummy_provider.get_vector_search_hint().is_none());
    }

    fn vec_distance_expr_qualified(
        function_name: &'static str,
        table_name: &str,
        column_name: &str,
    ) -> Expr {
        use datafusion_common::Column;

        let udf = create_udf(Arc::new(TestVectorFunction::new(function_name)));
        let qualified_col = Expr::Column(Column::new(Some(table_name.to_string()), column_name));
        Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(udf),
            vec![
                qualified_col,
                lit(ScalarValue::Utf8(Some("[1.0, 2.0]".to_string()))),
            ],
        ))
    }

    #[test]
    fn test_qualified_column_scopes_hint_to_correct_table() {
        let t1_provider = build_dummy_provider(10);
        let t2_provider = build_dummy_provider(20);
        let t1_source = Arc::new(DefaultTableSource::new(t1_provider.clone()));
        let t2_source = Arc::new(DefaultTableSource::new(t2_provider.clone()));

        let expr = vec_distance_expr_qualified(VEC_L2SQ_DISTANCE, "t2", "v");

        let t1_plan = LogicalPlanBuilder::scan_with_filters("t1", t1_source, None, vec![])
            .unwrap()
            .sort(vec![expr.clone().sort(true, false)])
            .unwrap()
            .limit(0, Some(5))
            .unwrap()
            .build()
            .unwrap();

        let t2_plan = LogicalPlanBuilder::scan_with_filters("t2", t2_source, None, vec![])
            .unwrap()
            .sort(vec![expr.sort(true, false)])
            .unwrap()
            .limit(0, Some(5))
            .unwrap()
            .build()
            .unwrap();

        let config = ConfigOptions::default();

        let _ = VectorSearchRule.analyze(t1_plan, &config).unwrap();
        assert!(t1_provider.get_vector_search_hint().is_none());

        let _ = VectorSearchRule.analyze(t2_plan, &config).unwrap();
        let hint = t2_provider.get_vector_search_hint().unwrap();
        assert_eq!(hint.column_id, 20);
        assert_eq!(hint.k, 5);
    }
}
