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

use std::collections::{HashMap, HashSet, VecDeque};

use common_function::scalars::vector::distance::{
    VEC_COS_DISTANCE, VEC_DOT_PRODUCT, VEC_L2SQ_DISTANCE,
};
use common_telemetry::debug;
use datafusion_common::ScalarValue;
use datafusion_expr::logical_plan::FetchType;
use datafusion_expr::utils::split_conjunction;
use datafusion_expr::{Expr, SortExpr};
use datafusion_sql::TableReference;
use datatypes::types::parse_string_to_vector_type_value;
use store_api::storage::{VectorDistanceMetric, VectorSearchRequest};

use crate::dummy_catalog::DummyTableProvider;

/// Tracks vector search hints while traversing the logical plan.
///
/// Vector search requests are emitted only when:
/// - ORDER BY uses a supported vector distance function and its direction matches the metric.
/// - A LIMIT (or Sort.fetch) is present to derive k.
/// - The hint stays within a single input chain (not across join/subquery branches).
/// - The target column is non-nullable, or an explicit IS NOT NULL filter exists.
#[derive(Default)]
pub(crate) struct VectorSearchState {
    current_distance: Option<VectorDistanceInfo>,
    current_limit: Option<VectorLimitInfo>,
    distance_stack: Vec<Option<VectorDistanceInfo>>,
    limit_stack: Vec<Option<VectorLimitInfo>>,
    non_null_columns: HashSet<ColumnKey>,
    non_null_stack: Vec<HashSet<ColumnKey>>,
    vector_hints: HashMap<TableReference, VecDeque<VectorHintEntry>>,
}

#[derive(Clone)]
struct VectorDistanceInfo {
    table_reference: Option<TableReference>,
    column_name: String,
    query_vector: Vec<f32>,
    metric: VectorDistanceMetric,
}

#[derive(Clone)]
struct VectorLimitInfo {
    fetch: usize,
    skip: usize,
}

impl VectorLimitInfo {
    fn k(&self) -> Option<usize> {
        self.fetch.checked_add(self.skip)
    }
}

#[derive(Clone)]
struct VectorHintEntry {
    distance: VectorDistanceInfo,
    limit: VectorLimitInfo,
    non_null_constraint: bool,
}

#[derive(Clone, Hash, PartialEq, Eq)]
struct ColumnKey {
    table_reference: Option<TableReference>,
    column_name: String,
}

impl VectorSearchState {
    pub(crate) fn need_rewrite(&self) -> bool {
        !self.vector_hints.is_empty()
    }

    pub(crate) fn on_branching_enter(&mut self) {
        // Clear per-branch state so hints are only derived within a single input chain.
        self.distance_stack.push(self.current_distance.take());
        self.limit_stack.push(self.current_limit.take());
        self.non_null_stack
            .push(std::mem::take(&mut self.non_null_columns));
    }

    pub(crate) fn on_branching_exit(&mut self) {
        // Restore the prior chain state after leaving the branch.
        if let Some(previous) = self.limit_stack.pop() {
            self.current_limit = previous;
        }
        if let Some(previous) = self.distance_stack.pop() {
            self.current_distance = previous;
        }
        if let Some(previous) = self.non_null_stack.pop() {
            self.non_null_columns = previous;
        }
    }

    pub(crate) fn on_limit_enter(&mut self, limit: &datafusion_expr::logical_plan::Limit) {
        self.limit_stack.push(self.current_limit.take());
        self.current_limit = Self::extract_limit_info(limit);
    }

    pub(crate) fn on_limit_exit(&mut self) {
        if let Some(previous) = self.limit_stack.pop() {
            self.current_limit = previous;
        }
    }

    pub(crate) fn on_sort_enter(&mut self, sort: &datafusion_expr::logical_plan::Sort) {
        // Distance is scoped to the nearest sort, while limit may be inherited from parents.
        self.distance_stack.push(self.current_distance.take());
        self.limit_stack.push(self.current_limit.clone());
        let distance = Self::extract_distance_from_sort(sort);
        self.current_distance = distance.clone();
        // Sort.fetch is a TopK limit, so we can infer k when no LIMIT is present.
        if self.current_limit.is_none() {
            self.current_limit = distance
                .as_ref()
                .and_then(|_| Self::extract_limit_from_sort(sort));
        }
    }

    pub(crate) fn on_sort_exit(&mut self) {
        if let Some(previous) = self.limit_stack.pop() {
            self.current_limit = previous;
        }
        if let Some(previous) = self.distance_stack.pop() {
            self.current_distance = previous;
        }
    }

    pub(crate) fn on_table_scan(&mut self, table_scan: &datafusion_expr::logical_plan::TableScan) {
        self.record_vector_hint(table_scan);
    }

    pub(crate) fn on_filter_enter(&mut self, predicate: &Expr) {
        self.non_null_stack.push(self.non_null_columns.clone());
        for expr in split_conjunction(predicate) {
            if let Expr::IsNotNull(inner) = expr
                && let Expr::Column(col) = inner.as_ref()
            {
                self.non_null_columns.insert(ColumnKey {
                    table_reference: col.relation.clone(),
                    column_name: col.name.clone(),
                });
            }
        }
        // TODO: detect non-null constraints from more complex predicates (casts/functions).
    }

    pub(crate) fn on_filter_exit(&mut self) {
        if let Some(previous) = self.non_null_stack.pop() {
            self.non_null_columns = previous;
        }
    }

    pub(crate) fn take_vector_request_from_dummy(
        &mut self,
        provider: &DummyTableProvider,
        table_name: &TableReference,
    ) -> Option<VectorSearchRequest> {
        let hint = self.take_vector_hint(table_name)?;
        self.build_vector_request_from_dummy(provider, table_name, &hint)
    }

    fn build_vector_request_from_dummy(
        &self,
        provider: &DummyTableProvider,
        table_name: &TableReference,
        hint: &VectorHintEntry,
    ) -> Option<VectorSearchRequest> {
        let info = &hint.distance;
        let k = hint.limit.k()?;

        if let Some(ref hint_table) = info.table_reference
            && table_name != hint_table
        {
            return None;
        }

        let metadata = provider.region_metadata();
        let column = metadata.column_by_name(&info.column_name)?;
        if column.column_schema.is_nullable() && !hint.non_null_constraint {
            debug!(
                "Skip vector hint: column '{}' is nullable without IS NOT NULL filter",
                info.column_name
            );
            return None;
        }

        Some(VectorSearchRequest {
            column_id: column.column_id,
            query_vector: info.query_vector.clone(),
            k,
            metric: info.metric,
        })
    }

    fn extract_distance_info(expr: &Expr) -> Option<VectorDistanceInfo> {
        let Expr::ScalarFunction(func) = expr else {
            return None;
        };

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
    }

    fn extract_distance_from_sort(
        sort: &datafusion_expr::logical_plan::Sort,
    ) -> Option<VectorDistanceInfo> {
        if sort.expr.len() != 1 {
            debug!(
                "Skip vector hint: Sort has {} expressions, expected 1",
                sort.expr.len()
            );
            return None;
        }
        let sort_expr: &SortExpr = &sort.expr[0];
        let info = Self::extract_distance_info(&sort_expr.expr)?;
        let expected_asc = info.metric != VectorDistanceMetric::InnerProduct;
        if sort_expr.asc == expected_asc {
            Some(info)
        } else {
            None
        }
    }

    fn extract_limit_info(limit: &datafusion_expr::logical_plan::Limit) -> Option<VectorLimitInfo> {
        let fetch = match limit.get_fetch_type().ok()? {
            FetchType::Literal(fetch) => fetch?,
            FetchType::UnsupportedExpr => return None,
        };
        let skip = match limit.get_skip_type().ok()? {
            datafusion_expr::logical_plan::SkipType::Literal(skip) => skip,
            datafusion_expr::logical_plan::SkipType::UnsupportedExpr => return None,
        };
        Some(VectorLimitInfo { fetch, skip })
    }

    fn extract_limit_from_sort(
        sort: &datafusion_expr::logical_plan::Sort,
    ) -> Option<VectorLimitInfo> {
        let fetch = sort.fetch?;
        Some(VectorLimitInfo { fetch, skip: 0 })
    }

    fn record_vector_hint(&mut self, table_scan: &datafusion_expr::logical_plan::TableScan) {
        let Some(limit) = self.current_limit.as_ref() else {
            return;
        };
        let Some(distance) = self.current_distance.as_ref() else {
            return;
        };
        // Only emit hints when distance+limit are present and the table matches the sort target.
        if let Some(ref hint_table) = distance.table_reference
            && hint_table != &table_scan.table_name
        {
            return;
        }

        let non_null_constraint =
            self.is_column_non_null(&distance.table_reference, &distance.column_name);
        self.vector_hints
            .entry(table_scan.table_name.clone())
            .or_default()
            .push_back(VectorHintEntry {
                distance: distance.clone(),
                limit: limit.clone(),
                non_null_constraint,
            });
    }

    fn take_vector_hint(&mut self, table_name: &TableReference) -> Option<VectorHintEntry> {
        let hints = self.vector_hints.get_mut(table_name)?;
        let hint = hints.pop_front();
        if hints.is_empty() {
            self.vector_hints.remove(table_name);
        }
        hint
    }

    fn is_column_non_null(
        &self,
        table_reference: &Option<TableReference>,
        column_name: &str,
    ) -> bool {
        self.non_null_columns.contains(&ColumnKey {
            table_reference: table_reference.clone(),
            column_name: column_name.to_string(),
        })
    }

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
        if bytes.is_empty() || !bytes.len().is_multiple_of(4) {
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
    use common_function::scalars::vector::distance::{VEC_DOT_PRODUCT, VEC_L2SQ_DISTANCE};
    use datafusion::datasource::DefaultTableSource;
    use datafusion::logical_expr::ColumnarValue;
    use datafusion_common::{Column, DataFusionError, Result, ScalarValue};
    use datafusion_expr::expr::ScalarFunction;
    use datafusion_expr::logical_plan::JoinType;
    use datafusion_expr::{
        Expr, LogicalPlan, LogicalPlanBuilder, Signature, Subquery, Volatility, col, lit,
    };
    use datafusion_optimizer::{OptimizerContext, OptimizerRule};
    use datatypes::schema::ColumnSchema;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::{ConcreteDataType, VectorDistanceMetric};

    use super::VectorSearchState;
    use crate::dummy_catalog::DummyTableProvider;
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
        build_dummy_provider_with_nullable(column_id, false)
    }

    fn build_dummy_provider_with_nullable(
        column_id: u32,
        nullable_vector: bool,
    ) -> Arc<DummyTableProvider> {
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
                column_schema: ColumnSchema::new(
                    "v",
                    ConcreteDataType::vector_datatype(2),
                    nullable_vector,
                ),
                semantic_type: SemanticType::Field,
                column_id,
            })
            .primary_key(vec![1]);
        let metadata = Arc::new(builder.build().unwrap());
        let engine = Arc::new(MetaRegionEngine::with_metadata(metadata.clone()));
        Arc::new(DummyTableProvider::new(0.into(), engine, metadata))
    }

    #[test]
    fn test_parse_json_vector() {
        assert_eq!(
            VectorSearchState::parse_json_vector("[1.0, 2.0, 3.0]"),
            Some(vec![1.0, 2.0, 3.0])
        );
        assert_eq!(
            VectorSearchState::parse_json_vector("[1.5, -2.3, 0.0]"),
            Some(vec![1.5, -2.3, 0.0])
        );
        assert_eq!(VectorSearchState::parse_json_vector("invalid"), None);
        assert_eq!(VectorSearchState::parse_json_vector("[]"), None);
        assert_eq!(VectorSearchState::parse_json_vector(""), None);
        assert_eq!(VectorSearchState::parse_json_vector("["), None);
        assert_eq!(VectorSearchState::parse_json_vector("[1.0, abc]"), None);
    }

    #[test]
    fn test_parse_binary_vector() {
        let v1: f32 = 1.0;
        let v2: f32 = 2.0;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&v1.to_le_bytes());
        bytes.extend_from_slice(&v2.to_le_bytes());

        let result = VectorSearchState::parse_binary_vector(&bytes);
        assert_eq!(result, Some(vec![1.0, 2.0]));

        let result = VectorSearchState::parse_binary_vector(&[1, 2, 3]);
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

        let context = OptimizerContext::default();
        let _ = ScanHintRule.rewrite(plan, &context).unwrap();

        let hint = dummy_provider.get_vector_search_hint().unwrap();
        assert_eq!(hint.column_id, 10);
        assert_eq!(hint.k, 5);
        assert_eq!(hint.metric, VectorDistanceMetric::L2sq);
        assert_eq!(hint.query_vector, vec![1.0, 2.0]);
    }

    #[test]
    fn test_limit_offset_for_vector_hint() {
        let dummy_provider = build_dummy_provider(10);
        let table_source = Arc::new(DefaultTableSource::new(dummy_provider.clone()));
        let expr = vec_distance_expr(VEC_L2SQ_DISTANCE);
        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .sort(vec![expr.sort(true, false)])
            .unwrap()
            .limit(5, Some(10))
            .unwrap()
            .build()
            .unwrap();

        let context = OptimizerContext::default();
        let _ = ScanHintRule.rewrite(plan, &context).unwrap();

        let hint = dummy_provider.get_vector_search_hint().unwrap();
        assert_eq!(hint.k, 15);
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

        let context = OptimizerContext::default();
        let _ = ScanHintRule.rewrite(plan, &context).unwrap();
        assert!(dummy_provider.get_vector_search_hint().is_none());

        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .sort(vec![expr.sort(false, false)])
            .unwrap()
            .limit(0, Some(3))
            .unwrap()
            .build()
            .unwrap();
        let _ = ScanHintRule.rewrite(plan, &context).unwrap();
        let hint = dummy_provider.get_vector_search_hint().unwrap();
        assert_eq!(hint.metric, VectorDistanceMetric::InnerProduct);
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

        let context = OptimizerContext::default();
        let _ = ScanHintRule.rewrite(plan, &context).unwrap();

        assert!(dummy_provider.get_vector_search_hint().is_none());
    }

    #[test]
    fn test_nullable_vector_requires_is_not_null_filter() {
        let dummy_provider = build_dummy_provider_with_nullable(10, true);
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

        let context = OptimizerContext::default();
        let _ = ScanHintRule.rewrite(plan, &context).unwrap();

        assert!(dummy_provider.get_vector_search_hint().is_none());
    }

    #[test]
    fn test_nullable_vector_with_is_not_null_filter() {
        let dummy_provider = build_dummy_provider_with_nullable(10, true);
        let table_source = Arc::new(DefaultTableSource::new(dummy_provider.clone()));
        let expr = vec_distance_expr(VEC_L2SQ_DISTANCE);
        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .filter(col("v").is_not_null())
            .unwrap()
            .sort(vec![expr.sort(true, false)])
            .unwrap()
            .limit(0, Some(5))
            .unwrap()
            .build()
            .unwrap();

        let context = OptimizerContext::default();
        let _ = ScanHintRule.rewrite(plan, &context).unwrap();

        let hint = dummy_provider.get_vector_search_hint().unwrap();
        assert_eq!(hint.column_id, 10);
        assert_eq!(hint.k, 5);
    }

    #[test]
    fn test_sort_fetch_for_vector_hint() {
        let dummy_provider = build_dummy_provider(10);
        let table_source = Arc::new(DefaultTableSource::new(dummy_provider.clone()));
        let expr = vec_distance_expr(VEC_L2SQ_DISTANCE);
        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .sort_with_limit(vec![expr.sort(true, false)], Some(4))
            .unwrap()
            .build()
            .unwrap();

        let context = OptimizerContext::default();
        let _ = ScanHintRule.rewrite(plan, &context).unwrap();

        let hint = dummy_provider.get_vector_search_hint().unwrap();
        assert_eq!(hint.k, 4);
    }

    #[test]
    fn test_limit_scoped_to_sort_branch() {
        let t1_provider = build_dummy_provider(10);
        let t2_provider = build_dummy_provider(20);
        let t1_source = Arc::new(DefaultTableSource::new(t1_provider.clone()));
        let t2_source = Arc::new(DefaultTableSource::new(t2_provider.clone()));
        let expr = vec_distance_expr(VEC_L2SQ_DISTANCE);

        let left = LogicalPlanBuilder::scan_with_filters("t1", t1_source, None, vec![])
            .unwrap()
            .limit(0, Some(5))
            .unwrap()
            .build()
            .unwrap();

        let right = LogicalPlanBuilder::scan_with_filters("t2", t2_source, None, vec![])
            .unwrap()
            .sort(vec![expr.sort(true, false)])
            .unwrap()
            .build()
            .unwrap();

        let join_keys: (Vec<Column>, Vec<Column>) = (vec![], vec![]);
        let plan = LogicalPlanBuilder::from(left)
            .join(right, JoinType::Inner, join_keys, None)
            .unwrap()
            .build()
            .unwrap();

        let context = OptimizerContext::default();
        let _ = ScanHintRule.rewrite(plan, &context).unwrap();

        assert!(t1_provider.get_vector_search_hint().is_none());
        assert!(t2_provider.get_vector_search_hint().is_none());
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
    fn test_no_vector_hint_above_join() {
        let t1_provider = build_dummy_provider(10);
        let t2_provider = build_dummy_provider(20);
        let t1_source = Arc::new(DefaultTableSource::new(t1_provider.clone()));
        let t2_source = Arc::new(DefaultTableSource::new(t2_provider.clone()));

        let left = LogicalPlanBuilder::scan_with_filters("t1", t1_source, None, vec![])
            .unwrap()
            .build()
            .unwrap();

        let right = LogicalPlanBuilder::scan_with_filters("t2", t2_source, None, vec![])
            .unwrap()
            .build()
            .unwrap();

        let join_keys: (Vec<Column>, Vec<Column>) = (vec![], vec![]);
        let join_plan = LogicalPlanBuilder::from(left)
            .join(right, JoinType::Inner, join_keys, None)
            .unwrap()
            .build()
            .unwrap();

        let expr = vec_distance_expr_qualified(VEC_L2SQ_DISTANCE, "t1", "v");
        let plan = LogicalPlanBuilder::from(join_plan)
            .sort(vec![expr.sort(true, false)])
            .unwrap()
            .limit(0, Some(5))
            .unwrap()
            .build()
            .unwrap();

        let context = OptimizerContext::default();
        let _ = ScanHintRule.rewrite(plan, &context).unwrap();

        assert!(t1_provider.get_vector_search_hint().is_none());
        assert!(t2_provider.get_vector_search_hint().is_none());
    }

    #[test]
    fn test_no_vector_hint_above_subquery() {
        let provider = build_dummy_provider(10);
        let table_source = Arc::new(DefaultTableSource::new(provider.clone()));
        let scan_plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .build()
            .unwrap();

        let subquery = LogicalPlan::Subquery(Subquery {
            subquery: Arc::new(scan_plan),
            outer_ref_columns: vec![],
            spans: Default::default(),
        });

        let expr = vec_distance_expr(VEC_L2SQ_DISTANCE);
        let plan = LogicalPlanBuilder::from(subquery)
            .sort(vec![expr.sort(true, false)])
            .unwrap()
            .limit(0, Some(5))
            .unwrap()
            .build()
            .unwrap();

        let context = OptimizerContext::default();
        let _ = ScanHintRule.rewrite(plan, &context).unwrap();

        assert!(provider.get_vector_search_hint().is_none());
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

        let context = OptimizerContext::default();

        let _ = ScanHintRule.rewrite(t1_plan, &context).unwrap();
        assert!(t1_provider.get_vector_search_hint().is_none());

        let _ = ScanHintRule.rewrite(t2_plan, &context).unwrap();
        let hint = t2_provider.get_vector_search_hint().unwrap();
        assert_eq!(hint.column_id, 20);
        assert_eq!(hint.k, 5);
    }
}
