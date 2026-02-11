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

use arrow_schema::SortOptions;
use common_recordbatch::OrderOption;
use common_telemetry::debug;
use datafusion_common::{Column, ScalarValue};
use datafusion_expr::utils::split_conjunction;
use datafusion_expr::{Expr, LogicalPlan, SortExpr};
use datafusion_sql::TableReference;
use datatypes::types::parse_string_to_vector_type_value;
use store_api::storage::{VectorDistanceMetric, VectorSearchRequest};

use crate::dummy_catalog::DummyTableProvider;
use crate::vector_search::utils::{
    MAX_ALIAS_DEPTH, distance_metric, extract_limit_info, resolve_from_projection_column,
};

/// Tracks vector search hints while traversing the logical plan.
///
/// Vector search requests are emitted only when:
/// - ORDER BY uses a supported vector distance function and its direction matches the metric.
/// - A LIMIT (or Sort.fetch) is present to derive k.
/// - The hint stays within a single input chain (not across join/subquery branches).
/// - The target column is non-nullable, or an explicit IS NOT NULL filter exists.
///
/// Known limitations:
/// - Dynamic overfetching is not implemented yet. When filters exist or ORDER BY includes
///   additional tie-breaker columns (e.g., ORDER BY distance, id), the current fixed k may
///   return incorrect results. A future improvement should dynamically adjust k based on
///   filter selectivity and secondary sort requirements.
/// - Hints only block at subquery boundaries when the subquery contains non-inlineable
///   operators (Limit/Sort/Distinct/Aggregate/Window). Simple subqueries without these
///   operators allow hints to propagate through. In distributed mode, the dist analyzer
///   may inline subqueries before this rule runs, further reducing isolation.
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
    tie_breakers: Vec<OrderOption>,
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
        self.current_limit =
            extract_limit_info(limit).map(|(fetch, skip)| VectorLimitInfo { fetch, skip });
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
            && !Self::table_reference_matches(hint_table, table_name)
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
            limit: Some(hint.limit.fetch),
            offset: Some(hint.limit.skip),
            tie_breakers: (!info.tie_breakers.is_empty()).then(|| info.tie_breakers.clone()),
        })
    }

    fn extract_distance_info(expr: &Expr) -> Option<VectorDistanceInfo> {
        let metric = distance_metric(expr)?;

        let Expr::ScalarFunction(func) = expr else {
            return None;
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
            tie_breakers: Vec::new(),
        })
    }

    fn extract_distance_from_sort(
        sort: &datafusion_expr::logical_plan::Sort,
    ) -> Option<VectorDistanceInfo> {
        if sort.expr.is_empty() {
            debug!("Skip vector hint: Sort has no expressions");
            return None;
        }
        let sort_expr: &SortExpr = &sort.expr[0];
        let info = Self::extract_distance_info_from_expr(sort.input.as_ref(), &sort_expr.expr)?;
        let expected_asc = info.metric != VectorDistanceMetric::InnerProduct;
        if sort_expr.asc != expected_asc {
            return None;
        }

        if Self::tie_breakers_allowed(&sort.expr[1..], &info) {
            let tie_breakers = Self::build_tie_breakers(&sort.expr[1..]);
            Some(VectorDistanceInfo {
                tie_breakers,
                ..info
            })
        } else {
            if sort.expr.len() > 1 {
                debug!(
                    "Skip vector hint: Sort has unsupported tie-breakers ({} expressions)",
                    sort.expr.len()
                );
            }
            None
        }
    }

    fn extract_distance_info_from_expr(
        sort_input: &LogicalPlan,
        expr: &Expr,
    ) -> Option<VectorDistanceInfo> {
        Self::extract_distance_info(expr).or_else(|| match expr {
            Expr::Alias(alias) => {
                Self::extract_distance_info_from_expr(sort_input, alias.expr.as_ref())
            }
            Expr::Column(column) => Self::extract_distance_info_from_column(sort_input, column, 0),
            _ => None,
        })
    }

    fn extract_distance_info_from_column(
        plan: &LogicalPlan,
        target: &Column,
        depth: usize,
    ) -> Option<VectorDistanceInfo> {
        resolve_from_projection_column(plan, target, depth, &mut |expr, next_depth| {
            Self::extract_distance_info_from_projection_expr(expr, next_depth)
        })
    }

    fn extract_distance_info_from_projection_expr(
        expr: &Expr,
        depth: usize,
    ) -> Option<VectorDistanceInfo> {
        if depth > MAX_ALIAS_DEPTH {
            return None;
        }

        Self::extract_distance_info(expr).or_else(|| match expr {
            Expr::Alias(alias) => {
                Self::extract_distance_info_from_projection_expr(alias.expr.as_ref(), depth + 1)
            }
            _ => None,
        })
    }

    fn tie_breakers_allowed(sort_exprs: &[SortExpr], distance_info: &VectorDistanceInfo) -> bool {
        if sort_exprs.is_empty() {
            return true;
        }

        sort_exprs.iter().all(|sort_expr| {
            let Expr::Column(col) = &sort_expr.expr else {
                return false;
            };

            match &distance_info.table_reference {
                Some(table) => col
                    .relation
                    .as_ref()
                    .is_some_and(|relation| Self::table_reference_matches(relation, table)),
                None => col.relation.is_none(),
            }
        })
    }

    fn build_tie_breakers(sort_exprs: &[SortExpr]) -> Vec<OrderOption> {
        sort_exprs
            .iter()
            .filter_map(|sort_expr| {
                let Expr::Column(col) = &sort_expr.expr else {
                    return None;
                };
                Some(OrderOption {
                    name: col.name.clone(),
                    options: SortOptions {
                        descending: !sort_expr.asc,
                        nulls_first: sort_expr.nulls_first,
                    },
                })
            })
            .collect()
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
            && !Self::table_reference_matches(hint_table, &table_scan.table_name)
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

    fn table_reference_matches(left: &TableReference, right: &TableReference) -> bool {
        left.resolved_eq(right)
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
    use common_function::scalars::vector::distance::{VEC_DOT_PRODUCT, VEC_L2SQ_DISTANCE};
    use datafusion::datasource::DefaultTableSource;
    use datafusion_common::Column;
    use datafusion_expr::logical_plan::JoinType;
    use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder, Subquery, col, lit};
    use datafusion_optimizer::{OptimizerContext, OptimizerRule};
    use datatypes::schema::ColumnSchema;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::{ConcreteDataType, VectorDistanceMetric};

    use super::VectorSearchState;
    use crate::dummy_catalog::DummyTableProvider;
    use crate::optimizer::scan_hint::ScanHintRule;
    use crate::optimizer::test_util::MetaRegionEngine;
    use crate::vector_search::test_utils::{vec_distance_expr, vec_distance_expr_qualified};

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

    fn build_dist_shape_dummy_provider(column_id: u32) -> Arc<DummyTableProvider> {
        let mut builder = RegionMetadataBuilder::new(0.into());
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "vec_id",
                    ConcreteDataType::int32_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "part_tag",
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Field,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "embedding",
                    ConcreteDataType::vector_datatype(2),
                    false,
                ),
                semantic_type: SemanticType::Field,
                column_id,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 4,
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
    fn test_dummy_provider_vector_hint_with_sort_alias() {
        let dummy_provider = build_dummy_provider(10);
        let table_source = Arc::new(DefaultTableSource::new(dummy_provider.clone()));
        let expr = vec_distance_expr(VEC_L2SQ_DISTANCE);
        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .project(vec![col("k0"), expr.alias("d")])
            .unwrap()
            .sort(vec![col("d").sort(true, false)])
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
    }

    #[test]
    fn test_vector_hint_with_tie_breaker_and_offset() {
        let dummy_provider = build_dummy_provider(10);
        let table_source = Arc::new(DefaultTableSource::new(dummy_provider.clone()));
        let expr = vec_distance_expr(VEC_L2SQ_DISTANCE);
        let plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .sort(vec![expr.sort(true, false), col("k0").sort(true, false)])
            .unwrap()
            .limit(3, Some(7))
            .unwrap()
            .build()
            .unwrap();

        let context = OptimizerContext::default();
        let _ = ScanHintRule.rewrite(plan, &context).unwrap();

        let hint = dummy_provider.get_vector_search_hint().unwrap();
        assert_eq!(hint.limit, Some(7));
        assert_eq!(hint.offset, Some(3));
        let tie_breakers = hint.tie_breakers.unwrap();
        assert_eq!(tie_breakers.len(), 1);
        assert_eq!(tie_breakers[0].name, "k0");
        assert!(!tie_breakers[0].options.descending);
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

    // Simple subqueries (without non-inlineable ops like Limit/Sort/Distinct/Aggregate/Window)
    // allow hints to propagate through. See known limitations in VectorSearchState docs.
    #[test]
    fn test_simple_subquery_allows_hint_propagation() {
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

        // Hint propagates through simple subquery
        let hint = provider.get_vector_search_hint().unwrap();
        assert_eq!(hint.k, 5);
    }

    // Subqueries with non-inlineable ops (Limit/Sort/Distinct/Aggregate/Window) block hint propagation.
    #[test]
    fn test_subquery_with_limit_blocks_hint() {
        let provider = build_dummy_provider(10);
        let table_source = Arc::new(DefaultTableSource::new(provider.clone()));
        let scan_plan = LogicalPlanBuilder::scan_with_filters("t", table_source, None, vec![])
            .unwrap()
            .limit(0, Some(100)) // non-inlineable op inside subquery
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

        // Hint does NOT propagate through subquery with non-inlineable ops
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

    #[test]
    fn test_full_table_name_matches_bare_vector_relation() {
        let provider = build_dummy_provider(10);
        let source = Arc::new(DefaultTableSource::new(provider.clone()));
        let expr = vec_distance_expr_qualified(VEC_L2SQ_DISTANCE, "t2", "v");

        let plan =
            LogicalPlanBuilder::scan_with_filters("greptime.public.t2", source, None, vec![])
                .unwrap()
                .sort(vec![expr.sort(true, false), col("k0").sort(true, false)])
                .unwrap()
                .limit(0, Some(7))
                .unwrap()
                .build()
                .unwrap();

        let context = OptimizerContext::default();
        let _ = ScanHintRule.rewrite(plan, &context).unwrap();

        let hint = provider.get_vector_search_hint().unwrap();
        assert_eq!(hint.column_id, 10);
        assert_eq!(hint.k, 7);
    }

    #[test]
    fn test_dist_shape_with_unaliased_projection_distance_column_builds_vector_hint() {
        use datafusion_common::Column as DfColumn;

        let provider = build_dist_shape_dummy_provider(10);
        let source = Arc::new(DefaultTableSource::new(provider.clone()));

        let distance_expr =
            vec_distance_expr_qualified(VEC_L2SQ_DISTANCE, "vectors_explain_dist", "embedding");
        let distance_col_name = distance_expr.schema_name().to_string();
        let tie_breaker = Expr::Column(DfColumn::new(Some("vectors_explain_dist"), "vec_id"));
        let filter = col("part_tag")
            .eq(lit("a"))
            .or(col("part_tag").eq(lit("z")));

        let plan = LogicalPlanBuilder::scan_with_filters(
            "greptime.public.vectors_explain_dist",
            source,
            None,
            vec![],
        )
        .unwrap()
        .filter(filter)
        .unwrap()
        .project(vec![col("vec_id"), col("part_tag"), distance_expr.clone()])
        .unwrap()
        .sort(vec![
            col(distance_col_name).sort(true, false),
            tie_breaker.sort(true, false),
        ])
        .unwrap()
        .limit(0, Some(22))
        .unwrap()
        .build()
        .unwrap();

        let context = OptimizerContext::default();
        let _ = ScanHintRule.rewrite(plan, &context).unwrap();

        let hint = provider.get_vector_search_hint().unwrap();
        assert_eq!(hint.column_id, 10);
        assert_eq!(hint.k, 22);
    }
}
