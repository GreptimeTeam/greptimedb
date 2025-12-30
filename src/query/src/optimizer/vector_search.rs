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

//! Optimizer rule for vector search (KNN queries).
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
//!
//! In standalone mode, the hint is set directly on `DummyTableProvider`.
//! In distributed mode, the plan is wrapped in `VectorScanLogicalPlan` which
//! is serialized via Substrait and transmitted to datanodes.

use datafusion::datasource::DefaultTableSource;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::logical_plan::FetchType;
use datafusion_expr::{Expr, LogicalPlan, SortExpr};
use datafusion_optimizer::AnalyzerRule;
use store_api::storage::{VectorDistanceMetric, VectorSearchRequest};
use table::table::adapter::DfTableProviderAdapter;

use crate::dummy_catalog::DummyTableProvider;

/// Vector distance function names.
const VEC_L2SQ_DISTANCE: &str = "vec_l2sq_distance";
const VEC_COS_DISTANCE: &str = "vec_cos_distance";
const VEC_DOT_PRODUCT: &str = "vec_dot_product";

/// Over-fetch multiplier for distributed vector search.
/// In distributed mode, we fetch more candidates from each datanode to account for
/// rows that may be filtered out by predicates after merging results from all datanodes.
/// Each datanode also does local overfetch, but we need extra margin for the merge phase.
const DISTRIBUTED_OVERFETCH_MULTIPLIER: usize = 2;

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
                    // Case 1: Standalone mode / datanode - DummyTableProvider
                    // Set the hint directly on the provider.
                    if let Some(dummy_provider) = source
                        .table_provider
                        .as_any()
                        .downcast_ref::<DummyTableProvider>()
                        && let Some(request) = visitor.build_request_from_dummy(dummy_provider)
                    {
                        dummy_provider.with_vector_search_hint(request);
                        return Ok(Transformed::yes(plan));
                    }

                    // Case 2: DfTableProviderAdapter - used in both standalone and distributed modes
                    // Set the hint directly on the adapter. This works for:
                    // - Standalone mode: The scan will use the hint directly
                    // - Distributed mode: The hint is also stored on the adapter, and the
                    //   DistPlannerAnalyzer will wrap this in MergeScanLogicalPlan which gets
                    //   serialized via Substrait (including the vector search hint).
                    if let Some(adapter) = source
                        .table_provider
                        .as_any()
                        .downcast_ref::<DfTableProviderAdapter>()
                        && let Some(request) = visitor.build_request_from_adapter(adapter)
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
    /// Whether the query has a Filter (WHERE clause) that requires post-filtering.
    has_filter: bool,
}

/// Information about a vector distance function call.
struct VectorDistanceInfo {
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
        // Extract LIMIT value
        if let LogicalPlan::Limit(limit) = node
            && let Ok(FetchType::Literal(Some(fetch))) = limit.get_fetch_type()
        {
            self.limit = Some(fetch);
        }

        // Extract ORDER BY with vector distance function
        if let LogicalPlan::Sort(sort) = node {
            // Only consider single column sort
            if sort.expr.len() == 1 {
                let sort_expr: &SortExpr = &sort.expr[0];
                if let Some(info) = Self::extract_distance_info(&sort_expr.expr) {
                    // Check sort order matches the metric:
                    // - L2sq/Cosine: smaller is better, should be ASC
                    // - InnerProduct: larger is better, should be DESC
                    let expected_asc = info.metric != VectorDistanceMetric::InnerProduct;
                    if sort_expr.asc == expected_asc {
                        self.distance_func = Some(info);
                    }
                }
            }
        }

        // Detect Filter (WHERE clause) that requires post-filtering
        if let LogicalPlan::Filter(_) = node {
            self.has_filter = true;
        }

        Ok(TreeNodeRecursion::Continue)
    }
}

impl VectorSearchVisitor {
    fn need_rewrite(&self) -> bool {
        self.distance_func.is_some() && self.limit.is_some()
    }

    /// Builds a VectorSearchRequest from a DummyTableProvider (standalone/datanode mode).
    fn build_request_from_dummy(
        &self,
        provider: &DummyTableProvider,
    ) -> Option<VectorSearchRequest> {
        let info = self.distance_func.as_ref()?;
        let k = self.limit?;

        // Find the column ID from region metadata.
        let metadata = provider.region_metadata();
        let column = metadata.column_by_name(&info.column_name)?;

        Some(VectorSearchRequest {
            column_id: column.column_id,
            query_vector: info.query_vector.clone(),
            k,
            metric: info.metric,
        })
    }

    /// Builds a VectorSearchRequest from a DfTableProviderAdapter (distributed frontend mode).
    fn build_request_from_adapter(
        &self,
        adapter: &DfTableProviderAdapter,
    ) -> Option<VectorSearchRequest> {
        let info = self.distance_func.as_ref()?;
        let k = self.limit?;

        // Find the column ID from table info.
        let table = adapter.table();
        let table_info = table.table_info();
        let schema = &table_info.meta.schema;

        // Get the column index by name.
        let column_index = schema.column_index_by_name(&info.column_name)?;

        // Get the column ID from the column_ids vector.
        let column_id = *table_info.meta.column_ids.get(column_index)?;

        // In distributed mode with filters, overfetch to account for predicate filtering
        // after merging results from all datanodes. Without filters, no overfetch needed.
        let effective_k = if self.has_filter {
            k * DISTRIBUTED_OVERFETCH_MULTIPLIER
        } else {
            k
        };

        Some(VectorSearchRequest {
            column_id,
            query_vector: info.query_vector.clone(),
            k: effective_k,
            metric: info.metric,
        })
    }

    /// Extracts vector distance info from a scalar function expression.
    fn extract_distance_info(expr: &Expr) -> Option<VectorDistanceInfo> {
        if let Expr::ScalarFunction(func) = expr {
            let func_name = func.name().to_lowercase();
            let metric = match func_name.as_str() {
                VEC_L2SQ_DISTANCE => VectorDistanceMetric::L2sq,
                VEC_COS_DISTANCE => VectorDistanceMetric::Cosine,
                VEC_DOT_PRODUCT => VectorDistanceMetric::InnerProduct,
                _ => return None,
            };

            // Expect 2 arguments: (column, query_vector)
            if func.args.len() != 2 {
                return None;
            }

            // First argument should be a column reference
            let column_name = match &func.args[0] {
                Expr::Column(col) => col.name.clone(),
                _ => return None,
            };

            // Second argument should be a literal string (JSON array) or binary
            let query_vector = Self::extract_query_vector(&func.args[1])?;

            Some(VectorDistanceInfo {
                column_name,
                query_vector,
                metric,
            })
        } else {
            None
        }
    }

    /// Extracts the query vector from a literal expression.
    fn extract_query_vector(expr: &Expr) -> Option<Vec<f32>> {
        match expr {
            Expr::Literal(scalar, _) => match scalar {
                ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                    // Parse JSON array like "[1.0, 2.0, 3.0]"
                    Self::parse_json_vector(s)
                }
                ScalarValue::Binary(Some(bytes)) | ScalarValue::LargeBinary(Some(bytes)) => {
                    // Parse binary representation (array of f32)
                    Self::parse_binary_vector(bytes)
                }
                _ => None,
            },
            _ => None,
        }
    }

    /// Parses a JSON array string into a vector of f32.
    fn parse_json_vector(s: &str) -> Option<Vec<f32>> {
        let s = s.trim();
        if !s.starts_with('[') || !s.ends_with(']') {
            return None;
        }
        let inner = &s[1..s.len() - 1];
        inner
            .split(',')
            .map(|part| part.trim().parse::<f32>().ok())
            .collect()
    }

    /// Parses a binary representation into a vector of f32.
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
    use super::*;

    #[test]
    fn test_parse_json_vector() {
        let result = VectorSearchVisitor::parse_json_vector("[1.0, 2.0, 3.0]");
        assert_eq!(result, Some(vec![1.0, 2.0, 3.0]));

        let result = VectorSearchVisitor::parse_json_vector("[1.5, -2.3, 0.0]");
        assert_eq!(result, Some(vec![1.5, -2.3, 0.0]));

        let result = VectorSearchVisitor::parse_json_vector("invalid");
        assert_eq!(result, None);
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

        // Invalid length
        let result = VectorSearchVisitor::parse_binary_vector(&[1, 2, 3]);
        assert_eq!(result, None);
    }
}
