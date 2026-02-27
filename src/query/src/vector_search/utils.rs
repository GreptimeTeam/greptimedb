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
use datafusion_common::Column;
use datafusion_expr::logical_plan::{FetchType, Limit, SkipType, Sort};
use datafusion_expr::{Expr, LogicalPlan, SortExpr};
use store_api::storage::VectorDistanceMetric;

pub(crate) const MAX_ALIAS_DEPTH: usize = 16;

/// Returns `true` if `sort` is a vector-distance sort with the expected ordering
/// (ascending for L2/Cosine, descending for InnerProduct).
pub(crate) fn is_vector_sort(sort: &Sort) -> bool {
    is_vector_sort_exprs(sort.input.as_ref(), &sort.expr)
}

pub(crate) fn is_vector_sort_exprs(input: &LogicalPlan, exprs: &[SortExpr]) -> bool {
    let Some(primary) = exprs.first() else {
        return false;
    };
    let Some(metric) = resolve_sort_metric(input, &primary.expr) else {
        return false;
    };
    let expected_asc = metric != VectorDistanceMetric::InnerProduct;
    primary.asc == expected_asc
}

pub(crate) fn distance_metric(expr: &Expr) -> Option<VectorDistanceMetric> {
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

pub(crate) fn extract_limit_info(limit: &Limit) -> Option<(usize, usize)> {
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

fn resolve_sort_metric(input: &LogicalPlan, expr: &Expr) -> Option<VectorDistanceMetric> {
    distance_metric(expr).or_else(|| match expr {
        Expr::Alias(alias) => resolve_sort_metric(input, alias.expr.as_ref()),
        Expr::Column(column) => resolve_metric_from_column(input, column, 0),
        _ => None,
    })
}

fn resolve_metric_from_column(
    plan: &LogicalPlan,
    target: &Column,
    depth: usize,
) -> Option<VectorDistanceMetric> {
    resolve_from_projection_column(plan, target, depth, &mut |expr, next_depth| {
        resolve_metric_from_expr(expr, next_depth)
    })
}

fn resolve_metric_from_expr(expr: &Expr, depth: usize) -> Option<VectorDistanceMetric> {
    if depth > MAX_ALIAS_DEPTH {
        return None;
    }

    distance_metric(expr).or_else(|| match expr {
        Expr::Alias(alias) => resolve_metric_from_expr(alias.expr.as_ref(), depth + 1),
        _ => None,
    })
}

fn projection_expr_matches_column(expr: &Expr, target: &Column) -> bool {
    match expr {
        Expr::Alias(alias) => alias.name == target.name,
        Expr::Column(column) => column.name == target.name,
        _ => expr.schema_name().to_string() == target.name,
    }
}

pub(crate) fn resolve_from_projection_column<T, F>(
    plan: &LogicalPlan,
    target: &Column,
    depth: usize,
    resolve_projection_expr: &mut F,
) -> Option<T>
where
    F: FnMut(&Expr, usize) -> Option<T>,
{
    if depth > MAX_ALIAS_DEPTH {
        return None;
    }

    if let LogicalPlan::Projection(projection) = plan {
        for expr in &projection.expr {
            if projection_expr_matches_column(expr, target)
                && let Some(value) = resolve_projection_expr(expr, depth + 1)
            {
                return Some(value);
            }
        }
    }

    let mut inputs = plan.inputs().into_iter();
    match (inputs.next(), inputs.next()) {
        (Some(child), None) => {
            resolve_from_projection_column(child, target, depth + 1, resolve_projection_expr)
        }
        _ => None,
    }
}
