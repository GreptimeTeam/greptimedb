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
use datafusion_expr::Expr;
use datafusion_expr::logical_plan::{FetchType, Limit, SkipType, Sort};
use store_api::storage::VectorDistanceMetric;

pub(crate) fn extract_vector_sort_metric(sort: &Sort) -> Option<VectorDistanceMetric> {
    let sort_expr = sort.expr.first()?;
    distance_metric(&sort_expr.expr)
}

pub(crate) fn is_vector_sort(sort: &Sort) -> bool {
    let Some(metric) = extract_vector_sort_metric(sort) else {
        return false;
    };
    let Some(sort_expr) = sort.expr.first() else {
        return false;
    };
    let expected_asc = metric != VectorDistanceMetric::InnerProduct;
    sort_expr.asc == expected_asc
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
