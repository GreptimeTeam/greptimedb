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
use datafusion_expr::logical_plan::{FetchType, Limit, SkipType, Sort};
use datafusion_expr::{Expr, SortExpr};
use store_api::storage::VectorDistanceMetric;

#[derive(Clone, Copy)]
pub(crate) struct VectorSortInfo<'a> {
    pub metric: VectorDistanceMetric,
    pub primary_sort_expr: &'a SortExpr,
}

impl<'a> VectorSortInfo<'a> {
    pub(crate) fn has_expected_order(&self) -> bool {
        let expected_asc = self.metric != VectorDistanceMetric::InnerProduct;
        self.primary_sort_expr.asc == expected_asc
    }
}

pub(crate) fn parse_vector_sort(sort: &Sort) -> Option<VectorSortInfo<'_>> {
    let primary_sort_expr = sort.expr.first()?;
    let metric = distance_metric(&primary_sort_expr.expr)?;
    Some(VectorSortInfo {
        metric,
        primary_sort_expr,
    })
}

pub(crate) fn is_vector_sort(sort: &Sort) -> bool {
    parse_vector_sort(sort).is_some_and(|info| info.has_expected_order())
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
