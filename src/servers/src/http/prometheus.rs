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

//! prom supply the prometheus HTTP API Server compliance

use std::borrow::Borrow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{
    Date32Type, Date64Type, Decimal128Type, Float32Type, Float64Type, Int8Type, Int16Type,
    Int32Type, Int64Type, IntervalDayTimeType, IntervalMonthDayNanoType, IntervalYearMonthType,
    UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_schema::{DataType, IntervalUnit};
use auth::{PermissionTableTarget, PermissionTableTargets};
use axum::extract::{Path, Query, State};
use axum::{Extension, Form};
use catalog::CatalogManagerRef;
use common_catalog::parse_catalog_and_schema_from_db_string;
use common_decimal::Decimal128;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_query::native_histogram::is_native_histogram_value_type;
use common_query::{Output, OutputData};
use common_recordbatch::{RecordBatch, RecordBatches};
use common_telemetry::{debug, tracing};
use common_time::util::{current_time_rfc3339, yesterday_rfc3339};
use common_time::{Date, IntervalDayTime, IntervalMonthDayNano, IntervalYearMonth};
use common_version::OwnedBuildInfo;
use datafusion_common::ScalarValue;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, SchemaRef};
use datatypes::types::jsonb_to_string;
use futures::future::join_all;
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use promql_parser::label::{METRIC_NAME, MatchOp, Matcher, Matchers};
use promql_parser::parser::token::{self};
use promql_parser::parser::value::ValueType;
use promql_parser::parser::{
    AggregateExpr, BinaryExpr, Call, Expr as PromqlExpr, LabelModifier, MatrixSelector, ParenExpr,
    SubqueryExpr, UnaryExpr, VectorSelector,
};
use query::parser::{DEFAULT_LOOKBACK_STRING, PromQuery, QueryStatement};
use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use session::context::{QueryContext, QueryContextRef};
use snafu::{Location, OptionExt, ResultExt};
use store_api::metric_engine_consts::{
    DATA_SCHEMA_TABLE_ID_COLUMN_NAME, DATA_SCHEMA_TSID_COLUMN_NAME, LOGICAL_TABLE_METADATA_KEY,
};
use table::TableRef;
use table::metadata::TableInfo;
use table::requests::{SEMANTIC_METRIC_TYPE, SEMANTIC_METRIC_UNIT};

pub use super::result::prometheus_resp::PrometheusJsonResponse;
use crate::error::{
    CollectRecordbatchSnafu, ConvertScalarValueSnafu, DataFusionSnafu, Error, InvalidQuerySnafu,
    NotSupportedSnafu, Result, TableNotFoundSnafu, UnexpectedResultSnafu,
};
use crate::http::header::collect_plan_metrics;
use crate::prom_store::{FIELD_NAME_LABEL, METRIC_NAME_LABEL, is_database_selection_label};
use crate::prometheus_handler::{
    ParsedPromQuery, PrometheusHandlerRef, resolve_schema_from_matchers,
};

/// For [ValueType::Vector] result type
#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PromSeriesVector {
    pub metric: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<(f64, String)>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub histogram: Option<(f64, PromNativeHistogram)>,
}

/// For [ValueType::Matrix] result type
#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PromSeriesMatrix {
    pub metric: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub values: Vec<(f64, String)>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub histograms: Vec<(f64, PromNativeHistogram)>,
}

#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct PromNativeHistogram {
    pub count: String,
    pub sum: String,
    pub buckets: Vec<(u8, String, String, String)>,
}

/// Variants corresponding to [ValueType]
#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum PromQueryResult {
    Matrix(Vec<PromSeriesMatrix>),
    Vector(Vec<PromSeriesVector>),
    Scalar(#[serde(skip_serializing_if = "Option::is_none")] Option<(f64, String)>),
    String(#[serde(skip_serializing_if = "Option::is_none")] Option<(f64, String)>),
}

impl Default for PromQueryResult {
    fn default() -> Self {
        PromQueryResult::Matrix(Default::default())
    }
}

#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct PromData {
    #[serde(rename = "resultType")]
    pub result_type: String,
    pub result: PromQueryResult,
}

#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PromMetadata {
    #[serde(rename = "type")]
    pub metric_type: String,
    pub unit: String,
    pub help: String,
}

/// A "holder" for the reference([Arc]) to a column name,
/// to help avoiding cloning [String]s when used as a [HashMap] key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Column(Arc<String>);

impl From<&str> for Column {
    fn from(s: &str) -> Self {
        Self(Arc::new(s.to_string()))
    }
}

#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum PrometheusResponse {
    PromData(PromData),
    Labels(Vec<String>),
    Series(Vec<HashMap<Column, String>>),
    Metadata(BTreeMap<String, Vec<PromMetadata>>),
    LabelValues(Vec<String>),
    FormatQuery(String),
    BuildInfo(OwnedBuildInfo),
    #[serde(skip_deserializing)]
    ParseResult(promql_parser::parser::Expr),
    #[default]
    None,
}

impl PrometheusResponse {
    /// Append the other [`PrometheusResponse]`.
    /// # NOTE
    ///   Only append matrix and vector results, otherwise just ignore the other response.
    fn append(&mut self, other: PrometheusResponse) {
        match (self, other) {
            (
                PrometheusResponse::PromData(PromData {
                    result: PromQueryResult::Matrix(lhs),
                    ..
                }),
                PrometheusResponse::PromData(PromData {
                    result: PromQueryResult::Matrix(rhs),
                    ..
                }),
            ) => {
                lhs.extend(rhs);
            }

            (
                PrometheusResponse::PromData(PromData {
                    result: PromQueryResult::Vector(lhs),
                    ..
                }),
                PrometheusResponse::PromData(PromData {
                    result: PromQueryResult::Vector(rhs),
                    ..
                }),
            ) => {
                lhs.extend(rhs);
            }
            _ => {
                // TODO(dennis): process other cases?
            }
        }
    }

    pub fn is_none(&self) -> bool {
        matches!(self, PrometheusResponse::None)
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct FormatQuery {
    query: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct MetadataQuery {
    db: Option<String>,
    limit: Option<usize>,
    limit_per_metric: Option<usize>,
    metric: Option<String>,
}

#[axum_macros::debug_handler]
#[tracing::instrument(
    skip_all,
    fields(protocol = "prometheus", request_type = "format_query")
)]
pub async fn format_query(
    State(_handler): State<PrometheusHandlerRef>,
    Query(params): Query<InstantQuery>,
    Extension(_query_ctx): Extension<QueryContext>,
    Form(form_params): Form<InstantQuery>,
) -> PrometheusJsonResponse {
    let query = params.query.or(form_params.query).unwrap_or_default();
    match promql_parser::parser::parse(&query) {
        Ok(expr) => {
            let pretty = expr.prettify();
            PrometheusJsonResponse::success(PrometheusResponse::FormatQuery(pretty))
        }
        Err(reason) => {
            let err = InvalidQuerySnafu { reason }.build();
            PrometheusJsonResponse::error(err.status_code(), err.output_msg())
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct BuildInfoQuery {}

#[axum_macros::debug_handler]
#[tracing::instrument(
    skip_all,
    fields(protocol = "prometheus", request_type = "build_info_query")
)]
pub async fn build_info_query() -> PrometheusJsonResponse {
    let build_info = common_version::build_info().clone();
    PrometheusJsonResponse::success(PrometheusResponse::BuildInfo(build_info.into()))
}

#[axum_macros::debug_handler]
#[tracing::instrument(
    skip_all,
    fields(protocol = "prometheus", request_type = "metadata_query")
)]
pub async fn metadata_query(
    State(handler): State<PrometheusHandlerRef>,
    Query(params): Query<MetadataQuery>,
    Extension(mut query_ctx): Extension<QueryContext>,
) -> PrometheusJsonResponse {
    let (catalog, schema) = get_catalog_schema(&params.db, &query_ctx);
    try_update_catalog_schema(&mut query_ctx, &catalog, &schema);

    let metadata = match retrieve_metric_metadata(&query_ctx, handler.catalog_manager(), &params)
        .await
    {
        Ok(metadata) => metadata,
        Err(err) => {
            return PrometheusJsonResponse::error(StatusCode::InvalidArguments, err.to_string());
        }
    };

    PrometheusJsonResponse::success(PrometheusResponse::Metadata(metadata))
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct InstantQuery {
    query: Option<String>,
    lookback: Option<String>,
    time: Option<String>,
    timeout: Option<String>,
    db: Option<String>,
}

/// Helper macro which try to evaluate the expression and return its results.
/// If the evaluation fails, return a `PrometheusJsonResponse` early.
macro_rules! try_call_return_response {
    (@output_msg $handle: expr, $status_code: expr) => {
        match $handle {
            Ok(res) => res,
            Err(err) => {
                let msg = err.output_msg();
                return PrometheusJsonResponse::error($status_code, msg);
            }
        }
    };
    ($handle: expr, $status_code: expr) => {
        match $handle {
            Ok(res) => res,
            Err(err) => {
                let msg = err.to_string();
                return PrometheusJsonResponse::error($status_code, msg);
            }
        }
    };
    ($handle: expr) => {
        match $handle {
            Ok(res) => res,
            Err(err) => {
                let status_code = err.status_code();
                let msg = err.to_string();
                return PrometheusJsonResponse::error(status_code, msg);
            }
        }
    };
}

#[axum_macros::debug_handler]
#[tracing::instrument(
    skip_all,
    fields(protocol = "prometheus", request_type = "instant_query")
)]
pub async fn instant_query(
    State(handler): State<PrometheusHandlerRef>,
    Query(params): Query<InstantQuery>,
    Extension(mut query_ctx): Extension<QueryContext>,
    Form(form_params): Form<InstantQuery>,
) -> PrometheusJsonResponse {
    // Extract time from query string, or use current server time if not specified.
    let time = params
        .time
        .or(form_params.time)
        .unwrap_or_else(current_time_rfc3339);
    let prom_query = PromQuery {
        query: params.query.or(form_params.query).unwrap_or_default(),
        start: time.clone(),
        end: time,
        step: "1s".to_string(),
        lookback: params
            .lookback
            .or(form_params.lookback)
            .unwrap_or_else(|| DEFAULT_LOOKBACK_STRING.to_string()),
        alias: None,
    };

    // update catalog and schema in query context if necessary
    if let Some(db) = &params.db {
        let (catalog, schema) = parse_catalog_and_schema_from_db_string(db);
        try_update_catalog_schema(&mut query_ctx, &catalog, &schema);
    }
    let query_ctx = Arc::new(query_ctx);
    let _timer = crate::metrics::METRIC_HTTP_PROMETHEUS_PROMQL_ELAPSED
        .with_label_values(&[query_ctx.get_db_string().as_str(), "instant_query"])
        .start_timer();
    let prom_query = try_call_return_response!(
        @output_msg ParsedPromQuery::parse(prom_query, &query_ctx),
        StatusCode::InvalidArguments
    );
    let promql_expr = prom_query.expr();

    let metric_name_discovery =
        try_call_return_response!(find_metric_name_not_equal_matchers(promql_expr));
    if let Some(discovery) = metric_name_discovery {
        debug!("Find metric name matchers: {:?}", discovery.name_matchers);

        try_call_return_response!(handler.check_query_permission(&[], &query_ctx).await);
        let (static_targets, unresolved_selectors) =
            try_call_return_response!(static_promql_targets(promql_expr, &query_ctx));
        try_call_return_response!(
            handler
                .check_query_target_permission(static_targets, &query_ctx,)
                .await
        );
        if unresolved_selectors != 1 {
            try_call_return_response!(
                handler
                    .check_query_target_permission(PermissionTableTargets::Unresolved, &query_ctx,)
                    .await
            );
            return do_instant_query(&handler, prom_query, query_ctx).await;
        }

        let schema = discovery
            .schema
            .unwrap_or_else(|| query_ctx.current_schema());
        let metric_names = try_call_return_response!(
            handler
                .query_metric_names(discovery.name_matchers, &schema, &query_ctx)
                .await
        );

        debug!("Find metric names: {:?}", metric_names);

        let prom_queries = expand_metric_name_queries(&prom_query, metric_names);
        try_call_return_response!(
            handler
                .check_query_permission_parsed(&prom_queries, &query_ctx)
                .await
        );

        if prom_queries.is_empty() {
            let result_type = promql_expr.value_type();

            return PrometheusJsonResponse::success(PrometheusResponse::PromData(PromData {
                result_type: result_type.to_string(),
                ..Default::default()
            }));
        }

        let responses = join_all(prom_queries.into_iter().map(|prom_query| {
            let query_ctx = query_ctx.clone();
            let handler = handler.clone();

            async move { do_instant_query(&handler, prom_query, query_ctx).await }
        }))
        .await;

        responses
            .into_iter()
            .reduce(|mut acc, resp| {
                acc.data.append(resp.data);
                acc
            })
            .unwrap()
    } else {
        do_instant_query(&handler, prom_query, query_ctx).await
    }
}

/// Executes a single instant query and returns response
async fn do_instant_query(
    handler: &PrometheusHandlerRef,
    prom_query: ParsedPromQuery,
    query_ctx: QueryContextRef,
) -> PrometheusJsonResponse {
    let (metric_name, result_type) = retrieve_metric_name_and_result_type(prom_query.expr());
    let result = handler.do_query_parsed(prom_query, query_ctx).await;
    PrometheusJsonResponse::from_query_result(result, metric_name, result_type).await
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct RangeQuery {
    query: Option<String>,
    start: Option<String>,
    end: Option<String>,
    step: Option<String>,
    lookback: Option<String>,
    timeout: Option<String>,
    db: Option<String>,
}

#[axum_macros::debug_handler]
#[tracing::instrument(
    skip_all,
    fields(protocol = "prometheus", request_type = "range_query")
)]
pub async fn range_query(
    State(handler): State<PrometheusHandlerRef>,
    Query(params): Query<RangeQuery>,
    Extension(mut query_ctx): Extension<QueryContext>,
    Form(form_params): Form<RangeQuery>,
) -> PrometheusJsonResponse {
    let prom_query = PromQuery {
        query: params.query.or(form_params.query).unwrap_or_default(),
        start: params.start.or(form_params.start).unwrap_or_default(),
        end: params.end.or(form_params.end).unwrap_or_default(),
        step: params.step.or(form_params.step).unwrap_or_default(),
        lookback: params
            .lookback
            .or(form_params.lookback)
            .unwrap_or_else(|| DEFAULT_LOOKBACK_STRING.to_string()),
        alias: None,
    };

    // update catalog and schema in query context if necessary
    if let Some(db) = &params.db {
        let (catalog, schema) = parse_catalog_and_schema_from_db_string(db);
        try_update_catalog_schema(&mut query_ctx, &catalog, &schema);
    }
    let query_ctx = Arc::new(query_ctx);
    let _timer = crate::metrics::METRIC_HTTP_PROMETHEUS_PROMQL_ELAPSED
        .with_label_values(&[query_ctx.get_db_string().as_str(), "range_query"])
        .start_timer();
    let prom_query = try_call_return_response!(
        @output_msg ParsedPromQuery::parse(prom_query, &query_ctx),
        StatusCode::InvalidArguments
    );
    let promql_expr = prom_query.expr();

    let metric_name_discovery =
        try_call_return_response!(find_metric_name_not_equal_matchers(promql_expr));
    if let Some(discovery) = metric_name_discovery {
        debug!("Find metric name matchers: {:?}", discovery.name_matchers);

        try_call_return_response!(handler.check_query_permission(&[], &query_ctx).await);
        let (static_targets, unresolved_selectors) =
            try_call_return_response!(static_promql_targets(promql_expr, &query_ctx));
        try_call_return_response!(
            handler
                .check_query_target_permission(static_targets, &query_ctx,)
                .await
        );
        if unresolved_selectors != 1 {
            try_call_return_response!(
                handler
                    .check_query_target_permission(PermissionTableTargets::Unresolved, &query_ctx,)
                    .await
            );
            return do_range_query(&handler, prom_query, query_ctx).await;
        }

        let schema = discovery
            .schema
            .unwrap_or_else(|| query_ctx.current_schema());
        let metric_names = try_call_return_response!(
            handler
                .query_metric_names(discovery.name_matchers, &schema, &query_ctx)
                .await
        );

        debug!("Find metric names: {:?}", metric_names);

        let prom_queries = expand_metric_name_queries(&prom_query, metric_names);
        try_call_return_response!(
            handler
                .check_query_permission_parsed(&prom_queries, &query_ctx)
                .await
        );

        if prom_queries.is_empty() {
            return PrometheusJsonResponse::success(PrometheusResponse::PromData(PromData {
                result_type: ValueType::Matrix.to_string(),
                ..Default::default()
            }));
        }

        let responses = join_all(prom_queries.into_iter().map(|prom_query| {
            let query_ctx = query_ctx.clone();
            let handler = handler.clone();

            async move { do_range_query(&handler, prom_query, query_ctx).await }
        }))
        .await;

        // Safety: at least one responses, checked above
        responses
            .into_iter()
            .reduce(|mut acc, resp| {
                acc.data.append(resp.data);
                acc
            })
            .unwrap()
    } else {
        do_range_query(&handler, prom_query, query_ctx).await
    }
}

/// Executes a single range query and returns response
async fn do_range_query(
    handler: &PrometheusHandlerRef,
    prom_query: ParsedPromQuery,
    query_ctx: QueryContextRef,
) -> PrometheusJsonResponse {
    let (metric_name, _) = retrieve_metric_name_and_result_type(prom_query.expr());
    let result = handler.do_query_parsed(prom_query, query_ctx).await;
    PrometheusJsonResponse::from_query_result(result, metric_name, ValueType::Matrix).await
}

#[derive(Debug, Default, Serialize)]
struct Matches(Vec<String>);

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct LabelsQuery {
    start: Option<String>,
    end: Option<String>,
    lookback: Option<String>,
    #[serde(flatten)]
    matches: Matches,
    db: Option<String>,
}

// Custom Deserialize method to support parsing repeated match[]
impl<'de> Deserialize<'de> for Matches {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Matches, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct MatchesVisitor;

        impl<'d> Visitor<'d> for MatchesVisitor {
            type Value = Vec<String>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a string")
            }

            fn visit_map<M>(self, mut access: M) -> std::result::Result<Self::Value, M::Error>
            where
                M: MapAccess<'d>,
            {
                let mut matches = Vec::new();
                while let Some((key, value)) = access.next_entry::<String, String>()? {
                    if key == "match[]" {
                        matches.push(value);
                    }
                }
                Ok(matches)
            }
        }
        Ok(Matches(deserializer.deserialize_map(MatchesVisitor)?))
    }
}

/// Handles schema errors, transforming a Result into an Option.
/// - If the input is `Ok(v)`, returns `Some(v)`
/// - If the input is `Err(err)` and the error status code is `TableNotFound` or
///   `TableColumnNotFound`, returns `None` (ignoring these specific errors)
/// - If the input is `Err(err)` with any other error code, directly returns a
///   `PrometheusJsonResponse::error`.
macro_rules! handle_schema_err {
    ($result:expr) => {
        match $result {
            Ok(v) => Some(v),
            Err(err) => {
                if err.status_code() == StatusCode::TableNotFound
                    || err.status_code() == StatusCode::TableColumnNotFound
                {
                    // Prometheus won't report error if querying nonexist label and metric
                    None
                } else {
                    return PrometheusJsonResponse::error(err.status_code(), err.output_msg());
                }
            }
        }
    };
}

#[axum_macros::debug_handler]
#[tracing::instrument(
    skip_all,
    fields(protocol = "prometheus", request_type = "labels_query")
)]
pub async fn labels_query(
    State(handler): State<PrometheusHandlerRef>,
    Query(params): Query<LabelsQuery>,
    Extension(mut query_ctx): Extension<QueryContext>,
    Form(form_params): Form<LabelsQuery>,
) -> PrometheusJsonResponse {
    let (catalog, schema) = get_catalog_schema(&params.db, &query_ctx);
    try_update_catalog_schema(&mut query_ctx, &catalog, &schema);
    let query_ctx = Arc::new(query_ctx);

    let mut queries = params.matches.0;
    if queries.is_empty() {
        queries = form_params.matches.0;
    }

    let _timer = crate::metrics::METRIC_HTTP_PROMETHEUS_PROMQL_ELAPSED
        .with_label_values(&[query_ctx.get_db_string().as_str(), "labels_query"])
        .start_timer();

    let prom_queries = if queries.is_empty() {
        try_call_return_response!(handler.check_query_permission(&[], &query_ctx).await);
        Vec::new()
    } else {
        let start = params
            .start
            .or(form_params.start)
            .unwrap_or_else(yesterday_rfc3339);
        let end = params
            .end
            .or(form_params.end)
            .unwrap_or_else(current_time_rfc3339);
        let lookback = params
            .lookback
            .or(form_params.lookback)
            .unwrap_or_else(|| DEFAULT_LOOKBACK_STRING.to_string());
        let prom_queries = queries
            .into_iter()
            .map(|query| PromQuery {
                query,
                start: start.clone(),
                end: end.clone(),
                step: DEFAULT_LOOKBACK_STRING.to_string(),
                lookback: lookback.clone(),
                alias: None,
            })
            .collect::<Vec<_>>();
        let prom_queries = try_call_return_response!(
            prom_queries
                .into_iter()
                .map(|query| ParsedPromQuery::parse(query, &query_ctx))
                .collect::<Result<Vec<_>>>()
        );
        try_call_return_response!(
            handler
                .check_query_permission_parsed(&prom_queries, &query_ctx)
                .await
        );
        prom_queries
    };

    // Fetch all columns if no query matcher is provided
    if prom_queries.is_empty() {
        let (mut labels, table_names) =
            match get_all_column_names(&catalog, &schema, &handler.catalog_manager()).await {
                Ok(result) => result,
                Err(e) => return PrometheusJsonResponse::error(e.status_code(), e.output_msg()),
            };
        try_call_return_response!(
            handler
                .check_query_target_permission(
                    current_schema_metric_targets(&query_ctx, &table_names),
                    &query_ctx,
                )
                .await
        );
        let _ = labels.insert(METRIC_NAME.to_string());
        let mut labels_vec = labels.into_iter().collect::<Vec<_>>();
        labels_vec.sort_unstable();
        return PrometheusJsonResponse::success(PrometheusResponse::Labels(labels_vec));
    }

    // Fetch tag columns only from the tables checked above.
    let mut labels = match resolved_promql_targets(&prom_queries, &query_ctx) {
        Some(targets) => {
            match get_target_column_names(&targets, &handler.catalog_manager(), &query_ctx).await {
                Ok(labels) => labels,
                Err(e) => return PrometheusJsonResponse::error(e.status_code(), e.output_msg()),
            }
        }
        None => match get_all_column_names(&catalog, &schema, &handler.catalog_manager()).await {
            Ok((labels, _)) => labels,
            Err(e) => return PrometheusJsonResponse::error(e.status_code(), e.output_msg()),
        },
    };
    let _ = labels.insert(METRIC_NAME.to_string());

    let mut fetched_labels = HashSet::new();
    let _ = fetched_labels.insert(METRIC_NAME.to_string());

    let mut merge_map = HashMap::new();
    for prom_query in prom_queries {
        let result = handler.do_query_parsed(prom_query, query_ctx.clone()).await;
        handle_schema_err!(
            retrieve_labels_name_from_query_result(result, &mut fetched_labels, &mut merge_map)
                .await
        );
    }

    // intersect `fetched_labels` with `labels` to filter out non-tag columns
    fetched_labels.retain(|l| labels.contains(l));
    let mut sorted_labels: Vec<String> = fetched_labels.into_iter().collect();
    sorted_labels.sort();
    let merge_map = merge_map
        .into_iter()
        .map(|(k, v)| (k, Value::from(v)))
        .collect();
    let mut resp = PrometheusJsonResponse::success(PrometheusResponse::Labels(sorted_labels));
    resp.resp_metrics = merge_map;
    resp
}

/// Get all tag column name of the given schema
async fn get_all_column_names(
    catalog: &str,
    schema: &str,
    manager: &CatalogManagerRef,
) -> std::result::Result<(HashSet<String>, Vec<String>), catalog::error::Error> {
    let mut labels = HashSet::new();
    let mut table_names = Vec::new();
    let mut tables = manager.tables(catalog, schema, None);
    while let Some(table) = tables.try_next().await? {
        if is_internal_physical_metric_table(&table) {
            continue;
        }
        table_names.push(table.table_info().name.clone());
        extend_tag_column_names(&mut labels, &table);
    }

    Ok((labels, table_names))
}

async fn get_target_column_names(
    targets: &[PermissionTableTarget],
    manager: &CatalogManagerRef,
    query_ctx: &QueryContext,
) -> std::result::Result<HashSet<String>, catalog::error::Error> {
    let mut labels = HashSet::new();
    for target in targets {
        if let Some(table) = manager
            .table(
                &target.catalog,
                &target.schema,
                &target.table,
                Some(query_ctx),
            )
            .await?
            && !is_internal_physical_metric_table(&table)
        {
            extend_tag_column_names(&mut labels, &table);
        }
    }
    Ok(labels)
}

fn extend_tag_column_names(labels: &mut HashSet<String>, table: &TableRef) {
    labels.extend(table.primary_key_columns().filter_map(|column| {
        (column.name != DATA_SCHEMA_TABLE_ID_COLUMN_NAME
            && column.name != DATA_SCHEMA_TSID_COLUMN_NAME)
            .then_some(column.name)
    }));
}

async fn retrieve_series_from_query_result(
    result: Result<Output>,
    series: &mut Vec<HashMap<Column, String>>,
    query_ctx: &QueryContext,
    target: &PermissionTableTarget,
    manager: &CatalogManagerRef,
    metrics: &mut HashMap<String, u64>,
) -> Result<()> {
    let result = result?;

    // fetch tag list
    let table = manager
        .table(
            &target.catalog,
            &target.schema,
            &target.table,
            Some(query_ctx),
        )
        .await?
        .with_context(|| TableNotFoundSnafu {
            catalog: &target.catalog,
            schema: &target.schema,
            table: &target.table,
        })?;
    let tag_columns = table
        .primary_key_columns()
        .map(|c| c.name)
        .collect::<HashSet<_>>();

    match result.data {
        OutputData::RecordBatches(batches) => {
            record_batches_to_series(batches, series, &target.table, &tag_columns)
        }
        OutputData::Stream(stream) => {
            let batches = RecordBatches::try_collect(stream)
                .await
                .context(CollectRecordbatchSnafu)?;
            record_batches_to_series(batches, series, &target.table, &tag_columns)
        }
        OutputData::AffectedRows(_) => Err(Error::UnexpectedResult {
            reason: "expected data result, but got affected rows".to_string(),
            location: Location::default(),
        }),
    }?;

    if let Some(ref plan) = result.meta.plan {
        collect_plan_metrics(plan, &mut [metrics]);
    }
    Ok(())
}

/// Retrieve labels name from query result
async fn retrieve_labels_name_from_query_result(
    result: Result<Output>,
    labels: &mut HashSet<String>,
    metrics: &mut HashMap<String, u64>,
) -> Result<()> {
    let result = result?;
    match result.data {
        OutputData::RecordBatches(batches) => record_batches_to_labels_name(batches, labels),
        OutputData::Stream(stream) => {
            let batches = RecordBatches::try_collect(stream)
                .await
                .context(CollectRecordbatchSnafu)?;
            record_batches_to_labels_name(batches, labels)
        }
        OutputData::AffectedRows(_) => UnexpectedResultSnafu {
            reason: "expected data result, but got affected rows".to_string(),
        }
        .fail(),
    }?;
    if let Some(ref plan) = result.meta.plan {
        collect_plan_metrics(plan, &mut [metrics]);
    }
    Ok(())
}

fn record_batches_to_series(
    batches: RecordBatches,
    series: &mut Vec<HashMap<Column, String>>,
    table_name: &str,
    tag_columns: &HashSet<String>,
) -> Result<()> {
    for batch in batches.iter() {
        // project record batch to only contains tag columns
        let projection = batch
            .schema
            .column_schemas()
            .iter()
            .enumerate()
            .filter_map(|(idx, col)| {
                if tag_columns.contains(&col.name) {
                    Some(idx)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let batch = batch
            .try_project(&projection)
            .context(CollectRecordbatchSnafu)?;

        let mut writer = RowWriter::new(&batch.schema, table_name);
        writer.write(batch, series)?;
    }
    Ok(())
}

/// Writer from a row in the record batch to a Prometheus time series:
///
/// `{__name__="<metric name>", <label name>="<label value>", ...}`
///
/// The metrics name is the table name; label names are the column names and
/// the label values are the corresponding row values (all are converted to strings).
struct RowWriter {
    /// The template that is to produce a Prometheus time series. It is pre-filled with metrics name
    /// and label names, waiting to be filled by row values afterward.
    template: HashMap<Column, Option<String>>,
    /// The current filling row.
    current: Option<HashMap<Column, Option<String>>>,
}

impl RowWriter {
    fn new(schema: &SchemaRef, table: &str) -> Self {
        let mut template = schema
            .column_schemas()
            .iter()
            .map(|x| (x.name.as_str().into(), None))
            .collect::<HashMap<Column, Option<String>>>();
        template.insert("__name__".into(), Some(table.to_string()));
        Self {
            template,
            current: None,
        }
    }

    fn insert(&mut self, column: ColumnRef, value: impl ToString) {
        let current = self.current.get_or_insert_with(|| self.template.clone());
        match current.get_mut(&column as &dyn AsColumnRef) {
            Some(x) => {
                let _ = x.insert(value.to_string());
            }
            None => {
                let _ = current.insert(column.0.into(), Some(value.to_string()));
            }
        }
    }

    fn insert_bytes(&mut self, column_schema: &ColumnSchema, bytes: &[u8]) -> Result<()> {
        let column_name = column_schema.name.as_str().into();

        if column_schema.data_type.is_json() {
            let s = jsonb_to_string(bytes).context(ConvertScalarValueSnafu)?;
            self.insert(column_name, s);
        } else {
            let hex = bytes
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<Vec<String>>()
                .join("");
            self.insert(column_name, hex);
        }
        Ok(())
    }

    fn finish(&mut self) -> HashMap<Column, String> {
        let Some(current) = self.current.take() else {
            return HashMap::new();
        };
        current
            .into_iter()
            .filter_map(|(k, v)| v.map(|v| (k, v)))
            .collect()
    }

    fn write(
        &mut self,
        record_batch: RecordBatch,
        series: &mut Vec<HashMap<Column, String>>,
    ) -> Result<()> {
        let schema = record_batch.schema.clone();
        let record_batch = record_batch.into_df_record_batch();
        for i in 0..record_batch.num_rows() {
            for (j, array) in record_batch.columns().iter().enumerate() {
                let column = schema.column_name_by_index(j).into();

                if array.is_null(i) {
                    self.insert(column, "Null");
                    continue;
                }

                match array.data_type() {
                    DataType::Null => {
                        self.insert(column, "Null");
                    }
                    DataType::Boolean => {
                        let array = array.as_boolean();
                        let v = array.value(i);
                        self.insert(column, v);
                    }
                    DataType::UInt8 => {
                        let array = array.as_primitive::<UInt8Type>();
                        let v = array.value(i);
                        self.insert(column, v);
                    }
                    DataType::UInt16 => {
                        let array = array.as_primitive::<UInt16Type>();
                        let v = array.value(i);
                        self.insert(column, v);
                    }
                    DataType::UInt32 => {
                        let array = array.as_primitive::<UInt32Type>();
                        let v = array.value(i);
                        self.insert(column, v);
                    }
                    DataType::UInt64 => {
                        let array = array.as_primitive::<UInt64Type>();
                        let v = array.value(i);
                        self.insert(column, v);
                    }
                    DataType::Int8 => {
                        let array = array.as_primitive::<Int8Type>();
                        let v = array.value(i);
                        self.insert(column, v);
                    }
                    DataType::Int16 => {
                        let array = array.as_primitive::<Int16Type>();
                        let v = array.value(i);
                        self.insert(column, v);
                    }
                    DataType::Int32 => {
                        let array = array.as_primitive::<Int32Type>();
                        let v = array.value(i);
                        self.insert(column, v);
                    }
                    DataType::Int64 => {
                        let array = array.as_primitive::<Int64Type>();
                        let v = array.value(i);
                        self.insert(column, v);
                    }
                    DataType::Float32 => {
                        let array = array.as_primitive::<Float32Type>();
                        let v = array.value(i);
                        self.insert(column, v);
                    }
                    DataType::Float64 => {
                        let array = array.as_primitive::<Float64Type>();
                        let v = array.value(i);
                        self.insert(column, v);
                    }
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                        let v = datatypes::arrow_array::string_array_value(array, i);
                        self.insert(column, v);
                    }
                    DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
                        let v = datatypes::arrow_array::binary_array_value(array, i);
                        let column_schema = &schema.column_schemas()[j];
                        self.insert_bytes(column_schema, v)?;
                    }
                    DataType::Date32 => {
                        let array = array.as_primitive::<Date32Type>();
                        let v = Date::new(array.value(i));
                        self.insert(column, v);
                    }
                    DataType::Date64 => {
                        let array = array.as_primitive::<Date64Type>();
                        // `Date64` values are milliseconds representation of `Date32` values,
                        // according to its specification. So we convert the `Date64` value here to
                        // the `Date32` value to process them unified.
                        let v = Date::new((array.value(i) / 86_400_000) as i32);
                        self.insert(column, v);
                    }
                    DataType::Timestamp(_, _) => {
                        let v = datatypes::arrow_array::timestamp_array_value(array, i);
                        self.insert(column, v.to_iso8601_string());
                    }
                    DataType::Time32(_) | DataType::Time64(_) => {
                        let v = datatypes::arrow_array::time_array_value(array, i);
                        self.insert(column, v.to_iso8601_string());
                    }
                    DataType::Interval(interval_unit) => match interval_unit {
                        IntervalUnit::YearMonth => {
                            let array = array.as_primitive::<IntervalYearMonthType>();
                            let v: IntervalYearMonth = array.value(i).into();
                            self.insert(column, v.to_iso8601_string());
                        }
                        IntervalUnit::DayTime => {
                            let array = array.as_primitive::<IntervalDayTimeType>();
                            let v: IntervalDayTime = array.value(i).into();
                            self.insert(column, v.to_iso8601_string());
                        }
                        IntervalUnit::MonthDayNano => {
                            let array = array.as_primitive::<IntervalMonthDayNanoType>();
                            let v: IntervalMonthDayNano = array.value(i).into();
                            self.insert(column, v.to_iso8601_string());
                        }
                    },
                    DataType::Duration(_) => {
                        let d = datatypes::arrow_array::duration_array_value(array, i);
                        self.insert(column, d);
                    }
                    DataType::List(_) => {
                        let v = ScalarValue::try_from_array(array, i).context(DataFusionSnafu)?;
                        self.insert(column, v);
                    }
                    DataType::Struct(_) => {
                        let v = ScalarValue::try_from_array(array, i).context(DataFusionSnafu)?;
                        self.insert(column, v);
                    }
                    DataType::Decimal128(precision, scale) => {
                        let array = array.as_primitive::<Decimal128Type>();
                        let v = Decimal128::new(array.value(i), *precision, *scale);
                        self.insert(column, v);
                    }
                    _ => {
                        return NotSupportedSnafu {
                            feat: format!("convert {} to http value", array.data_type()),
                        }
                        .fail();
                    }
                }
            }

            series.push(self.finish())
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct ColumnRef<'a>(&'a str);

impl<'a> From<&'a str> for ColumnRef<'a> {
    fn from(s: &'a str) -> Self {
        Self(s)
    }
}

trait AsColumnRef {
    fn as_ref(&self) -> ColumnRef<'_>;
}

impl AsColumnRef for Column {
    fn as_ref(&self) -> ColumnRef<'_> {
        self.0.as_str().into()
    }
}

impl AsColumnRef for ColumnRef<'_> {
    fn as_ref(&self) -> ColumnRef<'_> {
        *self
    }
}

impl<'a> PartialEq for dyn AsColumnRef + 'a {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl<'a> Eq for dyn AsColumnRef + 'a {}

impl<'a> Hash for dyn AsColumnRef + 'a {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_ref().0.hash(state);
    }
}

impl<'a> Borrow<dyn AsColumnRef + 'a> for Column {
    fn borrow(&self) -> &(dyn AsColumnRef + 'a) {
        self
    }
}

/// Retrieve labels name from record batches
fn record_batches_to_labels_name(
    batches: RecordBatches,
    labels: &mut HashSet<String>,
) -> Result<()> {
    let mut column_indices = Vec::new();
    let mut value_column_indices = Vec::new();
    for (i, column) in batches.schema().column_schemas().iter().enumerate() {
        if is_prometheus_value_column(&column.data_type) {
            value_column_indices.push(i);
        }
        column_indices.push(i);
    }

    if value_column_indices.is_empty() {
        return Err(Error::Internal {
            err_msg: "no value column found".to_string(),
        });
    }

    for batch in batches.iter() {
        let names = column_indices
            .iter()
            .map(|c| batches.schema().column_name_by_index(*c).to_string())
            .collect::<Vec<_>>();

        let value_columns = value_column_indices
            .iter()
            .map(|i| batch.column(*i))
            .collect::<Vec<_>>();

        for row_index in 0..batch.num_rows() {
            // if all value columns are null, skip this row
            if value_columns.iter().all(|c| c.is_null(row_index)) {
                continue;
            }

            // if a value is not null, record the tag name and return
            names.iter().for_each(|name| {
                let _ = labels.insert(name.clone());
            });
            return Ok(());
        }
    }
    Ok(())
}

fn is_prometheus_value_column(data_type: &ConcreteDataType) -> bool {
    matches!(data_type, ConcreteDataType::Float64(_)) || is_native_histogram_value_type(data_type)
}

pub(crate) fn retrieve_metric_name_and_result_type(
    promql_expr: &PromqlExpr,
) -> (Option<String>, ValueType) {
    (
        promql_expr_to_metric_name(promql_expr),
        promql_expr.value_type(),
    )
}

/// Tries to get catalog and schema from an optional db param. And retrieves
/// them from [QueryContext] if they don't present.
pub(crate) fn get_catalog_schema(db: &Option<String>, ctx: &QueryContext) -> (String, String) {
    if let Some(db) = db {
        parse_catalog_and_schema_from_db_string(db)
    } else {
        (
            ctx.current_catalog().to_string(),
            ctx.current_schema().clone(),
        )
    }
}

/// Update catalog and schema in [QueryContext] if necessary.
pub(crate) fn try_update_catalog_schema(ctx: &mut QueryContext, catalog: &str, schema: &str) {
    if ctx.current_catalog() != catalog || ctx.current_schema() != schema {
        ctx.set_current_catalog(catalog);
        ctx.set_current_schema(schema);
    }
}

fn current_schema_metric_targets(
    query_ctx: &QueryContext,
    metric_names: &[String],
) -> PermissionTableTargets {
    PermissionTableTargets::resolved(
        metric_names
            .iter()
            .map(|metric| {
                PermissionTableTarget::new(
                    query_ctx.current_catalog(),
                    query_ctx.current_schema(),
                    metric,
                )
            })
            .collect(),
    )
}

fn is_internal_physical_metric_table(table: &TableRef) -> bool {
    table.table_info().is_physical_table()
}

fn promql_expr_to_metric_name(expr: &PromqlExpr) -> Option<String> {
    let mut metric_names = HashSet::new();
    collect_metric_names(expr, &mut metric_names);

    // Return the metric name only if there's exactly one unique metric name
    if metric_names.len() == 1 {
        metric_names.into_iter().next()
    } else {
        None
    }
}

/// Recursively collect all metric names from a PromQL expression
fn collect_metric_names(expr: &PromqlExpr, metric_names: &mut HashSet<String>) {
    match expr {
        PromqlExpr::Aggregate(AggregateExpr { modifier, expr, .. }) => {
            match modifier {
                Some(LabelModifier::Include(labels))
                    if !labels.labels.contains(&METRIC_NAME.to_string()) =>
                {
                    metric_names.clear();
                    return;
                }
                Some(LabelModifier::Exclude(labels))
                    if labels.labels.contains(&METRIC_NAME.to_string()) =>
                {
                    metric_names.clear();
                    return;
                }
                _ => {}
            }
            collect_metric_names(expr, metric_names)
        }
        PromqlExpr::Unary(UnaryExpr { .. }) => metric_names.clear(),
        PromqlExpr::Binary(BinaryExpr { lhs, op, .. }) => {
            if matches!(
                op.id(),
                token::T_LAND // INTERSECT
                    | token::T_LOR // UNION
                    | token::T_LUNLESS // EXCEPT
            ) {
                collect_metric_names(lhs, metric_names)
            } else {
                metric_names.clear()
            }
        }
        PromqlExpr::Paren(ParenExpr { expr }) => collect_metric_names(expr, metric_names),
        PromqlExpr::Subquery(SubqueryExpr { expr, .. }) => collect_metric_names(expr, metric_names),
        PromqlExpr::VectorSelector(VectorSelector { name, matchers, .. }) => {
            if let Some(name) = name {
                metric_names.insert(name.clone());
            } else if let Some(matcher) = matchers.find_matchers(METRIC_NAME).into_iter().next() {
                metric_names.insert(matcher.value);
            }
        }
        PromqlExpr::MatrixSelector(MatrixSelector { vs, .. }) => {
            let VectorSelector { name, matchers, .. } = vs;
            if let Some(name) = name {
                metric_names.insert(name.clone());
            } else if let Some(matcher) = matchers.find_matchers(METRIC_NAME).into_iter().next() {
                metric_names.insert(matcher.value);
            }
        }
        PromqlExpr::Call(Call { args, .. }) => {
            args.args
                .iter()
                .for_each(|e| collect_metric_names(e, metric_names));
        }
        PromqlExpr::NumberLiteral(_) | PromqlExpr::StringLiteral(_) | PromqlExpr::Extension(_) => {}
    }
}

fn find_metric_name_and_matchers<E, F>(expr: &PromqlExpr, f: F) -> Option<E>
where
    F: Fn(&Option<String>, &Matchers) -> Option<E> + Clone,
{
    match expr {
        PromqlExpr::Aggregate(AggregateExpr { expr, .. }) => find_metric_name_and_matchers(expr, f),
        PromqlExpr::Unary(UnaryExpr { expr }) => find_metric_name_and_matchers(expr, f),
        PromqlExpr::Binary(BinaryExpr { lhs, rhs, .. }) => {
            find_metric_name_and_matchers(lhs, f.clone()).or(find_metric_name_and_matchers(rhs, f))
        }
        PromqlExpr::Paren(ParenExpr { expr }) => find_metric_name_and_matchers(expr, f),
        PromqlExpr::Subquery(SubqueryExpr { expr, .. }) => find_metric_name_and_matchers(expr, f),
        PromqlExpr::NumberLiteral(_) => None,
        PromqlExpr::StringLiteral(_) => None,
        PromqlExpr::Extension(_) => None,
        PromqlExpr::VectorSelector(VectorSelector { name, matchers, .. }) => f(name, matchers),
        PromqlExpr::MatrixSelector(MatrixSelector { vs, .. }) => {
            let VectorSelector { name, matchers, .. } = vs;

            f(name, matchers)
        }
        PromqlExpr::Call(Call { args, .. }) => args
            .args
            .iter()
            .find_map(|e| find_metric_name_and_matchers(e, f.clone())),
    }
}

struct MetricNameDiscovery {
    name_matchers: Vec<Matcher>,
    schema: Option<String>,
}

/// Finds non-equality `__name__` matchers and their unambiguous schema.
fn find_metric_name_not_equal_matchers(expr: &PromqlExpr) -> Result<Option<MetricNameDiscovery>> {
    if find_metric_name_and_matchers(expr, |name, matchers| {
        (name.is_none() && !matchers.or_matchers.is_empty()).then_some(())
    })
    .is_some()
    {
        return Ok(None);
    }

    find_metric_name_and_matchers(expr, |name, matchers| {
        // Has name, ignore the matchers
        if name.is_some() {
            return None;
        }

        let name_matchers = matchers.find_matchers(METRIC_NAME);
        if name_matchers.len() != 1 || name_matchers[0].op == MatchOp::Equal {
            return None;
        }

        Some(
            resolve_schema_from_matchers(&matchers.matchers).map(|schema| MetricNameDiscovery {
                name_matchers,
                schema,
            }),
        )
    })
    .transpose()
}

fn expand_metric_name_queries(
    query: &ParsedPromQuery,
    metric_names: Vec<String>,
) -> Vec<ParsedPromQuery> {
    metric_names
        .into_iter()
        .map(|metric| {
            let mut query = query.clone();
            query.update_expr(|expr| update_metric_name_matcher(expr, &metric));
            query
        })
        .collect()
}

fn static_promql_targets(
    expr: &PromqlExpr,
    query_ctx: &QueryContextRef,
) -> Result<(PermissionTableTargets, usize)> {
    let mut targets = Vec::new();
    let mut unresolved_selectors = 0;
    collect_static_promql_targets(expr, query_ctx, &mut targets, &mut unresolved_selectors)?;
    Ok((
        PermissionTableTargets::resolved(targets),
        unresolved_selectors,
    ))
}

fn resolved_promql_targets(
    queries: &[ParsedPromQuery],
    query_ctx: &QueryContextRef,
) -> Option<Vec<PermissionTableTarget>> {
    let mut targets = Vec::new();
    for query in queries {
        let (PermissionTableTargets::Resolved(query_targets), unresolved_selectors) =
            static_promql_targets(query.expr(), query_ctx).ok()?
        else {
            return None;
        };
        if unresolved_selectors != 0 {
            return None;
        }
        for target in query_targets {
            if !targets.contains(&target) {
                targets.push(target);
            }
        }
    }
    Some(targets)
}

fn collect_static_promql_targets(
    expr: &PromqlExpr,
    query_ctx: &QueryContextRef,
    targets: &mut Vec<PermissionTableTarget>,
    unresolved_selectors: &mut usize,
) -> Result<()> {
    match expr {
        PromqlExpr::Aggregate(AggregateExpr { expr, .. })
        | PromqlExpr::Unary(UnaryExpr { expr })
        | PromqlExpr::Paren(ParenExpr { expr })
        | PromqlExpr::Subquery(SubqueryExpr { expr, .. }) => {
            collect_static_promql_targets(expr, query_ctx, targets, unresolved_selectors)?
        }
        PromqlExpr::Binary(BinaryExpr { lhs, rhs, .. }) => {
            collect_static_promql_targets(lhs, query_ctx, targets, unresolved_selectors)?;
            collect_static_promql_targets(rhs, query_ctx, targets, unresolved_selectors)?;
        }
        PromqlExpr::VectorSelector(selector) => {
            collect_static_vector_target(selector, query_ctx, targets, unresolved_selectors)?
        }
        PromqlExpr::MatrixSelector(MatrixSelector { vs, .. }) => {
            collect_static_vector_target(vs, query_ctx, targets, unresolved_selectors)?
        }
        PromqlExpr::Call(Call { args, .. }) => {
            for expr in &args.args {
                collect_static_promql_targets(expr, query_ctx, targets, unresolved_selectors)?;
            }
        }
        PromqlExpr::NumberLiteral(_) | PromqlExpr::StringLiteral(_) | PromqlExpr::Extension(_) => {}
    }
    Ok(())
}

fn collect_static_vector_target(
    selector: &VectorSelector,
    query_ctx: &QueryContextRef,
    targets: &mut Vec<PermissionTableTarget>,
    unresolved_selectors: &mut usize,
) -> Result<()> {
    if selector.name.is_none() && !selector.matchers.or_matchers.is_empty() {
        *unresolved_selectors += 1;
        return Ok(());
    }

    let metric = selector.name.clone().or_else(|| {
        let mut matchers = selector.matchers.find_matchers(METRIC_NAME);
        if matchers.len() == 1 && matchers[0].op == MatchOp::Equal {
            matchers.pop().map(|matcher| matcher.value)
        } else {
            None
        }
    });
    let Some(metric) = metric else {
        *unresolved_selectors += 1;
        return Ok(());
    };

    let schema = resolve_schema_from_matchers(&selector.matchers.matchers)?
        .unwrap_or_else(|| query_ctx.current_schema());
    targets.push(PermissionTableTarget::new(
        query_ctx.current_catalog(),
        schema,
        metric,
    ));
    Ok(())
}

/// Update the `__name__` matchers in expression into special value
/// Returns the updated expression.
fn update_metric_name_matcher(expr: &mut PromqlExpr, metric_name: &str) {
    match expr {
        PromqlExpr::Aggregate(AggregateExpr { expr, .. }) => {
            update_metric_name_matcher(expr, metric_name)
        }
        PromqlExpr::Unary(UnaryExpr { expr }) => update_metric_name_matcher(expr, metric_name),
        PromqlExpr::Binary(BinaryExpr { lhs, rhs, .. }) => {
            update_metric_name_matcher(lhs, metric_name);
            update_metric_name_matcher(rhs, metric_name);
        }
        PromqlExpr::Paren(ParenExpr { expr }) => update_metric_name_matcher(expr, metric_name),
        PromqlExpr::Subquery(SubqueryExpr { expr, .. }) => {
            update_metric_name_matcher(expr, metric_name)
        }
        PromqlExpr::VectorSelector(VectorSelector { name, matchers, .. }) => {
            if name.is_some() {
                return;
            }

            for m in &mut matchers.matchers {
                if m.name == METRIC_NAME && m.op != MatchOp::Equal {
                    m.op = MatchOp::Equal;
                    m.value = metric_name.to_string();
                }
            }
        }
        PromqlExpr::MatrixSelector(MatrixSelector { vs, .. }) => {
            let VectorSelector { name, matchers, .. } = vs;
            if name.is_some() {
                return;
            }

            for m in &mut matchers.matchers {
                if m.name == METRIC_NAME && m.op != MatchOp::Equal {
                    m.op = MatchOp::Equal;
                    m.value = metric_name.to_string();
                }
            }
        }
        PromqlExpr::Call(Call { args, .. }) => {
            args.args.iter_mut().for_each(|e| {
                update_metric_name_matcher(e, metric_name);
            });
        }
        PromqlExpr::NumberLiteral(_) | PromqlExpr::StringLiteral(_) | PromqlExpr::Extension(_) => {}
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct LabelValueQuery {
    start: Option<String>,
    end: Option<String>,
    lookback: Option<String>,
    #[serde(flatten)]
    matches: Matches,
    db: Option<String>,
    limit: Option<usize>,
}

#[axum_macros::debug_handler]
#[tracing::instrument(
    skip_all,
    fields(protocol = "prometheus", request_type = "label_values_query")
)]
pub async fn label_values_query(
    State(handler): State<PrometheusHandlerRef>,
    Path(label_name): Path<String>,
    Extension(mut query_ctx): Extension<QueryContext>,
    Query(params): Query<LabelValueQuery>,
) -> PrometheusJsonResponse {
    let (catalog, schema) = get_catalog_schema(&params.db, &query_ctx);
    try_update_catalog_schema(&mut query_ctx, &catalog, &schema);
    let query_ctx = Arc::new(query_ctx);

    let _timer = crate::metrics::METRIC_HTTP_PROMETHEUS_PROMQL_ELAPSED
        .with_label_values(&[query_ctx.get_db_string().as_str(), "label_values_query"])
        .start_timer();

    let matches = params.matches.0;
    if label_name == METRIC_NAME_LABEL {
        try_call_return_response!(handler.check_query_permission(&[], &query_ctx).await);
        let exact_metric_names = matches
            .iter()
            .filter_map(|selector| retrieve_exact_metric_name_from_promql(selector))
            .collect::<Vec<_>>();
        try_call_return_response!(
            handler
                .check_query_target_permission(
                    current_schema_metric_targets(&query_ctx, &exact_metric_names),
                    &query_ctx,
                )
                .await
        );
        let catalog_manager = handler.catalog_manager();

        let mut table_names = try_call_return_response!(
            retrieve_table_names(&query_ctx, catalog_manager, matches).await
        );
        table_names = try_call_return_response!(
            handler
                .filter_metadata_metric_names(
                    table_names,
                    query_ctx.current_schema().as_str(),
                    &query_ctx,
                )
                .await
        );

        truncate_results(&mut table_names, params.limit);
        return PrometheusJsonResponse::success(PrometheusResponse::LabelValues(table_names));
    } else if label_name == FIELD_NAME_LABEL {
        try_call_return_response!(handler.check_query_permission(&[], &query_ctx).await);
        let enumerate_all = matches.is_empty();
        let metric_names = matches
            .iter()
            .map(|selector| retrieve_exact_metric_name_from_promql(selector))
            .collect::<Vec<_>>();
        if metric_names.iter().any(Option::is_none) {
            try_call_return_response!(
                handler
                    .check_query_target_permission(PermissionTableTargets::Unresolved, &query_ctx,)
                    .await
            );
        }
        let metric_names = metric_names.into_iter().flatten().collect::<Vec<_>>();

        try_call_return_response!(
            handler
                .check_query_target_permission(
                    current_schema_metric_targets(&query_ctx, &metric_names),
                    &query_ctx,
                )
                .await
        );

        let (field_columns, table_names) = if !enumerate_all && metric_names.is_empty() {
            (HashSet::new(), Vec::new())
        } else {
            handle_schema_err!(
                retrieve_field_names(&query_ctx, handler.catalog_manager(), metric_names).await
            )
            .unwrap_or_default()
        };
        try_call_return_response!(
            handler
                .check_query_target_permission(
                    current_schema_metric_targets(&query_ctx, &table_names),
                    &query_ctx,
                )
                .await
        );
        let mut field_columns = field_columns.into_iter().collect::<Vec<_>>();
        field_columns.sort_unstable();
        truncate_results(&mut field_columns, params.limit);
        return PrometheusJsonResponse::success(PrometheusResponse::LabelValues(field_columns));
    } else if is_database_selection_label(&label_name) {
        try_call_return_response!(handler.check_query_permission(&[], &query_ctx).await);
        let catalog_manager = handler.catalog_manager();

        let (mut schema_names, targets) = try_call_return_response!(
            retrieve_schema_names(&query_ctx, catalog_manager, matches).await
        );
        try_call_return_response!(
            handler
                .check_query_target_permission(targets, &query_ctx)
                .await
        );
        truncate_results(&mut schema_names, params.limit);
        return PrometheusJsonResponse::success(PrometheusResponse::LabelValues(schema_names));
    }

    let queries = matches;
    if queries.is_empty() {
        return PrometheusJsonResponse::error(
            StatusCode::InvalidArguments,
            "match[] parameter is required",
        );
    }

    let start = params.start.unwrap_or_else(yesterday_rfc3339);
    let end = params.end.unwrap_or_else(current_time_rfc3339);
    let lookback = params
        .lookback
        .unwrap_or_else(|| DEFAULT_LOOKBACK_STRING.to_string());
    let prom_queries = queries
        .into_iter()
        .map(|query| PromQuery {
            query,
            start: start.clone(),
            end: end.clone(),
            step: DEFAULT_LOOKBACK_STRING.to_string(),
            lookback: lookback.clone(),
            alias: None,
        })
        .collect::<Vec<_>>();
    let prom_queries = try_call_return_response!(
        prom_queries
            .into_iter()
            .map(|query| ParsedPromQuery::parse(query, &query_ctx))
            .collect::<Result<Vec<_>>>()
    );
    try_call_return_response!(
        handler
            .check_query_permission_parsed(&prom_queries, &query_ctx)
            .await
    );

    let mut label_values = HashSet::new();

    let Some(first_query) = prom_queries.first() else {
        unreachable!("empty match[] is rejected above")
    };
    let QueryStatement::Promql(eval_stmt, _) = first_query.statement() else {
        unreachable!("query is parsed from PromQL")
    };
    let start = eval_stmt.start;
    let end = eval_stmt.end;

    for prom_query in prom_queries {
        let (_, statement) = prom_query.into_parts();
        let QueryStatement::Promql(eval_stmt, _) = statement else {
            unreachable!("query is parsed from PromQL")
        };
        let PromqlExpr::VectorSelector(mut vector_selector) = eval_stmt.expr else {
            return PrometheusJsonResponse::error(
                StatusCode::InvalidArguments,
                "expected vector selector",
            );
        };
        let Some(name) = take_metric_name(&mut vector_selector) else {
            return PrometheusJsonResponse::error(
                StatusCode::InvalidArguments,
                "expected metric name",
            );
        };
        let VectorSelector { matchers, .. } = vector_selector;
        // Only use and filter matchers.
        let matchers = matchers.matchers;
        let result = handler
            .query_label_values(name, label_name.clone(), matchers, start, end, &query_ctx)
            .await;
        if let Some(result) = handle_schema_err!(result) {
            label_values.extend(result.into_iter());
        }
    }

    let mut label_values: Vec<_> = label_values.into_iter().collect();
    label_values.sort_unstable();
    truncate_results(&mut label_values, params.limit);

    PrometheusJsonResponse::success(PrometheusResponse::LabelValues(label_values))
}

fn truncate_results(label_values: &mut Vec<String>, limit: Option<usize>) {
    if let Some(limit) = limit
        && limit > 0
        && label_values.len() >= limit
    {
        label_values.truncate(limit);
    }
}

/// Take metric name from the [VectorSelector].
/// It takes the name in the selector or removes the name matcher.
fn take_metric_name(selector: &mut VectorSelector) -> Option<String> {
    if let Some(name) = selector.name.take() {
        return Some(name);
    }

    let (pos, matcher) = selector
        .matchers
        .matchers
        .iter()
        .find_position(|matcher| matcher.name == "__name__" && matcher.op == MatchOp::Equal)?;
    let name = matcher.value.clone();
    // We need to remove the name matcher to avoid using it as a filter in query.
    selector.matchers.matchers.remove(pos);

    Some(name)
}

async fn retrieve_table_names(
    query_ctx: &QueryContext,
    catalog_manager: CatalogManagerRef,
    matches: Vec<String>,
) -> Result<Vec<String>> {
    let catalog = query_ctx.current_catalog();
    let schema = query_ctx.current_schema();

    let mut tables_stream = catalog_manager.tables(catalog, &schema, Some(query_ctx));
    let mut table_names = Vec::new();

    let name_matchers = matches
        .iter()
        .map(|selector| {
            let expr = promql_parser::parser::parse(selector)
                .map_err(|reason| InvalidQuerySnafu { reason }.build())?;
            let PromqlExpr::VectorSelector(selector) = expr else {
                return InvalidQuerySnafu {
                    reason: "expected vector selector".to_string(),
                }
                .fail();
            };
            if !selector.matchers.or_matchers.is_empty() {
                return Ok(None);
            }
            if let Some(name) = selector.name {
                return Ok(Some(Matcher::new(MatchOp::Equal, METRIC_NAME_LABEL, &name)));
            }
            Ok(selector
                .matchers
                .matchers
                .into_iter()
                .find(|matcher| matcher.name == METRIC_NAME_LABEL))
        })
        .collect::<Result<Vec<_>>>()?;

    while let Some(table) = tables_stream.next().await {
        let table = table?;
        if !table
            .table_info()
            .meta
            .options
            .extra_options
            .contains_key(LOGICAL_TABLE_METADATA_KEY)
            || is_internal_physical_metric_table(&table)
        {
            // skip non-prometheus (non-metricengine) tables for __name__ query
            continue;
        }

        let table_name = &table.table_info().name;

        if name_matchers.is_empty()
            || name_matchers.iter().any(|matcher| match matcher {
                None => true,
                Some(matcher) => match &matcher.op {
                    MatchOp::Equal => table_name == &matcher.value,
                    MatchOp::Re(reg) => reg.is_match(table_name),
                    // != and !~ are not supported, so include every table and
                    // let the caller authorize the complete candidate set.
                    _ => true,
                },
            })
        {
            table_names.push(table_name.clone());
        }
    }

    table_names.sort_unstable();
    Ok(table_names)
}

async fn retrieve_metric_metadata(
    query_ctx: &QueryContext,
    manager: CatalogManagerRef,
    params: &MetadataQuery,
) -> Result<BTreeMap<String, Vec<PromMetadata>>> {
    let mut metadata = BTreeMap::new();
    if params.limit == Some(0) {
        return Ok(metadata);
    }

    let catalog = query_ctx.current_catalog();
    let schema = query_ctx.current_schema();
    let mut tables_stream = manager.tables(catalog, &schema, Some(query_ctx));

    while let Some(table) = tables_stream.next().await {
        let table = table?;
        let table_info = table.table_info();
        if !is_prometheus_metric_table(table_info.as_ref()) {
            continue;
        }

        if let Some(metric) = &params.metric
            && table_info.name.as_str() != metric
        {
            continue;
        }

        metadata.insert(
            table_info.name.clone(),
            vec![prometheus_metadata_from_table(table_info.as_ref())],
        );
        if let Some(limit) = params.limit
            && metadata.len() >= limit
        {
            break;
        }
    }

    Ok(metadata)
}

fn is_prometheus_metric_table(table_info: &TableInfo) -> bool {
    table_info
        .meta
        .options
        .extra_options
        .contains_key(LOGICAL_TABLE_METADATA_KEY)
}

fn prometheus_metadata_from_table(table_info: &TableInfo) -> PromMetadata {
    let options = &table_info.meta.options.extra_options;
    let metric_type = options
        .get(SEMANTIC_METRIC_TYPE)
        .cloned()
        .or_else(|| table_has_native_histogram_value(table_info).then_some("histogram".to_string()))
        .unwrap_or_default();
    let unit = options
        .get(SEMANTIC_METRIC_UNIT)
        .cloned()
        .unwrap_or_default();

    PromMetadata {
        metric_type,
        unit,
        help: String::new(),
    }
}

fn table_has_native_histogram_value(table_info: &TableInfo) -> bool {
    table_info
        .meta
        .schema
        .column_schemas()
        .iter()
        .any(|column| is_native_histogram_value_type(&column.data_type))
}

async fn retrieve_field_names(
    query_ctx: &QueryContext,
    manager: CatalogManagerRef,
    matches: Vec<String>,
) -> Result<(HashSet<String>, Vec<String>)> {
    let mut field_columns = HashSet::new();
    let mut table_names = Vec::new();
    let catalog = query_ctx.current_catalog();
    let schema = query_ctx.current_schema();

    if matches.is_empty() {
        // query all tables if no matcher is provided
        let mut tables = manager.tables(catalog, &schema, Some(query_ctx));
        while let Some(table) = tables.next().await {
            let table = table?;
            if is_internal_physical_metric_table(&table) {
                continue;
            }
            table_names.push(table.table_info().name.clone());
            for column in table.field_columns() {
                field_columns.insert(column.name);
            }
        }
        return Ok((field_columns, table_names));
    }

    for table_name in matches {
        let table = manager
            .table(catalog, &schema, &table_name, Some(query_ctx))
            .await?
            .with_context(|| TableNotFoundSnafu {
                catalog: catalog.to_string(),
                schema: schema.clone(),
                table: table_name.clone(),
            })?;

        if is_internal_physical_metric_table(&table) {
            continue;
        }
        table_names.push(table.table_info().name.clone());
        for column in table.field_columns() {
            field_columns.insert(column.name);
        }
    }
    Ok((field_columns, table_names))
}

async fn retrieve_schema_names(
    query_ctx: &QueryContext,
    catalog_manager: CatalogManagerRef,
    matches: Vec<String>,
) -> Result<(Vec<String>, PermissionTableTargets)> {
    let mut schemas = Vec::new();
    let mut targets = Vec::new();
    let catalog = query_ctx.current_catalog();
    let metric_names = matches
        .iter()
        .map(|match_item| retrieve_exact_metric_name_from_promql(match_item))
        .collect::<Vec<_>>();
    let unresolved = metric_names.is_empty() || metric_names.iter().any(Option::is_none);

    let candidate_schemas = catalog_manager
        .schema_names(catalog, Some(query_ctx))
        .await?;

    for schema in candidate_schemas {
        let mut found = true;
        for table_name in metric_names.iter().flatten() {
            let table = catalog_manager
                .table(catalog, &schema, table_name, Some(query_ctx))
                .await?;
            let Some(table) = table else {
                found = false;
                continue;
            };
            targets.push(PermissionTableTarget::new(catalog, &schema, table_name));
            if is_internal_physical_metric_table(&table) {
                found = false;
            }
        }

        if found {
            schemas.push(schema);
        }
    }

    schemas.sort_unstable();

    let targets = if unresolved {
        PermissionTableTargets::Unresolved
    } else {
        PermissionTableTargets::resolved(targets)
    };
    Ok((schemas, targets))
}

fn retrieve_exact_metric_name_from_promql(query: &str) -> Option<String> {
    let PromqlExpr::VectorSelector(selector) = promql_parser::parser::parse(query).ok()? else {
        return None;
    };
    if let Some(name) = selector.name {
        return (!name.is_empty()).then_some(name);
    }

    let mut name_matchers = selector.matchers.find_matchers(METRIC_NAME);
    if name_matchers.len() != 1 {
        return None;
    }
    let matcher = name_matchers.pop()?;
    (matcher.op == MatchOp::Equal && !matcher.value.is_empty()).then_some(matcher.value)
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SeriesQuery {
    start: Option<String>,
    end: Option<String>,
    lookback: Option<String>,
    #[serde(flatten)]
    matches: Matches,
    db: Option<String>,
}

#[axum_macros::debug_handler]
#[tracing::instrument(
    skip_all,
    fields(protocol = "prometheus", request_type = "series_query")
)]
pub async fn series_query(
    State(handler): State<PrometheusHandlerRef>,
    Query(params): Query<SeriesQuery>,
    Extension(mut query_ctx): Extension<QueryContext>,
    Form(form_params): Form<SeriesQuery>,
) -> PrometheusJsonResponse {
    let mut queries: Vec<String> = params.matches.0;
    if queries.is_empty() {
        queries = form_params.matches.0;
    }
    if queries.is_empty() {
        return PrometheusJsonResponse::error(
            StatusCode::Unsupported,
            "match[] parameter is required",
        );
    }
    let start = params
        .start
        .or(form_params.start)
        .unwrap_or_else(yesterday_rfc3339);
    let end = params
        .end
        .or(form_params.end)
        .unwrap_or_else(current_time_rfc3339);
    let lookback = params
        .lookback
        .or(form_params.lookback)
        .unwrap_or_else(|| DEFAULT_LOOKBACK_STRING.to_string());

    // update catalog and schema in query context if necessary
    if let Some(db) = &params.db {
        let (catalog, schema) = parse_catalog_and_schema_from_db_string(db);
        try_update_catalog_schema(&mut query_ctx, &catalog, &schema);
    }
    let query_ctx = Arc::new(query_ctx);

    let _timer = crate::metrics::METRIC_HTTP_PROMETHEUS_PROMQL_ELAPSED
        .with_label_values(&[query_ctx.get_db_string().as_str(), "series_query"])
        .start_timer();

    let prom_queries = queries
        .into_iter()
        .map(|query| PromQuery {
            query,
            start: start.clone(),
            end: end.clone(),
            // TODO: find a better value for step
            step: DEFAULT_LOOKBACK_STRING.to_string(),
            lookback: lookback.clone(),
            alias: None,
        })
        .collect::<Vec<_>>();
    let mut expanded_queries = Vec::new();
    for prom_query in prom_queries {
        let prom_query = try_call_return_response!(
            @output_msg ParsedPromQuery::parse(prom_query, &query_ctx),
            StatusCode::InvalidArguments
        );
        let promql_expr = prom_query.expr();
        let Some(discovery) =
            try_call_return_response!(find_metric_name_not_equal_matchers(promql_expr))
        else {
            expanded_queries.push(prom_query);
            continue;
        };

        try_call_return_response!(handler.check_query_permission(&[], &query_ctx).await);
        let (static_targets, unresolved_selectors) =
            try_call_return_response!(static_promql_targets(promql_expr, &query_ctx));
        try_call_return_response!(
            handler
                .check_query_target_permission(static_targets, &query_ctx)
                .await
        );
        if unresolved_selectors != 1 {
            try_call_return_response!(
                handler
                    .check_query_target_permission(PermissionTableTargets::Unresolved, &query_ctx)
                    .await
            );
            expanded_queries.push(prom_query);
            continue;
        }

        let schema = discovery
            .schema
            .unwrap_or_else(|| query_ctx.current_schema());
        let metric_names = try_call_return_response!(
            handler
                .query_metric_names(discovery.name_matchers, &schema, &query_ctx)
                .await
        );
        expanded_queries.extend(expand_metric_name_queries(&prom_query, metric_names));
    }
    let prom_queries = expanded_queries;
    try_call_return_response!(
        handler
            .check_query_permission_parsed(&prom_queries, &query_ctx)
            .await
    );

    let mut series = Vec::new();
    let mut merge_map = HashMap::new();
    for prom_query in prom_queries {
        let Some(target) = resolved_promql_targets(std::slice::from_ref(&prom_query), &query_ctx)
            .and_then(|targets| match targets.as_slice() {
                [target] => Some(target.clone()),
                _ => None,
            })
        else {
            return PrometheusJsonResponse::error(
                StatusCode::InvalidArguments,
                "series selector must resolve exactly one metric table",
            );
        };
        let result = handler.do_query_parsed(prom_query, query_ctx.clone()).await;

        handle_schema_err!(
            retrieve_series_from_query_result(
                result,
                &mut series,
                &query_ctx,
                &target,
                &handler.catalog_manager(),
                &mut merge_map,
            )
            .await
        );
    }
    let merge_map = merge_map
        .into_iter()
        .map(|(k, v)| (k, Value::from(v)))
        .collect();
    let mut resp = PrometheusJsonResponse::success(PrometheusResponse::Series(series));
    resp.resp_metrics = merge_map;
    resp
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ParseQuery {
    query: Option<String>,
    db: Option<String>,
}

#[axum_macros::debug_handler]
#[tracing::instrument(
    skip_all,
    fields(protocol = "prometheus", request_type = "parse_query")
)]
pub async fn parse_query(
    State(_handler): State<PrometheusHandlerRef>,
    Query(params): Query<ParseQuery>,
    Extension(_query_ctx): Extension<QueryContext>,
    Form(form_params): Form<ParseQuery>,
) -> PrometheusJsonResponse {
    if let Some(query) = params.query.or(form_params.query) {
        let ast = try_call_return_response!(
            promql_parser::parser::parse(&query),
            StatusCode::InvalidArguments
        );
        PrometheusJsonResponse::success(PrometheusResponse::ParseResult(ast))
    } else {
        PrometheusJsonResponse::error(StatusCode::InvalidArguments, "query is required")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};

    use catalog::memory::MemoryCatalogManager;
    use catalog::{RegisterSchemaRequest, RegisterTableRequest};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_query::native_histogram::native_histogram_value_type;
    use common_query::prelude::greptime_native_histogram;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use promql_parser::parser::value::ValueType;
    use table::metadata::{TableInfoBuilder, TableMetaBuilder, TableType, TableVersion};
    use table::requests::{SEMANTIC_METRIC_TYPE, SEMANTIC_METRIC_UNIT, TableOptions};
    use table::test_util::EmptyTable;
    use table::test_util::table_info::test_table_info;

    use super::*;
    use crate::prometheus_handler::PrometheusHandler;

    struct TestCase {
        name: &'static str,
        promql: &'static str,
        expected_metric: Option<&'static str>,
        expected_type: ValueType,
        should_error: bool,
    }

    struct TestPrometheusHandler {
        catalog_manager: CatalogManagerRef,
        denied_table: Option<&'static str>,
        metric_names: Vec<String>,
        queries: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl PrometheusHandler for TestPrometheusHandler {
        async fn do_query(&self, _: &PromQuery, _: QueryContextRef) -> Result<Output> {
            unreachable!("HTTP handlers should execute the parsed query")
        }

        async fn do_query_parsed(
            &self,
            query: ParsedPromQuery,
            _: QueryContextRef,
        ) -> Result<Output> {
            self.queries
                .lock()
                .unwrap()
                .push(query.query().query.clone());
            Ok(Output::new_with_record_batches(RecordBatches::empty()))
        }

        async fn check_query_permission(&self, _: &[PromQuery], _: &QueryContextRef) -> Result<()> {
            Ok(())
        }

        async fn check_query_target_permission(
            &self,
            targets: PermissionTableTargets,
            _: &QueryContextRef,
        ) -> Result<()> {
            if matches!(
                targets,
                PermissionTableTargets::Resolved(targets)
                    if self.denied_table.is_some_and(|denied| {
                        targets.iter().any(|target| target.table == denied)
                    })
            ) {
                return auth::error::PermissionDeniedSnafu
                    .fail()
                    .context(crate::error::AuthSnafu);
            }
            Ok(())
        }

        async fn filter_metadata_metric_names(
            &self,
            metric_names: Vec<String>,
            _: &str,
            _: &QueryContextRef,
        ) -> Result<Vec<String>> {
            Ok(metric_names
                .into_iter()
                .filter(|metric| metric != "denied")
                .collect())
        }

        async fn query_metric_names(
            &self,
            _: Vec<Matcher>,
            _: &str,
            _: &QueryContextRef,
        ) -> Result<Vec<String>> {
            Ok(self.metric_names.clone())
        }

        async fn query_label_values(
            &self,
            _: String,
            _: String,
            _: Vec<Matcher>,
            _: std::time::SystemTime,
            _: std::time::SystemTime,
            _: &QueryContextRef,
        ) -> Result<Vec<String>> {
            unreachable!()
        }

        fn catalog_manager(&self) -> CatalogManagerRef {
            self.catalog_manager.clone()
        }
    }

    fn permission_denied_response() -> PrometheusJsonResponse {
        let result: auth::error::Result<()> = auth::error::PermissionDeniedSnafu.fail();
        try_call_return_response!(result);
        unreachable!()
    }

    fn invalid_promql_response() -> PrometheusJsonResponse {
        let result = ParsedPromQuery::parse(
            PromQuery {
                query: "up{".to_string(),
                ..Default::default()
            },
            &QueryContext::arc(),
        );
        try_call_return_response!(@output_msg result, StatusCode::InvalidArguments);
        unreachable!()
    }

    #[test]
    fn test_try_call_return_response_preserves_status() {
        use axum::response::IntoResponse;

        let response = permission_denied_response();
        assert_eq!(Some(StatusCode::PermissionDenied), response.status_code);
        assert_eq!(
            axum::http::StatusCode::FORBIDDEN,
            response.into_response().status()
        );
    }

    #[test]
    fn test_try_call_return_response_preserves_error_source() {
        let response = invalid_promql_response();
        assert_eq!(Some(StatusCode::InvalidArguments), response.status_code);
        let error = response.error.unwrap();
        assert!(
            error.contains("unexpected end of input inside braces"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn test_promql_timer_records_parse_errors() {
        let handler: PrometheusHandlerRef = Arc::new(TestPrometheusHandler {
            catalog_manager: MemoryCatalogManager::new(),
            denied_table: None,
            metric_names: Vec::new(),
            queries: Mutex::new(Vec::new()),
        });
        let query_ctx = QueryContext::with("promql_timer_test", "parse_error");
        let db = query_ctx.get_db_string();

        let instant_histogram = crate::metrics::METRIC_HTTP_PROMETHEUS_PROMQL_ELAPSED
            .with_label_values(&[db.as_str(), "instant_query"]);
        let instant_count = instant_histogram.get_sample_count();
        let response = instant_query(
            State(handler.clone()),
            Query(InstantQuery {
                query: Some("up{".to_string()),
                ..Default::default()
            }),
            Extension(query_ctx),
            Form(InstantQuery::default()),
        )
        .await;
        assert_eq!(Some(StatusCode::InvalidArguments), response.status_code);
        assert_eq!(instant_count + 1, instant_histogram.get_sample_count());

        let range_histogram = crate::metrics::METRIC_HTTP_PROMETHEUS_PROMQL_ELAPSED
            .with_label_values(&[db.as_str(), "range_query"]);
        let range_count = range_histogram.get_sample_count();
        let response = range_query(
            State(handler),
            Query(RangeQuery {
                query: Some("up{".to_string()),
                start: Some("0".to_string()),
                end: Some("1".to_string()),
                step: Some("1s".to_string()),
                ..Default::default()
            }),
            Extension(QueryContext::with("promql_timer_test", "parse_error")),
            Form(RangeQuery::default()),
        )
        .await;
        assert_eq!(Some(StatusCode::InvalidArguments), response.status_code);
        assert_eq!(range_count + 1, range_histogram.get_sample_count());
    }

    #[tokio::test]
    async fn test_field_name_enumeration_fails_closed() {
        use axum::response::IntoResponse;

        let mut allowed = test_table_info(
            1024,
            "allowed",
            DEFAULT_SCHEMA_NAME,
            DEFAULT_CATALOG_NAME,
            Arc::new(Schema::new(vec![])),
        );
        allowed.meta.options.extra_options.insert(
            LOGICAL_TABLE_METADATA_KEY.to_string(),
            "physical_metrics".to_string(),
        );
        let manager = MemoryCatalogManager::new_with_table(EmptyTable::from_table_info(&allowed));
        let mut denied = allowed.clone();
        denied.ident.table_id = 1025;
        denied.name = "denied".to_string();
        manager
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: denied.name.clone(),
                table_id: denied.table_id(),
                table: EmptyTable::from_table_info(&denied),
            })
            .unwrap();
        let query_ctx = QueryContext::with(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME);
        assert_eq!(
            vec!["allowed".to_string(), "denied".to_string()],
            retrieve_table_names(&query_ctx, manager.clone(), Vec::new())
                .await
                .unwrap()
        );

        let response = label_values_query(
            State(Arc::new(TestPrometheusHandler {
                catalog_manager: manager,
                denied_table: Some("denied"),
                metric_names: Vec::new(),
                queries: Mutex::new(Vec::new()),
            })),
            Path(FIELD_NAME_LABEL.to_string()),
            Extension(query_ctx),
            Query(LabelValueQuery::default()),
        )
        .await;

        assert_eq!(Some(StatusCode::PermissionDenied), response.status_code);
        assert_eq!(
            axum::http::StatusCode::FORBIDDEN,
            response.into_response().status()
        );
    }

    #[tokio::test]
    async fn test_series_query_expands_metric_name_regex() {
        let cpu_user = test_table_info(
            1024,
            "cpu_user",
            DEFAULT_SCHEMA_NAME,
            DEFAULT_CATALOG_NAME,
            Arc::new(Schema::new(vec![])),
        );
        let manager = MemoryCatalogManager::new_with_table(EmptyTable::from_table_info(&cpu_user));
        let mut cpu_system = cpu_user.clone();
        cpu_system.ident.table_id = 1025;
        cpu_system.name = "cpu_system".to_string();
        manager
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: cpu_system.name.clone(),
                table_id: cpu_system.table_id(),
                table: EmptyTable::from_table_info(&cpu_system),
            })
            .unwrap();

        let handler = Arc::new(TestPrometheusHandler {
            catalog_manager: manager,
            denied_table: None,
            metric_names: vec!["cpu_user".to_string(), "cpu_system".to_string()],
            queries: Mutex::new(Vec::new()),
        });
        let state: PrometheusHandlerRef = handler.clone();
        let query_ctx = QueryContext::with(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME);
        let target_ctx = Arc::new(query_ctx.clone());
        let response = series_query(
            State(state),
            Query(SeriesQuery {
                matches: Matches(vec![r#"{__name__=~"cpu_.*"}"#.to_string()]),
                ..Default::default()
            }),
            Extension(query_ctx),
            Form(SeriesQuery::default()),
        )
        .await;

        assert!(
            response.status_code.is_none(),
            "status={:?}, error={:?}",
            response.status_code,
            response.error
        );
        let mut targets = handler
            .queries
            .lock()
            .unwrap()
            .iter()
            .map(|query| {
                let query = ParsedPromQuery::parse(
                    PromQuery {
                        query: query.clone(),
                        ..Default::default()
                    },
                    &target_ctx,
                )
                .unwrap();
                resolved_promql_targets(std::slice::from_ref(&query), &target_ctx)
                    .unwrap()
                    .pop()
                    .unwrap()
                    .table
            })
            .collect_vec();
        targets.sort_unstable();
        assert_eq!(vec!["cpu_system", "cpu_user"], targets);
    }

    #[test]
    fn test_retrieve_metric_name_and_result_type() {
        let test_cases = &[
            // Single metric cases
            TestCase {
                name: "simple metric",
                promql: "cpu_usage",
                expected_metric: Some("cpu_usage"),
                expected_type: ValueType::Vector,
                should_error: false,
            },
            TestCase {
                name: "metric with selector",
                promql: r#"cpu_usage{instance="localhost"}"#,
                expected_metric: Some("cpu_usage"),
                expected_type: ValueType::Vector,
                should_error: false,
            },
            TestCase {
                name: "metric with range selector",
                promql: "cpu_usage[5m]",
                expected_metric: Some("cpu_usage"),
                expected_type: ValueType::Matrix,
                should_error: false,
            },
            TestCase {
                name: "metric with __name__ matcher",
                promql: r#"{__name__="cpu_usage"}"#,
                expected_metric: Some("cpu_usage"),
                expected_type: ValueType::Vector,
                should_error: false,
            },
            TestCase {
                name: "metric with unary operator",
                promql: "-cpu_usage",
                expected_metric: None,
                expected_type: ValueType::Vector,
                should_error: false,
            },
            // Aggregation and function cases
            TestCase {
                name: "metric with aggregation",
                promql: "sum(cpu_usage)",
                expected_metric: Some("cpu_usage"),
                expected_type: ValueType::Vector,
                should_error: false,
            },
            TestCase {
                name: "complex aggregation",
                promql: r#"sum by (instance) (cpu_usage{job="node"})"#,
                expected_metric: None,
                expected_type: ValueType::Vector,
                should_error: false,
            },
            TestCase {
                name: "complex aggregation",
                promql: r#"sum by (__name__) (cpu_usage{job="node"})"#,
                expected_metric: Some("cpu_usage"),
                expected_type: ValueType::Vector,
                should_error: false,
            },
            TestCase {
                name: "complex aggregation",
                promql: r#"sum without (instance) (cpu_usage{job="node"})"#,
                expected_metric: Some("cpu_usage"),
                expected_type: ValueType::Vector,
                should_error: false,
            },
            // Same metric binary operations
            TestCase {
                name: "same metric addition",
                promql: "cpu_usage + cpu_usage",
                expected_metric: None,
                expected_type: ValueType::Vector,
                should_error: false,
            },
            TestCase {
                name: "metric with scalar addition",
                promql: r#"sum(rate(cpu_usage{job="node"}[5m])) + 100"#,
                expected_metric: None,
                expected_type: ValueType::Vector,
                should_error: false,
            },
            // Multiple metrics cases
            TestCase {
                name: "different metrics addition",
                promql: "cpu_usage + memory_usage",
                expected_metric: None,
                expected_type: ValueType::Vector,
                should_error: false,
            },
            TestCase {
                name: "different metrics subtraction",
                promql: "network_in - network_out",
                expected_metric: None,
                expected_type: ValueType::Vector,
                should_error: false,
            },
            // Unless operator cases
            TestCase {
                name: "unless with different metrics",
                promql: "cpu_usage unless memory_usage",
                expected_metric: Some("cpu_usage"),
                expected_type: ValueType::Vector,
                should_error: false,
            },
            TestCase {
                name: "unless with same metric",
                promql: "cpu_usage unless cpu_usage",
                expected_metric: Some("cpu_usage"),
                expected_type: ValueType::Vector,
                should_error: false,
            },
            // Subquery cases
            TestCase {
                name: "basic subquery",
                promql: "cpu_usage[5m:1m]",
                expected_metric: Some("cpu_usage"),
                expected_type: ValueType::Matrix,
                should_error: false,
            },
            TestCase {
                name: "subquery with multiple metrics",
                promql: "(cpu_usage + memory_usage)[5m:1m]",
                expected_metric: None,
                expected_type: ValueType::Matrix,
                should_error: false,
            },
            // Literal values
            TestCase {
                name: "scalar value",
                promql: "42",
                expected_metric: None,
                expected_type: ValueType::Scalar,
                should_error: false,
            },
            TestCase {
                name: "string literal",
                promql: r#""hello world""#,
                expected_metric: None,
                expected_type: ValueType::String,
                should_error: false,
            },
            // Error cases
            TestCase {
                name: "invalid syntax",
                promql: "cpu_usage{invalid=",
                expected_metric: None,
                expected_type: ValueType::Vector,
                should_error: true,
            },
            TestCase {
                name: "empty query",
                promql: "",
                expected_metric: None,
                expected_type: ValueType::Vector,
                should_error: true,
            },
            TestCase {
                name: "malformed brackets",
                promql: "cpu_usage[5m",
                expected_metric: None,
                expected_type: ValueType::Vector,
                should_error: true,
            },
        ];

        for test_case in test_cases {
            let result = promql_parser::parser::parse(test_case.promql)
                .map(|expr| retrieve_metric_name_and_result_type(&expr));

            if test_case.should_error {
                assert!(
                    result.is_err(),
                    "Test '{}' should have failed but succeeded with: {:?}",
                    test_case.name,
                    result
                );
            } else {
                let (metric_name, value_type) = result.unwrap_or_else(|e| {
                    panic!(
                        "Test '{}' should have succeeded but failed with error: {}",
                        test_case.name, e
                    )
                });

                let expected_metric_name = test_case.expected_metric.map(|s| s.to_string());
                assert_eq!(
                    metric_name, expected_metric_name,
                    "Test '{}': metric name mismatch. Expected: {:?}, Got: {:?}",
                    test_case.name, expected_metric_name, metric_name
                );

                assert_eq!(
                    value_type, test_case.expected_type,
                    "Test '{}': value type mismatch. Expected: {:?}, Got: {:?}",
                    test_case.name, test_case.expected_type, value_type
                );
            }
        }
    }

    #[tokio::test]
    async fn test_get_all_column_names_uses_tag_columns() {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "greptime_timestamp",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new("region", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new("value", ConcreteDataType::float64_datatype(), true),
            ColumnSchema::new(
                DATA_SCHEMA_TSID_COLUMN_NAME,
                ConcreteDataType::uint64_datatype(),
                true,
            ),
        ]));
        let mut options = TableOptions::default();
        options.extra_options.insert(
            LOGICAL_TABLE_METADATA_KEY.to_string(),
            "physical_metrics".to_string(),
        );
        let meta = TableMetaBuilder::empty()
            .schema(schema)
            .primary_key_indices(vec![1, 2, 4])
            .engine("metric".to_string())
            .next_column_id(5)
            .options(options)
            .build()
            .unwrap();
        let table_info = TableInfoBuilder::default()
            .table_id(1024)
            .table_version(0 as TableVersion)
            .name("cpu_usage")
            .catalog_name(DEFAULT_CATALOG_NAME)
            .schema_name(DEFAULT_SCHEMA_NAME)
            .table_type(TableType::Base)
            .meta(meta)
            .build()
            .unwrap();
        let manager =
            MemoryCatalogManager::new_with_table(EmptyTable::from_table_info(&table_info));

        let physical_schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "greptime_timestamp",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new(
                "physical_only_tag",
                ConcreteDataType::string_datatype(),
                false,
            ),
            ColumnSchema::new(
                "physical_only_value",
                ConcreteDataType::float64_datatype(),
                true,
            ),
        ]));
        let mut physical_options = TableOptions::default();
        physical_options.extra_options.insert(
            store_api::metric_engine_consts::PHYSICAL_TABLE_METADATA_KEY.to_string(),
            String::new(),
        );
        let physical_meta = TableMetaBuilder::empty()
            .schema(physical_schema)
            .primary_key_indices(vec![1])
            .engine("metric".to_string())
            .next_column_id(3)
            .options(physical_options)
            .build()
            .unwrap();
        let physical_table_info = TableInfoBuilder::default()
            .table_id(1025)
            .table_version(0 as TableVersion)
            .name("physical_metrics")
            .catalog_name(DEFAULT_CATALOG_NAME)
            .schema_name(DEFAULT_SCHEMA_NAME)
            .table_type(TableType::Base)
            .meta(physical_meta)
            .build()
            .unwrap();
        let physical_table = EmptyTable::from_table_info(&physical_table_info);
        manager
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: "physical_metrics".to_string(),
                table_id: 1025,
                table: physical_table,
            })
            .unwrap();
        let manager: CatalogManagerRef = manager;

        let (labels, table_names) =
            get_all_column_names(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, &manager)
                .await
                .unwrap();

        assert_eq!(
            labels,
            HashSet::from(["host".to_string(), "region".to_string()])
        );
        assert_eq!(table_names, vec!["cpu_usage"]);

        let query_ctx = QueryContext::with(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME);
        let (schemas, targets) =
            retrieve_schema_names(&query_ctx, manager.clone(), vec!["cpu_usage".to_string()])
                .await
                .unwrap();
        assert_eq!(schemas, vec![DEFAULT_SCHEMA_NAME]);
        assert_eq!(
            targets,
            PermissionTableTargets::Resolved(vec![PermissionTableTarget::new(
                DEFAULT_CATALOG_NAME,
                DEFAULT_SCHEMA_NAME,
                "cpu_usage",
            )])
        );

        let (_, targets) = retrieve_schema_names(
            &query_ctx,
            manager.clone(),
            vec![r#"{__name__=~"cpu.*"}"#.to_string()],
        )
        .await
        .unwrap();
        assert_eq!(targets, PermissionTableTargets::Unresolved);

        let (schemas, targets) = retrieve_schema_names(&query_ctx, manager.clone(), vec![])
            .await
            .unwrap();
        assert!(schemas.contains(&DEFAULT_SCHEMA_NAME.to_string()));
        assert_eq!(targets, PermissionTableTargets::Unresolved);

        let (schemas, targets) = retrieve_schema_names(
            &query_ctx,
            manager.clone(),
            vec!["physical_metrics".to_string()],
        )
        .await
        .unwrap();
        assert!(schemas.is_empty());
        assert_eq!(
            targets,
            PermissionTableTargets::Resolved(vec![PermissionTableTarget::new(
                DEFAULT_CATALOG_NAME,
                DEFAULT_SCHEMA_NAME,
                "physical_metrics",
            )])
        );

        let (schemas, targets) = retrieve_schema_names(
            &query_ctx,
            manager.clone(),
            vec!["missing".to_string(), "physical_metrics".to_string()],
        )
        .await
        .unwrap();
        assert!(schemas.is_empty());
        assert_eq!(
            targets,
            PermissionTableTargets::Resolved(vec![PermissionTableTarget::new(
                DEFAULT_CATALOG_NAME,
                DEFAULT_SCHEMA_NAME,
                "physical_metrics",
            )])
        );

        assert_eq!(
            vec!["cpu_usage".to_string()],
            retrieve_table_names(
                &query_ctx,
                manager.clone(),
                vec!["missing".to_string(), r#"{__name__=~"cpu.*"}"#.to_string(),],
            )
            .await
            .unwrap()
        );
        assert!(
            retrieve_table_names(
                &query_ctx,
                manager.clone(),
                vec!["cpu_usage".to_string(), "{".to_string()],
            )
            .await
            .is_err()
        );

        let (fields, table_names) = retrieve_field_names(
            &query_ctx,
            manager.clone(),
            vec!["physical_metrics".to_string()],
        )
        .await
        .unwrap();
        assert!(fields.is_empty());
        assert!(table_names.is_empty());

        let (fields, table_names) = retrieve_field_names(&query_ctx, manager, vec![])
            .await
            .unwrap();
        assert_eq!(fields, HashSet::from(["value".to_string()]));
        assert_eq!(table_names, vec!["cpu_usage"]);
    }

    #[tokio::test]
    async fn test_get_target_column_names_uses_selected_schema() {
        let manager = MemoryCatalogManager::with_default_setup();
        manager
            .register_schema_sync(RegisterSchemaRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: "private".to_string(),
            })
            .unwrap();

        for (schema_name, tag_name, table_id) in [
            (DEFAULT_SCHEMA_NAME, "public_tag", 2048),
            ("private", "private_tag", 2049),
        ] {
            let schema = Arc::new(Schema::new(vec![
                ColumnSchema::new(
                    "greptime_timestamp",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
                ColumnSchema::new(tag_name, ConcreteDataType::string_datatype(), false),
                ColumnSchema::new("value", ConcreteDataType::float64_datatype(), true),
            ]));
            let meta = TableMetaBuilder::empty()
                .schema(schema)
                .primary_key_indices(vec![1])
                .engine("metric".to_string())
                .next_column_id(3)
                .build()
                .unwrap();
            let table_info = TableInfoBuilder::default()
                .table_id(table_id)
                .table_version(0 as TableVersion)
                .name("cpu")
                .catalog_name(DEFAULT_CATALOG_NAME)
                .schema_name(schema_name)
                .table_type(TableType::Base)
                .meta(meta)
                .build()
                .unwrap();
            manager
                .register_table_sync(RegisterTableRequest {
                    catalog: DEFAULT_CATALOG_NAME.to_string(),
                    schema: schema_name.to_string(),
                    table_name: "cpu".to_string(),
                    table_id,
                    table: EmptyTable::from_table_info(&table_info),
                })
                .unwrap();
        }

        let query_ctx = Arc::new(QueryContext::with(
            DEFAULT_CATALOG_NAME,
            DEFAULT_SCHEMA_NAME,
        ));
        let queries = [ParsedPromQuery::parse(
            PromQuery {
                query: r#"{__name__="cpu",__schema__="private"}"#.to_string(),
                ..Default::default()
            },
            &query_ctx,
        )
        .unwrap()];
        let targets = resolved_promql_targets(&queries, &query_ctx).unwrap();
        let manager: CatalogManagerRef = manager;

        assert_eq!(
            get_target_column_names(&targets, &manager, &query_ctx)
                .await
                .unwrap(),
            HashSet::from(["private_tag".to_string()])
        );
    }

    #[test]
    fn test_metric_name_discovery_uses_selected_schema() {
        let expr =
            promql_parser::parser::parse(r#"{__name__=~"cpu.*",__database__="private"}"#).unwrap();
        let discovery = find_metric_name_not_equal_matchers(&expr).unwrap().unwrap();

        assert_eq!(discovery.schema.as_deref(), Some("private"));
        assert_eq!(discovery.name_matchers.len(), 1);
        assert_eq!(discovery.name_matchers[0].value, "cpu.*");
        assert!(matches!(discovery.name_matchers[0].op, MatchOp::Re(_)));

        let expr = promql_parser::parser::parse(
            r#"{__name__=~"cpu.*",__database__="private",__schema__="public"}"#,
        )
        .unwrap();
        let discovery = find_metric_name_not_equal_matchers(&expr).unwrap().unwrap();
        assert_eq!(discovery.schema.as_deref(), Some("public"));
    }

    #[test]
    fn test_metric_name_discovery_rejects_unsafe_selector_shapes() {
        for promql in [
            r#"{__name__="denied",__name__=~"no_such_.*"}"#,
            r#"{__name__=~"cpu.*" or job="api"}"#,
        ] {
            let expr = promql_parser::parser::parse(promql).unwrap();
            assert!(
                find_metric_name_not_equal_matchers(&expr)
                    .unwrap()
                    .is_none(),
                "{promql}"
            );
        }

        let expr = promql_parser::parser::parse(r#"{__name__=~"cpu.*" or job="api"}"#).unwrap();
        assert_eq!(
            (PermissionTableTargets::Resolved(vec![]), 1),
            static_promql_targets(&expr, &QueryContext::arc()).unwrap()
        );

        let expr =
            promql_parser::parser::parse(r#"allowed{job="api" or instance="host"}"#).unwrap();
        assert_eq!(
            (
                PermissionTableTargets::Resolved(vec![PermissionTableTarget::new(
                    "greptime", "public", "allowed",
                )]),
                0,
            ),
            static_promql_targets(&expr, &QueryContext::arc()).unwrap()
        );
    }

    #[test]
    fn test_metric_name_discovery_ignores_or_matchers_on_named_selector() {
        let expr = promql_parser::parser::parse(
            r#"{__name__=~"cpu.*"} + allowed{job="api" or instance="host"}"#,
        )
        .unwrap();

        let discovery = find_metric_name_not_equal_matchers(&expr).unwrap().unwrap();
        assert_eq!(discovery.name_matchers[0].value, "cpu.*");
    }

    #[test]
    fn test_retrieve_exact_metric_name_from_selector() {
        assert_eq!(
            retrieve_exact_metric_name_from_promql(r#"cpu{host="a"}"#).as_deref(),
            Some("cpu")
        );
        assert_eq!(
            retrieve_exact_metric_name_from_promql(r#"{__name__="cpu",host="a"}"#).as_deref(),
            Some("cpu")
        );
        assert!(retrieve_exact_metric_name_from_promql(r#"{__name__=~"cpu.*"}"#).is_none());
    }

    #[test]
    fn test_expand_metric_name_queries_preserves_schema_matcher() {
        let query = PromQuery {
            query: r#"{__name__=~"cpu.*",__database__="private"}"#.to_string(),
            ..Default::default()
        };
        let ctx = QueryContext::arc();
        let query = ParsedPromQuery::parse(query, &ctx).unwrap();
        let queries = expand_metric_name_queries(
            &query,
            vec!["cpu_user".to_string(), "cpu_system".to_string()],
        );

        assert_eq!(queries.len(), 2);
        for (query, metric_name) in queries.iter().zip(["cpu_user", "cpu_system"]) {
            let PromqlExpr::VectorSelector(selector) = query.expr() else {
                panic!("expected vector selector");
            };
            assert!(selector.matchers.matchers.iter().any(|matcher| {
                matcher.name == METRIC_NAME
                    && matcher.op == MatchOp::Equal
                    && matcher.value == metric_name
            }));
            assert!(selector.matchers.matchers.iter().any(|matcher| {
                matcher.name == "__database__"
                    && matcher.op == MatchOp::Equal
                    && matcher.value == "private"
            }));
        }
    }

    #[test]
    fn test_expand_metric_name_queries_preserves_exact_selector() {
        let query = PromQuery {
            query: r#"denied + {__name__=~"cpu.*"}"#.to_string(),
            ..Default::default()
        };
        let ctx = QueryContext::arc();
        let query = ParsedPromQuery::parse(query, &ctx).unwrap();
        let queries = expand_metric_name_queries(&query, vec!["cpu_user".to_string()]);

        assert_eq!(queries.len(), 1);
        let expanded = queries[0].expr();
        let ctx = Arc::new(QueryContext::with("greptime", "public"));
        let (PermissionTableTargets::Resolved(mut targets), unresolved_selectors) =
            static_promql_targets(expanded, &ctx).unwrap()
        else {
            panic!("expected resolved targets");
        };
        assert_eq!(unresolved_selectors, 0);
        targets.sort_unstable_by(|left, right| left.table.cmp(&right.table));
        assert_eq!(
            targets,
            vec![
                PermissionTableTarget::new("greptime", "public", "cpu_user"),
                PermissionTableTarget::new("greptime", "public", "denied"),
            ]
        );
    }

    #[test]
    fn test_static_promql_targets_keep_exact_selector_during_discovery() {
        let expr = promql_parser::parser::parse(
            r#"denied + {__name__=~"no_match_.*",__schema__="private"}"#,
        )
        .unwrap();
        let ctx = Arc::new(QueryContext::with("greptime", "public"));

        assert_eq!(
            static_promql_targets(&expr, &ctx).unwrap(),
            (
                PermissionTableTargets::resolved(vec![PermissionTableTarget::new(
                    "greptime", "public", "denied",
                )]),
                1,
            )
        );
    }

    #[test]
    fn test_static_promql_targets_count_unresolved_selectors() {
        let expr =
            promql_parser::parser::parse(r#"{__name__=~"no_such_metric"} or {job="api"}"#).unwrap();
        let ctx = Arc::new(QueryContext::with("greptime", "public"));

        assert_eq!(
            static_promql_targets(&expr, &ctx).unwrap(),
            (PermissionTableTargets::resolved(Vec::new()), 2)
        );
    }

    #[test]
    fn test_record_batches_to_labels_name_accepts_native_histogram_value() {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(
                greptime_native_histogram(),
                native_histogram_value_type().clone(),
                true,
            ),
        ]));
        let batch = RecordBatch::new_empty(schema.clone());
        let batches = RecordBatches::try_new(schema, vec![batch]).unwrap();
        let mut labels = HashSet::new();

        record_batches_to_labels_name(batches, &mut labels).unwrap();

        assert!(labels.is_empty());
    }

    #[tokio::test]
    async fn test_retrieve_metric_metadata_uses_semantic_options() {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "greptime_timestamp",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new("value", ConcreteDataType::float64_datatype(), true),
        ]));
        let mut options = TableOptions::default();
        options.extra_options.insert(
            LOGICAL_TABLE_METADATA_KEY.to_string(),
            "greptime_physical_table".to_string(),
        );
        options
            .extra_options
            .insert(SEMANTIC_METRIC_TYPE.to_string(), "counter".to_string());
        options
            .extra_options
            .insert(SEMANTIC_METRIC_UNIT.to_string(), "By".to_string());
        let meta = TableMetaBuilder::empty()
            .schema(schema)
            .primary_key_indices(vec![1])
            .engine("metric".to_string())
            .next_column_id(3)
            .options(options)
            .build()
            .unwrap();
        let table_info = TableInfoBuilder::default()
            .table_id(1025)
            .table_version(0 as TableVersion)
            .name("http_requests_total")
            .catalog_name(DEFAULT_CATALOG_NAME)
            .schema_name(DEFAULT_SCHEMA_NAME)
            .table_type(TableType::Base)
            .meta(meta)
            .build()
            .unwrap();
        let manager: CatalogManagerRef =
            MemoryCatalogManager::new_with_table(EmptyTable::from_table_info(&table_info));
        let query_ctx = QueryContext::with(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME);

        let metadata = retrieve_metric_metadata(&query_ctx, manager, &MetadataQuery::default())
            .await
            .unwrap();

        assert_eq!(
            metadata.get("http_requests_total"),
            Some(&vec![PromMetadata {
                metric_type: "counter".to_string(),
                unit: "By".to_string(),
                help: String::new(),
            }])
        );
    }
}
