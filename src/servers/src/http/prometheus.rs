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
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::{Extension, Form};
use catalog::CatalogManagerRef;
use common_catalog::parse_catalog_and_schema_from_db_string;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_query::{Output, OutputData};
use common_recordbatch::RecordBatches;
use common_telemetry::{debug, tracing};
use common_time::util::{current_time_rfc3339, yesterday_rfc3339};
use common_version::OwnedBuildInfo;
use datatypes::prelude::ConcreteDataType;
use datatypes::scalars::ScalarVector;
use datatypes::vectors::Float64Vector;
use futures::future::join_all;
use futures::StreamExt;
use promql_parser::label::{MatchOp, Matcher, Matchers, METRIC_NAME};
use promql_parser::parser::value::ValueType;
use promql_parser::parser::{
    AggregateExpr, BinaryExpr, Call, Expr as PromqlExpr, MatrixSelector, ParenExpr, SubqueryExpr,
    UnaryExpr, VectorSelector,
};
use query::parser::{PromQuery, QueryLanguageParser, DEFAULT_LOOKBACK_STRING};
use query::promql::planner::normalize_matcher;
use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use session::context::{QueryContext, QueryContextRef};
use snafu::{Location, OptionExt, ResultExt};
use store_api::metric_engine_consts::{
    DATA_SCHEMA_TABLE_ID_COLUMN_NAME, DATA_SCHEMA_TSID_COLUMN_NAME, PHYSICAL_TABLE_METADATA_KEY,
};

pub use super::result::prometheus_resp::PrometheusJsonResponse;
use crate::error::{
    CatalogSnafu, CollectRecordbatchSnafu, Error, InvalidQuerySnafu, ParseTimestampSnafu, Result,
    TableNotFoundSnafu, UnexpectedResultSnafu,
};
use crate::http::header::collect_plan_metrics;
use crate::prom_store::{FIELD_NAME_LABEL, METRIC_NAME_LABEL};
use crate::prometheus_handler::PrometheusHandlerRef;

/// For [ValueType::Vector] result type
#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct PromSeriesVector {
    pub metric: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<(f64, String)>,
}

/// For [ValueType::Matrix] result type
#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct PromSeriesMatrix {
    pub metric: HashMap<String, String>,
    pub values: Vec<(f64, String)>,
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

#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum PrometheusResponse {
    PromData(PromData),
    Labels(Vec<String>),
    Series(Vec<HashMap<String, String>>),
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
    ($handle: expr) => {
        match $handle {
            Ok(res) => res,
            Err(err) => {
                let msg = err.to_string();
                return PrometheusJsonResponse::error(StatusCode::InvalidArguments, msg);
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
    };

    let promql_expr = try_call_return_response!(promql_parser::parser::parse(&prom_query.query));

    // update catalog and schema in query context if necessary
    if let Some(db) = &params.db {
        let (catalog, schema) = parse_catalog_and_schema_from_db_string(db);
        try_update_catalog_schema(&mut query_ctx, &catalog, &schema);
    }
    let query_ctx = Arc::new(query_ctx);

    let _timer = crate::metrics::METRIC_HTTP_PROMETHEUS_PROMQL_ELAPSED
        .with_label_values(&[query_ctx.get_db_string().as_str(), "instant_query"])
        .start_timer();

    if let Some(name_matchers) = find_metric_name_not_equal_matchers(&promql_expr)
        && !name_matchers.is_empty()
    {
        debug!("Find metric name matchers: {:?}", name_matchers);

        let metric_names =
            try_call_return_response!(handler.query_metric_names(name_matchers, &query_ctx).await);

        debug!("Find metric names: {:?}", metric_names);

        if metric_names.is_empty() {
            let result_type = promql_expr.value_type();

            return PrometheusJsonResponse::success(PrometheusResponse::PromData(PromData {
                result_type: result_type.to_string(),
                ..Default::default()
            }));
        }

        let responses = join_all(metric_names.into_iter().map(|metric| {
            let mut prom_query = prom_query.clone();
            let mut promql_expr = promql_expr.clone();
            let query_ctx = query_ctx.clone();
            let handler = handler.clone();

            async move {
                update_metric_name_matcher(&mut promql_expr, &metric);
                let new_query = promql_expr.to_string();
                debug!(
                    "Updated promql, before: {}, after: {}",
                    &prom_query.query, new_query
                );
                prom_query.query = new_query;

                do_instant_query(&handler, &prom_query, query_ctx).await
            }
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
        do_instant_query(&handler, &prom_query, query_ctx).await
    }
}

/// Executes a single instant query and returns response
async fn do_instant_query(
    handler: &PrometheusHandlerRef,
    prom_query: &PromQuery,
    query_ctx: QueryContextRef,
) -> PrometheusJsonResponse {
    let result = handler.do_query(prom_query, query_ctx).await;
    let (metric_name, result_type) = match retrieve_metric_name_and_result_type(&prom_query.query) {
        Ok((metric_name, result_type)) => (metric_name.unwrap_or_default(), result_type),
        Err(err) => return PrometheusJsonResponse::error(err.status_code(), err.output_msg()),
    };
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
    };

    let promql_expr = try_call_return_response!(promql_parser::parser::parse(&prom_query.query));

    // update catalog and schema in query context if necessary
    if let Some(db) = &params.db {
        let (catalog, schema) = parse_catalog_and_schema_from_db_string(db);
        try_update_catalog_schema(&mut query_ctx, &catalog, &schema);
    }
    let query_ctx = Arc::new(query_ctx);
    let _timer = crate::metrics::METRIC_HTTP_PROMETHEUS_PROMQL_ELAPSED
        .with_label_values(&[query_ctx.get_db_string().as_str(), "range_query"])
        .start_timer();

    if let Some(name_matchers) = find_metric_name_not_equal_matchers(&promql_expr)
        && !name_matchers.is_empty()
    {
        debug!("Find metric name matchers: {:?}", name_matchers);

        let metric_names =
            try_call_return_response!(handler.query_metric_names(name_matchers, &query_ctx).await);

        debug!("Find metric names: {:?}", metric_names);

        if metric_names.is_empty() {
            return PrometheusJsonResponse::success(PrometheusResponse::PromData(PromData {
                result_type: ValueType::Matrix.to_string(),
                ..Default::default()
            }));
        }

        let responses = join_all(metric_names.into_iter().map(|metric| {
            let mut prom_query = prom_query.clone();
            let mut promql_expr = promql_expr.clone();
            let query_ctx = query_ctx.clone();
            let handler = handler.clone();

            async move {
                update_metric_name_matcher(&mut promql_expr, &metric);
                let new_query = promql_expr.to_string();
                debug!(
                    "Updated promql, before: {}, after: {}",
                    &prom_query.query, new_query
                );
                prom_query.query = new_query;

                do_range_query(&handler, &prom_query, query_ctx).await
            }
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
        do_range_query(&handler, &prom_query, query_ctx).await
    }
}

/// Executes a single range query and returns response
async fn do_range_query(
    handler: &PrometheusHandlerRef,
    prom_query: &PromQuery,
    query_ctx: QueryContextRef,
) -> PrometheusJsonResponse {
    let result = handler.do_query(prom_query, query_ctx).await;
    let metric_name = match retrieve_metric_name_and_result_type(&prom_query.query) {
        Err(err) => return PrometheusJsonResponse::error(err.status_code(), err.output_msg()),
        Ok((metric_name, _)) => metric_name.unwrap_or_default(),
    };
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

    // Fetch all tag columns. It will be used as white-list for tag names.
    let mut labels = match get_all_column_names(&catalog, &schema, &handler.catalog_manager()).await
    {
        Ok(labels) => labels,
        Err(e) => return PrometheusJsonResponse::error(e.status_code(), e.output_msg()),
    };
    // insert the special metric name label
    let _ = labels.insert(METRIC_NAME.to_string());

    // Fetch all columns if no query matcher is provided
    if queries.is_empty() {
        let mut labels_vec = labels.into_iter().collect::<Vec<_>>();
        labels_vec.sort_unstable();
        return PrometheusJsonResponse::success(PrometheusResponse::Labels(labels_vec));
    }

    // Otherwise, run queries and extract column name from result set.
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

    let mut fetched_labels = HashSet::new();
    let _ = fetched_labels.insert(METRIC_NAME.to_string());

    let mut merge_map = HashMap::new();
    for query in queries {
        let prom_query = PromQuery {
            query,
            start: start.clone(),
            end: end.clone(),
            step: DEFAULT_LOOKBACK_STRING.to_string(),
            lookback: lookback.clone(),
        };

        let result = handler.do_query(&prom_query, query_ctx.clone()).await;
        if let Err(err) =
            retrieve_labels_name_from_query_result(result, &mut fetched_labels, &mut merge_map)
                .await
        {
            // Prometheus won't report error if querying nonexist label and metric
            if err.status_code() != StatusCode::TableNotFound
                && err.status_code() != StatusCode::TableColumnNotFound
            {
                return PrometheusJsonResponse::error(err.status_code(), err.output_msg());
            }
        }
    }

    // intersect `fetched_labels` with `labels` to filter out non-tag columns
    fetched_labels.retain(|l| labels.contains(l));
    let _ = labels.insert(METRIC_NAME.to_string());

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
) -> std::result::Result<HashSet<String>, catalog::error::Error> {
    let table_names = manager.table_names(catalog, schema, None).await?;

    let mut labels = HashSet::new();
    for table_name in table_names {
        let Some(table) = manager.table(catalog, schema, &table_name, None).await? else {
            continue;
        };
        for column in table.primary_key_columns() {
            if column.name != DATA_SCHEMA_TABLE_ID_COLUMN_NAME
                && column.name != DATA_SCHEMA_TSID_COLUMN_NAME
            {
                labels.insert(column.name);
            }
        }
    }

    Ok(labels)
}

async fn retrieve_series_from_query_result(
    result: Result<Output>,
    series: &mut Vec<HashMap<String, String>>,
    query_ctx: &QueryContext,
    table_name: &str,
    manager: &CatalogManagerRef,
    metrics: &mut HashMap<String, u64>,
) -> Result<()> {
    let result = result?;

    // fetch tag list
    let table = manager
        .table(
            query_ctx.current_catalog(),
            &query_ctx.current_schema(),
            table_name,
            Some(query_ctx),
        )
        .await
        .context(CatalogSnafu)?
        .with_context(|| TableNotFoundSnafu {
            catalog: query_ctx.current_catalog(),
            schema: query_ctx.current_schema(),
            table: table_name,
        })?;
    let tag_columns = table
        .primary_key_columns()
        .map(|c| c.name)
        .collect::<HashSet<_>>();

    match result.data {
        OutputData::RecordBatches(batches) => {
            record_batches_to_series(batches, series, table_name, &tag_columns)
        }
        OutputData::Stream(stream) => {
            let batches = RecordBatches::try_collect(stream)
                .await
                .context(CollectRecordbatchSnafu)?;
            record_batches_to_series(batches, series, table_name, &tag_columns)
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
    series: &mut Vec<HashMap<String, String>>,
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

        for row in batch.rows() {
            let mut element: HashMap<String, String> = row
                .iter()
                .enumerate()
                .map(|(idx, column)| {
                    let column_name = batch.schema.column_name_by_index(idx);
                    (column_name.to_string(), column.to_string())
                })
                .collect();
            let _ = element.insert("__name__".to_string(), table_name.to_string());
            series.push(element);
        }
    }
    Ok(())
}

/// Retrieve labels name from record batches
fn record_batches_to_labels_name(
    batches: RecordBatches,
    labels: &mut HashSet<String>,
) -> Result<()> {
    let mut column_indices = Vec::new();
    let mut field_column_indices = Vec::new();
    for (i, column) in batches.schema().column_schemas().iter().enumerate() {
        if let ConcreteDataType::Float64(_) = column.data_type {
            field_column_indices.push(i);
        }
        column_indices.push(i);
    }

    if field_column_indices.is_empty() {
        return Err(Error::Internal {
            err_msg: "no value column found".to_string(),
        });
    }

    for batch in batches.iter() {
        let names = column_indices
            .iter()
            .map(|c| batches.schema().column_name_by_index(*c).to_string())
            .collect::<Vec<_>>();

        let field_columns = field_column_indices
            .iter()
            .map(|i| {
                batch
                    .column(*i)
                    .as_any()
                    .downcast_ref::<Float64Vector>()
                    .unwrap()
            })
            .collect::<Vec<_>>();

        for row_index in 0..batch.num_rows() {
            // if all field columns are null, skip this row
            if field_columns
                .iter()
                .all(|c| c.get_data(row_index).is_none())
            {
                continue;
            }

            // if a field is not null, record the tag name and return
            names.iter().for_each(|name| {
                let _ = labels.insert(name.to_string());
            });
            return Ok(());
        }
    }
    Ok(())
}

pub(crate) fn retrieve_metric_name_and_result_type(
    promql: &str,
) -> Result<(Option<String>, ValueType)> {
    let promql_expr = promql_parser::parser::parse(promql)
        .map_err(|reason| InvalidQuerySnafu { reason }.build())?;
    let metric_name = promql_expr_to_metric_name(&promql_expr);
    let result_type = promql_expr.value_type();

    Ok((metric_name, result_type))
}

/// Tries to get catalog and schema from an optional db param. And retrieves
/// them from [QueryContext] if they don't present.
pub(crate) fn get_catalog_schema(db: &Option<String>, ctx: &QueryContext) -> (String, String) {
    if let Some(db) = db {
        parse_catalog_and_schema_from_db_string(db)
    } else {
        (
            ctx.current_catalog().to_string(),
            ctx.current_schema().to_string(),
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

fn promql_expr_to_metric_name(expr: &PromqlExpr) -> Option<String> {
    find_metric_name_and_matchers(expr, |name, matchers| {
        name.clone().or(matchers
            .find_matchers(METRIC_NAME)
            .into_iter()
            .next()
            .map(|m| m.value))
    })
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

/// Try to find the `__name__` matchers which op is not `MatchOp::Equal`.
fn find_metric_name_not_equal_matchers(expr: &PromqlExpr) -> Option<Vec<Matcher>> {
    find_metric_name_and_matchers(expr, |name, matchers| {
        // Has name, ignore the matchers
        if name.is_some() {
            return None;
        }

        // FIXME(dennis): we don't consider the nested and `or` matchers yet.
        Some(matchers.find_matchers(METRIC_NAME))
    })
    .map(|matchers| {
        matchers
            .into_iter()
            .filter(|m| !matches!(m.op, MatchOp::Equal))
            .map(normalize_matcher)
            .collect::<Vec<_>>()
    })
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
                if m.name == METRIC_NAME {
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
                if m.name == METRIC_NAME {
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

    if label_name == METRIC_NAME_LABEL {
        let catalog_manager = handler.catalog_manager();
        let mut tables_stream = catalog_manager.tables(&catalog, &schema, Some(&query_ctx));
        let mut table_names = Vec::new();
        while let Some(table) = tables_stream.next().await {
            // filter out physical tables
            match table {
                Ok(table) => {
                    if table
                        .table_info()
                        .meta
                        .options
                        .extra_options
                        .contains_key(PHYSICAL_TABLE_METADATA_KEY)
                    {
                        continue;
                    }

                    table_names.push(table.table_info().name.clone());
                }
                Err(e) => {
                    return PrometheusJsonResponse::error(e.status_code(), e.output_msg());
                }
            }
        }
        table_names.sort_unstable();
        return PrometheusJsonResponse::success(PrometheusResponse::LabelValues(table_names));
    } else if label_name == FIELD_NAME_LABEL {
        let field_columns =
            match retrieve_field_names(&query_ctx, handler.catalog_manager(), params.matches.0)
                .await
            {
                Ok(table_names) => table_names,
                Err(e) => {
                    return PrometheusJsonResponse::error(e.status_code(), e.output_msg());
                }
            };
        let mut field_columns = field_columns.into_iter().collect::<Vec<_>>();
        field_columns.sort_unstable();
        return PrometheusJsonResponse::success(PrometheusResponse::LabelValues(field_columns));
    }

    let queries = params.matches.0;
    if queries.is_empty() {
        return PrometheusJsonResponse::error(
            StatusCode::InvalidArguments,
            "match[] parameter is required",
        );
    }

    let start = params.start.unwrap_or_else(yesterday_rfc3339);
    let end = params.end.unwrap_or_else(current_time_rfc3339);
    let mut label_values = HashSet::new();

    let start = try_call_return_response!(QueryLanguageParser::parse_promql_timestamp(&start)
        .context(ParseTimestampSnafu { timestamp: &start }));
    let end = try_call_return_response!(QueryLanguageParser::parse_promql_timestamp(&end)
        .context(ParseTimestampSnafu { timestamp: &end }));

    for query in queries {
        let promql_expr = try_call_return_response!(promql_parser::parser::parse(&query));
        let PromqlExpr::VectorSelector(VectorSelector { name, matchers, .. }) = promql_expr else {
            return PrometheusJsonResponse::error(
                StatusCode::InvalidArguments,
                "expected vector selector",
            );
        };
        let Some(name) = name else {
            return PrometheusJsonResponse::error(
                StatusCode::InvalidArguments,
                "expected metric name",
            );
        };
        // Only use and filter matchers.
        let matchers = matchers.matchers;
        let result = handler
            .query_label_values(
                name,
                label_name.to_string(),
                matchers,
                start,
                end,
                &query_ctx,
            )
            .await;

        match result {
            Ok(result) => {
                label_values.extend(result.into_iter());
            }
            Err(err) => {
                // Prometheus won't report error if querying nonexist label and metric
                if err.status_code() != StatusCode::TableNotFound
                    && err.status_code() != StatusCode::TableColumnNotFound
                {
                    return PrometheusJsonResponse::error(err.status_code(), err.output_msg());
                }
            }
        }
    }

    let mut label_values: Vec<_> = label_values.into_iter().collect();
    label_values.sort_unstable();
    PrometheusJsonResponse::success(PrometheusResponse::LabelValues(label_values))
}

async fn retrieve_field_names(
    query_ctx: &QueryContext,
    manager: CatalogManagerRef,
    matches: Vec<String>,
) -> Result<HashSet<String>> {
    let mut field_columns = HashSet::new();
    let catalog = query_ctx.current_catalog();
    let schema = query_ctx.current_schema();

    if matches.is_empty() {
        // query all tables if no matcher is provided
        while let Some(table) = manager
            .tables(catalog, &schema, Some(query_ctx))
            .next()
            .await
        {
            let table = table.context(CatalogSnafu)?;
            for column in table.field_columns() {
                field_columns.insert(column.name);
            }
        }
        return Ok(field_columns);
    }

    for table_name in matches {
        let table = manager
            .table(catalog, &schema, &table_name, Some(query_ctx))
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                catalog: catalog.to_string(),
                schema: schema.to_string(),
                table: table_name.to_string(),
            })?;

        for column in table.field_columns() {
            field_columns.insert(column.name);
        }
    }
    Ok(field_columns)
}

/// Try to parse and extract the name of referenced metric from the promql query.
///
/// Returns the metric name if a single metric is referenced, otherwise None.
fn retrieve_metric_name_from_promql(query: &str) -> Option<String> {
    let promql_expr = promql_parser::parser::parse(query).ok()?;

    struct MetricNameVisitor {
        metric_name: Option<String>,
    }

    impl promql_parser::util::ExprVisitor for MetricNameVisitor {
        type Error = ();

        fn pre_visit(&mut self, plan: &PromqlExpr) -> std::result::Result<bool, Self::Error> {
            let query_metric_name = match plan {
                PromqlExpr::VectorSelector(vs) => vs
                    .matchers
                    .find_matchers(METRIC_NAME)
                    .into_iter()
                    .next()
                    .map(|m| m.value)
                    .or_else(|| vs.name.clone()),
                PromqlExpr::MatrixSelector(ms) => ms
                    .vs
                    .matchers
                    .find_matchers(METRIC_NAME)
                    .into_iter()
                    .next()
                    .map(|m| m.value)
                    .or_else(|| ms.vs.name.clone()),
                _ => return Ok(true),
            };

            // set it to empty string if multiple metrics are referenced.
            if self.metric_name.is_some() && query_metric_name.is_some() {
                self.metric_name = Some(String::new());
            } else {
                self.metric_name = query_metric_name.or_else(|| self.metric_name.clone());
            }

            Ok(true)
        }
    }

    let mut visitor = MetricNameVisitor { metric_name: None };
    promql_parser::util::walk_expr(&mut visitor, &promql_expr).ok()?;
    visitor.metric_name
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

    let mut series = Vec::new();
    let mut merge_map = HashMap::new();
    for query in queries {
        let table_name = retrieve_metric_name_from_promql(&query).unwrap_or_default();
        let prom_query = PromQuery {
            query,
            start: start.clone(),
            end: end.clone(),
            // TODO: find a better value for step
            step: DEFAULT_LOOKBACK_STRING.to_string(),
            lookback: lookback.clone(),
        };
        let result = handler.do_query(&prom_query, query_ctx.clone()).await;

        if let Err(err) = retrieve_series_from_query_result(
            result,
            &mut series,
            &query_ctx,
            &table_name,
            &handler.catalog_manager(),
            &mut merge_map,
        )
        .await
        {
            return PrometheusJsonResponse::error(err.status_code(), err.output_msg());
        }
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
        let ast = try_call_return_response!(promql_parser::parser::parse(&query));
        PrometheusJsonResponse::success(PrometheusResponse::ParseResult(ast))
    } else {
        PrometheusJsonResponse::error(StatusCode::InvalidArguments, "query is required")
    }
}
