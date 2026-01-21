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
use axum::extract::{Path, Query, State};
use axum::{Extension, Form};
use catalog::CatalogManagerRef;
use common_catalog::parse_catalog_and_schema_from_db_string;
use common_decimal::Decimal128;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
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
use futures::StreamExt;
use futures::future::join_all;
use itertools::Itertools;
use promql_parser::label::{METRIC_NAME, MatchOp, Matcher, Matchers};
use promql_parser::parser::token::{self};
use promql_parser::parser::value::ValueType;
use promql_parser::parser::{
    AggregateExpr, BinaryExpr, Call, Expr as PromqlExpr, LabelModifier, MatrixSelector, ParenExpr,
    SubqueryExpr, UnaryExpr, VectorSelector,
};
use query::parser::{DEFAULT_LOOKBACK_STRING, PromQuery, QueryLanguageParser};
use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use session::context::{QueryContext, QueryContextRef};
use snafu::{Location, OptionExt, ResultExt};
use store_api::metric_engine_consts::{
    DATA_SCHEMA_TABLE_ID_COLUMN_NAME, DATA_SCHEMA_TSID_COLUMN_NAME, LOGICAL_TABLE_METADATA_KEY,
};

pub use super::result::prometheus_resp::PrometheusJsonResponse;
use crate::error::{
    CatalogSnafu, CollectRecordbatchSnafu, ConvertScalarValueSnafu, DataFusionSnafu, Error,
    InvalidQuerySnafu, NotSupportedSnafu, ParseTimestampSnafu, Result, TableNotFoundSnafu,
    UnexpectedResultSnafu,
};
use crate::http::header::collect_plan_metrics;
use crate::prom_store::{FIELD_NAME_LABEL, METRIC_NAME_LABEL, is_database_selection_label};
use crate::prometheus_handler::PrometheusHandlerRef;

/// For [ValueType::Vector] result type
#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct PromSeriesVector {
    pub metric: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<(f64, String)>,
}

/// For [ValueType::Matrix] result type
#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct PromSeriesMatrix {
    pub metric: BTreeMap<String, String>,
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
        alias: None,
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
        Ok((metric_name, result_type)) => (metric_name, result_type),
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
        alias: None,
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
        Ok((metric_name, _)) => metric_name,
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
            alias: None,
        };

        let result = handler.do_query(&prom_query, query_ctx.clone()).await;
        handle_schema_err!(
            retrieve_labels_name_from_query_result(result, &mut fetched_labels, &mut merge_map)
                .await
        );
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
    series: &mut Vec<HashMap<Column, String>>,
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
                let column = batch.column(*i);
                column.as_primitive::<Float64Type>()
            })
            .collect::<Vec<_>>();

        for row_index in 0..batch.num_rows() {
            // if all field columns are null, skip this row
            if field_columns.iter().all(|c| c.is_null(row_index)) {
                continue;
            }

            // if a field is not null, record the tag name and return
            names.iter().for_each(|name| {
                let _ = labels.insert(name.clone());
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
                Some(LabelModifier::Include(labels)) => {
                    if !labels.labels.contains(&METRIC_NAME.to_string()) {
                        metric_names.clear();
                        return;
                    }
                }
                Some(LabelModifier::Exclude(labels)) => {
                    if labels.labels.contains(&METRIC_NAME.to_string()) {
                        metric_names.clear();
                        return;
                    }
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

    if label_name == METRIC_NAME_LABEL {
        let catalog_manager = handler.catalog_manager();

        let mut table_names = try_call_return_response!(
            retrieve_table_names(&query_ctx, catalog_manager, params.matches.0).await
        );

        truncate_results(&mut table_names, params.limit);
        return PrometheusJsonResponse::success(PrometheusResponse::LabelValues(table_names));
    } else if label_name == FIELD_NAME_LABEL {
        let field_columns = handle_schema_err!(
            retrieve_field_names(&query_ctx, handler.catalog_manager(), params.matches.0).await
        )
        .unwrap_or_default();
        let mut field_columns = field_columns.into_iter().collect::<Vec<_>>();
        field_columns.sort_unstable();
        truncate_results(&mut field_columns, params.limit);
        return PrometheusJsonResponse::success(PrometheusResponse::LabelValues(field_columns));
    } else if is_database_selection_label(&label_name) {
        let catalog_manager = handler.catalog_manager();

        let mut schema_names = try_call_return_response!(
            retrieve_schema_names(&query_ctx, catalog_manager, params.matches.0).await
        );
        truncate_results(&mut schema_names, params.limit);
        return PrometheusJsonResponse::success(PrometheusResponse::LabelValues(schema_names));
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

    let start = try_call_return_response!(
        QueryLanguageParser::parse_promql_timestamp(&start)
            .context(ParseTimestampSnafu { timestamp: &start })
    );
    let end = try_call_return_response!(
        QueryLanguageParser::parse_promql_timestamp(&end)
            .context(ParseTimestampSnafu { timestamp: &end })
    );

    for query in queries {
        let promql_expr = try_call_return_response!(promql_parser::parser::parse(&query));
        let PromqlExpr::VectorSelector(mut vector_selector) = promql_expr else {
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

    // we only provide very limited support for matcher against __name__
    let name_matcher = matches
        .first()
        .and_then(|matcher| promql_parser::parser::parse(matcher).ok())
        .and_then(|expr| {
            if let PromqlExpr::VectorSelector(vector_selector) = expr {
                let matchers = vector_selector.matchers.matchers;
                for matcher in matchers {
                    if matcher.name == METRIC_NAME_LABEL {
                        return Some(matcher);
                    }
                }

                None
            } else {
                None
            }
        });

    while let Some(table) = tables_stream.next().await {
        let table = table.context(CatalogSnafu)?;
        if !table
            .table_info()
            .meta
            .options
            .extra_options
            .contains_key(LOGICAL_TABLE_METADATA_KEY)
        {
            // skip non-prometheus (non-metricengine) tables for __name__ query
            continue;
        }

        let table_name = &table.table_info().name;

        if let Some(matcher) = &name_matcher {
            match &matcher.op {
                MatchOp::Equal => {
                    if table_name == &matcher.value {
                        table_names.push(table_name.clone());
                    }
                }
                MatchOp::Re(reg) => {
                    if reg.is_match(table_name) {
                        table_names.push(table_name.clone());
                    }
                }
                _ => {
                    // != and !~ are not supported:
                    // vector must contains at least one non-empty matcher
                    table_names.push(table_name.clone());
                }
            }
        } else {
            table_names.push(table_name.clone());
        }
    }

    table_names.sort_unstable();
    Ok(table_names)
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
                schema: schema.clone(),
                table: table_name.clone(),
            })?;

        for column in table.field_columns() {
            field_columns.insert(column.name);
        }
    }
    Ok(field_columns)
}

async fn retrieve_schema_names(
    query_ctx: &QueryContext,
    catalog_manager: CatalogManagerRef,
    matches: Vec<String>,
) -> Result<Vec<String>> {
    let mut schemas = Vec::new();
    let catalog = query_ctx.current_catalog();

    let candidate_schemas = catalog_manager
        .schema_names(catalog, Some(query_ctx))
        .await
        .context(CatalogSnafu)?;

    for schema in candidate_schemas {
        let mut found = true;
        for match_item in &matches {
            if let Some(table_name) = retrieve_metric_name_from_promql(match_item) {
                let exists = catalog_manager
                    .table_exists(catalog, &schema, &table_name, Some(query_ctx))
                    .await
                    .context(CatalogSnafu)?;
                if !exists {
                    found = false;
                    break;
                }
            }
        }

        if found {
            schemas.push(schema);
        }
    }

    schemas.sort_unstable();

    Ok(schemas)
}

/// Try to parse and extract the name of referenced metric from the promql query.
///
/// Returns the metric name if exactly one unique metric is referenced, otherwise None.
/// Multiple references to the same metric are allowed.
fn retrieve_metric_name_from_promql(query: &str) -> Option<String> {
    let promql_expr = promql_parser::parser::parse(query).ok()?;
    promql_expr_to_metric_name(&promql_expr)
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
            alias: None,
        };
        let result = handler.do_query(&prom_query, query_ctx.clone()).await;

        handle_schema_err!(
            retrieve_series_from_query_result(
                result,
                &mut series,
                &query_ctx,
                &table_name,
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
        let ast = try_call_return_response!(promql_parser::parser::parse(&query));
        PrometheusJsonResponse::success(PrometheusResponse::ParseResult(ast))
    } else {
        PrometheusJsonResponse::error(StatusCode::InvalidArguments, "query is required")
    }
}

#[cfg(test)]
mod tests {
    use promql_parser::parser::value::ValueType;

    use super::*;

    struct TestCase {
        name: &'static str,
        promql: &'static str,
        expected_metric: Option<&'static str>,
        expected_type: ValueType,
        should_error: bool,
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
            let result = retrieve_metric_name_and_result_type(test_case.promql);

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
}
