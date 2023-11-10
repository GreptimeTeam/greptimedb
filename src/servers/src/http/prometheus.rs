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
use std::collections::{BTreeMap, HashMap, HashSet};

use axum::extract::{Path, Query, State};
use axum::{Extension, Form, Json};
use catalog::CatalogManagerRef;
use common_catalog::consts::DEFAULT_SCHEMA_NAME;
use common_catalog::parse_catalog_and_schema_from_db_string;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_query::Output;
use common_recordbatch::RecordBatches;
use common_time::util::{current_time_rfc3339, yesterday_rfc3339};
use datatypes::prelude::ConcreteDataType;
use datatypes::scalars::ScalarVector;
use datatypes::vectors::{Float64Vector, StringVector, TimestampMillisecondVector};
use promql_parser::label::METRIC_NAME;
use promql_parser::parser::{
    AggregateExpr, BinaryExpr, Call, Expr as PromqlExpr, MatrixSelector, ParenExpr, SubqueryExpr,
    UnaryExpr, ValueType, VectorSelector,
};
use query::parser::{PromQuery, DEFAULT_LOOKBACK_STRING};
use schemars::JsonSchema;
use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Serialize};
use session::context::QueryContextRef;
use snafu::{Location, OptionExt, ResultExt};

use crate::error::{
    CollectRecordbatchSnafu, Error, InternalSnafu, InvalidQuerySnafu, Result, UnexpectedResultSnafu,
};
use crate::prom_store::{FIELD_COLUMN_NAME, METRIC_NAME_LABEL, TIMESTAMP_COLUMN_NAME};
use crate::prometheus_handler::PrometheusHandlerRef;

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct PromSeries {
    pub metric: HashMap<String, String>,
    /// For [ValueType::Matrix] result type
    pub values: Vec<(f64, String)>,
    /// For [ValueType::Vector] result type
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<(f64, String)>,
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct PromData {
    #[serde(rename = "resultType")]
    pub result_type: String,
    pub result: Vec<PromSeries>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(untagged)]
pub enum PrometheusResponse {
    PromData(PromData),
    Labels(Vec<String>),
    Series(Vec<HashMap<String, String>>),
    LabelValues(Vec<String>),
    FormatQuery(String),
}

impl Default for PrometheusResponse {
    fn default() -> Self {
        PrometheusResponse::PromData(Default::default())
    }
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct PrometheusJsonResponse {
    pub status: String,
    pub data: PrometheusResponse,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "errorType")]
    pub error_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warnings: Option<Vec<String>>,
}

impl PrometheusJsonResponse {
    pub fn error<S1, S2>(error_type: S1, reason: S2) -> Json<Self>
    where
        S1: Into<String>,
        S2: Into<String>,
    {
        Json(PrometheusJsonResponse {
            status: "error".to_string(),
            data: PrometheusResponse::default(),
            error: Some(reason.into()),
            error_type: Some(error_type.into()),
            warnings: None,
        })
    }

    pub fn success(data: PrometheusResponse) -> Json<Self> {
        Json(PrometheusJsonResponse {
            status: "success".to_string(),
            data,
            error: None,
            error_type: None,
            warnings: None,
        })
    }

    /// Convert from `Result<Output>`
    pub async fn from_query_result(
        result: Result<Output>,
        metric_name: String,
        result_type: ValueType,
    ) -> Json<Self> {
        let response: Result<Json<Self>> = try {
            let json = match result? {
                Output::RecordBatches(batches) => Self::success(Self::record_batches_to_data(
                    batches,
                    metric_name,
                    result_type,
                )?),
                Output::Stream(stream) => {
                    let record_batches = RecordBatches::try_collect(stream)
                        .await
                        .context(CollectRecordbatchSnafu)?;
                    Self::success(Self::record_batches_to_data(
                        record_batches,
                        metric_name,
                        result_type,
                    )?)
                }
                Output::AffectedRows(_) => {
                    Self::error("Unexpected", "expected data result, but got affected rows")
                }
            };

            json
        };

        let result_type_string = result_type.to_string();

        match response {
            Ok(resp) => resp,
            Err(err) => {
                // Prometheus won't report error if querying nonexist label and metric
                if err.status_code() == StatusCode::TableNotFound
                    || err.status_code() == StatusCode::TableColumnNotFound
                {
                    Self::success(PrometheusResponse::PromData(PromData {
                        result_type: result_type_string,
                        ..Default::default()
                    }))
                } else {
                    Self::error(err.status_code().to_string(), err.output_msg())
                }
            }
        }
    }

    /// Convert [RecordBatches] to [PromData]
    fn record_batches_to_data(
        batches: RecordBatches,
        metric_name: String,
        result_type: ValueType,
    ) -> Result<PrometheusResponse> {
        // infer semantic type of each column from schema.
        // TODO(ruihang): wish there is a better way to do this.
        let mut timestamp_column_index = None;
        let mut tag_column_indices = Vec::new();
        let mut first_field_column_index = None;

        for (i, column) in batches.schema().column_schemas().iter().enumerate() {
            match column.data_type {
                ConcreteDataType::Timestamp(datatypes::types::TimestampType::Millisecond(_)) => {
                    if timestamp_column_index.is_none() {
                        timestamp_column_index = Some(i);
                    }
                }
                ConcreteDataType::Float64(_) => {
                    if first_field_column_index.is_none() {
                        first_field_column_index = Some(i);
                    }
                }
                ConcreteDataType::String(_) => {
                    tag_column_indices.push(i);
                }
                _ => {}
            }
        }

        let timestamp_column_index = timestamp_column_index.context(InternalSnafu {
            err_msg: "no timestamp column found".to_string(),
        })?;
        let first_field_column_index = first_field_column_index.context(InternalSnafu {
            err_msg: "no value column found".to_string(),
        })?;

        let metric_name = (METRIC_NAME.to_string(), metric_name);
        let mut buffer = BTreeMap::<Vec<(String, String)>, Vec<(f64, String)>>::new();

        for batch in batches.iter() {
            // prepare things...
            let tag_columns = tag_column_indices
                .iter()
                .map(|i| {
                    batch
                        .column(*i)
                        .as_any()
                        .downcast_ref::<StringVector>()
                        .unwrap()
                })
                .collect::<Vec<_>>();
            let tag_names = tag_column_indices
                .iter()
                .map(|c| batches.schema().column_name_by_index(*c).to_string())
                .collect::<Vec<_>>();
            let timestamp_column = batch
                .column(timestamp_column_index)
                .as_any()
                .downcast_ref::<TimestampMillisecondVector>()
                .unwrap();
            let field_column = batch
                .column(first_field_column_index)
                .as_any()
                .downcast_ref::<Float64Vector>()
                .unwrap();

            // assemble rows
            for row_index in 0..batch.num_rows() {
                // retrieve tags
                // TODO(ruihang): push table name `__metric__`
                let mut tags = vec![metric_name.clone()];
                for (tag_column, tag_name) in tag_columns.iter().zip(tag_names.iter()) {
                    // TODO(ruihang): add test for NULL tag
                    if let Some(tag_value) = tag_column.get_data(row_index) {
                        tags.push((tag_name.to_string(), tag_value.to_string()));
                    }
                }

                // retrieve timestamp
                let timestamp_millis: i64 = timestamp_column.get_data(row_index).unwrap().into();
                let timestamp = timestamp_millis as f64 / 1000.0;

                // retrieve value
                if let Some(v) = field_column.get_data(row_index) {
                    buffer
                        .entry(tags)
                        .or_default()
                        .push((timestamp, Into::<f64>::into(v).to_string()));
                };
            }
        }

        let result = buffer
            .into_iter()
            .map(|(tags, mut values)| {
                let metric = tags.into_iter().collect();
                match result_type {
                    ValueType::Vector | ValueType::Scalar | ValueType::String => Ok(PromSeries {
                        metric,
                        value: values.pop(),
                        ..Default::default()
                    }),
                    ValueType::Matrix => Ok(PromSeries {
                        metric,
                        values,
                        ..Default::default()
                    }),
                }
            })
            .collect::<Result<Vec<_>>>()?;

        let result_type_string = result_type.to_string();
        let data = PrometheusResponse::PromData(PromData {
            result_type: result_type_string,
            result,
        });

        Ok(data)
    }
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct FormatQuery {
    query: Option<String>,
}

#[axum_macros::debug_handler]
pub async fn format_query(
    State(_handler): State<PrometheusHandlerRef>,
    Query(params): Query<InstantQuery>,
    Extension(_query_ctx): Extension<QueryContextRef>,
    Form(form_params): Form<InstantQuery>,
) -> Json<PrometheusJsonResponse> {
    let _timer = crate::metrics::METRIC_HTTP_PROMQL_FORMAT_QUERY_ELAPSED.start_timer();

    let query = params.query.or(form_params.query).unwrap_or_default();
    match promql_parser::parser::parse(&query) {
        Ok(expr) => {
            let pretty = expr.prettify();
            PrometheusJsonResponse::success(PrometheusResponse::FormatQuery(pretty))
        }
        Err(reason) => {
            let err = InvalidQuerySnafu { reason }.build();
            PrometheusJsonResponse::error(err.status_code().to_string(), err.output_msg())
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct InstantQuery {
    query: Option<String>,
    time: Option<String>,
    timeout: Option<String>,
    db: Option<String>,
}

#[axum_macros::debug_handler]
pub async fn instant_query(
    State(handler): State<PrometheusHandlerRef>,
    Query(params): Query<InstantQuery>,
    Extension(query_ctx): Extension<QueryContextRef>,
    Form(form_params): Form<InstantQuery>,
) -> Json<PrometheusJsonResponse> {
    let _timer = crate::metrics::METRIC_HTTP_PROMQL_INSTANT_QUERY_ELAPSED.start_timer();
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
    };

    let result = handler.do_query(&prom_query, query_ctx).await;
    let (metric_name, result_type) = match retrieve_metric_name_and_result_type(&prom_query.query) {
        Ok((metric_name, result_type)) => (metric_name.unwrap_or_default(), result_type),
        Err(err) => {
            return PrometheusJsonResponse::error(err.status_code().to_string(), err.output_msg())
        }
    };
    PrometheusJsonResponse::from_query_result(result, metric_name, result_type).await
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct RangeQuery {
    query: Option<String>,
    start: Option<String>,
    end: Option<String>,
    step: Option<String>,
    timeout: Option<String>,
    db: Option<String>,
}

#[axum_macros::debug_handler]
pub async fn range_query(
    State(handler): State<PrometheusHandlerRef>,
    Query(params): Query<RangeQuery>,
    Extension(query_ctx): Extension<QueryContextRef>,
    Form(form_params): Form<RangeQuery>,
) -> Json<PrometheusJsonResponse> {
    let _timer = crate::metrics::METRIC_HTTP_PROMQL_RANGE_QUERY_ELAPSED.start_timer();
    let prom_query = PromQuery {
        query: params.query.or(form_params.query).unwrap_or_default(),
        start: params.start.or(form_params.start).unwrap_or_default(),
        end: params.end.or(form_params.end).unwrap_or_default(),
        step: params.step.or(form_params.step).unwrap_or_default(),
    };

    let result = handler.do_query(&prom_query, query_ctx).await;
    let metric_name = match retrieve_metric_name_and_result_type(&prom_query.query) {
        Err(err) => {
            return PrometheusJsonResponse::error(err.status_code().to_string(), err.output_msg())
        }
        Ok((metric_name, _)) => metric_name.unwrap_or_default(),
    };
    PrometheusJsonResponse::from_query_result(result, metric_name, ValueType::Matrix).await
}

#[derive(Debug, Default, Serialize, JsonSchema)]
struct Matches(Vec<String>);

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct LabelsQuery {
    start: Option<String>,
    end: Option<String>,
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
pub async fn labels_query(
    State(handler): State<PrometheusHandlerRef>,
    Query(params): Query<LabelsQuery>,
    Extension(query_ctx): Extension<QueryContextRef>,
    Form(form_params): Form<LabelsQuery>,
) -> Json<PrometheusJsonResponse> {
    let _timer = crate::metrics::METRIC_HTTP_PROMQL_LABEL_QUERY_ELAPSED.start_timer();

    let db = &params.db.unwrap_or(DEFAULT_SCHEMA_NAME.to_string());
    let (catalog, schema) = parse_catalog_and_schema_from_db_string(db);

    let mut queries = params.matches.0;
    if queries.is_empty() {
        queries = form_params.matches.0;
    }
    if queries.is_empty() {
        match get_all_column_names(catalog, schema, &handler.catalog_manager()).await {
            Ok(labels) => {
                return PrometheusJsonResponse::success(PrometheusResponse::Labels(labels))
            }
            Err(e) => {
                return PrometheusJsonResponse::error(e.status_code().to_string(), e.output_msg())
            }
        }
    }

    let start = params
        .start
        .or(form_params.start)
        .unwrap_or_else(yesterday_rfc3339);
    let end = params
        .end
        .or(form_params.end)
        .unwrap_or_else(current_time_rfc3339);

    let mut labels = HashSet::new();
    let _ = labels.insert(METRIC_NAME.to_string());

    for query in queries {
        let prom_query = PromQuery {
            query,
            start: start.clone(),
            end: end.clone(),
            step: DEFAULT_LOOKBACK_STRING.to_string(),
        };

        let result = handler.do_query(&prom_query, query_ctx.clone()).await;

        let response = retrieve_labels_name_from_query_result(result, &mut labels).await;

        if let Err(err) = response {
            // Prometheus won't report error if querying nonexist label and metric
            if err.status_code() != StatusCode::TableNotFound
                && err.status_code() != StatusCode::TableColumnNotFound
            {
                return PrometheusJsonResponse::error(
                    err.status_code().to_string(),
                    err.output_msg(),
                );
            }
        }
    }

    let _ = labels.remove(TIMESTAMP_COLUMN_NAME);
    let _ = labels.remove(FIELD_COLUMN_NAME);

    let mut sorted_labels: Vec<String> = labels.into_iter().collect();
    sorted_labels.sort();
    PrometheusJsonResponse::success(PrometheusResponse::Labels(sorted_labels))
}

async fn get_all_column_names(
    catalog: &str,
    schema: &str,
    manager: &CatalogManagerRef,
) -> std::result::Result<Vec<String>, catalog::error::Error> {
    let table_names = manager.table_names(catalog, schema).await?;

    let mut labels = HashSet::new();
    for table_name in table_names {
        let Some(table) = manager.table(catalog, schema, &table_name).await? else {
            continue;
        };
        let schema = table.schema();
        for column in schema.column_schemas() {
            labels.insert(column.name.to_string());
        }
    }

    let mut labels_vec = labels.into_iter().collect::<Vec<_>>();
    labels_vec.sort_unstable();
    Ok(labels_vec)
}

async fn retrieve_series_from_query_result(
    result: Result<Output>,
    series: &mut Vec<HashMap<String, String>>,
    table_name: &str,
) -> Result<()> {
    match result? {
        Output::RecordBatches(batches) => {
            record_batches_to_series(batches, series, table_name)?;
            Ok(())
        }
        Output::Stream(stream) => {
            let batches = RecordBatches::try_collect(stream)
                .await
                .context(CollectRecordbatchSnafu)?;
            record_batches_to_series(batches, series, table_name)?;
            Ok(())
        }
        Output::AffectedRows(_) => Err(Error::UnexpectedResult {
            reason: "expected data result, but got affected rows".to_string(),
            location: Location::default(),
        }),
    }
}

/// Retrieve labels name from query result
async fn retrieve_labels_name_from_query_result(
    result: Result<Output>,
    labels: &mut HashSet<String>,
) -> Result<()> {
    match result? {
        Output::RecordBatches(batches) => {
            record_batches_to_labels_name(batches, labels)?;
            Ok(())
        }
        Output::Stream(stream) => {
            let batches = RecordBatches::try_collect(stream)
                .await
                .context(CollectRecordbatchSnafu)?;
            record_batches_to_labels_name(batches, labels)?;
            Ok(())
        }
        Output::AffectedRows(_) => UnexpectedResultSnafu {
            reason: "expected data result, but got affected rows".to_string(),
        }
        .fail(),
    }
}

fn record_batches_to_series(
    batches: RecordBatches,
    series: &mut Vec<HashMap<String, String>>,
    table_name: &str,
) -> Result<()> {
    for batch in batches.iter() {
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

fn promql_expr_to_metric_name(expr: &PromqlExpr) -> Option<String> {
    match expr {
        PromqlExpr::Aggregate(AggregateExpr { expr, .. }) => promql_expr_to_metric_name(expr),
        PromqlExpr::Unary(UnaryExpr { expr }) => promql_expr_to_metric_name(expr),
        PromqlExpr::Binary(BinaryExpr { lhs, rhs, .. }) => {
            promql_expr_to_metric_name(lhs).or(promql_expr_to_metric_name(rhs))
        }
        PromqlExpr::Paren(ParenExpr { expr }) => promql_expr_to_metric_name(expr),
        PromqlExpr::Subquery(SubqueryExpr { expr, .. }) => promql_expr_to_metric_name(expr),
        PromqlExpr::NumberLiteral(_) => Some(String::new()),
        PromqlExpr::StringLiteral(_) => Some(String::new()),
        PromqlExpr::Extension(_) => None,
        PromqlExpr::VectorSelector(VectorSelector { name, matchers, .. }) => {
            name.clone().or(matchers.find_matcher(METRIC_NAME))
        }
        PromqlExpr::MatrixSelector(MatrixSelector { vs, .. }) => {
            let VectorSelector { name, matchers, .. } = vs;
            name.clone().or(matchers.find_matcher(METRIC_NAME))
        }
        PromqlExpr::Call(Call { args, .. }) => {
            args.args.iter().find_map(|e| promql_expr_to_metric_name(e))
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct LabelValueQuery {
    start: Option<String>,
    end: Option<String>,
    #[serde(flatten)]
    matches: Matches,
    db: Option<String>,
}

#[axum_macros::debug_handler]
pub async fn label_values_query(
    State(handler): State<PrometheusHandlerRef>,
    Path(label_name): Path<String>,
    Extension(query_ctx): Extension<QueryContextRef>,
    Query(params): Query<LabelValueQuery>,
) -> Json<PrometheusJsonResponse> {
    let _timer = crate::metrics::METRIC_HTTP_PROMQL_LABEL_VALUE_QUERY_ELAPSED.start_timer();

    let db = &params.db.unwrap_or(DEFAULT_SCHEMA_NAME.to_string());
    let (catalog, schema) = parse_catalog_and_schema_from_db_string(db);

    if label_name == METRIC_NAME_LABEL {
        let mut table_names = match handler.catalog_manager().table_names(catalog, schema).await {
            Ok(table_names) => table_names,
            Err(e) => {
                return PrometheusJsonResponse::error(e.status_code().to_string(), e.output_msg());
            }
        };
        table_names.sort_unstable();
        return PrometheusJsonResponse::success(PrometheusResponse::LabelValues(table_names));
    }

    let queries = params.matches.0;
    if queries.is_empty() {
        return PrometheusJsonResponse::error("Invalid argument", "match[] parameter is required");
    }

    let start = params.start.unwrap_or_else(yesterday_rfc3339);
    let end = params.end.unwrap_or_else(current_time_rfc3339);

    let mut label_values = HashSet::new();

    for query in queries {
        let prom_query = PromQuery {
            query,
            start: start.clone(),
            end: end.clone(),
            step: DEFAULT_LOOKBACK_STRING.to_string(),
        };
        let result = handler.do_query(&prom_query, query_ctx.clone()).await;
        let result = retrieve_label_values(result, &label_name, &mut label_values).await;
        if let Err(err) = result {
            // Prometheus won't report error if querying nonexist label and metric
            if err.status_code() != StatusCode::TableNotFound
                && err.status_code() != StatusCode::TableColumnNotFound
            {
                return PrometheusJsonResponse::error(
                    err.status_code().to_string(),
                    err.output_msg(),
                );
            }
        }
    }

    let mut label_values: Vec<_> = label_values.into_iter().collect();
    label_values.sort();
    PrometheusJsonResponse::success(PrometheusResponse::LabelValues(label_values))
}

async fn retrieve_label_values(
    result: Result<Output>,
    label_name: &str,
    labels_values: &mut HashSet<String>,
) -> Result<()> {
    match result? {
        Output::RecordBatches(batches) => {
            retrieve_label_values_from_record_batch(batches, label_name, labels_values).await
        }
        Output::Stream(stream) => {
            let batches = RecordBatches::try_collect(stream)
                .await
                .context(CollectRecordbatchSnafu)?;
            retrieve_label_values_from_record_batch(batches, label_name, labels_values).await
        }
        Output::AffectedRows(_) => UnexpectedResultSnafu {
            reason: "expected data result, but got affected rows".to_string(),
        }
        .fail(),
    }
}

async fn retrieve_label_values_from_record_batch(
    batches: RecordBatches,
    label_name: &str,
    labels_values: &mut HashSet<String>,
) -> Result<()> {
    let Some(label_col_idx) = batches.schema().column_index_by_name(label_name) else {
        return Ok(());
    };

    // check whether label_name belongs to tag column
    match batches
        .schema()
        .column_schema_by_name(label_name)
        .unwrap()
        .data_type
    {
        ConcreteDataType::String(_) => {}
        _ => return Ok(()),
    }
    for batch in batches.iter() {
        let label_column = batch
            .column(label_col_idx)
            .as_any()
            .downcast_ref::<StringVector>()
            .unwrap();
        for row_index in 0..batch.num_rows() {
            if let Some(label_value) = label_column.get_data(row_index) {
                let _ = labels_values.insert(label_value.to_string());
            }
        }
    }

    Ok(())
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct SeriesQuery {
    start: Option<String>,
    end: Option<String>,
    #[serde(flatten)]
    matches: Matches,
    db: Option<String>,
}

#[axum_macros::debug_handler]
pub async fn series_query(
    State(handler): State<PrometheusHandlerRef>,
    Query(params): Query<SeriesQuery>,
    Extension(query_ctx): Extension<QueryContextRef>,
    Form(form_params): Form<SeriesQuery>,
) -> Json<PrometheusJsonResponse> {
    let _timer = crate::metrics::METRIC_HTTP_PROMQL_SERIES_QUERY_ELAPSED.start_timer();
    let mut queries: Vec<String> = params.matches.0;
    if queries.is_empty() {
        queries = form_params.matches.0;
    }
    if queries.is_empty() {
        return PrometheusJsonResponse::error("Unsupported", "match[] parameter is required");
    }
    let start = params
        .start
        .or(form_params.start)
        .unwrap_or_else(yesterday_rfc3339);
    let end = params
        .end
        .or(form_params.end)
        .unwrap_or_else(current_time_rfc3339);

    let mut series = Vec::new();
    for query in queries {
        let table_name = query.clone();
        let prom_query = PromQuery {
            query,
            start: start.clone(),
            end: end.clone(),
            // TODO: find a better value for step
            step: DEFAULT_LOOKBACK_STRING.to_string(),
        };
        let result = handler.do_query(&prom_query, query_ctx.clone()).await;
        if let Err(err) = retrieve_series_from_query_result(result, &mut series, &table_name).await
        {
            return PrometheusJsonResponse::error(err.status_code().to_string(), err.output_msg());
        }
    }
    PrometheusJsonResponse::success(PrometheusResponse::Series(series))
}
