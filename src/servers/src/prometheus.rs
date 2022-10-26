//! promethues protcol supportings
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use api::prometheus::remote::{
    label_matcher::Type as MatcherType, Label, Query, Sample, TimeSeries, WriteRequest,
};
use api::v1::codec::InsertBatch;
use api::v1::{
    codec::SelectResult, column, column::SemanticType, insert_expr, Column, ColumnDataType,
    InsertExpr,
};
use openmetrics_parser::{MetricsExposition, PrometheusType, PrometheusValue};
use snafu::OptionExt;

use crate::error::{self, Result};

const TIMESTAMP_COLUMN_NAME: &str = "greptime_timestamp";
const VALUE_COLUMN_NAME: &str = "greptime_value";
const METRIC_NAME_LABEL: &str = "__name__";

pub struct Metrics {
    pub exposition: MetricsExposition<PrometheusType, PrometheusValue>,
}

/// Generate a sql from a remote request query
/// TODO(dennis): maybe use logical plan in future?
pub fn query_to_sql(q: &Query) -> Result<(String, String)> {
    let start_timestamp_ms = q.start_timestamp_ms;
    let end_timestamp_ms = q.end_timestamp_ms;

    let label_matches = &q.matchers;
    let table_name = label_matches
        .iter()
        .find_map(|m| {
            if m.name == METRIC_NAME_LABEL {
                Some(m.value.to_string())
            } else {
                None
            }
        })
        .context(error::InvalidPromRemoteRequstSnafu {
            msg: "missing '__name__' label in timeseries",
        })?;

    let mut conditions = format!(
        "{}>={} and {}<={}",
        TIMESTAMP_COLUMN_NAME, start_timestamp_ms, TIMESTAMP_COLUMN_NAME, end_timestamp_ms,
    );

    let mut label_conditions: Vec<String> = Vec::with_capacity(label_matches.len());
    for m in label_matches {
        let name = &m.name;

        if name == METRIC_NAME_LABEL {
            continue;
        }

        let value = &m.value;
        let m_type =
            MatcherType::from_i32(m.r#type).context(error::InvalidPromRemoteRequstSnafu {
                msg: format!("invaid LabelMatcher type: {}", m.r#type),
            })?;

        match m_type {
            MatcherType::Eq => {
                label_conditions.push(format!("{}='{}'", name, value));
            }
            MatcherType::Neq => {
                label_conditions.push(format!("{}!='{}'", name, value));
            }
            MatcherType::Re => {
                label_conditions.push(format!("{}~*'{}'", name, value));
            }
            MatcherType::Nre => {
                label_conditions.push(format!("{}!~*'{}'", name, value));
            }
        }
    }

    if !label_conditions.is_empty() {
        conditions.push_str(" and ");
        conditions.push_str(&label_conditions.join(" and "));
    }

    Ok((
        table_name.to_string(),
        format!(
            "select * from {} where {} order by {}",
            table_name, conditions, TIMESTAMP_COLUMN_NAME,
        ),
    ))
}

#[inline]
fn new_label(name: String, value: String) -> Label {
    Label { name, value }
}

// A timeseries id
#[derive(Debug)]
struct TimeSeriesId {
    labels: Vec<Label>,
}

/// Because Label in protobuf doesn't impl `Eq`, so we have to do it by ourselves.
impl PartialEq for TimeSeriesId {
    fn eq(&self, other: &Self) -> bool {
        if self.labels.len() != other.labels.len() {
            return false;
        }

        self.labels
            .iter()
            .zip(other.labels.iter())
            .all(|(l, r)| l.name == r.name && l.value == r.value)
    }
}
impl Eq for TimeSeriesId {}

impl Hash for TimeSeriesId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for label in &self.labels {
            label.name.hash(state);
            label.value.hash(state);
        }
    }
}

pub fn select_result_to_timeseries(
    table_name: &str,
    select_result: SelectResult,
) -> Vec<TimeSeries> {
    let row_count = select_result.row_count as usize;

    if row_count == 0 {
        return vec![];
    }

    let columns = select_result.columns;

    let ts_column = columns
        .iter()
        .find(|c| c.column_name == TIMESTAMP_COLUMN_NAME)
        .unwrap();
    let value_column = columns
        .iter()
        .find(|c| c.column_name == VALUE_COLUMN_NAME)
        .unwrap();

    // This processing is ugly, hope https://github.com/GreptimeTeam/greptimedb/issues/336 making some progress in future.
    // First, collect each row's timeseries id
    let mut timeseries_ids: HashMap<usize, TimeSeriesId> = HashMap::with_capacity(row_count);

    for row in 0..row_count {
        let mut labels = vec![new_label(
            METRIC_NAME_LABEL.to_string(),
            table_name.to_string(),
        )];

        for column in &columns {
            let column_name = &column.column_name;
            let null_mask = &column.null_mask;
            let values = &column.values;

            if column_name == VALUE_COLUMN_NAME || column_name == TIMESTAMP_COLUMN_NAME {
                continue;
            }

            // TODO(dennis): if the value is missing, return an empty string is correct?
            if !null_mask.is_empty() && null_mask[row] == 0 {
                labels.push(new_label(column_name.to_string(), "".to_string()));
            } else {
                labels.push(new_label(
                    column_name.to_string(),
                    values
                        .as_ref()
                        .map(|vs| vs.string_values[row].to_string())
                        .unwrap_or_else(|| "".to_string()),
                ));
            }
        }
        timeseries_ids.insert(row, TimeSeriesId { labels });
    }

    // Then, group timeseries by it's id.
    let mut timeseries_map: HashMap<&TimeSeriesId, TimeSeries> = HashMap::default();

    for row in 0..row_count {
        // It's safe to unwrap, it should be exists.
        let timeseries_id = timeseries_ids.get(&row).unwrap();

        let timeseries = timeseries_map
            .entry(timeseries_id)
            .or_insert_with(|| TimeSeries {
                labels: timeseries_id.labels.clone(),
                ..Default::default()
            });

        let sample = Sample {
            value: value_column
                .values
                .as_ref()
                .map(|vs| vs.f64_values[row])
                .unwrap_or(0.0f64),
            timestamp: ts_column
                .values
                .as_ref()
                .map(|vs| vs.ts_millis_values[row])
                .unwrap_or(0i64),
        };

        timeseries.samples.push(sample);
    }

    timeseries_map.values().cloned().collect()
}

/// Cast a remote write request into gRPC's InsertExpr.
pub fn write_request_to_insert_exprs(mut request: WriteRequest) -> Result<Vec<InsertExpr>> {
    let timeseries = std::mem::take(&mut request.timeseries);

    timeseries
        .into_iter()
        .map(timeseries_to_insert_expr)
        .collect()
}

fn timeseries_to_insert_expr(mut timeseries: TimeSeries) -> Result<InsertExpr> {
    // TODO(dennis): save exemplars into a column
    let labels = std::mem::take(&mut timeseries.labels);
    let samples = std::mem::take(&mut timeseries.samples);

    let row_count = samples.len();
    let mut columns = Vec::with_capacity(2 + labels.len());

    let ts_column = Column {
        column_name: TIMESTAMP_COLUMN_NAME.to_string(),
        values: Some(column::Values {
            ts_millis_values: samples.iter().map(|x| x.timestamp).collect(),
            ..Default::default()
        }),
        semantic_type: SemanticType::Timestamp as i32,
        datatype: ColumnDataType::Timestamp as i32,
        ..Default::default()
    };
    columns.push(ts_column);

    let value_column = Column {
        column_name: VALUE_COLUMN_NAME.to_string(),
        values: Some(column::Values {
            f64_values: samples.iter().map(|x| x.value).collect(),
            ..Default::default()
        }),
        semantic_type: SemanticType::Field as i32,
        datatype: ColumnDataType::Float64 as i32,
        ..Default::default()
    };
    columns.push(value_column);

    let mut table_name = None;

    for label in labels {
        let tagk = label.name;
        let tagv = label.value;

        // The metric name is a special label
        if tagk == METRIC_NAME_LABEL {
            table_name = Some(tagv);
            continue;
        }

        columns.push(Column {
            column_name: tagk.to_string(),
            values: Some(column::Values {
                string_values: std::iter::repeat(tagv).take(row_count).collect(),
                ..Default::default()
            }),
            semantic_type: SemanticType::Tag as i32,
            datatype: ColumnDataType::String as i32,
            ..Default::default()
        });
    }

    let batch = InsertBatch {
        columns,
        row_count: row_count as u32,
    };
    Ok(InsertExpr {
        table_name: table_name.context(error::InvalidPromRemoteRequstSnafu {
            msg: "missing '__name__' label in timeseries",
        })?,

        expr: Some(insert_expr::Expr::Values(insert_expr::Values {
            values: vec![batch.into()],
        })),
        options: HashMap::default(),
    })
}
