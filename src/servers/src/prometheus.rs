//! promethues protcol supportings
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
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
use snafu::{OptionExt, ResultExt};
use snap::raw::{Decoder, Encoder};

use crate::error::{self, Result};

const TIMESTAMP_COLUMN_NAME: &str = "greptime_timestamp";
const VALUE_COLUMN_NAME: &str = "greptime_value";
pub const METRIC_NAME_LABEL: &str = "__name__";

/// Metrics for push gateway protocol
pub struct Metrics {
    pub exposition: MetricsExposition<PrometheusType, PrometheusValue>,
}

/// Generate a sql from a remote request query
/// TODO(dennis): maybe use logical plan in future to prevent sql injection
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
        .context(error::InvalidPromRemoteRequestSnafu {
            msg: "missing '__name__' label in timeseries",
        })?;

    let mut conditions: Vec<String> = Vec::with_capacity(label_matches.len());

    conditions.push(format!(
        "{}>={} AND {}<={}",
        TIMESTAMP_COLUMN_NAME, start_timestamp_ms, TIMESTAMP_COLUMN_NAME, end_timestamp_ms,
    ));

    for m in label_matches {
        let name = &m.name;

        if name == METRIC_NAME_LABEL {
            continue;
        }

        let value = &m.value;
        let m_type =
            MatcherType::from_i32(m.r#type).context(error::InvalidPromRemoteRequestSnafu {
                msg: format!("invaid LabelMatcher type: {}", m.r#type),
            })?;

        match m_type {
            MatcherType::Eq => {
                conditions.push(format!("{}='{}'", name, value));
            }
            MatcherType::Neq => {
                conditions.push(format!("{}!='{}'", name, value));
            }
            // Case senstive regexp match
            MatcherType::Re => {
                conditions.push(format!("{}~'{}'", name, value));
            }
            // Case senstive regexp not match
            MatcherType::Nre => {
                conditions.push(format!("{}!~'{}'", name, value));
            }
        }
    }

    let conditions = conditions.join(" AND ");

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

/// For Sorting timeseries
impl Ord for TimeSeriesId {
    fn cmp(&self, other: &Self) -> Ordering {
        let ordering = self.labels.len().cmp(&other.labels.len());
        if ordering != Ordering::Equal {
            return ordering;
        }

        for (l, r) in self.labels.iter().zip(other.labels.iter()) {
            let ordering = l.name.cmp(&r.name);

            if ordering != Ordering::Equal {
                return ordering;
            }

            let ordering = l.value.cmp(&r.value);

            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    }
}

impl PartialOrd for TimeSeriesId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Collect each row's timeseries id
/// This processing is ugly, hope https://github.com/GreptimeTeam/greptimedb/issues/336 making some progress in future.
fn collect_timeseries_ids(
    table_name: &str,
    row_count: usize,
    columns: &[Column],
) -> Vec<TimeSeriesId> {
    let mut timeseries_ids = Vec::with_capacity(row_count);

    let mut columns_rows = vec![0; columns.len()];

    for row in 0..row_count {
        let mut labels = Vec::with_capacity(columns.len() - 1);

        labels.push(new_label(
            METRIC_NAME_LABEL.to_string(),
            table_name.to_string(),
        ));

        for (i, column) in columns.iter().enumerate() {
            let column_name = &column.column_name;
            let null_mask = &column.null_mask;
            let values = &column.values;

            if column_name == VALUE_COLUMN_NAME || column_name == TIMESTAMP_COLUMN_NAME {
                continue;
            }

            // A label with an empty label value is considered equivalent to a label that does not exist.
            if !null_mask.is_empty() && null_mask[row] == 0 {
                continue;
            }

            let row = columns_rows[i];
            columns_rows[i] += 1;

            let column_value = values.as_ref().map(|vs| vs.string_values[row].to_string());
            if let Some(value) = column_value {
                labels.push(new_label(column_name.to_string(), value));
            }
        }
        timeseries_ids.push(TimeSeriesId { labels });
    }
    timeseries_ids
}

pub fn select_result_to_timeseries(
    table_name: &str,
    select_result: SelectResult,
) -> Result<Vec<TimeSeries>> {
    let row_count = select_result.row_count as usize;
    let columns = select_result.columns;
    let ts_column = columns
        .iter()
        .find(|c| c.column_name == TIMESTAMP_COLUMN_NAME)
        .context(error::InvalidPromRemoteReadQueryResultSnafu {
            msg: "missing greptime_timestamp column in query result",
        })?;

    let value_column = columns
        .iter()
        .find(|c| c.column_name == VALUE_COLUMN_NAME)
        .context(error::InvalidPromRemoteReadQueryResultSnafu {
            msg: "missing greptime_value column in query result",
        })?;
    // First, collect each row's timeseries id
    let timeseries_ids = collect_timeseries_ids(table_name, row_count, &columns);
    // Then, group timeseries by it's id.
    let mut timeseries_map: BTreeMap<&TimeSeriesId, TimeSeries> = BTreeMap::default();

    for (row, timeseries_id) in timeseries_ids.iter().enumerate() {
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

    Ok(timeseries_map.into_values().collect())
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
        table_name: table_name.context(error::InvalidPromRemoteRequestSnafu {
            msg: "missing '__name__' label in timeseries",
        })?,

        expr: Some(insert_expr::Expr::Values(insert_expr::Values {
            values: vec![batch.into()],
        })),
        options: HashMap::default(),
    })
}

#[inline]
pub fn snappy_decompress(buf: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = Decoder::new();
    decoder
        .decompress_vec(buf)
        .context(error::DecompressPromRemoteRequestSnafu)
}

#[inline]
pub fn snappy_compress(buf: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = Encoder::new();
    encoder
        .compress_vec(buf)
        .context(error::DecompressPromRemoteRequestSnafu)
}

/// Mock timeseries for test, it is both used in servers and frontend crate
/// So we present it here
pub fn mock_timeseries() -> Vec<TimeSeries> {
    vec![
        TimeSeries {
            labels: vec![
                new_label(METRIC_NAME_LABEL.to_string(), "metric1".to_string()),
                new_label("job".to_string(), "spark".to_string()),
            ],
            samples: vec![
                Sample {
                    value: 1.0f64,
                    timestamp: 1000,
                },
                Sample {
                    value: 2.0f64,
                    timestamp: 2000,
                },
            ],
            ..Default::default()
        },
        TimeSeries {
            labels: vec![
                new_label(METRIC_NAME_LABEL.to_string(), "metric2".to_string()),
                new_label("instance".to_string(), "test_host1".to_string()),
                new_label("idc".to_string(), "z001".to_string()),
            ],
            samples: vec![
                Sample {
                    value: 3.0f64,
                    timestamp: 1000,
                },
                Sample {
                    value: 4.0f64,
                    timestamp: 2000,
                },
            ],
            ..Default::default()
        },
        TimeSeries {
            labels: vec![
                new_label(METRIC_NAME_LABEL.to_string(), "metric3".to_string()),
                new_label("idc".to_string(), "z002".to_string()),
                new_label("app".to_string(), "biz".to_string()),
            ],
            samples: vec![
                Sample {
                    value: 5.0f64,
                    timestamp: 1000,
                },
                Sample {
                    value: 6.0f64,
                    timestamp: 2000,
                },
                Sample {
                    value: 7.0f64,
                    timestamp: 3000,
                },
            ],
            ..Default::default()
        },
    ]
}

#[cfg(test)]
mod tests {
    use api::prometheus::remote::LabelMatcher;

    use super::*;

    const EQ_TYPE: i32 = MatcherType::Eq as i32;
    const NEQ_TYPE: i32 = MatcherType::Neq as i32;
    const RE_TYPE: i32 = MatcherType::Re as i32;

    #[test]
    fn test_query_to_sql() {
        let q = Query {
            start_timestamp_ms: 1000,
            end_timestamp_ms: 2000,
            matchers: vec![],
            ..Default::default()
        };
        let err = query_to_sql(&q).unwrap_err();
        assert!(matches!(err, error::Error::InvalidPromRemoteRequest { .. }));

        let q = Query {
            start_timestamp_ms: 1000,
            end_timestamp_ms: 2000,
            matchers: vec![LabelMatcher {
                name: METRIC_NAME_LABEL.to_string(),
                value: "test".to_string(),
                r#type: EQ_TYPE,
            }],
            ..Default::default()
        };
        let (table, sql) = query_to_sql(&q).unwrap();
        assert_eq!("test", table);
        assert_eq!("select * from test where greptime_timestamp>=1000 AND greptime_timestamp<=2000 order by greptime_timestamp", sql);

        let q = Query {
            start_timestamp_ms: 1000,
            end_timestamp_ms: 2000,
            matchers: vec![
                LabelMatcher {
                    name: METRIC_NAME_LABEL.to_string(),
                    value: "test".to_string(),
                    r#type: EQ_TYPE,
                },
                LabelMatcher {
                    name: "job".to_string(),
                    value: "*prom*".to_string(),
                    r#type: RE_TYPE,
                },
                LabelMatcher {
                    name: "instance".to_string(),
                    value: "localhost".to_string(),
                    r#type: NEQ_TYPE,
                },
            ],
            ..Default::default()
        };
        let (table, sql) = query_to_sql(&q).unwrap();
        assert_eq!("test", table);
        assert_eq!("select * from test where greptime_timestamp>=1000 AND greptime_timestamp<=2000 AND job~'*prom*' AND instance!='localhost' order by greptime_timestamp", sql);
    }

    #[test]
    fn test_write_request_to_insert_exprs() {
        let write_request = WriteRequest {
            timeseries: mock_timeseries(),
            ..Default::default()
        };

        let exprs = write_request_to_insert_exprs(write_request).unwrap();
        assert_eq!(3, exprs.len());
        assert_eq!("metric1", exprs[0].table_name);
        assert_eq!("metric2", exprs[1].table_name);
        assert_eq!("metric3", exprs[2].table_name);

        let values = exprs[0].clone().expr.unwrap();
        match values {
            insert_expr::Expr::Values(insert_expr::Values { values }) => {
                assert_eq!(1, values.len());
                let batch = InsertBatch::try_from(values[0].as_slice()).unwrap();
                assert_eq!(2, batch.row_count);
                let columns = batch.columns;
                assert_eq!(columns.len(), 3);

                assert_eq!(columns[0].column_name, TIMESTAMP_COLUMN_NAME);
                assert_eq!(
                    columns[0].values.as_ref().unwrap().ts_millis_values,
                    vec![1000, 2000]
                );

                assert_eq!(columns[1].column_name, VALUE_COLUMN_NAME);
                assert_eq!(
                    columns[1].values.as_ref().unwrap().f64_values,
                    vec![1.0, 2.0]
                );

                assert_eq!(columns[2].column_name, "job");
                assert_eq!(
                    columns[2].values.as_ref().unwrap().string_values,
                    vec!["spark", "spark"]
                );
            }
            _ => unreachable!(),
        }

        let values = exprs[1].clone().expr.unwrap();
        match values {
            insert_expr::Expr::Values(insert_expr::Values { values }) => {
                assert_eq!(1, values.len());
                let batch = InsertBatch::try_from(values[0].as_slice()).unwrap();
                assert_eq!(2, batch.row_count);
                let columns = batch.columns;
                assert_eq!(columns.len(), 4);

                assert_eq!(columns[0].column_name, TIMESTAMP_COLUMN_NAME);
                assert_eq!(
                    columns[0].values.as_ref().unwrap().ts_millis_values,
                    vec![1000, 2000]
                );

                assert_eq!(columns[1].column_name, VALUE_COLUMN_NAME);
                assert_eq!(
                    columns[1].values.as_ref().unwrap().f64_values,
                    vec![3.0, 4.0]
                );

                assert_eq!(columns[2].column_name, "instance");
                assert_eq!(
                    columns[2].values.as_ref().unwrap().string_values,
                    vec!["test_host1", "test_host1"]
                );
                assert_eq!(columns[3].column_name, "idc");
                assert_eq!(
                    columns[3].values.as_ref().unwrap().string_values,
                    vec!["z001", "z001"]
                );
            }
            _ => unreachable!(),
        }

        let values = exprs[2].clone().expr.unwrap();
        match values {
            insert_expr::Expr::Values(insert_expr::Values { values }) => {
                assert_eq!(1, values.len());
                let batch = InsertBatch::try_from(values[0].as_slice()).unwrap();
                assert_eq!(3, batch.row_count);
                let columns = batch.columns;
                assert_eq!(columns.len(), 4);

                assert_eq!(columns[0].column_name, TIMESTAMP_COLUMN_NAME);
                assert_eq!(
                    columns[0].values.as_ref().unwrap().ts_millis_values,
                    vec![1000, 2000, 3000]
                );

                assert_eq!(columns[1].column_name, VALUE_COLUMN_NAME);
                assert_eq!(
                    columns[1].values.as_ref().unwrap().f64_values,
                    vec![5.0, 6.0, 7.0]
                );

                assert_eq!(columns[2].column_name, "idc");
                assert_eq!(
                    columns[2].values.as_ref().unwrap().string_values,
                    vec!["z002", "z002", "z002"]
                );
                assert_eq!(columns[3].column_name, "app");
                assert_eq!(
                    columns[3].values.as_ref().unwrap().string_values,
                    vec!["biz", "biz", "biz"]
                );
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_select_result_to_timeseries() {
        let select_result = SelectResult {
            row_count: 2,
            columns: vec![
                Column {
                    column_name: TIMESTAMP_COLUMN_NAME.to_string(),
                    values: Some(column::Values {
                        ts_millis_values: vec![1000, 2000],
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                Column {
                    column_name: VALUE_COLUMN_NAME.to_string(),
                    values: Some(column::Values {
                        f64_values: vec![3.0, 7.0],
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                Column {
                    column_name: "instance".to_string(),
                    values: Some(column::Values {
                        string_values: vec!["host1".to_string(), "host2".to_string()],
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ],
        };

        let timeseries = select_result_to_timeseries("metric1", select_result).unwrap();
        assert_eq!(2, timeseries.len());

        assert_eq!(
            vec![
                Label {
                    name: METRIC_NAME_LABEL.to_string(),
                    value: "metric1".to_string(),
                },
                Label {
                    name: "instance".to_string(),
                    value: "host1".to_string(),
                },
            ],
            timeseries[0].labels
        );

        assert_eq!(
            timeseries[0].samples,
            vec![Sample {
                value: 3.0,
                timestamp: 1000,
            }]
        );

        assert_eq!(
            vec![
                Label {
                    name: METRIC_NAME_LABEL.to_string(),
                    value: "metric1".to_string(),
                },
                Label {
                    name: "instance".to_string(),
                    value: "host2".to_string(),
                },
            ],
            timeseries[1].labels
        );
        assert_eq!(
            timeseries[1].samples,
            vec![Sample {
                value: 7.0,
                timestamp: 2000,
            }]
        );
    }
}
