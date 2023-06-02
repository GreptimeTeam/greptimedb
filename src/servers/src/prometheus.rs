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

//! prometheus protocol supportings
//! handles prometheus remote_write, remote_read logic
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

use api::prometheus::remote::label_matcher::Type as MatcherType;
use api::prometheus::remote::{Label, Query, Sample, TimeSeries, WriteRequest};
use api::v1::column::SemanticType;
use api::v1::{column, Column, ColumnDataType, InsertRequest as GrpcInsertRequest, InsertRequests};
use common_recordbatch::{RecordBatch, RecordBatches};
use common_time::timestamp::TimeUnit;
use datatypes::prelude::{ConcreteDataType, Value};
use openmetrics_parser::{MetricsExposition, PrometheusType, PrometheusValue};
use snafu::{ensure, OptionExt, ResultExt};
use snap::raw::{Decoder, Encoder};

use crate::error::{self, Result};

pub const TIMESTAMP_COLUMN_NAME: &str = "greptime_timestamp";
pub const FIELD_COLUMN_NAME: &str = "greptime_value";
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
        "{TIMESTAMP_COLUMN_NAME}>={start_timestamp_ms} AND {TIMESTAMP_COLUMN_NAME}<={end_timestamp_ms}",
    ));

    for m in label_matches {
        let name = &m.name;

        if name == METRIC_NAME_LABEL {
            continue;
        }

        let value = &m.value;
        let m_type =
            MatcherType::from_i32(m.r#type).context(error::InvalidPromRemoteRequestSnafu {
                msg: format!("invalid LabelMatcher type: {}", m.r#type),
            })?;

        match m_type {
            MatcherType::Eq => {
                conditions.push(format!("{name}='{value}'"));
            }
            MatcherType::Neq => {
                conditions.push(format!("{name}!='{value}'"));
            }
            // Case sensitive regexp match
            MatcherType::Re => {
                conditions.push(format!("{name}~'{value}'"));
            }
            // Case sensitive regexp not match
            MatcherType::Nre => {
                conditions.push(format!("{name}!~'{value}'"));
            }
        }
    }

    let conditions = conditions.join(" AND ");

    Ok((
        table_name.to_string(),
        format!("select * from {table_name} where {conditions} order by {TIMESTAMP_COLUMN_NAME}",),
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
fn collect_timeseries_ids(table_name: &str, recordbatch: &RecordBatch) -> Vec<TimeSeriesId> {
    let row_count = recordbatch.num_rows();
    let mut timeseries_ids = Vec::with_capacity(row_count);

    for row in 0..row_count {
        let mut labels = Vec::with_capacity(recordbatch.num_columns() - 1);
        labels.push(new_label(
            METRIC_NAME_LABEL.to_string(),
            table_name.to_string(),
        ));

        for (i, column_schema) in recordbatch.schema.column_schemas().iter().enumerate() {
            if column_schema.name == FIELD_COLUMN_NAME
                || column_schema.name == TIMESTAMP_COLUMN_NAME
            {
                continue;
            }

            let column = &recordbatch.columns()[i];
            // A label with an empty label value is considered equivalent to a label that does not exist.
            if column.is_null(row) {
                continue;
            }

            let value = column.get(row).to_string();
            labels.push(new_label(column_schema.name.clone(), value));
        }
        timeseries_ids.push(TimeSeriesId { labels });
    }
    timeseries_ids
}

pub fn recordbatches_to_timeseries(
    table_name: &str,
    recordbatches: RecordBatches,
) -> Result<Vec<TimeSeries>> {
    Ok(recordbatches
        .take()
        .into_iter()
        .map(|x| recordbatch_to_timeseries(table_name, x))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect())
}

fn recordbatch_to_timeseries(table: &str, recordbatch: RecordBatch) -> Result<Vec<TimeSeries>> {
    let ts_column = recordbatch.column_by_name(TIMESTAMP_COLUMN_NAME).context(
        error::InvalidPromRemoteReadQueryResultSnafu {
            msg: "missing greptime_timestamp column in query result",
        },
    )?;
    ensure!(
        ts_column.data_type() == ConcreteDataType::timestamp_millisecond_datatype(),
        error::InvalidPromRemoteReadQueryResultSnafu {
            msg: format!(
                "Expect timestamp column of datatype Timestamp(Millisecond), actual {:?}",
                ts_column.data_type()
            )
        }
    );

    let field_column = recordbatch.column_by_name(FIELD_COLUMN_NAME).context(
        error::InvalidPromRemoteReadQueryResultSnafu {
            msg: "missing greptime_value column in query result",
        },
    )?;
    ensure!(
        field_column.data_type() == ConcreteDataType::float64_datatype(),
        error::InvalidPromRemoteReadQueryResultSnafu {
            msg: format!(
                "Expect value column of datatype Float64, actual {:?}",
                field_column.data_type()
            )
        }
    );

    // First, collect each row's timeseries id
    let timeseries_ids = collect_timeseries_ids(table, &recordbatch);
    // Then, group timeseries by it's id.
    let mut timeseries_map: BTreeMap<&TimeSeriesId, TimeSeries> = BTreeMap::default();

    for (row, timeseries_id) in timeseries_ids.iter().enumerate() {
        let timeseries = timeseries_map
            .entry(timeseries_id)
            .or_insert_with(|| TimeSeries {
                labels: timeseries_id.labels.clone(),
                ..Default::default()
            });

        if ts_column.is_null(row) || field_column.is_null(row) {
            continue;
        }

        let value: f64 = match field_column.get(row) {
            Value::Float64(value) => value.into(),
            _ => unreachable!("checked by the \"ensure\" above"),
        };
        let timestamp = match ts_column.get(row) {
            Value::Timestamp(t) if t.unit() == TimeUnit::Millisecond => t.value(),
            _ => unreachable!("checked by the \"ensure\" above"),
        };
        let sample = Sample { value, timestamp };

        timeseries.samples.push(sample);
    }

    Ok(timeseries_map.into_values().collect())
}

pub fn to_grpc_insert_requests(request: WriteRequest) -> Result<(InsertRequests, usize)> {
    let (inserts, samples_counts) = itertools::process_results(
        request.timeseries.into_iter().map(to_grpc_insert_request),
        |x| x.unzip::<_, _, Vec<_>, Vec<_>>(),
    )?;
    Ok((
        InsertRequests { inserts },
        samples_counts.into_iter().sum::<usize>(),
    ))
}

fn to_grpc_insert_request(timeseries: TimeSeries) -> Result<(GrpcInsertRequest, usize)> {
    let samples_count = timeseries.samples.len();

    // TODO(dennis): save exemplars into a column
    let labels = timeseries.labels;
    let samples = timeseries.samples;

    let row_count = samples.len();
    let mut columns = Vec::with_capacity(2 + labels.len());

    let ts_column = Column {
        column_name: TIMESTAMP_COLUMN_NAME.to_string(),
        values: Some(column::Values {
            ts_millisecond_values: samples.iter().map(|x| x.timestamp).collect(),
            ..Default::default()
        }),
        semantic_type: SemanticType::Timestamp as i32,
        datatype: ColumnDataType::TimestampMillisecond as i32,
        ..Default::default()
    };
    columns.push(ts_column);

    let field_column = Column {
        column_name: FIELD_COLUMN_NAME.to_string(),
        values: Some(column::Values {
            f64_values: samples.iter().map(|x| x.value).collect(),
            ..Default::default()
        }),
        semantic_type: SemanticType::Field as i32,
        datatype: ColumnDataType::Float64 as i32,
        ..Default::default()
    };
    columns.push(field_column);

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

    let request = GrpcInsertRequest {
        table_name: table_name.context(error::InvalidPromRemoteRequestSnafu {
            msg: "missing '__name__' label in timeseries",
        })?,
        region_number: 0,
        columns,
        row_count: row_count as u32,
    };
    Ok((request, samples_count))
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
    use std::sync::Arc;

    use api::prometheus::remote::LabelMatcher;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{Float64Vector, StringVector, TimestampMillisecondVector};

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

        let exprs = to_grpc_insert_requests(write_request).unwrap().0.inserts;
        assert_eq!(3, exprs.len());
        assert_eq!("metric1", exprs[0].table_name);
        assert_eq!("metric2", exprs[1].table_name);
        assert_eq!("metric3", exprs[2].table_name);

        let expr = exprs.get(0).unwrap();

        let columns = &expr.columns;
        let row_count = expr.row_count;

        assert_eq!(2, row_count);
        assert_eq!(columns.len(), 3);

        assert_eq!(columns[0].column_name, TIMESTAMP_COLUMN_NAME);
        assert_eq!(
            columns[0].values.as_ref().unwrap().ts_millisecond_values,
            vec![1000, 2000]
        );

        assert_eq!(columns[1].column_name, FIELD_COLUMN_NAME);
        assert_eq!(
            columns[1].values.as_ref().unwrap().f64_values,
            vec![1.0, 2.0]
        );

        assert_eq!(columns[2].column_name, "job");
        assert_eq!(
            columns[2].values.as_ref().unwrap().string_values,
            vec!["spark", "spark"]
        );

        let expr = exprs.get(1).unwrap();

        let columns = &expr.columns;
        let row_count = expr.row_count;

        assert_eq!(2, row_count);
        assert_eq!(columns.len(), 4);

        assert_eq!(columns[0].column_name, TIMESTAMP_COLUMN_NAME);
        assert_eq!(
            columns[0].values.as_ref().unwrap().ts_millisecond_values,
            vec![1000, 2000]
        );

        assert_eq!(columns[1].column_name, FIELD_COLUMN_NAME);
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

        let expr = exprs.get(2).unwrap();

        let columns = &expr.columns;
        let row_count = expr.row_count;

        assert_eq!(3, row_count);
        assert_eq!(columns.len(), 4);

        assert_eq!(columns[0].column_name, TIMESTAMP_COLUMN_NAME);
        assert_eq!(
            columns[0].values.as_ref().unwrap().ts_millisecond_values,
            vec![1000, 2000, 3000]
        );

        assert_eq!(columns[1].column_name, FIELD_COLUMN_NAME);
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

    #[test]
    fn test_recordbatches_to_timeseries() {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                TIMESTAMP_COLUMN_NAME,
                ConcreteDataType::timestamp_millisecond_datatype(),
                true,
            ),
            ColumnSchema::new(
                FIELD_COLUMN_NAME,
                ConcreteDataType::float64_datatype(),
                true,
            ),
            ColumnSchema::new("instance", ConcreteDataType::string_datatype(), true),
        ]));

        let recordbatches = RecordBatches::try_new(
            schema.clone(),
            vec![
                RecordBatch::new(
                    schema.clone(),
                    vec![
                        Arc::new(TimestampMillisecondVector::from_vec(vec![1000])) as _,
                        Arc::new(Float64Vector::from_vec(vec![3.0])) as _,
                        Arc::new(StringVector::from(vec!["host1"])) as _,
                    ],
                )
                .unwrap(),
                RecordBatch::new(
                    schema,
                    vec![
                        Arc::new(TimestampMillisecondVector::from_vec(vec![2000])) as _,
                        Arc::new(Float64Vector::from_vec(vec![7.0])) as _,
                        Arc::new(StringVector::from(vec!["host2"])) as _,
                    ],
                )
                .unwrap(),
            ],
        )
        .unwrap();

        let timeseries = recordbatches_to_timeseries("metric1", recordbatches).unwrap();
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
