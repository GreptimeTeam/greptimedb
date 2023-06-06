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

//! Prometheus remote read helper functions.
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

use api::prometheus::remote::label_matcher::Type as MatcherType;
use api::prometheus::remote::{Label, Query, Sample, TimeSeries};
use common_recordbatch::{RecordBatch, RecordBatches};
use common_time::timestamp::TimeUnit;
use datatypes::prelude::{ConcreteDataType, Value};
use snafu::{ensure, OptionExt};

use crate::error::{self, Result};
use crate::prometheus::{FIELD_COLUMN_NAME, METRIC_NAME_LABEL, TIMESTAMP_COLUMN_NAME};

/// Generate a sql from a remote request query
/// TODO(dennis): maybe use logical plan in future to prevent sql injection
pub fn query_to_sql(q: &Query) -> Result<(String, String)> {
    let start_timestamp_ms = q.start_timestamp_ms;
    let end_timestamp_ms = q.end_timestamp_ms;

    let label_matches = &q.matchers;

    let name = label_matches.iter().find_map(|m| {
        if m.name == METRIC_NAME_LABEL {
            Some(&m.value)
        } else {
            None
        }
    });

    let table_name = crate::prometheus::find_table_by_name(name);

    let mut conditions: Vec<String> = Vec::with_capacity(label_matches.len());

    for m in label_matches {
        let name = &m.name;

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

    conditions.push(format!(
        "{TIMESTAMP_COLUMN_NAME}>={start_timestamp_ms} AND {TIMESTAMP_COLUMN_NAME}<={end_timestamp_ms}",
    ));

    let conditions = conditions.join(" AND ");
    let sql =
        format!("select * from {table_name} where {conditions} order by {TIMESTAMP_COLUMN_NAME}");

    Ok((table_name, sql))
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

fn recordbatch_to_timeseries(_table: &str, recordbatch: RecordBatch) -> Result<Vec<TimeSeries>> {
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
    let timeseries_ids = collect_timeseries_ids(&recordbatch);
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
fn collect_timeseries_ids(recordbatch: &RecordBatch) -> Vec<TimeSeriesId> {
    let row_count = recordbatch.num_rows();
    let mut timeseries_ids = Vec::with_capacity(row_count);

    for row in 0..row_count {
        let mut labels = Vec::with_capacity(recordbatch.num_columns());

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

#[inline]
pub fn new_label(name: String, value: String) -> Label {
    Label { name, value }
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
        let (_, sql) = query_to_sql(&q).unwrap();
        assert_eq!("select * from prometheus_metrics where greptime_timestamp>=1000 AND greptime_timestamp<=2000 order by greptime_timestamp", sql);

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
        let (_, sql) = query_to_sql(&q).unwrap();
        assert_eq!("select * from prometheus_metrics where __name__='test' AND greptime_timestamp>=1000 AND greptime_timestamp<=2000 order by greptime_timestamp", sql);

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
        let (_, sql) = query_to_sql(&q).unwrap();
        assert_eq!("select * from prometheus_metrics where __name__='test' AND job~'*prom*' AND instance!='localhost' AND greptime_timestamp>=1000 AND greptime_timestamp<=2000 order by greptime_timestamp", sql);
    }

    #[test]
    fn test_recordbatches_to_timeseries() {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(METRIC_NAME_LABEL, ConcreteDataType::string_datatype(), true),
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
                        Arc::new(StringVector::from(vec!["metric1"])) as _,
                        Arc::new(TimestampMillisecondVector::from_vec(vec![1000])) as _,
                        Arc::new(Float64Vector::from_vec(vec![3.0])) as _,
                        Arc::new(StringVector::from(vec!["host1"])) as _,
                    ],
                )
                .unwrap(),
                RecordBatch::new(
                    schema,
                    vec![
                        Arc::new(StringVector::from(vec!["metric2"])) as _,
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
                    value: "metric2".to_string(),
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
