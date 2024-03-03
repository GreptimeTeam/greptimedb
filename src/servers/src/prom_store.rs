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

use api::prom_store::remote::label_matcher::Type as MatcherType;
use api::prom_store::remote::{Label, Query, Sample, TimeSeries, WriteRequest};
use api::v1::RowInsertRequests;
use common_query::prelude::{GREPTIME_TIMESTAMP, GREPTIME_VALUE};
use common_recordbatch::{RecordBatch, RecordBatches};
use common_time::timestamp::TimeUnit;
use datafusion::prelude::{col, lit, regexp_match, Expr};
use datafusion_common::ScalarValue;
use datatypes::prelude::{ConcreteDataType, Value};
use openmetrics_parser::{MetricsExposition, PrometheusType, PrometheusValue};
use query::dataframe::DataFrame;
use query::plan::LogicalPlan;
use snafu::{ensure, OptionExt, ResultExt};
use snap::raw::{Decoder, Encoder};

use crate::error::{self, Result};
use crate::row_writer::{self, MultiTableData};

pub const METRIC_NAME_LABEL: &str = "__name__";

/// Metrics for push gateway protocol
pub struct Metrics {
    pub exposition: MetricsExposition<PrometheusType, PrometheusValue>,
}

/// Get table name from remote query
pub fn table_name(q: &Query) -> Result<String> {
    let label_matches = &q.matchers;

    label_matches
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
        })
}

/// Create a DataFrame from a remote Query
pub fn query_to_plan(dataframe: DataFrame, q: &Query) -> Result<LogicalPlan> {
    let DataFrame::DataFusion(dataframe) = dataframe;

    let start_timestamp_ms = q.start_timestamp_ms;
    let end_timestamp_ms = q.end_timestamp_ms;

    let label_matches = &q.matchers;

    let mut conditions = Vec::with_capacity(label_matches.len() + 1);

    conditions.push(col(GREPTIME_TIMESTAMP).gt_eq(lit_timestamp_millisecond(start_timestamp_ms)));
    conditions.push(col(GREPTIME_TIMESTAMP).lt_eq(lit_timestamp_millisecond(end_timestamp_ms)));

    for m in label_matches {
        let name = &m.name;

        if name == METRIC_NAME_LABEL {
            continue;
        }

        let value = &m.value;
        let m_type = MatcherType::try_from(m.r#type).map_err(|e| {
            error::InvalidPromRemoteRequestSnafu {
                msg: format!("invalid LabelMatcher type, decode error: {e}",),
            }
            .build()
        })?;

        match m_type {
            MatcherType::Eq => {
                conditions.push(col(name).eq(lit(value)));
            }
            MatcherType::Neq => {
                conditions.push(col(name).not_eq(lit(value)));
            }
            // Case sensitive regexp match
            MatcherType::Re => {
                conditions.push(regexp_match(vec![col(name), lit(value)]).is_not_null());
            }
            // Case sensitive regexp not match
            MatcherType::Nre => {
                conditions.push(regexp_match(vec![col(name), lit(value)]).is_null());
            }
        }
    }

    // Safety: conditions MUST not be empty, reduce always return Some(expr).
    let conditions = conditions.into_iter().reduce(Expr::and).unwrap();

    let dataframe = dataframe
        .filter(conditions)
        .context(error::DataFrameSnafu)?;

    Ok(LogicalPlan::DfPlan(dataframe.into_parts().1))
}

#[inline]
fn new_label(name: String, value: String) -> Label {
    Label { name, value }
}

fn lit_timestamp_millisecond(ts: i64) -> Expr {
    Expr::Literal(ScalarValue::TimestampMillisecond(Some(ts), None))
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
/// This processing is ugly, hope <https://github.com/GreptimeTeam/greptimedb/issues/336> making some progress in future.
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
            if column_schema.name == GREPTIME_VALUE || column_schema.name == GREPTIME_TIMESTAMP {
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
    let ts_column = recordbatch.column_by_name(GREPTIME_TIMESTAMP).context(
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

    let field_column = recordbatch.column_by_name(GREPTIME_VALUE).context(
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

pub fn to_grpc_row_insert_requests(request: WriteRequest) -> Result<(RowInsertRequests, usize)> {
    let _timer = crate::metrics::METRIC_HTTP_PROM_STORE_CONVERT_ELAPSED.start_timer();

    let mut multi_table_data = MultiTableData::new();

    for series in request.timeseries {
        let table_name = &series
            .labels
            .iter()
            .find(|label| {
                // The metric name is a special label
                label.name == METRIC_NAME_LABEL
            })
            .context(error::InvalidPromRemoteRequestSnafu {
                msg: "missing '__name__' label in time-series",
            })?
            .value;

        // The metric name is a special label,
        // num_columns = labels.len() - 1 + 1 (value) + 1 (timestamp)
        let num_columns = series.labels.len() + 1;

        let table_data = multi_table_data.get_or_default_table_data(
            table_name,
            num_columns,
            series.samples.len(),
        );

        // labels
        let kvs = series.labels.into_iter().filter_map(|label| {
            if label.name == METRIC_NAME_LABEL {
                None
            } else {
                Some((label.name, label.value))
            }
        });

        if series.samples.len() == 1 {
            let mut one_row = table_data.alloc_one_row();

            row_writer::write_tags(table_data, kvs, &mut one_row)?;
            // value
            row_writer::write_f64(
                table_data,
                GREPTIME_VALUE,
                series.samples[0].value,
                &mut one_row,
            )?;
            // timestamp
            row_writer::write_ts_millis(
                table_data,
                GREPTIME_TIMESTAMP,
                Some(series.samples[0].timestamp),
                &mut one_row,
            )?;

            table_data.add_row(one_row);
        } else {
            for Sample { value, timestamp } in &series.samples {
                let mut one_row = table_data.alloc_one_row();

                // labels
                let kvs = kvs.clone();
                row_writer::write_tags(table_data, kvs, &mut one_row)?;
                // value
                row_writer::write_f64(table_data, GREPTIME_VALUE, *value, &mut one_row)?;
                // timestamp
                row_writer::write_ts_millis(
                    table_data,
                    GREPTIME_TIMESTAMP,
                    Some(*timestamp),
                    &mut one_row,
                )?;

                table_data.add_row(one_row);
            }
        }
    }

    Ok(multi_table_data.into_row_insert_requests())
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
        .context(error::CompressPromRemoteRequestSnafu)
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

    use api::prom_store::remote::LabelMatcher;
    use api::v1::{ColumnDataType, Row, SemanticType};
    use datafusion::prelude::SessionContext;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{Float64Vector, StringVector, TimestampMillisecondVector};
    use table::table::adapter::DfTableProviderAdapter;
    use table::test_util::MemTable;

    use super::*;

    const EQ_TYPE: i32 = MatcherType::Eq as i32;
    const NEQ_TYPE: i32 = MatcherType::Neq as i32;
    const RE_TYPE: i32 = MatcherType::Re as i32;

    #[test]
    fn test_table_name() {
        let q = Query {
            start_timestamp_ms: 1000,
            end_timestamp_ms: 2000,
            matchers: vec![],
            ..Default::default()
        };
        let err = table_name(&q).unwrap_err();
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
        assert_eq!("test", table_name(&q).unwrap());
    }

    #[test]
    fn test_query_to_plan() {
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

        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                GREPTIME_TIMESTAMP,
                ConcreteDataType::timestamp_millisecond_datatype(),
                true,
            ),
            ColumnSchema::new(GREPTIME_VALUE, ConcreteDataType::float64_datatype(), true),
            ColumnSchema::new("instance", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("job", ConcreteDataType::string_datatype(), true),
        ]));
        let recordbatch = RecordBatch::new(
            schema,
            vec![
                Arc::new(TimestampMillisecondVector::from_vec(vec![1000])) as _,
                Arc::new(Float64Vector::from_vec(vec![3.0])) as _,
                Arc::new(StringVector::from(vec!["host1"])) as _,
                Arc::new(StringVector::from(vec!["job"])) as _,
            ],
        )
        .unwrap();

        let ctx = SessionContext::new();
        let table = MemTable::table("test", recordbatch);
        let table_provider = Arc::new(DfTableProviderAdapter::new(table));

        let dataframe = ctx.read_table(table_provider.clone()).unwrap();
        let plan = query_to_plan(DataFrame::DataFusion(dataframe), &q).unwrap();
        let display_string = format!("{}", plan.display_indent());

        assert_eq!("Filter: ?table?.greptime_timestamp >= TimestampMillisecond(1000, None) AND ?table?.greptime_timestamp <= TimestampMillisecond(2000, None)\n  TableScan: ?table?", display_string);

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

        let dataframe = ctx.read_table(table_provider).unwrap();
        let plan = query_to_plan(DataFrame::DataFusion(dataframe), &q).unwrap();
        let display_string = format!("{}", plan.display_indent());

        assert_eq!("Filter: ?table?.greptime_timestamp >= TimestampMillisecond(1000, None) AND ?table?.greptime_timestamp <= TimestampMillisecond(2000, None) AND regexp_match(?table?.job, Utf8(\"*prom*\")) IS NOT NULL AND ?table?.instance != Utf8(\"localhost\")\n  TableScan: ?table?", display_string);
    }

    fn column_schemas_with(
        mut kts_iter: Vec<(&str, ColumnDataType, SemanticType)>,
    ) -> Vec<api::v1::ColumnSchema> {
        kts_iter.push((
            "greptime_value",
            ColumnDataType::Float64,
            SemanticType::Field,
        ));
        kts_iter.push((
            "greptime_timestamp",
            ColumnDataType::TimestampMillisecond,
            SemanticType::Timestamp,
        ));

        kts_iter
            .into_iter()
            .map(|(k, t, s)| api::v1::ColumnSchema {
                column_name: k.to_string(),
                datatype: t as i32,
                semantic_type: s as i32,
                ..Default::default()
            })
            .collect()
    }

    fn make_row_with_label(l1: &str, value: f64, timestamp: i64) -> Row {
        Row {
            values: vec![
                api::v1::Value {
                    value_data: Some(api::v1::value::ValueData::StringValue(l1.to_string())),
                },
                api::v1::Value {
                    value_data: Some(api::v1::value::ValueData::F64Value(value)),
                },
                api::v1::Value {
                    value_data: Some(api::v1::value::ValueData::TimestampMillisecondValue(
                        timestamp,
                    )),
                },
            ],
        }
    }

    fn make_row_with_2_labels(l1: &str, l2: &str, value: f64, timestamp: i64) -> Row {
        Row {
            values: vec![
                api::v1::Value {
                    value_data: Some(api::v1::value::ValueData::StringValue(l1.to_string())),
                },
                api::v1::Value {
                    value_data: Some(api::v1::value::ValueData::StringValue(l2.to_string())),
                },
                api::v1::Value {
                    value_data: Some(api::v1::value::ValueData::F64Value(value)),
                },
                api::v1::Value {
                    value_data: Some(api::v1::value::ValueData::TimestampMillisecondValue(
                        timestamp,
                    )),
                },
            ],
        }
    }

    #[test]
    fn test_write_request_to_row_insert_exprs() {
        let write_request = WriteRequest {
            timeseries: mock_timeseries(),
            ..Default::default()
        };

        let mut exprs = to_grpc_row_insert_requests(write_request)
            .unwrap()
            .0
            .inserts;
        exprs.sort_unstable_by(|l, r| l.table_name.cmp(&r.table_name));
        assert_eq!(3, exprs.len());
        assert_eq!("metric1", exprs[0].table_name);
        assert_eq!("metric2", exprs[1].table_name);
        assert_eq!("metric3", exprs[2].table_name);

        let rows = exprs[0].rows.as_ref().unwrap();
        let schema = &rows.schema;
        let rows = &rows.rows;
        assert_eq!(2, rows.len());
        assert_eq!(3, schema.len());
        assert_eq!(
            column_schemas_with(vec![("job", ColumnDataType::String, SemanticType::Tag)]),
            *schema
        );
        assert_eq!(
            &vec![
                make_row_with_label("spark", 1.0, 1000),
                make_row_with_label("spark", 2.0, 2000),
            ],
            rows
        );

        let rows = exprs[1].rows.as_ref().unwrap();
        let schema = &rows.schema;
        let rows = &rows.rows;
        assert_eq!(2, rows.len());
        assert_eq!(4, schema.len());
        assert_eq!(
            column_schemas_with(vec![
                ("instance", ColumnDataType::String, SemanticType::Tag),
                ("idc", ColumnDataType::String, SemanticType::Tag)
            ]),
            *schema
        );
        assert_eq!(
            &vec![
                make_row_with_2_labels("test_host1", "z001", 3.0, 1000),
                make_row_with_2_labels("test_host1", "z001", 4.0, 2000),
            ],
            rows
        );

        let rows = exprs[2].rows.as_ref().unwrap();
        let schema = &rows.schema;
        let rows = &rows.rows;
        assert_eq!(3, rows.len());
        assert_eq!(4, schema.len());
        assert_eq!(
            column_schemas_with(vec![
                ("idc", ColumnDataType::String, SemanticType::Tag),
                ("app", ColumnDataType::String, SemanticType::Tag)
            ]),
            *schema
        );
        assert_eq!(
            &vec![
                make_row_with_2_labels("z002", "biz", 5.0, 1000),
                make_row_with_2_labels("z002", "biz", 6.0, 2000),
                make_row_with_2_labels("z002", "biz", 7.0, 3000),
            ],
            rows
        );
    }

    #[test]
    fn test_recordbatches_to_timeseries() {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                GREPTIME_TIMESTAMP,
                ConcreteDataType::timestamp_millisecond_datatype(),
                true,
            ),
            ColumnSchema::new(GREPTIME_VALUE, ConcreteDataType::float64_datatype(), true),
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
