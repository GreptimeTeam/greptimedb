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
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use api::prom_store::remote::label_matcher::Type as MatcherType;
use api::prom_store::remote::{Label, Query, ReadRequest, Sample, TimeSeries, WriteRequest};
use api::v1::RowInsertRequests;
use arrow::array::{Array, AsArray, LargeStringArray, StringArray, StringViewArray};
use arrow::datatypes::{Float64Type, TimestampMillisecondType};
use common_grpc::precision::Precision;
use common_query::prelude::{greptime_timestamp, greptime_value};
use common_recordbatch::{RecordBatch, RecordBatches};
use common_telemetry::{tracing, warn};
use datafusion::dataframe::DataFrame;
use datafusion::prelude::{Expr, col, lit, regexp_match};
use datafusion_common::ScalarValue;
use datafusion_expr::LogicalPlan;
use openmetrics_parser::{MetricsExposition, PrometheusType, PrometheusValue};
use snafu::{OptionExt, ResultExt, ensure};
use snap::raw::{Decoder, Encoder};

use crate::error::{self, Result};
use crate::row_writer::{self, MultiTableData};

pub const METRIC_NAME_LABEL: &str = "__name__";
pub const METRIC_NAME_LABEL_BYTES: &[u8] = b"__name__";

/// special label for selecting database name on remote write
pub const DATABASE_LABEL: &str = "x_greptime_database";
pub const DATABASE_LABEL_BYTES: &[u8] = b"x_greptime_database";
pub const DATABASE_LABEL_ALT: &str = "__database__";
pub const DATABASE_LABEL_ALT_BYTES: &[u8] = b"__database__";

/// deprecated, use DATABASE_LABEL instead
#[deprecated(note = "use DATABASE_LABEL instead")]
pub const SCHEMA_LABEL: &str = "__schema__";
#[deprecated(note = "use DATABASE_LABEL_BYTES instead")]
pub const SCHEMA_LABEL_BYTES: &[u8] = b"__schema__";

/// special label for selecting physical table name on remote write
pub const PHYSICAL_TABLE_LABEL: &str = "x_greptime_physical_table";
pub const PHYSICAL_TABLE_LABEL_BYTES: &[u8] = b"x_greptime_physical_table";
pub const PHYSICAL_TABLE_LABEL_ALT: &str = "__physical_table__";
pub const PHYSICAL_TABLE_LABEL_ALT_BYTES: &[u8] = b"__physical_table__";

/// The same as `FIELD_COLUMN_MATCHER` in `promql` crate
pub const FIELD_NAME_LABEL: &str = "__field__";

/// Check if given label is a special label for remote write
#[allow(deprecated)]
pub fn is_remote_write_special_label(label: &str) -> bool {
    label == DATABASE_LABEL
        || label == DATABASE_LABEL_ALT
        || label == PHYSICAL_TABLE_LABEL
        || label == PHYSICAL_TABLE_LABEL_ALT
        || label == SCHEMA_LABEL
}

#[allow(deprecated)]
pub fn is_remote_read_special_label(label: &str) -> bool {
    label == METRIC_NAME_LABEL
        || label == DATABASE_LABEL
        || label == DATABASE_LABEL_ALT
        || label == SCHEMA_LABEL
}

/// Check if given label is a database selection label
#[allow(deprecated)]
pub fn is_database_selection_label(label: &str) -> bool {
    label == DATABASE_LABEL || label == DATABASE_LABEL_ALT || label == SCHEMA_LABEL
}

/// Check if given label is a physical table selection label
pub fn is_physical_table_selection_label(label: &str) -> bool {
    label == PHYSICAL_TABLE_LABEL || label == PHYSICAL_TABLE_LABEL_ALT
}

/// Metrics for push gateway protocol
pub struct Metrics {
    pub exposition: MetricsExposition<PrometheusType, PrometheusValue>,
}

/// Get table name from remote query
pub fn table_name(q: &Query) -> Result<String> {
    let mut matchers = q
        .matchers
        .iter()
        .filter(|matcher| matcher.name == METRIC_NAME_LABEL);
    let matcher = matchers
        .next()
        .context(error::InvalidPromRemoteRequestSnafu {
            msg: "missing '__name__' label in timeseries",
        })?;

    if matcher.r#type != MatcherType::Eq as i32
        || matcher.value.is_empty()
        || matchers.next().is_some()
    {
        return Err(error::InvalidPromRemoteRequestSnafu {
            msg: "expected exactly one non-empty equality matcher for '__name__'".to_string(),
        }
        .build());
    }

    Ok(matcher.value.clone())
}

/// Extract schema from remote read request. Returns the first schema found from any query's matchers.
pub fn extract_schema_from_read_request(request: &ReadRequest) -> Option<String> {
    for query in &request.queries {
        for matcher in &query.matchers {
            if is_database_selection_label(&matcher.name)
                && matcher.r#type == MatcherType::Eq as i32
            {
                return Some(matcher.value.clone());
            }
        }
    }

    None
}

/// Create a DataFrame from a remote Query
#[tracing::instrument(skip_all)]
pub fn query_to_plan(dataframe: DataFrame, q: &Query) -> Result<LogicalPlan> {
    let start_timestamp_ms = q.start_timestamp_ms;
    let end_timestamp_ms = q.end_timestamp_ms;

    let label_matches = &q.matchers;

    let mut conditions = Vec::with_capacity(label_matches.len() + 1);

    conditions.push(col(greptime_timestamp()).gt_eq(lit_timestamp_millisecond(start_timestamp_ms)));
    conditions.push(col(greptime_timestamp()).lt_eq(lit_timestamp_millisecond(end_timestamp_ms)));

    for m in label_matches {
        let name = &m.name;

        if is_remote_read_special_label(name) {
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
                conditions.push(regexp_match(col(name), lit(value), None).is_not_null());
            }
            // Case sensitive regexp not match
            MatcherType::Nre => {
                conditions.push(regexp_match(col(name), lit(value), None).is_null());
            }
        }
    }

    // Safety: conditions MUST not be empty, reduce always return Some(expr).
    let conditions = conditions.into_iter().reduce(Expr::and).unwrap();

    let dataframe = dataframe
        .filter(conditions)
        .context(error::DataFrameSnafu)?;

    Ok(dataframe.into_parts().1)
}

#[inline]
fn new_label(name: String, value: String) -> Label {
    Label { name, value }
}

fn lit_timestamp_millisecond(ts: i64) -> Expr {
    Expr::Literal(ScalarValue::TimestampMillisecond(Some(ts), None), None)
}

/// Sort timeseries by labels, matching the former `BTreeMap` order.
fn compare_timeseries_labels(left: &[Label], right: &[Label]) -> Ordering {
    let ordering = left.len().cmp(&right.len());
    if ordering != Ordering::Equal {
        return ordering;
    }

    for (left, right) in left.iter().zip(right) {
        let ordering = left.name.cmp(&right.name);
        if ordering != Ordering::Equal {
            return ordering;
        }

        let ordering = left.value.cmp(&right.value);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }

    Ordering::Equal
}

enum LabelValues<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Utf8View(&'a StringViewArray),
    Other(Vec<Option<String>>),
}

impl LabelValues<'_> {
    fn value(&self, row: usize) -> Option<&str> {
        match self {
            Self::Utf8(values) => values.is_valid(row).then(|| values.value(row)),
            Self::LargeUtf8(values) => values.is_valid(row).then(|| values.value(row)),
            Self::Utf8View(values) => values.is_valid(row).then(|| values.value(row)),
            Self::Other(values) => values.get(row).and_then(Option::as_deref),
        }
    }
}

fn row_labels<'a>(
    columns: &'a [LabelColumn<'a>],
    row: usize,
) -> impl Iterator<Item = (&'a str, &'a str)> {
    columns
        .iter()
        .filter_map(move |column| column.values.value(row).map(|value| (column.name, value)))
}

struct LabelColumn<'a> {
    name: &'a str,
    values: LabelValues<'a>,
}

fn label_columns(recordbatch: &RecordBatch) -> Result<Vec<LabelColumn<'_>>> {
    recordbatch
        .schema
        .column_schemas()
        .iter()
        .enumerate()
        .filter(|(_, column_schema)| {
            column_schema.name != greptime_timestamp() && column_schema.name != greptime_value()
        })
        .map(|(index, column_schema)| {
            let array = recordbatch.column(index);
            let values = match array.data_type() {
                arrow::datatypes::DataType::Utf8 => LabelValues::Utf8(array.as_string::<i32>()),
                arrow::datatypes::DataType::LargeUtf8 => {
                    LabelValues::LargeUtf8(array.as_string::<i64>())
                }
                arrow::datatypes::DataType::Utf8View => {
                    LabelValues::Utf8View(array.as_string_view())
                }
                _ => {
                    let values = recordbatch.iter_column_as_string(index).collect::<Vec<_>>();
                    ensure!(
                        values.len() == recordbatch.num_rows(),
                        error::InvalidPromRemoteReadQueryResultSnafu {
                            msg: format!(
                                "Cannot convert label column '{}' of datatype {:?} to string",
                                column_schema.name,
                                array.data_type()
                            ),
                        }
                    );
                    LabelValues::Other(values)
                }
            };
            Ok(LabelColumn {
                name: &column_schema.name,
                values,
            })
        })
        .collect()
}

fn hash_timeseries(columns: &[LabelColumn<'_>], row: usize) -> u64 {
    let mut hasher = DefaultHasher::new();

    for (name, value) in row_labels(columns, row) {
        name.hash(&mut hasher);
        value.hash(&mut hasher);
    }

    hasher.finish()
}

fn matches_timeseries(labels: &[Label], columns: &[LabelColumn<'_>], row: usize) -> bool {
    let mut labels = labels.iter().skip(1);
    for (name, value) in row_labels(columns, row) {
        let Some(label) = labels.next() else {
            return false;
        };
        if label.name != name || label.value != value {
            return false;
        }
    }

    labels.next().is_none()
}

fn new_timeseries(table: &str, columns: &[LabelColumn<'_>], row: usize) -> TimeSeries {
    let mut labels = Vec::with_capacity(columns.len() + 1);
    labels.push(new_label(METRIC_NAME_LABEL.to_string(), table.to_string()));

    for (name, value) in row_labels(columns, row) {
        labels.push(new_label(name.to_string(), value.to_string()));
    }

    TimeSeries {
        labels,
        ..Default::default()
    }
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
    let ts_column = recordbatch.column_by_name(greptime_timestamp()).context(
        error::InvalidPromRemoteReadQueryResultSnafu {
            msg: "missing greptime_timestamp column in query result",
        },
    )?;
    let ts_column = ts_column
        .as_primitive_opt::<TimestampMillisecondType>()
        .with_context(|| error::InvalidPromRemoteReadQueryResultSnafu {
            msg: format!(
                "Expect timestamp column of datatype Timestamp(Millisecond), actual {:?}",
                ts_column.data_type()
            ),
        })?;

    let field_column = recordbatch.column_by_name(greptime_value()).context(
        error::InvalidPromRemoteReadQueryResultSnafu {
            msg: "missing greptime_value column in query result",
        },
    )?;
    let field_column = field_column
        .as_primitive_opt::<Float64Type>()
        .with_context(|| error::InvalidPromRemoteReadQueryResultSnafu {
            msg: format!(
                "Expect value column of datatype Float64, actual {:?}",
                field_column.data_type()
            ),
        })?;

    let columns = label_columns(&recordbatch)?;
    let mut timeseries: Vec<TimeSeries> = Vec::new();
    let mut timeseries_by_hash: HashMap<u64, Vec<usize>> = HashMap::new();
    let mut previous_timeseries: Option<usize> = None;

    for row in 0..recordbatch.num_rows() {
        let timeseries_index = match previous_timeseries {
            Some(index) if matches_timeseries(&timeseries[index].labels, &columns, row) => index,
            _ => {
                let hash = hash_timeseries(&columns, row);
                let candidates = timeseries_by_hash.entry(hash).or_default();
                match candidates
                    .iter()
                    .copied()
                    .find(|index| matches_timeseries(&timeseries[*index].labels, &columns, row))
                {
                    Some(index) => index,
                    None => {
                        let index = timeseries.len();
                        timeseries.push(new_timeseries(table, &columns, row));
                        candidates.push(index);
                        index
                    }
                }
            }
        };
        previous_timeseries = Some(timeseries_index);

        if ts_column.is_null(row) || field_column.is_null(row) {
            continue;
        }

        let value = field_column.value(row);
        let timestamp = ts_column.value(row);
        let sample = Sample { value, timestamp };

        timeseries[timeseries_index].samples.push(sample);
    }

    timeseries
        .sort_unstable_by(|left, right| compare_timeseries_labels(&left.labels, &right.labels));
    Ok(timeseries)
}

pub fn to_grpc_row_insert_requests(request: &WriteRequest) -> Result<(RowInsertRequests, usize)> {
    let _timer = crate::metrics::METRIC_HTTP_PROM_STORE_CONVERT_ELAPSED.start_timer();

    let mut multi_table_data = MultiTableData::new();

    for series in &request.timeseries {
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
        let kvs = series.labels.iter().filter_map(|label| {
            if label.name == METRIC_NAME_LABEL {
                None
            } else {
                Some((label.name.clone(), label.value.clone()))
            }
        });

        if series.samples.len() == 1 {
            let mut one_row = table_data.alloc_one_row();

            row_writer::write_tags(table_data, kvs, &mut one_row)?;
            // value
            row_writer::write_f64(
                table_data,
                greptime_value(),
                series.samples[0].value,
                &mut one_row,
            )?;
            // timestamp
            row_writer::write_ts_to_millis(
                table_data,
                greptime_timestamp(),
                Some(series.samples[0].timestamp),
                Precision::Millisecond,
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
                row_writer::write_f64(table_data, greptime_value(), *value, &mut one_row)?;
                // timestamp
                row_writer::write_ts_to_millis(
                    table_data,
                    greptime_timestamp(),
                    Some(*timestamp),
                    Precision::Millisecond,
                    &mut one_row,
                )?;

                table_data.add_row(one_row);
            }
        }

        if !series.histograms.is_empty() {
            warn!("Native histograms are not supported yet, data ignored");
        }
    }

    Ok(multi_table_data.into_row_insert_requests())
}

#[inline]
pub fn snappy_decompress(buf: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = Decoder::new();
    decoder
        .decompress_vec(buf)
        .context(error::DecompressSnappyPromRemoteRequestSnafu)
}

#[inline]
pub fn snappy_compress(buf: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = Encoder::new();
    encoder
        .compress_vec(buf)
        .context(error::CompressPromRemoteRequestSnafu)
}

#[inline]
pub fn zstd_decompress(buf: &[u8]) -> Result<Vec<u8>> {
    zstd::stream::decode_all(buf).context(error::DecompressZstdPromRemoteRequestSnafu)
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

/// Add new labels to the mock timeseries.
pub fn mock_timeseries_new_label() -> Vec<TimeSeries> {
    let ts_demo_metrics = TimeSeries {
        labels: vec![
            new_label(METRIC_NAME_LABEL.to_string(), "demo_metrics".to_string()),
            new_label("idc".to_string(), "idc3".to_string()),
            new_label("new_label1".to_string(), "foo".to_string()),
        ],
        samples: vec![Sample {
            value: 42.0,
            timestamp: 3000,
        }],
        ..Default::default()
    };
    let ts_multi_labels = TimeSeries {
        labels: vec![
            new_label(METRIC_NAME_LABEL.to_string(), "metric1".to_string()),
            new_label("idc".to_string(), "idc4".to_string()),
            new_label("env".to_string(), "prod".to_string()),
            new_label("host".to_string(), "host9".to_string()),
            new_label("new_label2".to_string(), "bar".to_string()),
        ],
        samples: vec![Sample {
            value: 99.0,
            timestamp: 4000,
        }],
        ..Default::default()
    };

    vec![ts_demo_metrics, ts_multi_labels]
}

/// Add new labels to the mock timeseries.
pub fn mock_timeseries_special_labels() -> Vec<TimeSeries> {
    let idc3_schema = TimeSeries {
        labels: vec![
            new_label(METRIC_NAME_LABEL.to_string(), "idc3_lo_table".to_string()),
            new_label(DATABASE_LABEL.to_string(), "idc3".to_string()),
            new_label(PHYSICAL_TABLE_LABEL.to_string(), "f1".to_string()),
        ],
        samples: vec![Sample {
            value: 42.0,
            timestamp: 3000,
        }],
        ..Default::default()
    };
    let idc4_schema = TimeSeries {
        labels: vec![
            new_label(
                METRIC_NAME_LABEL.to_string(),
                "idc4_local_table".to_string(),
            ),
            new_label(DATABASE_LABEL.to_string(), "idc4".to_string()),
            new_label(PHYSICAL_TABLE_LABEL.to_string(), "f2".to_string()),
        ],
        samples: vec![Sample {
            value: 99.0,
            timestamp: 4000,
        }],
        ..Default::default()
    };

    vec![idc3_schema, idc4_schema]
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::prom_store::remote::LabelMatcher;
    use api::v1::{ColumnDataType, Row, SemanticType};
    use arrow::array::{
        DictionaryArray, Float64Array, StringArray, TimestampMillisecondArray, UInt32Array,
    };
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema, UInt32Type};
    use common_recordbatch::DfRecordBatch;
    use datafusion::prelude::SessionContext;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{
        Float64Vector, Int32Vector, StringVector, TimestampMillisecondVector,
    };
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

        for matchers in [
            vec![LabelMatcher {
                name: METRIC_NAME_LABEL.to_string(),
                value: "test.*".to_string(),
                r#type: RE_TYPE,
            }],
            vec![LabelMatcher {
                name: METRIC_NAME_LABEL.to_string(),
                value: String::new(),
                r#type: EQ_TYPE,
            }],
            vec![
                LabelMatcher {
                    name: METRIC_NAME_LABEL.to_string(),
                    value: "test".to_string(),
                    r#type: EQ_TYPE,
                },
                LabelMatcher {
                    name: METRIC_NAME_LABEL.to_string(),
                    value: "other".to_string(),
                    r#type: EQ_TYPE,
                },
            ],
        ] {
            let q = Query {
                matchers,
                ..Default::default()
            };
            assert!(matches!(
                table_name(&q),
                Err(error::Error::InvalidPromRemoteRequest { .. })
            ));
        }
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
                greptime_timestamp(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                true,
            ),
            ColumnSchema::new(greptime_value(), ConcreteDataType::float64_datatype(), true),
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
        let plan = query_to_plan(dataframe, &q).unwrap();
        let display_string = format!("{}", plan.display_indent());

        let ts_col = greptime_timestamp();
        let expected = format!(
            "Filter: ?table?.{} >= TimestampMillisecond(1000, None) AND ?table?.{} <= TimestampMillisecond(2000, None)\n  TableScan: ?table?",
            ts_col, ts_col
        );
        assert_eq!(expected, display_string);

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
        let plan = query_to_plan(dataframe, &q).unwrap();
        let display_string = format!("{}", plan.display_indent());

        let ts_col = greptime_timestamp();
        let expected = format!(
            "Filter: ?table?.{} >= TimestampMillisecond(1000, None) AND ?table?.{} <= TimestampMillisecond(2000, None) AND regexp_match(?table?.job, Utf8(\"*prom*\")) IS NOT NULL AND ?table?.instance != Utf8(\"localhost\")\n  TableScan: ?table?",
            ts_col, ts_col
        );
        assert_eq!(expected, display_string);
    }

    fn column_schemas_with(
        mut kts_iter: Vec<(&str, ColumnDataType, SemanticType)>,
    ) -> Vec<api::v1::ColumnSchema> {
        kts_iter.push((
            greptime_value(),
            ColumnDataType::Float64,
            SemanticType::Field,
        ));
        kts_iter.push((
            greptime_timestamp(),
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

        let mut exprs = to_grpc_row_insert_requests(&write_request)
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
                greptime_timestamp(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                true,
            ),
            ColumnSchema::new(greptime_value(), ConcreteDataType::float64_datatype(), true),
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

    #[test]
    fn test_recordbatches_to_timeseries_omits_dictionary_value_null_label() {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                greptime_timestamp(),
                ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(greptime_value(), ArrowDataType::Float64, false),
            Field::new_dictionary("instance", ArrowDataType::UInt32, ArrowDataType::Utf8, true),
        ]));
        let schema = Arc::new(Schema::try_from(arrow_schema.clone()).unwrap());
        let instance = DictionaryArray::<UInt32Type>::new(
            UInt32Array::from(vec![0]),
            Arc::new(StringArray::from(vec![None::<&str>])),
        );
        let batch = DfRecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![1000])),
                Arc::new(Float64Array::from(vec![3.0])),
                Arc::new(instance),
            ],
        )
        .unwrap();
        let recordbatches = RecordBatches::try_new(
            schema.clone(),
            vec![RecordBatch::from_df_record_batch(schema, batch)],
        )
        .unwrap();

        let timeseries = recordbatches_to_timeseries("metric1", recordbatches).unwrap();

        assert_eq!(1, timeseries.len());
        assert_eq!(
            vec![Label {
                name: METRIC_NAME_LABEL.to_string(),
                value: "metric1".to_string(),
            }],
            timeseries[0].labels
        );
        assert_eq!(
            vec![Sample {
                value: 3.0,
                timestamp: 1000,
            }],
            timeseries[0].samples
        );
    }

    #[test]
    fn test_recordbatch_to_timeseries_groups_non_contiguous_series() {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                greptime_timestamp(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                true,
            ),
            ColumnSchema::new(greptime_value(), ConcreteDataType::float64_datatype(), true),
            ColumnSchema::new("instance", ConcreteDataType::string_datatype(), true),
        ]));
        let recordbatch = RecordBatch::new(
            schema,
            vec![
                Arc::new(TimestampMillisecondVector::from_vec(vec![1000, 2000, 3000])) as _,
                Arc::new(Float64Vector::from_vec(vec![1.0, 2.0, 3.0])) as _,
                Arc::new(StringVector::from(vec!["host2", "host1", "host2"])) as _,
            ],
        )
        .unwrap();

        let timeseries = recordbatch_to_timeseries("metric1", recordbatch).unwrap();

        // The result stays sorted by labels as it was with the previous BTreeMap.
        assert_eq!("host1", timeseries[0].labels[1].value);
        assert_eq!("host2", timeseries[1].labels[1].value);
        assert_eq!(
            vec![
                Sample {
                    value: 1.0,
                    timestamp: 1000,
                },
                Sample {
                    value: 3.0,
                    timestamp: 3000,
                },
            ],
            timeseries[1].samples
        );
    }

    #[test]
    fn test_recordbatch_to_timeseries_arrow_label_types_and_nulls() {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                greptime_timestamp(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                true,
            ),
            ColumnSchema::new(greptime_value(), ConcreteDataType::float64_datatype(), true),
            ColumnSchema::new("instance", ConcreteDataType::large_string_datatype(), true),
            ColumnSchema::new("zone", ConcreteDataType::utf8_view_datatype(), true),
            ColumnSchema::new("shard", ConcreteDataType::int32_datatype(), true),
        ]));
        let recordbatch = RecordBatch::new(
            schema,
            vec![
                Arc::new(TimestampMillisecondVector::from_vec(vec![1000, 2000, 3000])) as _,
                Arc::new(Float64Vector::from_vec(vec![1.0, 2.0, 3.0])) as _,
                Arc::new(StringVector::from(LargeStringArray::from(vec![
                    "host2", "host1", "host2",
                ]))) as _,
                Arc::new(StringVector::from(StringViewArray::from(vec![
                    Some("west"),
                    None,
                    Some("west"),
                ]))) as _,
                Arc::new(Int32Vector::from_vec(vec![2, 1, 2])) as _,
            ],
        )
        .unwrap();

        let timeseries = recordbatch_to_timeseries("metric1", recordbatch).unwrap();

        assert_eq!(2, timeseries.len());
        assert_eq!(
            vec![
                new_label(METRIC_NAME_LABEL.to_string(), "metric1".to_string()),
                new_label("instance".to_string(), "host1".to_string()),
                new_label("shard".to_string(), "1".to_string()),
            ],
            timeseries[0].labels
        );
        assert_eq!(
            vec![
                new_label(METRIC_NAME_LABEL.to_string(), "metric1".to_string()),
                new_label("instance".to_string(), "host2".to_string()),
                new_label("zone".to_string(), "west".to_string()),
                new_label("shard".to_string(), "2".to_string()),
            ],
            timeseries[1].labels
        );
        assert_eq!(
            vec![
                Sample {
                    value: 1.0,
                    timestamp: 1000,
                },
                Sample {
                    value: 3.0,
                    timestamp: 3000,
                },
            ],
            timeseries[1].samples
        );
    }
}
