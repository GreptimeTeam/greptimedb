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

//! Normalized metric exemplars shared by Remote Write and OTLP conversion.

use std::collections::{BTreeMap, BTreeSet};

use api::v1::value::ValueData;
use api::v1::{ColumnDataType, RowInsertRequests};
use common_grpc::precision::Precision;
use common_query::prelude::{
    EXEMPLAR_LABELS_COLUMN, EXEMPLAR_METRIC_COLUMN, METRICS_EXEMPLARS_TABLE, greptime_timestamp,
    greptime_value,
};
use snafu::{OptionExt, ensure};
use store_api::storage::consts::is_internal_column;

use crate::error::{InvalidPromRemoteRequestSnafu, Result};
use crate::row_writer::{self, MultiTableData};

/// One observation belonging to a query-visible metric series.
#[derive(Debug, Clone, PartialEq)]
pub struct Exemplar {
    pub metric: String,
    pub series_labels: BTreeMap<String, String>,
    pub timestamp: i64,
    pub value: f64,
    pub labels: BTreeMap<String, String>,
}

/// Exemplars grouped by the source series' optional database routing control.
pub type ExemplarRequests = BTreeMap<Option<String>, Vec<Exemplar>>;

/// Validates labels with the protocol's name policy, preserving duplicates as errors.
pub fn labels<I>(
    labels: I,
    exemplar_owned: bool,
    validate_name: impl Fn(&[u8]) -> bool,
) -> Result<BTreeMap<String, String>>
where
    I: IntoIterator<Item = (String, String)>,
{
    let mut result = BTreeMap::new();
    let mut length = 0usize;
    for (name, value) in labels {
        ensure!(
            validate_name(name.as_bytes()),
            InvalidPromRemoteRequestSnafu {
                msg: format!("invalid exemplar label name: {name}"),
            }
        );
        if !exemplar_owned {
            ensure!(
                ![
                    greptime_timestamp(),
                    greptime_value(),
                    EXEMPLAR_METRIC_COLUMN,
                    EXEMPLAR_LABELS_COLUMN
                ]
                .contains(&name.as_str())
                    && !is_internal_column(&name),
                InvalidPromRemoteRequestSnafu {
                    msg: format!("inherited exemplar label collides with reserved column: {name}"),
                }
            );
        }
        length = length
            .checked_add(name.chars().count())
            .and_then(|n| n.checked_add(value.chars().count()))
            .context(InvalidPromRemoteRequestSnafu {
                msg: "exemplar label length overflow",
            })?;
        ensure!(
            !exemplar_owned || length <= 128,
            InvalidPromRemoteRequestSnafu {
                msg: "exemplar label names and values exceed 128 characters",
            }
        );
        ensure!(
            result.insert(name.clone(), value).is_none(),
            InvalidPromRemoteRequestSnafu {
                msg: format!("duplicate exemplar label: {name}"),
            }
        );
    }
    Ok(result)
}

/// Converts Remote Write milliseconds without losing overflow information.
pub fn timestamp_from_millis(timestamp: i64) -> Result<i64> {
    timestamp
        .checked_mul(1_000_000)
        .context(InvalidPromRemoteRequestSnafu {
            msg: "exemplar timestamp is outside the signed nanosecond range",
        })
}

/// Builds ordinary-table rows and retains source targets for authorization.
pub fn into_rows(exemplars: Vec<Exemplar>) -> Result<(RowInsertRequests, BTreeSet<String>)> {
    let mut tables = MultiTableData::default();
    let mut sources = BTreeSet::new();
    for exemplar in exemplars {
        ensure!(
            !exemplar.metric.is_empty() && exemplar.metric != METRICS_EXEMPLARS_TABLE,
            InvalidPromRemoteRequestSnafu {
                msg: "invalid or reserved exemplar source metric name"
            }
        );
        sources.insert(exemplar.metric.clone());
        let table = tables.get_or_default_table_data(
            METRICS_EXEMPLARS_TABLE,
            exemplar.series_labels.len() + 4,
            1,
        );
        let mut row = table.alloc_one_row();
        row_writer::write_tag(table, EXEMPLAR_METRIC_COLUMN, &exemplar.metric, &mut row)?;
        row_writer::write_tags(table, exemplar.series_labels.into_iter(), &mut row)?;
        row_writer::write_fields(
            table,
            std::iter::once((
                greptime_value().to_string(),
                ColumnDataType::Float64,
                Some(ValueData::F64Value(exemplar.value)),
            )),
            &mut row,
        )?;
        row_writer::write_ts_to_nanos(
            table,
            greptime_timestamp(),
            Some(exemplar.timestamp),
            Precision::Nanosecond,
            &mut row,
        )?;
        row_writer::write_by_schema(
            table,
            std::iter::once((
                row_writer::build_json2_column_schema(EXEMPLAR_LABELS_COLUMN)?,
                Some(row_writer::encode_json2(exemplar.labels)?),
            )),
            &mut row,
        )?;
        table.add_row(row);
    }
    Ok((tables.into_row_insert_requests().0, sources))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prom_remote_write::validation::validate_label_name;

    #[test]
    fn test_exemplar_labels_and_timestamp_validation() {
        assert!(
            labels(
                [(String::from("traceID"), String::from("abc"))],
                true,
                validate_label_name
            )
            .is_ok()
        );
        assert!(
            labels(
                [
                    (String::from("job"), String::new()),
                    (String::from("job"), String::new())
                ],
                false,
                validate_label_name,
            )
            .is_err()
        );
        assert!(
            labels(
                [(EXEMPLAR_METRIC_COLUMN.into(), "x".into())],
                false,
                validate_label_name
            )
            .is_err()
        );
        assert!(labels([("id".into(), "x".repeat(127))], true, validate_label_name).is_err());
        for name in ["", "request.method", "请求"] {
            assert!(labels([(name.into(), "x".into())], true, validate_label_name).is_err());
        }
        assert_eq!(timestamp_from_millis(-1).unwrap(), -1_000_000);
        assert!(timestamp_from_millis(i64::MAX).is_err());
    }
}
