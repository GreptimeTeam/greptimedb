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

pub mod query;

use std::collections::{BTreeMap, BTreeSet};

use common_query::prelude::{
    EXEMPLAR_METRIC_COLUMN, greptime_exemplar_labels, greptime_timestamp, greptime_value,
};
use datatypes::json;
pub use operator::insert::{Exemplar, ExemplarBatch};
use snafu::{OptionExt, ensure};
use store_api::metric_engine_consts::{
    DATA_SCHEMA_TABLE_ID_COLUMN_NAME, DATA_SCHEMA_TSID_COLUMN_NAME,
};
use store_api::storage::consts::is_internal_column;

use crate::error::{InvalidPromRemoteRequestSnafu, Result};

/// Exemplars grouped by the source series' optional database routing control.
pub type ExemplarRequests = BTreeMap<Option<String>, Vec<Exemplar>>;

/// Bounds linear sparse conversion memory without quadratic `rows * tags` blowup.
pub fn estimated_row_bytes<'a>(
    requests: impl IntoIterator<Item = &'a [Exemplar]>,
) -> Option<usize> {
    requests.into_iter().try_fold(0usize, |total, exemplars| {
        let mut tags = BTreeSet::new();
        let mut payload = 0usize;
        for exemplar in exemplars {
            tags.extend(exemplar.series_labels.keys());
            payload = payload.checked_add(exemplar.metric.len().checked_mul(4)?)?;
            for (name, value) in exemplar.series_labels.iter().chain(&exemplar.labels) {
                // Include owned strings, encoded keys/labels and JSON2 encoding scratch.
                payload = payload.checked_add(
                    name.len()
                        .checked_add(value.len())?
                        .checked_mul(4)?
                        .checked_add(256)?,
                )?;
            }
        }
        let columns = tags.len().checked_add(6)?;
        let rows = exemplars.len().checked_mul(512)?;
        total
            .checked_add(rows)?
            .checked_add(columns.checked_mul(512)?)?
            .checked_add(payload)
    })
}

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
        if exemplar_owned {
            ensure!(
                name != json::JSON2_REMAINDER_FIELD_NAME,
                InvalidPromRemoteRequestSnafu {
                    msg: "exemplar label name is reserved by JSON2",
                }
            );
        } else {
            ensure!(
                ![
                    greptime_timestamp(),
                    greptime_value(),
                    EXEMPLAR_METRIC_COLUMN,
                    greptime_exemplar_labels(),
                    DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
                    DATA_SCHEMA_TSID_COLUMN_NAME,
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

/// Validates exemplar observations into a sparse [`ExemplarBatch`].
pub fn prepare(exemplars: Vec<Exemplar>) -> Result<ExemplarBatch> {
    Ok(ExemplarBatch::try_new(exemplars)?)
}

#[cfg(test)]
mod tests {
    use metric_engine::row_modifier::TsidGenerator;

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
        for name in [
            DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
            DATA_SCHEMA_TSID_COLUMN_NAME,
            greptime_exemplar_labels(),
        ] {
            assert!(labels([(name.into(), "x".into())], false, validate_label_name).is_err());
        }
        assert!(
            labels(
                [("exemplar_labels".into(), "x".into())],
                false,
                validate_label_name,
            )
            .is_ok()
        );
        assert_eq!(timestamp_from_millis(-1).unwrap(), -1_000_000);
        assert!(timestamp_from_millis(i64::MAX).is_err());
    }

    #[test]
    fn test_exemplar_tsid_uses_only_source_labels() {
        let exemplar = Exemplar {
            metric: "cpu".into(),
            series_labels: BTreeMap::from([("job".into(), "api".into())]).into(),
            timestamp: 1,
            value: 2.0,
            labels: BTreeMap::from([("trace_id".into(), "a".into())]),
        };
        let mut other = exemplar.clone();
        other.metric = "memory".into();
        other.labels.insert("trace_id".into(), "b".into());
        let batch = prepare(vec![exemplar, other]).unwrap();
        assert_eq!(batch.sources().len(), 2);
        let tsids: Vec<u64> = batch
            .exemplars()
            .iter()
            .map(|e| {
                TsidGenerator::from_sorted_tags(
                    e.series_labels
                        .iter()
                        .map(|(name, value)| (name.as_str(), value.as_str())),
                )
            })
            .collect();
        let expected = TsidGenerator::from_sorted_tags([("job", "api")].into_iter());
        assert_eq!(tsids, vec![expected, expected]);
    }
}
