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

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use api::v1::{ColumnSchema, SemanticType, Value, value::ValueData};
use otel_arrow_rust::proto::opentelemetry::metrics::v1::{Exemplar, exemplar};
use session::protocol_ctx::OtlpMetricCtx;
use snafu::{OptionExt, ensure};

use crate::error::{InvalidOtlpMetricInputSnafu, Result};
use crate::exemplar::{self as normalized, Exemplar as Observation};
use crate::otlp::metrics::{scalar_value_string, translate_label_name};

/// Observations whose inherited labels are bound after source-row normalization.
pub(crate) struct RowExemplars {
    pub(crate) metric: Arc<str>,
    /// Starts as an emitted table's row offset; preparation maps it into `row_locations`.
    pub(crate) row: usize,
    observations: Vec<Observation>,
}

impl RowExemplars {
    /// Drops observations whose inherited tags cannot be represented by the exemplar schema.
    pub(crate) fn finish(mut self, schema: &[ColumnSchema], row: &[Value]) -> Vec<Observation> {
        let labels = normalized::labels(
            schema.iter().zip(row).filter_map(|(column, value)| {
                if column.semantic_type != SemanticType::Tag as i32 {
                    return None;
                }
                match &value.value_data {
                    Some(ValueData::StringValue(value)) => {
                        Some((column.column_name.clone(), value.clone()))
                    }
                    _ => None,
                }
            }),
            false,
            |name| !name.is_empty(),
        );
        let Ok(labels) = labels else {
            crate::metrics::EXEMPLARS_DROPPED_TOTAL
                .with_label_values(&["otlp"])
                .inc_by(self.observations.len() as u64);
            return Vec::new();
        };
        let labels = Arc::new(labels);
        for observation in &mut self.observations {
            observation.series_labels = labels.clone();
        }
        self.observations
    }
}

pub(super) fn value(exemplar: &Exemplar) -> Result<f64> {
    match exemplar
        .value
        .as_ref()
        .context(InvalidOtlpMetricInputSnafu {
            reason: "exemplar is missing its value",
        })? {
        exemplar::Value::AsDouble(value) => Ok(*value),
        exemplar::Value::AsInt(value) => Ok(*value as f64),
    }
}

/// Retains the emitted row's identity until destination defaults have been filled.
pub(crate) fn append<'a>(
    output: &mut Vec<RowExemplars>,
    exemplars: impl IntoIterator<Item = &'a Exemplar>,
    metric: &str,
    row: usize,
    ctx: &OtlpMetricCtx,
) {
    let mut exemplars = exemplars.into_iter().peekable();
    if exemplars.peek().is_none() {
        return;
    }
    let validate_name = |name: &[u8]| !name.is_empty();
    let series_labels = Arc::new(BTreeMap::new());
    let metric: Arc<str> = metric.into();
    let mut pending = RowExemplars {
        metric: metric.clone(),
        row,
        observations: Vec::new(),
    };
    for exemplar in exemplars {
        let observation: Result<_> = (|| {
            let mut names = HashSet::new();
            let mut attributes = Vec::new();
            for attribute in &exemplar.filtered_attributes {
                ensure!(
                    names.insert(&attribute.key),
                    InvalidOtlpMetricInputSnafu {
                        reason: format!("duplicate filtered exemplar attribute: {}", attribute.key)
                    }
                );
                if let Some(value) = scalar_value_string(attribute.value.as_ref()) {
                    let name =
                        translate_label_name(&attribute.key, ctx.metric_translation_strategy);
                    attributes.push((name, value));
                }
            }
            let mut labels = normalized::labels(attributes, true, validate_name)?;
            for (name, bytes, length) in [
                ("trace_id", &exemplar.trace_id, 16),
                ("span_id", &exemplar.span_id, 8),
            ] {
                if !bytes.is_empty() {
                    ensure!(
                        bytes.len() == length,
                        InvalidOtlpMetricInputSnafu {
                            reason: format!("exemplar {name} must contain {length} bytes")
                        }
                    );
                    labels.insert(name.to_string(), hex::encode(bytes));
                }
            }
            let labels = normalized::labels(labels, true, validate_name)?;
            let timestamp = i64::try_from(exemplar.time_unix_nano).map_err(|_| {
                InvalidOtlpMetricInputSnafu {
                    reason: "exemplar timestamp exceeds signed nanosecond range",
                }
                .build()
            })?;
            Ok(Observation {
                metric: metric.clone(),
                series_labels: series_labels.clone(),
                timestamp,
                value: value(exemplar)?,
                labels,
            })
        })();
        match observation {
            Ok(observation) => pending.observations.push(observation),
            Err(_) => crate::metrics::EXEMPLARS_DROPPED_TOTAL
                .with_label_values(&["otlp"])
                .inc(),
        }
    }
    if !pending.observations.is_empty() {
        output.push(pending);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::otlp::metrics::string_key_value;
    use crate::row_writer;
    use crate::row_writer::TableData;

    #[test]
    fn test_exemplar_labels_follow_otlp_translation_strategy() {
        use otel_arrow_rust::proto::opentelemetry::metrics::v1::{
            Gauge, NumberDataPoint, number_data_point,
        };
        use session::protocol_ctx::OtlpMetricTranslationStrategy;

        use crate::otlp::metrics::encode_gauge;
        use crate::row_writer::MultiTableData;

        for strategy in [
            OtlpMetricTranslationStrategy::NoTranslation,
            OtlpMetricTranslationStrategy::NoUtf8EscapingWithSuffixes,
            OtlpMetricTranslationStrategy::UnderscoreEscapingWithSuffixes,
            OtlpMetricTranslationStrategy::UnderscoreEscapingWithoutSuffixes,
        ] {
            for name in ["request.method", "请求"] {
                let gauge = Gauge {
                    data_points: vec![NumberDataPoint {
                        attributes: vec![string_key_value(name, "GET".into())],
                        time_unix_nano: 1,
                        value: Some(number_data_point::Value::AsDouble(1.0)),
                        exemplars: vec![Exemplar {
                            filtered_attributes: vec![string_key_value(name, "POST".into())],
                            time_unix_nano: 1,
                            value: Some(exemplar::Value::AsDouble(0.5)),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                };
                let mut tables = MultiTableData::default();
                let mut output = Vec::new();
                encode_gauge(
                    &mut tables,
                    "requests",
                    &gauge,
                    None,
                    None,
                    &OtlpMetricCtx {
                        metric_translation_strategy: strategy,
                        ..Default::default()
                    },
                    &mut output,
                )
                .unwrap();
                let (requests, rows) = tables.into_row_insert_requests();
                assert_eq!(rows, 1);
                let rows = requests.inserts[0].rows.as_ref().unwrap();
                let output = output
                    .into_iter()
                    .flat_map(|pending| {
                        let row = pending.row;
                        pending.finish(&rows.schema, &rows.rows[row].values)
                    })
                    .collect::<Vec<_>>();
                let name = translate_label_name(name, strategy);
                assert_eq!(output.len(), 1);
                assert_eq!(output[0].series_labels[&name], "GET");
                assert_eq!(output[0].labels[&name], "POST");
                assert!(
                    requests.inserts[0]
                        .rows
                        .as_ref()
                        .unwrap()
                        .schema
                        .iter()
                        .any(|column| column.column_name == name)
                );
            }
        }
    }

    #[test]
    fn own_time_labels_and_dedicated_id_precedence() {
        let mut table = TableData::new(2, 1);
        let mut row = table.alloc_one_row();
        row_writer::write_tag(&mut table, "job", "api", &mut row).unwrap();
        let exemplar = Exemplar {
            filtered_attributes: vec![
                string_key_value("trace_id", "fallback".into()),
                string_key_value("span_id", "fallback_span".into()),
                string_key_value("request.id", "r1".into()),
            ],
            time_unix_nano: 1720000000123456789,
            trace_id: vec![0xab; 16],
            span_id: vec![0xcd; 8],
            value: Some(exemplar::Value::AsDouble(0.32)),
        };
        let ctx = OtlpMetricCtx::default();
        let mut output = Vec::new();
        append(&mut output, [&exemplar], "latency", table.num_rows(), &ctx);
        let first = output.pop().unwrap().finish(table.columns(), &row);
        assert_eq!(first[0].timestamp, 1720000000123456789);
        assert_eq!(first[0].series_labels["job"], "api");
        assert_eq!(first[0].labels["trace_id"], "ab".repeat(16));
        assert_eq!(first[0].labels["span_id"], "cd".repeat(8));
        let mut without_id = exemplar.clone();
        without_id.trace_id.clear();
        append(
            &mut output,
            [&without_id],
            "latency",
            table.num_rows(),
            &ctx,
        );
        let second = output.pop().unwrap().finish(table.columns(), &row);
        assert_eq!(second[0].labels["trace_id"], "fallback");
    }
}
