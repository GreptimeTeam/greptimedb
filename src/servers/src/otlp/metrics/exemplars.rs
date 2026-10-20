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

use std::collections::HashSet;

use api::v1::{SemanticType, Value, value::ValueData};
use otel_arrow_rust::proto::opentelemetry::metrics::v1::{Exemplar, exemplar};
use session::protocol_ctx::OtlpMetricCtx;
use snafu::{OptionExt, ensure};

use super::{legacy_normalize_otlp_name, scalar_value_string, translate_label_name};
use crate::error::{InvalidOtlpMetricInputSnafu, Result};
use crate::exemplar::{self as normalized, Exemplar as Observation};
use crate::row_writer::TableData;

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

/// Reads the emitted row so resource promotion and label precedence cannot diverge.
pub(super) fn append<'a>(
    output: &mut Vec<Observation>,
    exemplars: impl IntoIterator<Item = &'a Exemplar>,
    metric: &str,
    table: &TableData,
    row: &[Value],
    ctx: &OtlpMetricCtx,
) -> Result<()> {
    let mut exemplars = exemplars.into_iter().peekable();
    if exemplars.peek().is_none() {
        return Ok(());
    }
    // Names have already been translated according to the configured OTLP strategy.
    let validate_name = |name: &[u8]| !name.is_empty();
    let series_labels = normalized::labels(
        table
            .columns()
            .iter()
            .zip(row)
            .filter_map(|(column, value)| {
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
        validate_name,
    )?;
    for exemplar in exemplars {
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
                let name = if ctx.is_legacy {
                    legacy_normalize_otlp_name(&attribute.key)
                } else {
                    translate_label_name(&attribute.key, ctx.metric_translation_strategy)
                };
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
        output.push(Observation {
            metric: metric.to_string(),
            series_labels: series_labels.clone(),
            timestamp,
            value: value(exemplar)?,
            labels,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::otlp::metrics::string_key_value;
    use crate::row_writer;

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
                let name = translate_label_name(name, strategy);
                assert_eq!(output.len(), 1);
                assert_eq!(output[0].series_labels[&name], "GET");
                assert_eq!(output[0].labels[&name], "POST");
                let (requests, rows) = tables.into_row_insert_requests();
                assert_eq!(rows, 1);
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
    fn exemplar_histogram_bucket_parity_and_rejected_parent() {
        use crate::otlp::metrics::{MetricsIngestOutcome, encode_histogram};
        use crate::row_writer::MultiTableData;
        use otel_arrow_rust::proto::opentelemetry::metrics::v1::{
            AggregationTemporality, Histogram, HistogramDataPoint,
        };
        for is_legacy in [false, true] {
            for mode in 0..3 {
                let exemplars = [5.0, 1.0, f64::NEG_INFINITY, f64::NAN]
                    .into_iter()
                    .enumerate()
                    .map(|(i, v)| Exemplar {
                        time_unix_nano: i as u64 + 1,
                        value: Some(exemplar::Value::AsDouble(v)),
                        ..Default::default()
                    })
                    .collect();
                let mut point = HistogramDataPoint {
                    time_unix_nano: 100,
                    count: 4,
                    explicit_bounds: vec![1.0, 2.0],
                    bucket_counts: vec![1, 1, 2],
                    exemplars,
                    ..Default::default()
                };
                if mode == 1 {
                    point.explicit_bounds.clear();
                    point.bucket_counts.clear();
                }
                if mode == 2 {
                    point.flags = 1;
                }
                let rejected = HistogramDataPoint {
                    explicit_bounds: vec![1.0],
                    bucket_counts: vec![4],
                    count: 4,
                    exemplars: vec![Exemplar {
                        value: None,
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                let hist = Histogram {
                    data_points: vec![point, rejected],
                    aggregation_temporality: AggregationTemporality::Delta as i32,
                };
                let mut tables = MultiTableData::default();
                let mut output = Vec::new();
                let mut outcome = MetricsIngestOutcome::default();
                encode_histogram(
                    &mut tables,
                    "latency",
                    &hist,
                    None,
                    None,
                    &OtlpMetricCtx {
                        is_legacy,
                        ..Default::default()
                    },
                    &mut outcome,
                    &mut output,
                )
                .unwrap();
                assert_eq!(outcome.accepted_data_points, 1);
                assert_eq!(outcome.rejected_data_points, 1);
                output.sort_by_key(|e| e.timestamp);
                assert_eq!(output.len(), 4);
                assert!(output.iter().all(|e| e.metric == "latency_bucket"));
                assert_eq!(
                    output
                        .iter()
                        .map(|e| e.series_labels["le"].as_str())
                        .collect::<Vec<_>>(),
                    if mode == 1 {
                        vec!["inf"; 4]
                    } else {
                        vec!["inf", "1", "1", "inf"]
                    }
                );
                let (requests, _) = tables.into_row_insert_requests();
                let rows = requests
                    .inserts
                    .iter()
                    .find(|r| r.table_name == "latency_bucket")
                    .unwrap()
                    .rows
                    .as_ref()
                    .unwrap();
                let le = rows
                    .schema
                    .iter()
                    .position(|c| c.column_name == "le")
                    .unwrap();
                for e in output {
                    assert!(rows.rows.iter().any(|r| r.values[le].value_data.as_ref()
                        == Some(&ValueData::StringValue(e.series_labels["le"].clone()))));
                }
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
        for is_legacy in [false, true] {
            let ctx = OtlpMetricCtx {
                is_legacy,
                ..Default::default()
            };
            let mut output = Vec::new();
            append(&mut output, [&exemplar], "latency", &table, &row, &ctx).unwrap();
            assert_eq!(output[0].timestamp, 1720000000123456789);
            assert_eq!(output[0].series_labels["job"], "api");
            assert_eq!(output[0].labels["trace_id"], "ab".repeat(16));
            assert_eq!(output[0].labels["span_id"], "cd".repeat(8));
            let mut without_id = exemplar.clone();
            without_id.trace_id.clear();
            append(&mut output, [&without_id], "latency", &table, &row, &ctx).unwrap();
            assert_eq!(output[1].labels["trace_id"], "fallback");
            for invalid in [
                Exemplar {
                    value: None,
                    ..exemplar.clone()
                },
                Exemplar {
                    time_unix_nano: u64::MAX,
                    ..exemplar.clone()
                },
                Exemplar {
                    filtered_attributes: vec![
                        string_key_value("a", "1".into()),
                        string_key_value("a", "2".into()),
                    ],
                    ..exemplar.clone()
                },
            ] {
                assert!(
                    append(&mut Vec::new(), [&invalid], "latency", &table, &row, &ctx).is_err()
                );
            }
        }
    }
}
