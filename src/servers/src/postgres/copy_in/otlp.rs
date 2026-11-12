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

//! The `otlp` format for COPY FROM STDIN: one binary OTLP payload for
//! any of the three signals — `ExportMetricsServiceRequest`,
//! `ExportTraceServiceRequest` or `ExportLogsServiceRequest`.
//!
//! `COPY FROM STDIN WITH (FORMAT otlp [, signal 'traces'] [, <options>])`
//! is a GreptimeDB-specific, **binary** format (wire format code 1).
//! Like the influxdb format it is table-less and does not decode here:
//! the whole payload is buffered until `CopyDone` — exactly like an
//! OTLP/HTTP request body — then decoded and completed through the
//! frontend's `OpenTelemetryProtocolHandler`, the same path the
//! `/v1/otlp/v1/{metrics,traces,logs}` endpoints use, under the
//! connection's regular postgres query context.
//!
//! The `WITH` options carry what the HTTP endpoints take as request
//! headers; values already provided by the postgres session (database,
//! user, permissions) are not repeated. Option semantics, defaults and
//! parsing helpers are shared with the HTTP extractors (`truthy`,
//! `SelectInfo`, `GreptimePipelineParams`, `PipelineWay`), so the two
//! ingestion routes cannot drift.
//!
//! One COPY statement is one bounded ingestion request (see
//! [`crate::postgres::copy_in::COPY_IN_MAX_PAYLOAD_BYTES`]).

use ahash::HashSet;

use pgwire::error::PgWireResult;
use pipeline::truthy;
use session::protocol_ctx::OtlpMetricTranslationStrategy;

use crate::postgres::copy_in::bad_copy_data;
use crate::postgres::copy_in::codec::CopyFormat;
use crate::postgres::copy_in::options::{CopyOptionSet, CopyOptionValue};

/// The format's validated options: the signal and its header-equivalent
/// configuration, carried to the flush that completes the copy.
#[derive(Debug, Clone)]
pub(super) struct OtlpOptions {
    pub(super) signal: OtlpSignal,
}

#[derive(Debug, Clone)]
pub(super) enum OtlpSignal {
    /// Metric options mirroring the `x-greptime-otlp-metric-*` headers.
    Metrics {
        /// Persist all resource attributes; `resource_attrs` then acts
        /// as an exclude list, otherwise as an include list.
        promote_all_resource_attrs: bool,
        resource_attrs: HashSet<String>,
        promote_scope_attrs: bool,
        translation_strategy: OtlpMetricTranslationStrategy,
    },
    /// Trace options mirroring `x-greptime-trace-table-name` and the
    /// pipeline headers. The raw pipeline-params string is parsed by the
    /// shared `GreptimePipelineParams::from_params` when the copy
    /// completes, like the HTTP request does.
    Traces {
        table_name: Option<String>,
        pipeline_name: Option<String>,
        pipeline_version: Option<String>,
        pipeline_params: Option<String>,
    },
    /// Log options mirroring `x-greptime-log-table-name`,
    /// `x-greptime-log-extract-keys` and the pipeline headers.
    Logs {
        table_name: Option<String>,
        pipeline_name: Option<String>,
        pipeline_version: Option<String>,
        pipeline_params: Option<String>,
        extract_keys: Option<String>,
    },
}

pub(super) fn build_otlp_codec(mut options: CopyOptionSet) -> PgWireResult<CopyFormat> {
    let signal = options.take_string("signal")?;
    let metric_options = (
        take_truthy(&mut options, "promote_all_resource_attrs")?,
        options.take_string("resource_attrs")?,
        take_truthy(&mut options, "promote_scope_attrs")?,
        options.take_string("translation_strategy")?,
    );
    let trace_log_options = (
        options.take_string("table_name")?,
        options.take_string("pipeline_name")?,
        options.take_string("pipeline_version")?,
        options.take_string("pipeline_params")?,
    );
    let extract_keys = options.take_string("extract_keys")?;
    options.reject_leftovers("otlp")?;

    let unexpected = |option: &str| {
        bad_copy_data(format!(
            "COPY option \"{option}\" does not apply to the selected signal"
        ))
    };
    let signal = match signal.as_deref() {
        None | Some("metrics") => {
            if trace_log_options != (None, None, None, None) || extract_keys.is_some() {
                return Err(unexpected("table_name/pipeline/extract_keys"));
            }
            let (promote_all, resource_attrs, promote_scope, strategy) = metric_options;
            let promote_all = promote_all.unwrap_or(false);
            let promote_scope = promote_scope.unwrap_or(false);
            let translation_strategy = match strategy.as_deref() {
                None => OtlpMetricTranslationStrategy::default(),
                // Same values as the HTTP header (`VALUES`), parsed by
                // the same `FromStr`.
                Some(value) => value.parse().map_err(|_| {
                    bad_copy_data(format!(
                        "invalid translation_strategy \"{value}\", expected one of: {}",
                        OtlpMetricTranslationStrategy::VALUES.join(", ")
                    ))
                })?,
            };
            let resource_attrs = resource_attrs
                // Same ';'-separated list and trimming as the header
                // extractor.
                .map(|s| s.split(';').map(|s| s.trim().to_string()).collect())
                .unwrap_or_default();
            OtlpSignal::Metrics {
                promote_all_resource_attrs: promote_all,
                resource_attrs,
                promote_scope_attrs: promote_scope,
                translation_strategy,
            }
        }
        Some("traces") | Some("logs") => {
            if metric_options != (None, None, None, None) {
                return Err(unexpected("promote_*/resource_attrs/translation_strategy"));
            }
            let (table_name, pipeline_name, pipeline_version, pipeline_params) = trace_log_options;
            if signal.as_deref() == Some("traces") {
                if extract_keys.is_some() {
                    // `extract_keys` only applies to logs.
                    return Err(unexpected("extract_keys"));
                }
                OtlpSignal::Traces {
                    table_name,
                    pipeline_name,
                    pipeline_version,
                    pipeline_params,
                }
            } else {
                OtlpSignal::Logs {
                    table_name,
                    pipeline_name,
                    pipeline_version,
                    pipeline_params,
                    extract_keys,
                }
            }
        }
        Some(unknown) => {
            return Err(bad_copy_data(format!(
                "invalid signal \"{unknown}\", expected one of: metrics, traces, logs"
            )));
        }
    };
    Ok(CopyFormat::Otlp(OtlpOptions { signal }))
}

/// Takes a boolean option with the HTTP header's exact truthy
/// spellings (`true`, `1`, `yes`, `on`, `t`), plus the COPY forms
/// (bare flag or `TRUE`/`FALSE`), so both routes accept the same values.
fn take_truthy(options: &mut CopyOptionSet, key: &str) -> PgWireResult<Option<bool>> {
    match options.take_value(key) {
        None => Ok(None),
        Some(CopyOptionValue::Flag) => Ok(Some(true)),
        Some(CopyOptionValue::Boolean(b)) => Ok(Some(b)),
        Some(CopyOptionValue::String(v)) | Some(CopyOptionValue::Ident(v)) => Ok(Some(truthy(v))),
        Some(other) => Err(bad_copy_data(format!(
            "COPY option {key} requires a boolean value, got {}",
            other.kind()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::copy_in::options::CopyOptionValue;

    fn opts(entries: &[(&str, CopyOptionValue)]) -> CopyOptionSet {
        let mut set = CopyOptionSet::default();
        for (k, v) in entries {
            set.insert((*k).into(), v.clone()).unwrap();
        }
        set
    }

    #[test]
    fn test_signal_defaults_and_options() {
        // Default signal is metrics.
        assert!(matches!(
            build_otlp_codec(CopyOptionSet::default()).unwrap(),
            CopyFormat::Otlp(OtlpOptions {
                signal: OtlpSignal::Metrics { .. }
            })
        ));

        // Metric options parse like the headers.
        let format = build_otlp_codec(opts(&[
            ("promote_all_resource_attrs", CopyOptionValue::Flag),
            (
                "resource_attrs",
                CopyOptionValue::String("service.name; host.id ".into()),
            ),
            ("promote_scope_attrs", CopyOptionValue::Flag),
            (
                "translation_strategy",
                CopyOptionValue::String("NoTranslation".into()),
            ),
        ]))
        .unwrap();
        let CopyFormat::Otlp(OtlpOptions {
            signal:
                OtlpSignal::Metrics {
                    promote_all_resource_attrs,
                    resource_attrs,
                    promote_scope_attrs,
                    translation_strategy,
                },
        }) = format
        else {
            panic!("expected metrics");
        };
        assert!(promote_all_resource_attrs && promote_scope_attrs);
        assert_eq!(
            resource_attrs,
            ["service.name", "host.id"]
                .into_iter()
                .map(String::from)
                .collect()
        );
        assert!(matches!(
            translation_strategy,
            OtlpMetricTranslationStrategy::NoTranslation
        ));

        // Traces and logs accept their header equivalents.
        assert!(matches!(
            build_otlp_codec(opts(&[(
                "signal",
                CopyOptionValue::String("traces".into())
            )]))
            .unwrap(),
            CopyFormat::Otlp(OtlpOptions {
                signal: OtlpSignal::Traces { .. }
            })
        ));
        assert!(matches!(
            build_otlp_codec(opts(&[
                ("signal", CopyOptionValue::String("logs".into())),
                ("table_name", CopyOptionValue::String("my_logs".into())),
                ("extract_keys", CopyOptionValue::String("a,b".into())),
            ]))
            .unwrap(),
            CopyFormat::Otlp(OtlpOptions {
                signal: OtlpSignal::Logs { .. }
            })
        ));
    }

    #[test]
    fn test_rejects_unknown_and_mismatched_options() {
        // Unknown signal.
        let set = opts(&[("signal", CopyOptionValue::String("spans".into()))]);
        assert!(build_otlp_codec(set).is_err());

        // Unknown option.
        let set = opts(&[("precision", CopyOptionValue::String("ns".into()))]);
        assert!(build_otlp_codec(set).is_err());

        // Signal-specific options do not cross.
        let set = opts(&[
            ("signal", CopyOptionValue::String("traces".into())),
            ("promote_scope_attrs", CopyOptionValue::Flag),
        ]);
        assert!(build_otlp_codec(set).is_err());
        let set = opts(&[
            ("signal", CopyOptionValue::String("metrics".into())),
            ("table_name", CopyOptionValue::String("t".into())),
        ]);
        assert!(build_otlp_codec(set).is_err());
        let set = opts(&[
            ("signal", CopyOptionValue::String("traces".into())),
            (
                "extract_keys",
                CopyOptionValue::String("attributes.foo".into()),
            ),
        ]);
        assert!(build_otlp_codec(set).is_err());
    }
}
