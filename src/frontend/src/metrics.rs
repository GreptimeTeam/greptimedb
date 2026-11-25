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

use common_frontend::metrics::{CACHE_HIT, CACHE_MISS};
use lazy_static::lazy_static;
use prometheus::*;

/// The `outcome` label values of [`PROMQL_ROLLUP_CACHE_LOOKUPS`].
const ROLLUP_CACHE_LOOKUP_OUTCOMES: [&str; 3] = ["hit", "miss", "bypass"];
/// The `reason` label values of [`PROMQL_ROLLUP_CACHE_MISSES`].
///
/// `not_found` and `expired` refine a `miss` lookup; `empty_chunk` refines a
/// `bypass` one, which is the same thing said about the chunk: nothing was served.
const ROLLUP_CACHE_MISS_REASONS: [&str; 3] = ["not_found", "expired", "empty_chunk"];
/// The `result` label values of [`PROMQL_ROLLUP_CACHE_FILLS`].
const ROLLUP_CACHE_FILL_RESULTS: [&str; 2] = ["stored", "bypassed"];
/// The `reason` label values of [`PROMQL_ROLLUP_CACHE_BYPASSES`].
const ROLLUP_CACHE_BYPASS_REASONS: [&str; 7] = [
    "not_windowable",
    "invalid_span",
    "overflow",
    "step_mismatch",
    "no_chunks",
    "schema_mismatch",
    "nocache",
];

lazy_static! {
    /// Timer of handling query in RPC handler.
    pub static ref GRPC_HANDLE_QUERY_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_frontend_grpc_handle_query_elapsed",
        "Elapsed time of handling queries in RPC handler",
        &["type"],
        vec![0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 60.0, 300.0]
    )
    .unwrap();
    pub static ref GRPC_HANDLE_SQL_ELAPSED: Histogram = GRPC_HANDLE_QUERY_ELAPSED
        .with_label_values(&["sql"]);
    pub static ref GRPC_HANDLE_PLAN_ELAPSED: Histogram = GRPC_HANDLE_QUERY_ELAPSED
        .with_label_values(&["plan"]);
    pub static ref GRPC_HANDLE_PROMQL_ELAPSED: Histogram = GRPC_HANDLE_QUERY_ELAPSED
        .with_label_values(&["promql"]);

    pub static ref PROMQL_QUERY_METRICS_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_frontend_promql_query_metrics_elapsed",
        "frontend promql query metric names elapsed",
        &["db"]
    )
    .unwrap();

    /// The number of OpenTelemetry metrics send by frontend node.
    pub static ref OTLP_METRICS_ROWS: IntCounter = register_int_counter!(
        "greptime_frontend_otlp_metrics_rows",
        "frontend otlp metrics rows"
    )
    .unwrap();

    /// Failed writes of the synthesized OTLP resource descriptor table; these
    /// surface as OTLP partial-success warnings, not request failures.
    pub static ref OTLP_RESOURCE_INFO_WRITE_ERRORS: IntCounter = register_int_counter!(
        "greptime_frontend_otlp_resource_info_write_errors",
        "frontend otlp resource descriptor write errors"
    )
    .unwrap();

    /// The number of OpenTelemetry traces send by frontend node.
    pub static ref OTLP_TRACES_ROWS: IntCounter = register_int_counter!(
        "greptime_frontend_otlp_traces_rows",
        "frontend otlp traces rows"
    )
    .unwrap();

    /// The number of OpenTelemetry trace ingest failures on the frontend node.
    pub static ref OTLP_TRACES_FAILURE_COUNT: IntCounterVec = register_int_counter_vec!(
        "greptime_frontend_otlp_traces_failure_count",
        "frontend otlp trace ingest failure count",
        &["label"]
    )
    .unwrap();

    /// Initial trace auxiliary-cache lookups, excluding admission and write rechecks.
    pub static ref OTLP_TRACE_AUX_CACHE_HIT: IntCounter =
        CACHE_HIT.with_label_values(&["otlp_trace_aux"]);
    pub static ref OTLP_TRACE_AUX_CACHE_MISS: IntCounter =
        CACHE_MISS.with_label_values(&["otlp_trace_aux"]);

    /// The number of OpenTelemetry logs send by frontend node.
    pub static ref OTLP_LOGS_ROWS: IntCounter = register_int_counter!(
        "greptime_frontend_otlp_logs_rows",
        "frontend otlp logs rows"
    )
    .unwrap();

    /// The number of heartbeats send by frontend node.
    pub static ref HEARTBEAT_SENT_COUNT: IntCounter = register_int_counter!(
        "greptime_frontend_heartbeat_send_count",
        "frontend heartbeat sent",
    )
    .unwrap();
    /// The number of heartbeats received by frontend node, labeled with result type.
    pub static ref HEARTBEAT_RECV_COUNT: IntCounterVec = register_int_counter_vec!(
        "greptime_frontend_heartbeat_recv_count",
        "frontend heartbeat received",
        &["result"]
    )
    .unwrap();

    /// The chunk lookups of the frontend-local PromQL rollup cache, by outcome.
    ///
    /// A `hit` served the chunk from memory, a `miss` had to execute it and fill it,
    /// and a `bypass` is a chunk the cache refuses in either direction. Every lookup
    /// takes exactly one outcome.
    pub static ref PROMQL_ROLLUP_CACHE_LOOKUPS: IntCounterVec = {
        let lookups = register_int_counter_vec!(
            "greptime_frontend_promql_rollup_cache_lookups_total",
            "frontend promql rollup cache chunk lookups",
            &["outcome"]
        )
        .unwrap();
        // Every outcome is exported from the first update, so a cache that has only
        // missed so far still reports a zero `hit` series.
        for outcome in ROLLUP_CACHE_LOOKUP_OUTCOMES {
            lookups.with_label_values(&[outcome]);
        }
        lookups
    };

    /// Why the PromQL rollup cache served no chunk, refining
    /// [`PROMQL_ROLLUP_CACHE_LOOKUPS`] by reason.
    pub static ref PROMQL_ROLLUP_CACHE_MISSES: IntCounterVec = {
        let misses = register_int_counter_vec!(
            "greptime_frontend_promql_rollup_cache_misses_total",
            "frontend promql rollup cache chunk misses",
            &["reason"]
        )
        .unwrap();
        for reason in ROLLUP_CACHE_MISS_REASONS {
            misses.with_label_values(&[reason]);
        }
        misses
    };

    /// What the PromQL rollup cache did with the chunk a miss read.
    ///
    /// `stored` wrote the chunk, `bypassed` refused to, since an empty chunk is
    /// nothing that was ever materialized.
    pub static ref PROMQL_ROLLUP_CACHE_FILLS: IntCounterVec = {
        let fills = register_int_counter_vec!(
            "greptime_frontend_promql_rollup_cache_fills_total",
            "frontend promql rollup cache chunk fills",
            &["result"]
        )
        .unwrap();
        for result in ROLLUP_CACHE_FILL_RESULTS {
            fills.with_label_values(&[result]);
        }
        fills
    };

    /// Whole requests the PromQL rollup cache refused, by reason.
    ///
    /// A bypass is decided by planning alone: nothing of such a request is served
    /// from the cache, filled into it, or counted as a lookup.
    pub static ref PROMQL_ROLLUP_CACHE_BYPASSES: IntCounterVec = {
        let bypasses = register_int_counter_vec!(
            "greptime_frontend_promql_rollup_cache_bypasses_total",
            "frontend promql rollup cache request bypasses",
            &["reason"]
        )
        .unwrap();
        for reason in ROLLUP_CACHE_BYPASS_REASONS {
            bypasses.with_label_values(&[reason]);
        }
        bypasses
    };

    /// The number of times this frontend emptied its local PromQL rollup cache.
    pub static ref PROMQL_ROLLUP_CACHE_RESETS: IntCounter = register_int_counter!(
        "greptime_frontend_promql_rollup_cache_resets_total",
        "frontend promql rollup cache resets"
    )
    .unwrap();

    /// The chunks resident in the PromQL rollup cache once its last fill or reset
    /// settled.
    ///
    /// It is a gauge of the cache's own accounting, not a walk of it on a lookup:
    /// a read path never counts what the cache holds.
    pub static ref PROMQL_ROLLUP_CACHE_ENTRIES: IntGauge = register_int_gauge!(
        "greptime_frontend_promql_rollup_cache_entries",
        "frontend promql rollup cache resident entries"
    )
    .unwrap();

    /// The bytes the resident chunks of the PromQL rollup cache are charged, summed
    /// over their weights once its last fill or reset settled.
    pub static ref PROMQL_ROLLUP_CACHE_BYTES: IntGauge = register_int_gauge!(
        "greptime_frontend_promql_rollup_cache_bytes",
        "frontend promql rollup cache resident entry bytes"
    )
    .unwrap();
}

#[cfg(test)]
mod tests {
    use prometheus::proto::{MetricFamily, MetricType};

    use super::*;

    /// The scraped family `name`.
    fn family<'a>(families: &'a [MetricFamily], name: &str) -> &'a MetricFamily {
        families
            .iter()
            .find(|family| family.name() == name)
            .unwrap_or_else(|| panic!("{name} is not registered"))
    }

    /// The values of `label` in `family`, sorted.
    fn label_values(family: &MetricFamily, label: &str) -> Vec<String> {
        let name = family.name();
        let mut values = family
            .get_metric()
            .iter()
            .map(|metric| {
                metric
                    .get_label()
                    .iter()
                    .find(|pair| pair.name() == label)
                    .unwrap_or_else(|| panic!("{name} has no {label} label"))
                    .value()
                    .to_string()
            })
            .collect::<Vec<_>>();
        values.sort();
        values
    }

    /// The sorted strings of `values`.
    fn sorted(values: &[&str]) -> Vec<String> {
        let mut values = values
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>();
        values.sort();
        values
    }

    /// Every rollup cache metric is registered under the name an operator scrapes,
    /// with exactly the label values it can take.
    #[test]
    fn test_rollup_cache_metrics_registration() {
        // Touch every metric: a metric is registered when it is first used, and each
        // vector exports a zero series for every label value it can take.
        for outcome in ROLLUP_CACHE_LOOKUP_OUTCOMES {
            PROMQL_ROLLUP_CACHE_LOOKUPS.with_label_values(&[outcome]);
        }
        for reason in ROLLUP_CACHE_MISS_REASONS {
            PROMQL_ROLLUP_CACHE_MISSES.with_label_values(&[reason]);
        }
        for result in ROLLUP_CACHE_FILL_RESULTS {
            PROMQL_ROLLUP_CACHE_FILLS.with_label_values(&[result]);
        }
        for reason in ROLLUP_CACHE_BYPASS_REASONS {
            PROMQL_ROLLUP_CACHE_BYPASSES.with_label_values(&[reason]);
        }
        // Reading the unlabeled metrics registers them; their values are what the
        // last fill or reset left them at, which no test here changes.
        let _ = PROMQL_ROLLUP_CACHE_RESETS.get();
        let _ = PROMQL_ROLLUP_CACHE_ENTRIES.get();
        let _ = PROMQL_ROLLUP_CACHE_BYTES.get();

        let families = prometheus::gather();
        for (name, label, values) in [
            (
                "greptime_frontend_promql_rollup_cache_lookups_total",
                "outcome",
                &ROLLUP_CACHE_LOOKUP_OUTCOMES[..],
            ),
            (
                "greptime_frontend_promql_rollup_cache_misses_total",
                "reason",
                &ROLLUP_CACHE_MISS_REASONS[..],
            ),
            (
                "greptime_frontend_promql_rollup_cache_fills_total",
                "result",
                &ROLLUP_CACHE_FILL_RESULTS[..],
            ),
            (
                "greptime_frontend_promql_rollup_cache_bypasses_total",
                "reason",
                &ROLLUP_CACHE_BYPASS_REASONS[..],
            ),
        ] {
            let family = family(&families, name);
            assert_eq!(MetricType::COUNTER, family.get_field_type(), "{name}");
            assert_eq!(values.len(), family.get_metric().len(), "{name}");
            assert_eq!(sorted(values), label_values(family, label), "{name}");
        }

        for (name, metric_type) in [
            (
                "greptime_frontend_promql_rollup_cache_resets_total",
                MetricType::COUNTER,
            ),
            (
                "greptime_frontend_promql_rollup_cache_entries",
                MetricType::GAUGE,
            ),
            (
                "greptime_frontend_promql_rollup_cache_bytes",
                MetricType::GAUGE,
            ),
        ] {
            let family = family(&families, name);
            assert_eq!(metric_type, family.get_field_type(), "{name}");
            assert_eq!(1, family.get_metric().len(), "{name} is one series");
            assert!(
                family.get_metric()[0].get_label().is_empty(),
                "{name} is unlabeled"
            );
        }
    }
}
