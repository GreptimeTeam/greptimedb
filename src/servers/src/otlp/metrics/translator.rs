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

use ahash::HashMap;
use lazy_static::lazy_static;
use otel_arrow_rust::proto::opentelemetry::metrics::v1::Metric;
use regex::Regex;
use session::protocol_ctx::{MetricType, OtlpMetricTranslationStrategy};

const UNDERSCORE: &str = "_";
const DOUBLE_UNDERSCORE: &str = "__";
const TOTAL: &str = "total";
const RATIO: &str = "ratio";
const PER_PREFIX: &str = "per_";

lazy_static! {
    static ref NON_ALPHA_NUM_CHAR: Regex = Regex::new(r"[^a-zA-Z0-9]").unwrap();
    static ref UNIT_MAP: HashMap<String, String> = [
        // Time
        ("d", "days"),
        ("h", "hours"),
        ("min", "minutes"),
        ("s", "seconds"),
        ("ms", "milliseconds"),
        ("us", "microseconds"),
        ("ns", "nanoseconds"),
        // Bytes
        ("By", "bytes"),
        ("KiBy", "kibibytes"),
        ("MiBy", "mebibytes"),
        ("GiBy", "gibibytes"),
        ("TiBy", "tibibytes"),
        ("KBy", "kilobytes"),
        ("MBy", "megabytes"),
        ("GBy", "gigabytes"),
        ("TBy", "terabytes"),
        // SI
        ("m", "meters"),
        ("V", "volts"),
        ("A", "amperes"),
        ("J", "joules"),
        ("W", "watts"),
        ("g", "grams"),
        // Misc
        ("Cel", "celsius"),
        ("Hz", "hertz"),
        ("1", ""),
        ("%", "percent"),
    ]
    .iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    static ref PER_UNIT_MAP: HashMap<String, String> = [
        ("s", "second"),
        ("m", "minute"),
        ("h", "hour"),
        ("d", "day"),
        ("w", "week"),
        ("mo", "month"),
        ("y", "year"),
    ]
    .iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
}

pub fn translate_metric_name(
    metric: &Metric,
    metric_type: &MetricType,
    strategy: OtlpMetricTranslationStrategy,
) -> String {
    match (strategy.should_escape(), strategy.should_add_suffixes()) {
        (true, true) => normalize_metric_name(metric, metric_type),
        (true, false) => normalize_metric_name_without_suffixes(&metric.name),
        (false, true) => build_utf8_metric_name(&metric.name, &metric.unit, metric_type),
        (false, false) => metric.name.clone(),
    }
}

pub fn translate_label_name(name: &str, strategy: OtlpMetricTranslationStrategy) -> String {
    if strategy.should_escape() {
        normalize_label_name(name)
    } else {
        name.to_string()
    }
}

// See https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/145942706622aba5c276ca47f48df438228bfea4/pkg/translator/prometheus/normalize_name.go#L55
pub fn normalize_metric_name(metric: &Metric, metric_type: &MetricType) -> String {
    normalize_metric_name_with_suffixes(&metric.name, &metric.unit, metric_type)
}

fn normalize_metric_name_with_suffixes(name: &str, unit: &str, metric_type: &MetricType) -> String {
    let mut name_tokens = metric_name_tokens(name);

    if !unit.is_empty() {
        let (main, per) = build_clean_unit_suffix(unit);
        if let Some(main) = main
            && !name_tokens.contains(&main)
        {
            name_tokens.push(main);
        }
        if let Some(per) = per
            && !name_tokens.contains(&per)
        {
            name_tokens.push("per".to_string());
            name_tokens.push(per);
        }
    }

    if matches!(metric_type, MetricType::MonotonicSum) {
        name_tokens.retain(|t| t != TOTAL);
        name_tokens.push(TOTAL.to_string());
    }

    if unit == "1" && matches!(metric_type, MetricType::Gauge) {
        name_tokens.retain(|t| t != RATIO);
        name_tokens.push(RATIO.to_string());
    }

    prefix_digit_metric_name(name_tokens.join(UNDERSCORE))
}

fn normalize_metric_name_without_suffixes(name: &str) -> String {
    prefix_digit_metric_name(metric_name_tokens(name).join(UNDERSCORE))
}

fn metric_name_tokens(name: &str) -> Vec<String> {
    NON_ALPHA_NUM_CHAR
        .split(name)
        .filter_map(|s| {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        })
        .collect()
}

fn prefix_digit_metric_name(name: String) -> String {
    if let Some((_, first)) = name.char_indices().next()
        && first.is_ascii_digit()
    {
        format!("_{}", name)
    } else {
        name
    }
}

fn build_utf8_metric_name(input_name: &str, unit: &str, metric_type: &MetricType) -> String {
    let mut name = input_name.to_string();

    let append_ratio = unit == "1" && matches!(metric_type, MetricType::Gauge);
    if append_ratio {
        name = trim_suffix_and_delimiter(&name, RATIO);
    }

    let append_total = matches!(metric_type, MetricType::MonotonicSum);
    if append_total {
        name = trim_suffix_and_delimiter(&name, TOTAL);
    }

    let (main_unit_suffix, per_unit_suffix) = build_unit_suffixes(unit);
    let append_per = !per_unit_suffix.is_empty();
    if append_per {
        name = trim_suffix_and_delimiter(&name, &per_unit_suffix);
    }

    if !main_unit_suffix.is_empty() && !name.ends_with(&main_unit_suffix) {
        name.push('_');
        name.push_str(&main_unit_suffix);
    }
    if append_per {
        name.push('_');
        name.push_str(&per_unit_suffix);
    }
    if append_total {
        name.push_str("_total");
    }
    if append_ratio {
        name.push_str("_ratio");
    }

    name
}

fn trim_suffix_and_delimiter(name: &str, suffix: &str) -> String {
    if name.ends_with(suffix) && name.len() > suffix.len() + 1 {
        name[..name.len() - (suffix.len() + 1)].to_string()
    } else {
        name.to_string()
    }
}

fn build_clean_unit_suffix(unit: &str) -> (Option<String>, Option<String>) {
    let (main, per) = build_unit_suffixes(unit);
    let main = clean_unit_name(&main);
    let per = per
        .strip_prefix(PER_PREFIX)
        .map(clean_unit_name)
        .unwrap_or_default();

    (
        (!main.is_empty()).then_some(main),
        (!per.is_empty()).then_some(per),
    )
}

fn build_unit_suffixes(unit: &str) -> (String, String) {
    let (main, per) = unit.split_once('/').unwrap_or((unit, ""));
    let main_unit_suffix = unit_suffix(main, &UNIT_MAP);
    let per_unit_suffix = unit_suffix(per, &PER_UNIT_MAP);

    if per_unit_suffix.is_empty() {
        (main_unit_suffix, per_unit_suffix)
    } else {
        (main_unit_suffix, format!("{PER_PREFIX}{per_unit_suffix}"))
    }
}

fn unit_suffix(unit_str: &str, unit_map: &HashMap<String, String>) -> String {
    let unit = unit_str.trim();
    if unit.is_empty() || unit.contains('{') || unit.contains('}') {
        return String::new();
    }

    unit_map
        .get(unit)
        .map(|s| s.as_ref())
        .unwrap_or(unit)
        .to_string()
}

pub(crate) fn clean_unit_name(name: &str) -> String {
    NON_ALPHA_NUM_CHAR
        .split(name)
        .filter(|s| !s.is_empty())
        .collect::<Vec<&str>>()
        .join(UNDERSCORE)
        .trim_matches('_')
        .to_string()
}

// See https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/145942706622aba5c276ca47f48df438228bfea4/pkg/translator/prometheus/normalize_label.go#L27
pub fn normalize_label_name(name: &str) -> String {
    if name.is_empty() {
        return name.to_string();
    }

    let n = NON_ALPHA_NUM_CHAR.replace_all(name, UNDERSCORE);
    if let Some((_, first)) = n.char_indices().next()
        && first.is_ascii_digit()
    {
        return format!("key_{}", n);
    }
    if n.starts_with(UNDERSCORE) && !n.starts_with(DOUBLE_UNDERSCORE) {
        return format!("key{}", n);
    }
    n.to_string()
}

/// Normalize otlp instrumentation, metric and attribute names
///
/// <https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/api.md#instrument-name-syntax>
/// - since the name are case-insensitive, we transform them to lowercase for
///   better sql usability
/// - replace `.` and `-` with `_`
pub fn legacy_normalize_otlp_name(name: &str) -> String {
    name.to_lowercase().replace(['.', '-'], "_")
}

#[cfg(test)]
mod tests {
    use otel_arrow_rust::proto::opentelemetry::metrics::v1::Metric;
    use session::protocol_ctx::OtlpMetricTranslationStrategy::{
        NoTranslation, NoUtf8EscapingWithSuffixes, UnderscoreEscapingWithSuffixes,
        UnderscoreEscapingWithoutSuffixes,
    };

    use super::*;

    #[test]
    fn test_legacy_normalize_otlp_name() {
        assert_eq!(
            legacy_normalize_otlp_name("jvm.memory.free"),
            "jvm_memory_free"
        );
        assert_eq!(
            legacy_normalize_otlp_name("jvm-memory-free"),
            "jvm_memory_free"
        );
        assert_eq!(
            legacy_normalize_otlp_name("jvm_memory_free"),
            "jvm_memory_free"
        );
        assert_eq!(
            legacy_normalize_otlp_name("JVM_MEMORY_FREE"),
            "jvm_memory_free"
        );
        assert_eq!(
            legacy_normalize_otlp_name("JVM_memory_FREE"),
            "jvm_memory_free"
        );
    }

    #[test]
    fn test_translate_metric_name_strategies() {
        let metric = Metric {
            name: "http.server.duration_total".to_string(),
            unit: "s".to_string(),
            ..Default::default()
        };

        assert_eq!(
            translate_metric_name(
                &metric,
                &MetricType::MonotonicSum,
                UnderscoreEscapingWithSuffixes
            ),
            "http_server_duration_seconds_total"
        );
        assert_eq!(
            translate_metric_name(
                &metric,
                &MetricType::MonotonicSum,
                UnderscoreEscapingWithoutSuffixes,
            ),
            "http_server_duration_total"
        );
        assert_eq!(
            translate_metric_name(
                &metric,
                &MetricType::MonotonicSum,
                NoUtf8EscapingWithSuffixes
            ),
            "http.server.duration_seconds_total"
        );
        assert_eq!(
            translate_metric_name(&metric, &MetricType::MonotonicSum, NoTranslation),
            "http.server.duration_total"
        );
    }

    #[test]
    fn test_translate_metric_name_no_utf8_suffix_ordering() {
        let metric = Metric {
            name: "request.rate_per_second_total".to_string(),
            unit: "1/s".to_string(),
            ..Default::default()
        };
        assert_eq!(
            translate_metric_name(
                &metric,
                &MetricType::MonotonicSum,
                NoUtf8EscapingWithSuffixes
            ),
            "request.rate_per_second_total"
        );

        let metric = Metric {
            name: "cpu.utilization_ratio".to_string(),
            unit: "1".to_string(),
            ..Default::default()
        };
        assert_eq!(
            translate_metric_name(&metric, &MetricType::Gauge, NoUtf8EscapingWithSuffixes),
            "cpu.utilization_ratio"
        );
    }

    #[test]
    fn test_translate_label_name_strategies() {
        assert_eq!(
            translate_label_name("service.name", UnderscoreEscapingWithSuffixes),
            "service_name"
        );
        assert_eq!(
            translate_label_name("_foo", UnderscoreEscapingWithoutSuffixes),
            "key_foo"
        );
        assert_eq!(
            translate_label_name("service.name", NoUtf8EscapingWithSuffixes),
            "service.name"
        );
        assert_eq!(translate_label_name("_foo", NoTranslation), "_foo");
    }

    #[test]
    fn test_clean_unit_name() {
        assert_eq!(clean_unit_name("faults"), "faults");
        assert_eq!(clean_unit_name("{faults}"), "faults");
        assert_eq!(clean_unit_name("req/sec"), "req_sec");
        assert_eq!(clean_unit_name("m/s"), "m_s");
        assert_eq!(clean_unit_name("___test___"), "test");
        assert_eq!(
            clean_unit_name("multiple__underscores"),
            "multiple_underscores"
        );
        assert_eq!(clean_unit_name(""), "");
        assert_eq!(clean_unit_name("___"), "");
        assert_eq!(clean_unit_name("bytes.per.second"), "bytes_per_second");
    }
}
