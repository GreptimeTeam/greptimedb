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

use crate::go_format::{format_float_f, format_open_metrics_float};

/// Prometheus `labels.FormatOpenMetricsFloat`; `open_metrics_cases.txt` comes from promql-compat
/// `narthex-golden-formatfloat -openmetrics` as `<hex bits> <formatted>`.
#[test]
fn open_metrics_floats_format_like_prometheus() {
    let mut checked = 0;
    for line in include_str!("open_metrics_cases.txt").lines() {
        let (bits, expected) = line.split_once(' ').unwrap();
        let value = f64::from_bits(u64::from_str_radix(bits, 16).unwrap());
        assert_eq!(format_open_metrics_float(value), expected, "{bits}");
        checked += 1;
    }
    assert!(checked > 300, "{checked}");
}

/// Go's `strconv.FormatFloat(v, 'f', -1, 64)`, the form `count_values` writes value labels in,
/// including the round-half-even choice of the last shortest digit and the round-trip check at
/// powers of two; `float_cases.txt` comes from promql-compat `narthex-golden-formatfloat`.
#[test]
fn floats_format_like_go() {
    let mut checked = 0;
    for line in include_str!("float_cases.txt").lines() {
        let (bits, expected) = line.split_once(' ').unwrap();
        let value = f64::from_bits(u64::from_str_radix(bits, 16).unwrap());
        assert_eq!(format_float_f(value), expected, "{bits}");
        checked += 1;
    }
    assert!(checked > 2_400, "{checked}");
    for (value, expected) in [
        (f64::NAN, "NaN"),
        (f64::INFINITY, "+Inf"),
        (f64::NEG_INFINITY, "-Inf"),
    ] {
        assert_eq!(format_float_f(value), expected);
    }
}
