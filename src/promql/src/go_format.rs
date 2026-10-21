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

//! The shortest float formats of Go's `strconv`, which Prometheus writes label values in.

#[cfg(test)]
mod tests;

/// Go's `strconv.FormatFloat(value, 'f', -1, 64)`, the form of `count_values` labels.
pub(crate) fn format_float_f(value: f64) -> String {
    match special(value) {
        Some(text) => text.to_string(),
        None => {
            let (negative, digits, point) = shortest(value);
            positional(negative, &digits, point)
        }
    }
}

/// Prometheus `labels.FormatOpenMetricsFloat`, the form of `histogram_quantiles` labels: Go's
/// shortest `'g'` format, with `.0` appended to an integer and `0`, `1` and `-1` written as
/// `0.0`, `1.0` and `-1.0`.
pub(crate) fn format_open_metrics_float(value: f64) -> String {
    if value == 1.0 {
        return "1.0".to_string();
    }
    if value == 0.0 {
        return "0.0".to_string();
    }
    if value == -1.0 {
        return "-1.0".to_string();
    }
    if let Some(text) = special(value) {
        return text.to_string();
    }
    let mut text = format_float_g(value);
    if !text.contains(['e', '.']) {
        text.push_str(".0");
    }
    text
}

/// Go's `strconv.FormatFloat(value, 'g', -1, 64)`, which `%g` and `%v` print a float in: the
/// shortest digits, with an exponent of at least two digits below 1e-4 and from 1e6 on.
pub(crate) fn format_float_g(value: f64) -> String {
    if let Some(text) = special(value) {
        return text.to_string();
    }
    let (negative, digits, point) = shortest(value);
    let exponent = point - 1;
    if (-4..6).contains(&exponent) {
        return positional(negative, &digits, point);
    }
    let mut text = String::from(if negative { "-" } else { "" });
    text.push_str(&digits[..1]);
    if digits.len() > 1 {
        text.push('.');
        text.push_str(&digits[1..]);
    }
    text.push_str(&format!(
        "e{}{:02}",
        if exponent < 0 { '-' } else { '+' },
        exponent.abs()
    ));
    text
}

fn special(value: f64) -> Option<&'static str> {
    match value {
        f64::INFINITY => Some("+Inf"),
        f64::NEG_INFINITY => Some("-Inf"),
        _ if value.is_nan() => Some("NaN"),
        _ => None,
    }
}

/// The fewest significant digits of a finite `value` that round-trip, without trailing zeros, and
/// the position of the decimal point among them: `value = ±0.digits × 10^point`. Zero is `"0"` at
/// point 1.
///
/// When two candidates of that length are equally close, Go takes the even one while Rust's
/// shortest formatting rounds up, so the digits are recomputed with Rust's exact formatting,
/// which rounds half to even. The recomputed digits only count when they still parse back to
/// `value`: just above a power of two the lower neighbour is half as far away, so the even
/// candidate can name that neighbour, and only the other one round-trips.
fn shortest(value: f64) -> (bool, String, i64) {
    let shortest = format!("{value:e}");
    let mantissa = shortest.split_once('e').expect("exponent format").0;
    let significant = mantissa.chars().filter(char::is_ascii_digit).count();
    let exact = format!("{value:.precision$e}", precision = significant - 1);
    let chosen = if exact.parse::<f64>() == Ok(value) {
        &exact
    } else {
        &shortest
    };
    let (mantissa, exponent) = chosen.split_once('e').expect("exponent format");
    let exponent: i64 = exponent.parse().expect("integer exponent");
    let (negative, mantissa) = match mantissa.strip_prefix('-') {
        Some(unsigned) => (true, unsigned),
        None => (false, mantissa),
    };
    let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    let trimmed = digits.trim_end_matches('0');
    let digits = if trimmed.is_empty() { "0" } else { trimmed };
    (negative, digits.to_string(), exponent + 1)
}

/// `±0.digits × 10^point` in positional notation without trailing fractional zeros.
fn positional(negative: bool, digits: &str, point: i64) -> String {
    let mut out = String::from(if negative { "-" } else { "" });
    if point <= 0 {
        out.push_str("0.");
        out.extend(std::iter::repeat_n('0', (-point) as usize));
        out.push_str(digits);
    } else if point as usize >= digits.len() {
        out.push_str(digits);
        out.extend(std::iter::repeat_n('0', point as usize - digits.len()));
    } else {
        let (whole, fraction) = digits.split_at(point as usize);
        out.push_str(whole);
        out.push('.');
        out.push_str(fraction);
    }
    out
}
