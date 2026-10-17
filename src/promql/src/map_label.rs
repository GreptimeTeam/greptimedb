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

//! `label_replace` and `label_join` over evaluated series, following Prometheus
//! `evalLabelReplace`, `evalLabelJoin` and `mergeSeriesWithSameLabelset`.

use std::sync::{Arc, LazyLock};

use datafusion::error::{DataFusionError, Result};
use datatypes::series::MetricName;
use datatypes::series::samples::{Series, SeriesSchema};
use regex::Regex;

use crate::grid_series::merge_same_labelset;

#[cfg(test)]
mod tests;

/// `label_replace(v, dst, replacement, src, regex)`: where `regex` matches the whole value of
/// `src`, `dst` is set to `replacement` with `$1`, `${name}` and `$$` expanded.
///
/// `regex` compiles with the `regex` crate, whose dialect differs from Go's RE2 used by
/// Prometheus: Perl classes (`\d`, `\w`, `\s`, `\b`) are Unicode-aware, `\Q...\E` is rejected,
/// `(?x)` and repeat counts above 1000 are accepted, and `\<`, `\>` are word boundaries instead
/// of literal characters.
pub fn label_replace(
    schema: &Arc<SeriesSchema>,
    mut series: Vec<Series>,
    dst: &str,
    replacement: &str,
    src: &str,
    regex: &str,
) -> Result<Vec<Series>> {
    let compiled = Regex::new(&format!("^(?s:{regex})$")).map_err(|_| {
        DataFusionError::Plan(format!(
            "invalid regular expression in label_replace(): {regex}"
        ))
    })?;
    if dst.is_empty() {
        return Err(DataFusionError::Plan(
            "invalid destination label name in label_replace(): ".into(),
        ));
    }
    let mut value = String::new();
    for series in &mut series {
        let Some(captures) = compiled.captures(series.metric.get(src).unwrap_or_default()) else {
            continue;
        };
        value.clear();
        expand(&compiled, &captures, replacement, &mut value);
        set_label(&mut series.metric, dst, &value)?;
    }
    merge_same_labelset(schema, series)
}

/// Go `regexp.Expand` reads a group name as Unicode letters (L), decimal digits (Nd) and `_`,
/// narrower than `char::is_alphanumeric`.
static GROUP_NAME: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[\p{L}\p{Nd}_]*").unwrap());

/// Go `regexp.Expand`, whose template syntax differs from `regex::Captures::expand`: a numeric
/// reference with a leading zero, like `$01`, or of more than nine digits is a group name.
fn expand(regex: &Regex, captures: &regex::Captures<'_>, template: &str, out: &mut String) {
    let mut rest = template;
    while let Some(dollar) = rest.find('$') {
        out.push_str(&rest[..dollar]);
        rest = &rest[dollar + 1..];
        if let Some(after) = rest.strip_prefix('$') {
            out.push('$');
            rest = after;
            continue;
        }
        let (braced, body) = match rest.strip_prefix('{') {
            Some(body) => (true, body),
            None => (false, rest),
        };
        let len = GROUP_NAME.find(body).map_or(0, |name| name.end());
        let name = &body[..len];
        let closed = !braced || body[len..].starts_with('}');
        if name.is_empty() || !closed {
            // Malformed; Go keeps the `$` as text.
            out.push('$');
            continue;
        }
        rest = &body[len + usize::from(braced)..];
        let number = (name.bytes().all(|b| b.is_ascii_digit())
            && !(name.len() > 1 && name.starts_with('0')))
        .then(|| name.parse::<usize>().ok())
        .flatten()
        .filter(|_| name.len() <= 9);
        let group = match number {
            Some(number) => captures.get(number),
            None => regex
                .capture_names()
                .position(|group| group == Some(name))
                .and_then(|index| captures.get(index)),
        };
        if let Some(group) = group {
            out.push_str(group.as_str());
        }
    }
    out.push_str(rest);
}

/// `label_join(v, dst, separator, src...)`: `dst` is set to the values of `src` joined by
/// `separator`, an absent label joining as the empty string.
pub fn label_join(
    schema: &Arc<SeriesSchema>,
    mut series: Vec<Series>,
    dst: &str,
    separator: &str,
    src: &[&str],
) -> Result<Vec<Series>> {
    if let Some(src) = src.iter().find(|src| src.is_empty()) {
        return Err(DataFusionError::Plan(format!(
            "invalid source label name in label_join(): {src}"
        )));
    }
    if dst.is_empty() {
        return Err(DataFusionError::Plan(
            "invalid destination label name in label_join(): ".into(),
        ));
    }
    let mut value = String::new();
    for series in &mut series {
        value.clear();
        for (index, src) in src.iter().enumerate() {
            if index > 0 {
                value.push_str(separator);
            }
            value.push_str(series.metric.get(src).unwrap_or_default());
        }
        set_label(&mut series.metric, dst, &value)?;
    }
    merge_same_labelset(schema, series)
}

/// An empty value removes the label, as Prometheus label sets have no empty values.
fn set_label(metric: &mut MetricName, name: &str, value: &str) -> Result<()> {
    if value.is_empty() {
        metric.remove(name);
        return Ok(());
    }
    metric
        .set(name, Arc::from(value))
        .map_err(|error| DataFusionError::External(Box::new(error)))
}
