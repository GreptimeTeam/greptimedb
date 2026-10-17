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

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datatypes::series::MetricName;
use datatypes::series::samples::{Series, SeriesSchema};
use regex::Regex;

use crate::grid_series::merge_same_labelset;

#[cfg(test)]
mod tests;

/// `label_replace(v, dst, replacement, src, regex)`: where `regex` matches the whole value of
/// `src`, `dst` is set to `replacement` with `$1`, `${name}` and `$$` expanded.
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
        captures.expand(replacement, &mut value);
        set_label(&mut series.metric, dst, &value)?;
    }
    merge_same_labelset(schema, series)
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
