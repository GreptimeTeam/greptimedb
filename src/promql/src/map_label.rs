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

use std::fmt;
use std::sync::{Arc, LazyLock};

use datafusion::error::{DataFusionError, Result};
use datatypes::series::SeriesLabels;
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
    series: Vec<Series>,
    dst: &str,
    replacement: &str,
    src: &str,
    regex: &str,
) -> Result<Vec<Series>> {
    rewrite_all(
        schema,
        series,
        &LabelRewrite::replace(dst, replacement, src, regex)?,
    )
}

/// `label_join(v, dst, separator, src...)`: `dst` is set to the values of `src` joined by
/// `separator`, an absent label joining as the empty string.
pub fn label_join(
    schema: &Arc<SeriesSchema>,
    series: Vec<Series>,
    dst: &str,
    separator: &str,
    src: &[&str],
) -> Result<Vec<Series>> {
    rewrite_all(schema, series, &LabelRewrite::join(dst, separator, src)?)
}

fn rewrite_all(
    schema: &Arc<SeriesSchema>,
    mut series: Vec<Series>,
    rewrite: &LabelRewrite,
) -> Result<Vec<Series>> {
    for series in &mut series {
        rewrite.apply(&mut series.metric)?;
    }
    merge_same_labelset(schema, series)
}

/// The label change of `label_replace` or `label_join`, checked and compiled once and applied to
/// each series on its own. Series that end up with one label set are the caller's to merge.
///
/// Built only through [`Self::replace`] and [`Self::join`], so the expression is anchored and the
/// label names are checked.
#[derive(Debug)]
pub struct LabelRewrite(Change);
impl serde::Serialize for LabelRewrite {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        #[derive(serde::Serialize)]
        enum Wire<'a> {
            Replace {
                dst: &'a str,
                replacement: &'a str,
                src: &'a str,
                pattern: &'a str,
            },
            Join {
                dst: &'a str,
                separator: &'a str,
                src: Vec<&'a str>,
            },
        }
        let wire = match &self.0 {
            Change::Replace {
                dst,
                replacement,
                src,
                pattern,
                ..
            } => Wire::Replace {
                dst,
                replacement,
                src,
                pattern,
            },
            Change::Join {
                dst,
                separator,
                src,
            } => Wire::Join {
                dst,
                separator,
                src: src.iter().map(AsRef::as_ref).collect(),
            },
        };
        serde::Serialize::serialize(&wire, serializer)
    }
}
impl<'de> serde::Deserialize<'de> for LabelRewrite {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        enum Wire {
            Replace {
                dst: String,
                replacement: String,
                src: String,
                pattern: String,
            },
            Join {
                dst: String,
                separator: String,
                src: Vec<String>,
            },
        }
        match <Wire as serde::Deserialize>::deserialize(deserializer)? {
            Wire::Replace {
                dst,
                replacement,
                src,
                pattern,
            } => {
                Self::replace(&dst, &replacement, &src, &pattern).map_err(serde::de::Error::custom)
            }
            Wire::Join {
                dst,
                separator,
                src,
            } => {
                let refs: Vec<_> = src.iter().map(String::as_str).collect();
                Self::join(&dst, &separator, &refs).map_err(serde::de::Error::custom)
            }
        }
    }
}

#[derive(Debug)]
enum Change {
    Replace {
        dst: Arc<str>,
        replacement: Arc<str>,
        src: Arc<str>,
        /// The expression as written, which `regex` anchors.
        pattern: Arc<str>,
        regex: Regex,
    },
    Join {
        dst: Arc<str>,
        separator: Arc<str>,
        src: Arc<[Arc<str>]>,
    },
}

impl PartialEq for LabelRewrite {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (
                Change::Replace {
                    dst: a,
                    replacement: b,
                    src: c,
                    pattern: d,
                    ..
                },
                Change::Replace {
                    dst: w,
                    replacement: x,
                    src: y,
                    pattern: z,
                    ..
                },
            ) => (a, b, c, d) == (w, x, y, z),
            (
                Change::Join {
                    dst: a,
                    separator: b,
                    src: c,
                },
                Change::Join {
                    dst: w,
                    separator: x,
                    src: y,
                },
            ) => (a, b, c) == (w, x, y),
            _ => false,
        }
    }
}
impl Eq for LabelRewrite {}
impl std::hash::Hash for LabelRewrite {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match &self.0 {
            Change::Replace {
                dst,
                replacement,
                src,
                pattern,
                ..
            } => {
                0u8.hash(state);
                dst.hash(state);
                replacement.hash(state);
                src.hash(state);
                pattern.hash(state);
            }
            Change::Join {
                dst,
                separator,
                src,
            } => {
                1u8.hash(state);
                dst.hash(state);
                separator.hash(state);
                src.hash(state);
            }
        }
    }
}

impl LabelRewrite {
    /// `label_replace`'s change, failing on an invalid expression or an empty destination.
    pub fn replace(dst: &str, replacement: &str, src: &str, regex: &str) -> Result<Self> {
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
        Ok(Self(Change::Replace {
            dst: Arc::from(dst),
            replacement: Arc::from(replacement),
            src: Arc::from(src),
            pattern: Arc::from(regex),
            regex: compiled,
        }))
    }

    /// `label_join`'s change, failing on an empty source or destination.
    pub fn join(dst: &str, separator: &str, src: &[&str]) -> Result<Self> {
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
        Ok(Self(Change::Join {
            dst: Arc::from(dst),
            separator: Arc::from(separator),
            src: src.iter().map(|src| Arc::from(*src)).collect(),
        }))
    }

    /// An upper bound on the bytes the label this change writes to `metric` takes, read before it
    /// is built.
    ///
    /// The written value can be far larger than the input: `label_join` concatenates its sources,
    /// listed any number of times, and every `$` reference of a `label_replace` template can expand
    /// to the whole source value. The bound counts each source once per listing and each `$` as
    /// one whole source value.
    pub fn written_bytes(&self, metric: &SeriesLabels) -> usize {
        let value = |name: &str| metric.get(name).map_or(0, str::len);
        match &self.0 {
            Change::Replace {
                dst,
                replacement,
                src,
                ..
            } => {
                let references = replacement.matches('$').count();
                dst.len()
                    .saturating_add(replacement.len())
                    .saturating_add(references.saturating_mul(value(src)))
            }
            Change::Join {
                dst,
                separator,
                src,
            } => src.iter().fold(
                dst.len()
                    .saturating_add(separator.len().saturating_mul(src.len())),
                |total, name| total.saturating_add(value(name)),
            ),
        }
    }

    /// Changes `metric` the way the function changes one series.
    pub fn apply(&self, metric: &mut SeriesLabels) -> Result<()> {
        let mut value = String::new();
        match &self.0 {
            Change::Replace {
                dst,
                replacement,
                src,
                regex,
                ..
            } => {
                let Some(captures) = regex.captures(metric.get(src).unwrap_or_default()) else {
                    return Ok(());
                };
                expand(regex, &captures, replacement, &mut value);
                set_label(metric, dst, &value)
            }
            Change::Join {
                dst,
                separator,
                src,
            } => {
                for (index, src) in src.iter().enumerate() {
                    if index > 0 {
                        value.push_str(separator);
                    }
                    value.push_str(metric.get(src).unwrap_or_default());
                }
                set_label(metric, dst, &value)
            }
        }
    }
}

/// The call with its constant arguments, strings in Rust's debug quoting.
impl fmt::Display for LabelRewrite {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            Change::Replace {
                dst,
                replacement,
                src,
                pattern,
                ..
            } => write!(
                f,
                "label_replace {dst:?} {replacement:?} {src:?} {pattern:?}"
            ),
            Change::Join {
                dst,
                separator,
                src,
            } => write!(f, "label_join {dst:?} {separator:?} {src:?}"),
        }
    }
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

/// An empty value removes the label, as Prometheus label sets have no empty values.
fn set_label(metric: &mut SeriesLabels, name: &str, value: &str) -> Result<()> {
    if value.is_empty() {
        metric.remove(name);
        return Ok(());
    }
    metric
        .set(name, Arc::from(value))
        .map_err(|error| DataFusionError::External(Box::new(error)))
}
