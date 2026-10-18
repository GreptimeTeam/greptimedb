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

//! Environment overrides for configuration keys holding a sequence of tagged tables.
//!
//! Such a sequence is what a `Vec` of externally tagged enums serializes to, for
//! example the `region_engine` option:
//!
//! ```text
//! [{"mito": {...}}, {"file": {...}}]
//! ```
//!
//! Handing the plain [`Environment`] source to `config` cannot work for these keys:
//! `config` writes every flat dotted key by walking down its path, and the walk resets
//! any intermediate value that is not a table. Walking `region_engine.mito.index.aux_path`
//! therefore destroys the default sequence before anything is set.
//!
//! This source instead addresses the entries by their tag name and merges the
//! overrides onto the matching default entries:
//!
//! ```text
//! GREPTIMEDB_STANDALONE__REGION_ENGINE__MITO__INDEX__AUX_PATH=...
//! ```
//!
//! Entries that only the environment mentions are appended, and every field that is
//! not mentioned keeps its default value. A configuration file that configures the
//! whole sequence replaces the merged one, which makes these variables ineffective;
//! the file has to spell out every entry it wants.

use config::{ConfigError, Environment, Map, Source, Value, ValueKind};
use serde_json::Value as JsonValue;

use crate::error::{
    Error, Result, TaggedSeqInvalidDefaultSnafu, TaggedSeqMissingFieldSnafu,
    TaggedSeqMissingTagSnafu, TaggedSeqNotATableSnafu, TaggedSeqNumericTagSnafu,
    TaggedSeqOverridesConflictSnafu,
};

/// A [`Source`] that merges environment overrides into sequences of tagged tables.
///
/// See the [module documentation](self) for the addressed syntax.
#[derive(Debug)]
pub(crate) struct TaggedSeqEnvSource {
    /// The wrapped environment source. Everything it collects is kept as is, apart
    /// from the entries that address one of the `targets`.
    inner: Environment,
    /// The keys whose entries are addressed by tag name.
    targets: Vec<Target>,
}

/// One sequence of tagged tables that the environment may override.
#[derive(Debug, Clone)]
struct Target {
    /// The dotted key of the sequence, e.g. `region_engine`.
    key: String,
    /// The default value of `key`, whose entries the overrides are merged into.
    default: Option<JsonValue>,
}

impl TaggedSeqEnvSource {
    /// Wraps `inner` so that the sequence at every key of `keys` accepts tag name
    /// overrides from the environment, merged onto the value that the same key has
    /// in `defaults`.
    pub(crate) fn new(
        inner: Environment,
        keys: &'static [&'static str],
        defaults: &JsonValue,
    ) -> Self {
        let targets = keys
            .iter()
            .map(|key| Target {
                key: (*key).to_string(),
                default: defaults.get(key).cloned(),
            })
            .collect();

        Self { inner, targets }
    }
}

impl Source for TaggedSeqEnvSource {
    fn clone_into_box(&self) -> Box<dyn Source + Send + Sync> {
        Box::new(Self {
            inner: self.inner.clone(),
            targets: self.targets.clone(),
        })
    }

    fn collect(&self) -> std::result::Result<Map<String, Value>, ConfigError> {
        let mut collected = self.inner.collect()?;

        for target in &self.targets {
            // Take the entries out first: `config` walks the collected map in an
            // unspecified order, so a leftover `region_engine.mito.x` entry could be
            // written after the sequence and destroy it again.
            let entries = take_entries(&mut collected, &target.key);
            let merged = merge(target, entries).map_err(|e| ConfigError::Message(e.to_string()))?;
            collected.insert(target.key.clone(), merged);
        }

        Ok(collected)
    }
}

/// Removes and returns the entries that address `key` itself or one of its children.
fn take_entries(collected: &mut Map<String, Value>, key: &str) -> Vec<(String, Value)> {
    let child_prefix = format!("{key}.");
    let mut keys = collected
        .keys()
        .filter(|existing| *existing == key || existing.starts_with(&child_prefix))
        .cloned()
        .collect::<Vec<_>>();
    // `Map` is unordered, sort so that a reported error is deterministic.
    keys.sort();

    keys.into_iter()
        .filter_map(|key| collected.remove(&key).map(|value| (key, value)))
        .collect()
}

/// Rebuilds the sequence of `target` with the environment overrides `entries` applied.
fn merge(target: &Target, entries: Vec<(String, Value)>) -> Result<Value> {
    let mut overrides = group_entries(target, entries)?;

    let mut merged = Vec::with_capacity(overrides.len() + 1);
    for entry in default_entries(target)? {
        let mut entry = json_to_value(entry);
        let tag = single_tag(target, &entry)?;

        if let Some(value) = overrides.remove(&tag) {
            apply_override(target, &tag, &mut entry, value)?;
        }

        merged.push(entry);
    }

    // Entries that the default sequence does not have are appended.
    let mut appended = overrides.into_iter().collect::<Vec<_>>();
    appended.sort_by(|a, b| a.0.cmp(&b.0));
    for (tag, value) in appended {
        let mut table = Map::new();
        table.insert(tag, value);
        merged.push(Value::new(None, ValueKind::Table(table)));
    }

    Ok(Value::new(None, ValueKind::Array(merged)))
}

/// Merges the override of the entry `tag` into `entry`, whose only key is `tag`.
fn apply_override(target: &Target, tag: &str, entry: &mut Value, value: Value) -> Result<()> {
    let ValueKind::Table(body) = &mut entry.kind else {
        return invalid_default(target, "expected an array of tables");
    };

    let Some(field) = body.get_mut(tag) else {
        return invalid_default(target, "expected each entry to hold exactly one tag");
    };

    // Both sides are tables, this only fails on a field whose default value cannot hold
    // the sub-table that the environment sets.
    deep_merge(field, value).map_err(|path| not_a_table(&entry_path(target, tag, &path)))
}

/// Returns the default entries of `target`, each one a single-key table `{tag: table}`.
fn default_entries(target: &Target) -> Result<&[JsonValue]> {
    match &target.default {
        None | Some(JsonValue::Null) => Ok(&[]),
        Some(JsonValue::Array(entries)) => Ok(entries),
        Some(_) => invalid_default(target, "expected an array of tables"),
    }
}

/// Returns the tag of `entry`, which must map one tag to a table.
fn single_tag(target: &Target, entry: &Value) -> Result<String> {
    let ValueKind::Table(table) = &entry.kind else {
        return invalid_default(target, "expected an array of tables");
    };

    let mut keys = table.keys();
    let (Some(tag), None) = (keys.next(), keys.next()) else {
        return invalid_default(target, "expected each entry to hold exactly one tag");
    };
    if !matches!(&table[tag].kind, ValueKind::Table(_)) {
        return invalid_default(target, "expected each entry to map its tag to a table");
    }

    Ok(tag.clone())
}

/// Groups the overrides by tag, nesting every field path into a table.
fn group_entries(target: &Target, entries: Vec<(String, Value)>) -> Result<Map<String, Value>> {
    // Every entry but `key` itself is addressed as `<key>.<tag>.<field>...`.
    let offset = target.key.len() + 1;
    let mut groups: Map<String, Value> = Map::new();

    for (key, value) in entries {
        if key == target.key {
            return TaggedSeqMissingTagSnafu {
                key,
                target: target.key.clone(),
            }
            .fail();
        }

        let rest = key[offset..].to_string();
        let Some((tag, field)) = rest.split_once('.') else {
            return TaggedSeqMissingFieldSnafu {
                key,
                tag: rest,
                target: target.key.clone(),
            }
            .fail();
        };

        if is_index_like(tag) {
            return TaggedSeqNumericTagSnafu {
                key,
                tag: tag.to_string(),
                target: target.key.clone(),
            }
            .fail();
        }

        let field = nest(field, value);
        match groups.get_mut(tag) {
            Some(group) => deep_merge(group, field)
                .map_err(|path| overrides_conflict(&key, &entry_path(target, tag, &path)))?,
            None => {
                groups.insert(tag.to_string(), field);
            }
        }
    }

    Ok(groups)
}

/// Whether `tag` looks like a position in the sequence rather than an engine name.
///
/// `config` also accepts negative and floating point looking path segments (`[-1]`,
/// `[1e3]`), so anything that starts like a number is reported as a misplaced index;
/// otherwise those variables would only fail later with the generic `unknown variant`
/// error from serde.
fn is_index_like(tag: &str) -> bool {
    let mut chars = tag.chars();
    match chars.next() {
        Some(first) if first.is_ascii_digit() => true,
        Some('-') | Some('+') => {
            matches!(chars.next(), Some(next) if next.is_ascii_digit() || next == '.')
        }
        _ => false,
    }
}

/// Builds a table whose key path is the dotted `path`, e.g. `a.b` and `v` give
/// `{a: {b: v}}`.
fn nest(path: &str, value: Value) -> Value {
    let mut table = Map::new();
    match path.split_once('.') {
        Some((head, tail)) => {
            table.insert(head.to_string(), nest(tail, value));
        }
        None => {
            table.insert(path.to_string(), value);
        }
    }

    Value::new(None, ValueKind::Table(table))
}

/// Merges `incoming` into `base`, recursively for tables. Returns the dotted path of
/// the field that cannot be merged into, when `base` holds a non-table value where
/// `incoming` needs a table.
fn deep_merge(base: &mut Value, incoming: Value) -> std::result::Result<(), String> {
    merge_at(base, incoming, "")
}

fn merge_at(base: &mut Value, incoming: Value, path: &str) -> std::result::Result<(), String> {
    match incoming.kind {
        ValueKind::Table(incoming) => {
            let ValueKind::Table(base) = &mut base.kind else {
                return Err(path.to_string());
            };
            for (field, value) in incoming {
                let field_path = if path.is_empty() {
                    field.clone()
                } else {
                    format!("{path}.{field}")
                };
                // A non-table override replaces whatever the key already holds. It is inserted
                // as is, so the value keeps the origin it was collected with.
                if matches!(value.kind, ValueKind::Table(_)) {
                    match base.get_mut(&field) {
                        Some(existing) => merge_at(existing, value, &field_path)?,
                        None => {
                            base.insert(field, value);
                        }
                    }
                } else {
                    base.insert(field, value);
                }
            }
            Ok(())
        }
        kind => {
            // A non-table override replaces whatever the key already holds.
            base.kind = kind;
            Ok(())
        }
    }
}

fn invalid_default<T>(target: &Target, reason: &str) -> Result<T> {
    TaggedSeqInvalidDefaultSnafu {
        target: target.key.clone(),
        reason: reason.to_string(),
    }
    .fail()
}

/// Builds the dotted path of `path` below the entry `tag` of `target`.
fn entry_path(target: &Target, tag: &str, path: &str) -> String {
    if path.is_empty() {
        format!("{}.{}", target.key, tag)
    } else {
        format!("{}.{}.{}", target.key, tag, path)
    }
}

/// Error for a field whose default value is not a table, so nothing can be set below it.
fn not_a_table(path: &str) -> Error {
    TaggedSeqNotATableSnafu {
        path: path.to_string(),
    }
    .build()
}

/// Error for a key that sets a field below a path another key sets as a whole.
fn overrides_conflict(key: &str, path: &str) -> Error {
    TaggedSeqOverridesConflictSnafu {
        key: key.to_string(),
        path: path.to_string(),
    }
    .build()
}

/// Converts a default value parsed from the JSON serialization of the options.
fn json_to_value(json: &JsonValue) -> Value {
    let kind = match json {
        JsonValue::Null => ValueKind::Nil,
        JsonValue::Bool(value) => ValueKind::Boolean(*value),
        JsonValue::Number(value) => match (value.as_i64(), value.as_u64()) {
            (Some(value), _) => ValueKind::I64(value),
            (None, Some(value)) => ValueKind::U64(value),
            (None, None) => ValueKind::Float(value.as_f64().unwrap_or_default()),
        },
        JsonValue::String(value) => ValueKind::String(value.clone()),
        JsonValue::Array(values) => ValueKind::Array(values.iter().map(json_to_value).collect()),
        JsonValue::Object(values) => ValueKind::Table(
            values
                .iter()
                .map(|(key, value)| (key.clone(), json_to_value(value)))
                .collect(),
        ),
    };

    Value::new(None, kind)
}
