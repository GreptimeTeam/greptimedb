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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const KEY: &str = "region_engine";
    const KEYS: &[&str] = &[KEY];

    /// The default sequence used by the tests: two exported entries, `mito` before `file`,
    /// with one nested table to override.
    fn defaults() -> JsonValue {
        json!({
            "region_engine": [
                {
                    "mito": {
                        "global_write_buffer_size": "64MB",
                        "index": {
                            "aux_path": "/default/aux",
                            "cache_percent": 30
                        }
                    }
                },
                {"file": {"data_dir": "/default/data"}}
            ],
            "data_home": "/default/data_home"
        })
    }

    /// A fixed set of variables, so the tests never touch the process environment and can
    /// run in parallel.
    fn env(vars: &[(&str, &str)]) -> Environment {
        let mut vars_map = Map::new();
        for (key, value) in vars {
            vars_map.insert((*key).to_string(), (*value).to_string());
        }

        Environment::default()
            .separator("__")
            .try_parsing(true)
            .source(Some(vars_map))
    }

    fn collect_with(vars: &[(&str, &str)], defaults: &JsonValue) -> Map<String, Value> {
        TaggedSeqEnvSource::new(env(vars), KEYS, defaults)
            .collect()
            .unwrap()
    }

    fn collect(vars: &[(&str, &str)]) -> Map<String, Value> {
        collect_with(vars, &defaults())
    }

    fn to_json(collected: Map<String, Value>) -> JsonValue {
        let mut map = serde_json::Map::new();
        for (key, value) in collected {
            map.insert(key, value.try_deserialize().unwrap());
        }

        JsonValue::Object(map)
    }

    fn collect_json(vars: &[(&str, &str)]) -> JsonValue {
        to_json(collect(vars))
    }

    fn collect_json_with(vars: &[(&str, &str)], defaults: &JsonValue) -> JsonValue {
        to_json(collect_with(vars, defaults))
    }

    fn collect_error_with(vars: &[(&str, &str)], defaults: &JsonValue) -> String {
        match TaggedSeqEnvSource::new(env(vars), KEYS, defaults).collect() {
            Ok(_) => panic!("expected an error, got {vars:?}"),
            Err(ConfigError::Message(message)) => message,
            Err(other) => panic!("expected a message error, got: {other}"),
        }
    }

    fn collect_error(vars: &[(&str, &str)]) -> String {
        collect_error_with(vars, &defaults())
    }

    #[test]
    fn test_collect_overrides_a_scalar_field() {
        let collected = collect_json(&[("REGION_ENGINE__MITO__GLOBAL_WRITE_BUFFER_SIZE", "128MB")]);

        let engines = collected["region_engine"].as_array().unwrap();
        assert_eq!(engines.len(), 2);
        assert_eq!(engines[0]["mito"]["global_write_buffer_size"], "128MB");
        // The fields next to the override keep their default value.
        assert_eq!(engines[0]["mito"]["index"]["aux_path"], "/default/aux");
        assert_eq!(engines[1]["file"]["data_dir"], "/default/data");
    }

    #[test]
    fn test_collect_overrides_a_nested_field() {
        let collected = collect_json(&[("REGION_ENGINE__MITO__INDEX__AUX_PATH", "/tmp/aux")]);

        assert_eq!(
            collected["region_engine"][0]["mito"]["index"]["aux_path"],
            "/tmp/aux"
        );
        assert_eq!(
            collected["region_engine"][0]["mito"]["index"]["cache_percent"],
            30
        );
        assert_eq!(
            collected["region_engine"][0]["mito"]["global_write_buffer_size"],
            "64MB"
        );
    }

    #[test]
    fn test_collect_merges_several_variables_of_one_entry() {
        let collected = collect_json(&[
            ("REGION_ENGINE__MITO__INDEX__AUX_PATH", "/tmp/aux"),
            ("REGION_ENGINE__MITO__INDEX__CACHE_PERCENT", "50"),
        ]);

        assert_eq!(
            collected["region_engine"][0]["mito"]["index"]["aux_path"],
            "/tmp/aux"
        );
        assert_eq!(
            collected["region_engine"][0]["mito"]["index"]["cache_percent"],
            50
        );
    }

    #[test]
    fn test_collect_appends_unknown_tags_in_tag_order() {
        let collected = collect_json(&[
            (
                "REGION_ENGINE__METRIC__FLUSH_METADATA_REGION_INTERVAL",
                "1m",
            ),
            ("REGION_ENGINE__AAA__DATA_DIR", "/tmp/aaa"),
        ]);

        let engines = collected["region_engine"].as_array().unwrap();
        assert_eq!(engines.len(), 4);
        // The default entries stay first and keep their order.
        assert_eq!(engines[0]["mito"]["global_write_buffer_size"], "64MB");
        assert_eq!(engines[1]["file"]["data_dir"], "/default/data");
        // Entries that only the environment mentions are appended, sorted by tag.
        assert_eq!(engines[2], json!({"aaa": {"data_dir": "/tmp/aaa"}}));
        assert_eq!(
            engines[3],
            json!({"metric": {"flush_metadata_region_interval": "1m"}})
        );
    }

    #[test]
    fn test_collect_keeps_the_default_sequence_without_overrides() {
        let collected = collect_json(&[("DATA_HOME", "/srv/data")]);

        assert_eq!(collected["region_engine"], defaults()["region_engine"]);
        assert_eq!(collected["data_home"], "/srv/data");
    }

    #[test]
    fn test_collect_removes_the_flat_keys_of_the_sequence() {
        let collected = collect(&[
            ("REGION_ENGINE__MITO__INDEX__AUX_PATH", "/tmp/aux"),
            ("REGION_ENGINE_EXTRA__MITO__AUX_PATH", "/tmp/other"),
        ]);

        // A leftover `region_engine.x` key would be written after the sequence and destroy
        // it again, so all of them must be consumed by the merge.
        assert!(
            !collected
                .keys()
                .any(|key| key.starts_with("region_engine.")),
            "flat keys left in the collected map: {:?}",
            collected.keys().collect::<Vec<_>>()
        );
        // A key that only shares the prefix is not part of the sequence.
        assert!(collected.contains_key("region_engine_extra.mito.aux_path"));
    }

    #[test]
    fn test_collect_without_default_entries() {
        let collected = collect_json_with(
            &[
                (
                    "REGION_ENGINE__METRIC__FLUSH_METADATA_REGION_INTERVAL",
                    "1m",
                ),
                ("REGION_ENGINE__FILE__DATA_DIR", "/tmp/data"),
            ],
            &json!({}),
        );

        assert_eq!(
            collected["region_engine"],
            json!([
                {"file": {"data_dir": "/tmp/data"}},
                {"metric": {"flush_metadata_region_interval": "1m"}}
            ])
        );
    }

    #[test]
    fn test_collect_with_null_default_entries() {
        let collected = collect_json_with(
            &[("REGION_ENGINE__FILE__DATA_DIR", "/tmp/data")],
            &json!({"region_engine": null}),
        );

        assert_eq!(
            collected["region_engine"],
            json!([{"file": {"data_dir": "/tmp/data"}}])
        );
    }

    #[test]
    fn test_collect_rejects_setting_the_whole_sequence() {
        let message = collect_error(&[("REGION_ENGINE", "mito")]);

        assert!(
            message.contains("cannot be overridden as a whole"),
            "{message}"
        );
        assert!(
            message.contains("region_engine__<TAG>__<FIELD>"),
            "{message}"
        );
    }

    #[test]
    fn test_collect_rejects_a_numeric_index() {
        let message = collect_error(&[("REGION_ENGINE__0__MITO__INDEX__AUX_PATH", "/tmp/aux")]);

        assert!(message.contains("numeric index"), "{message}");
        assert!(message.contains("addressed by tag name"), "{message}");
    }

    #[test]
    fn test_collect_rejects_a_tag_without_field() {
        let message = collect_error(&[("REGION_ENGINE__MITO", "mito")]);

        assert!(message.contains("needs a field"), "{message}");
    }

    #[test]
    fn test_collect_rejects_conflicting_overrides() {
        let message = collect_error(&[
            ("REGION_ENGINE__MITO__INDEX", "not-a-table"),
            ("REGION_ENGINE__MITO__INDEX__AUX_PATH", "/tmp/aux"),
        ]);

        assert!(
            message.contains("another environment variable sets 'region_engine.mito.index'"),
            "{message}"
        );
    }

    #[test]
    fn test_collect_rejects_a_field_below_a_scalar_default() {
        let message = collect_error(&[("REGION_ENGINE__FILE__DATA_DIR__SUB", "/tmp/sub")]);

        assert!(message.contains("region_engine.file.data_dir"), "{message}");
        assert!(
            message.contains("default value is not a table"),
            "{message}"
        );
    }

    #[test]
    fn test_collect_rejects_a_scalar_default_sequence() {
        let message = collect_error_with(
            &[("REGION_ENGINE__MITO__INDEX__AUX_PATH", "/tmp/aux")],
            &json!({"region_engine": "not-an-array"}),
        );

        assert!(
            message.contains("not a sequence of single-key tables"),
            "{message}"
        );
    }

    #[test]
    fn test_collect_rejects_a_default_entry_with_two_tags() {
        let message = collect_error_with(
            &[("REGION_ENGINE__MITO__INDEX__AUX_PATH", "/tmp/aux")],
            &json!({"region_engine": [{"mito": {}, "file": {}}]}),
        );

        assert!(message.contains("exactly one tag"), "{message}");
    }

    #[test]
    fn test_take_entries_keeps_unrelated_keys() {
        let mut collected = Map::new();
        for key in [
            "region_engine",
            "region_engine.mito.index",
            "region_engine_extra.mito",
            "other",
        ] {
            collected.insert(
                (*key).to_string(),
                Value::new(None, ValueKind::String("v".to_string())),
            );
        }

        let entries = take_entries(&mut collected, "region_engine");

        assert_eq!(
            entries
                .iter()
                .map(|(key, _)| key.as_str())
                .collect::<Vec<_>>(),
            vec!["region_engine", "region_engine.mito.index"]
        );
        assert!(!collected.contains_key("region_engine"));
        assert!(!collected.contains_key("region_engine.mito.index"));
        assert!(collected.contains_key("region_engine_extra.mito"));
        assert!(collected.contains_key("other"));
    }

    #[test]
    fn test_is_index_like() {
        for tag in ["0", "12", "-1", "+1", "1e3", "-1.5"] {
            assert!(is_index_like(tag), "{tag} should look like an index");
        }

        for tag in ["mito", "file", "metric", "", "m1", "-", "+"] {
            assert!(!is_index_like(tag), "{tag} should not look like an index");
        }
    }
}
