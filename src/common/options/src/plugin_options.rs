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

use std::sync::{Arc, Mutex};

use serde::de::DeserializeOwned;
use serde_json::Value;

/// A trait for serializing Metasrv config to a JSON string.
/// So it can be used in the metasrv's crate instead of depending on the plugins' crate.
pub trait PluginOptionsSerializer: Send + Sync {
    fn serialize(&self) -> Result<String, serde_json::Error>;
}
pub type PluginOptionsSerializerRef = Arc<dyn PluginOptionsSerializer>;

/// A trait for deserializing Metasrv config from a JSON string.
pub trait PluginOptionsDeserializer<T: DeserializeOwned>: Send + Sync {
    fn deserialize(&self, payload: &str) -> Result<T, serde_json::Error>;
}

/// A flag for stating the standalone mode in the plugins.
///
/// The standalone build and start process calls `setup_frontend_plugins_pre_build` and `setup_datanode_plugins_pre_build`,
/// so we add a flag to the plugins to indicate that the plugins are running in the standalone mode.
#[derive(Clone, Copy, Debug)]
pub struct StandaloneFlag;

/// Buffer of plugin option tags that were dropped during config loading because
/// they are not recognized by the current build.
///
/// Config loading typically happens *before* the global tracing subscriber is
/// installed, so a `warn!` emitted at that point is silently lost. Instead we
/// buffer the dropped tags here and let the server flush them (via
/// [`take_dropped_plugin_warnings`]) once logging is initialized, so the warning
/// actually reaches the configured log output.
static DROPPED_PLUGIN_TAGS: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// Records a plugin option tag that was dropped because it is not recognized by
/// this build (for example, an enterprise plugin option seen by an open-source
/// build).
pub fn record_dropped_plugin(tag: &str) {
    if let Ok(mut tags) = DROPPED_PLUGIN_TAGS.lock() {
        tags.push(tag.to_string());
    }
}

/// Takes (and clears) the buffered dropped-plugin tags so they can be logged
/// after the global logging is up.
pub fn take_dropped_plugin_warnings() -> Vec<String> {
    DROPPED_PLUGIN_TAGS
        .lock()
        .map(|mut tags| std::mem::take(&mut *tags))
        .unwrap_or_default()
}

/// Deserializes a list of plugin options leniently.
///
/// Each entry is normally a single-key `{"tag": payload}` object, but a
/// *multi-key* object is also accepted and split: every top-level key is
/// treated as an independent plugin option. Keys naming a *known* variant are
/// deserialized and kept; keys naming a variant this build doesn't recognize are
/// dropped (and buffered via [`record_dropped_plugin`] for deferred logging).
/// This lets a single config file be shared across builds that compile different
/// sets of plugins — including a table that mixes a known plugin with unknown
/// ones — without aborting startup.
///
/// A *known* variant whose payload is genuinely malformed still propagates its
/// error, so a misconfigured plugin is never silently disabled; only genuinely
/// unknown options are ignored. For privacy only the unrecognized variant *tag*
/// is recorded, never the raw payload (which may contain secrets).
///
/// This is generic over `T` so the open-source `plugins` crate and the
/// enterprise `ent-plugins` crate share a single implementation without either
/// having to enumerate its variants (see [`is_unknown_variant_tag`]).
pub fn filter_known_plugin_options<T: DeserializeOwned>(
    values: Vec<Value>,
) -> Result<Vec<T>, serde_json::Error> {
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        collect_from_entry::<T>(&value, &mut out)?;
    }
    Ok(out)
}

/// Extracts the recognized plugin options from one config entry.
///
/// A normal entry is a single-key `{"tag": payload}` object. We additionally
/// accept a *multi-key* object and treat every top-level key as an independent
/// plugin option: each key naming a *known* variant is deserialized (and a
/// malformed payload for a known variant still errors, so a misconfigured plugin
/// is never silently disabled), while keys naming unknown variants are dropped
/// with a warning. A non-object entry (e.g. a bare number) is deserialized
/// directly and surfaces any error.
fn collect_from_entry<T: DeserializeOwned>(
    value: &Value,
    out: &mut Vec<T>,
) -> Result<(), serde_json::Error> {
    let Some(map) = value.as_object() else {
        // Not a tagged object (e.g. a bare number); a bare string can still
        // deserialize a unit variant. Surface any error.
        out.push(T::deserialize(value)?);
        return Ok(());
    };
    for (tag, payload) in map {
        if is_unknown_variant_tag::<T>(tag) {
            record_dropped_plugin(tag);
            continue;
        }
        // Known variant: deserialize just `{"tag": payload}` so a malformed
        // payload is reported against this variant rather than swallowed.
        let mut single = serde_json::Map::new();
        single.insert(tag.clone(), payload.clone());
        out.push(T::deserialize(&Value::Object(single))?);
    }
    Ok(())
}

/// Decides whether `tag` names a variant unknown to `T`, *without* `T` having
/// to enumerate its variants.
///
/// Two probe payloads (`{"tag": null}` and `{"tag": true}`) are re-deserialized
/// for the same tag:
/// - If `T` accepts either payload, the tag is recognized (a known variant).
/// - If both are rejected, the tag is unknown iff the two errors are identical.
///   An unknown variant yields a payload-independent "unknown variant" error for
///   both probes, whereas a known variant rejects `null` and `bool` with
///   *different* "invalid type" messages.
fn is_unknown_variant_tag<T: DeserializeOwned>(tag: &str) -> bool {
    let probe = |payload: Value| {
        let mut map = serde_json::Map::new();
        map.insert(tag.to_string(), payload);
        Value::Object(map)
    };
    match (
        T::deserialize(&probe(Value::Null)),
        T::deserialize(&probe(Value::Bool(true))),
    ) {
        (Ok(_), _) | (_, Ok(_)) => false,
        (Err(a), Err(b)) => a.to_string() == b.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, OnceLock};

    use serde::Deserialize;

    use super::*;

    // The dropped-tag buffer is process-global, so every test in this module
    // serializes through this lock to keep buffer assertions deterministic.
    fn lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    #[derive(Debug, PartialEq, Deserialize)]
    struct UnitPayload;

    /// A stand-in for a real `PluginOptions` enum covering the variant shapes
    /// that occur in practice (newtype-over-unit, struct, newtype-over-scalar).
    #[derive(Debug, PartialEq, Deserialize)]
    enum DummyPlugins {
        Unit(UnitPayload),
        Struct { x: u32 },
        Newtype(String),
    }

    #[test]
    fn detects_unknown_vs_known_tags() {
        let _g = lock();
        // `is_unknown_variant_tag` never touches the dropped-tag buffer.
        for tag in ["Bogus", "DoesNotExist", "NotARealVariant"] {
            assert!(
                is_unknown_variant_tag::<DummyPlugins>(tag),
                "{tag} should be unknown"
            );
        }
        for tag in ["Unit", "Struct", "Newtype"] {
            assert!(
                !is_unknown_variant_tag::<DummyPlugins>(tag),
                "{tag} should be known"
            );
        }
    }

    #[test]
    fn filter_drops_unknown_keeps_known() {
        let _g = lock();
        let _ = take_dropped_plugin_warnings();
        let values: Vec<Value> = serde_json::from_str(
            r#"[
                {"Unit": null},
                {"Bogus": {"a": 1}},
                {"AnotherUnknown": {}},
                {"Struct": {"x": 7}},
                {"Newtype": "hi"}
            ]"#,
        )
        .unwrap();
        let kept = filter_known_plugin_options::<DummyPlugins>(values).unwrap();
        assert_eq!(
            kept,
            vec![
                DummyPlugins::Unit(UnitPayload),
                DummyPlugins::Struct { x: 7 },
                DummyPlugins::Newtype("hi".to_string()),
            ]
        );
    }

    #[test]
    fn filter_errors_on_malformed_known_variant() {
        let _g = lock();
        // struct/newtype variants cannot come from these payloads, so a *known*
        // variant's malformed payload must error (not be silently dropped).
        for bad in [
            r#"[{"Struct": 5}]"#,
            r#"[{"Struct": null}]"#,
            r#"[{"Struct": "oops"}]"#,
            r#"[{"Newtype": 7}]"#,
        ] {
            let values: Vec<Value> = serde_json::from_str(bad).unwrap();
            assert!(
                filter_known_plugin_options::<DummyPlugins>(values).is_err(),
                "expected error for {bad}"
            );
        }
    }

    #[test]
    fn filter_errors_on_non_tagged_shape() {
        let _g = lock();
        let values: Vec<Value> = serde_json::from_str(r#"[123]"#).unwrap();
        assert!(filter_known_plugin_options::<DummyPlugins>(values).is_err());
    }

    #[test]
    fn filter_drops_multi_key_all_unknown_entry() {
        let _g = lock();
        let _ = take_dropped_plugin_warnings();
        // A single entry carrying several *unknown* keys must be dropped, not
        // error with serde's "expected map with a single key".
        let values: Vec<Value> =
            serde_json::from_str(r#"[{"unknown_one": 1, "unknown_two": 2}]"#).unwrap();
        let kept = filter_known_plugin_options::<DummyPlugins>(values).unwrap();
        assert!(kept.is_empty());
        let mut recorded = take_dropped_plugin_warnings();
        recorded.sort();
        assert_eq!(
            recorded,
            vec!["unknown_one".to_string(), "unknown_two".to_string()]
        );
    }

    #[test]
    fn filter_keeps_known_drops_unknown_in_mixed_entry() {
        let _g = lock();
        let _ = take_dropped_plugin_warnings();
        // A table mixing a *known* plugin with unknown ones keeps the known
        // plugin and drops (warns) the unknown ones — it must not error.
        let values: Vec<Value> =
            serde_json::from_str(r#"[{"Struct": {"x": 1}, "bogus": 2}]"#).unwrap();
        let kept = filter_known_plugin_options::<DummyPlugins>(values).unwrap();
        assert_eq!(kept, vec![DummyPlugins::Struct { x: 1 }]);
        assert_eq!(take_dropped_plugin_warnings(), vec!["bogus".to_string()]);
    }

    #[test]
    fn filter_errors_on_malformed_known_in_mixed_entry() {
        let _g = lock();
        let _ = take_dropped_plugin_warnings();
        // A *known* variant with a genuinely malformed payload still errors,
        // even when an unknown key sits next to it in the same table.
        let values: Vec<Value> = serde_json::from_str(r#"[{"Struct": 5, "bogus": 2}]"#).unwrap();
        assert!(filter_known_plugin_options::<DummyPlugins>(values).is_err());
        // The malformed known variant aborted before the unknown was recorded.
        assert!(take_dropped_plugin_warnings().is_empty());
    }

    #[test]
    fn dropped_tags_are_buffered_with_tag_only() {
        let _g = lock();
        let _ = take_dropped_plugin_warnings();
        // Privacy: only the tag is recorded, never the raw payload.
        let values: Vec<Value> =
            serde_json::from_str(r#"[{"SecretPlugin": {"token": "hunter2"}}]"#).unwrap();
        filter_known_plugin_options::<DummyPlugins>(values).unwrap();
        assert_eq!(
            take_dropped_plugin_warnings(),
            vec!["SecretPlugin".to_string()]
        );
    }
}
