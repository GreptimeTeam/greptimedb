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

use common_options::plugin_options::{
    PluginOptionsDeserializer, PluginOptionsSerializer, record_dropped_plugin,
};
use common_telemetry::warn;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DummyOptions;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PluginOptions {
    Dummy(DummyOptions),
}

pub struct PluginOptionsList(pub Vec<PluginOptions>);

impl PluginOptionsSerializer for PluginOptionsList {
    fn serialize(&self) -> Result<String, serde_json::Error> {
        if self.0.is_empty() {
            Ok(String::new())
        } else {
            serde_json::to_string(&self.0)
        }
    }
}

pub struct PluginOptionsDeserializerImpl;

impl PluginOptionsDeserializer<Vec<PluginOptions>> for PluginOptionsDeserializerImpl {
    fn deserialize(&self, payload: &str) -> Result<Vec<PluginOptions>, serde_json::Error> {
        if payload.is_empty() {
            Ok(vec![])
        } else {
            let values: Vec<Value> = serde_json::from_str(payload)?;
            filter_known_plugin_options(values)
        }
    }
}

/// Keeps the plugin options that the current build recognizes, dropping the
/// unrecognized ones with a warning.
///
/// An unrecognized option usually belongs to a plugin that is not compiled into
/// the current build (for example, an enterprise plugin option seen by an
/// open-source build). Rather than aborting startup, the unknown option is
/// ignored so a shared config file can be used across builds.
///
/// Only genuinely unknown options are dropped: a *known* variant whose payload
/// is malformed is still reported as an error, so that a misconfigured plugin is
/// never silently disabled. The dropped tag is buffered (via
/// [`record_dropped_plugin`]) in addition to being warned here, because config
/// loading typically happens before the tracing subscriber is installed and the
/// immediate warning would be silently lost; the server flushes the buffered
/// tags once logging is initialized.
///
/// For privacy, only the unrecognized variant tag is logged/recorded, never the
/// raw option payload (which may contain secrets such as tokens or DSNs).
fn filter_known_plugin_options(
    values: Vec<Value>,
) -> Result<Vec<PluginOptions>, serde_json::Error> {
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        match PluginOptions::deserialize(&value) {
            Ok(options) => out.push(options),
            Err(err) => match extract_plugin_tag(&value) {
                // A known variant with a malformed payload: must not be dropped
                // silently, otherwise the intended plugin gets disabled without
                // notice.
                Some(tag) if is_known_plugin_tag(tag) => return Err(err),
                // Genuinely unknown variant for this build: drop it.
                Some(tag) => {
                    warn!(
                        "Ignoring unrecognized plugin option `{tag}` in the config; \
                         it likely belongs to a plugin that is not compiled into this build."
                    );
                    record_dropped_plugin(tag);
                }
                // Not a recognizable plugin entry at all: surface the error.
                None => return Err(err),
            },
        }
    }
    Ok(out)
}

/// Extracts the variant tag from an externally-tagged enum value, i.e. the
/// single key of `{"Tag": payload}` (or the string `"Tag"` for the unit form).
fn extract_plugin_tag(value: &Value) -> Option<&str> {
    if let Some(map) = value.as_object()
        && map.len() == 1
    {
        return map.keys().next().map(String::as_str);
    }
    value.as_str()
}

/// Returns whether `tag` names a [`PluginOptions`] variant known to this build.
///
/// Keep this in sync with [`PluginOptions`]. It is used to tell apart a
/// genuinely unknown plugin option (dropped) from a malformed payload for a
/// known variant (reported as an error).
fn is_known_plugin_tag(tag: &str) -> bool {
    matches!(tag, "Dummy")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_drops_unknown_keeps_known() {
        // `null` is the serialized form of the unit-struct `DummyOptions`.
        let payload = r#"[
            {"Dummy": null},
            {"SomeEnterpriseThing": {"foo": "bar", "num": 5}},
            {"AnotherUnknown": {}}
        ]"#;
        let values: Vec<Value> = serde_json::from_str(payload).unwrap();
        let kept = filter_known_plugin_options(values).unwrap();
        assert_eq!(kept, vec![PluginOptions::Dummy(DummyOptions)]);
        // Unknown tags were buffered for deferred logging.
        assert_eq!(
            common_options::plugin_options::take_dropped_plugin_warnings(),
            vec![
                "SomeEnterpriseThing".to_string(),
                "AnotherUnknown".to_string()
            ]
        );
    }

    #[test]
    fn test_unknown_variant_tag_is_not_in_payload_warning() {
        // Privacy (#5): only the tag is recorded, never the raw payload.
        let values: Vec<Value> =
            serde_json::from_str(r#"[{"SecretPlugin": {"token": "hunter2"}}]"#).unwrap();
        filter_known_plugin_options(values).unwrap();
        let recorded = common_options::plugin_options::take_dropped_plugin_warnings();
        assert_eq!(recorded, vec!["SecretPlugin".to_string()]);
    }

    #[test]
    fn test_known_variant_with_malformed_payload_errors() {
        // Regression (#3): a known variant with a bad payload must NOT be
        // silently dropped.
        for bad in [r#"[{"Dummy": 1}]"#, r#"[{"Dummy": {"x": 1}}]"#] {
            let values: Vec<Value> = serde_json::from_str(bad).unwrap();
            let err = filter_known_plugin_options(values).unwrap_err();
            assert!(
                err.to_string().contains("Dummy"),
                "expected the error to reference the Dummy variant, got: {err}"
            );
        }
        // Nothing should have been buffered.
        assert!(common_options::plugin_options::take_dropped_plugin_warnings().is_empty());
    }

    #[test]
    fn test_unrecognized_shape_errors() {
        // Not a recognizable plugin entry: surface the error.
        let values: Vec<Value> = serde_json::from_str(r#"[123]"#).unwrap();
        assert!(filter_known_plugin_options(values).is_err());
    }

    #[test]
    fn test_deserializer_impl_is_lenient_for_unknown() {
        let payload = r#"[{"Dummy": null}, {"UnknownPlugin": {"x": 1}}]"#;
        let kept = PluginOptionsDeserializerImpl.deserialize(payload).unwrap();
        assert_eq!(kept, vec![PluginOptions::Dummy(DummyOptions)]);
        common_options::plugin_options::take_dropped_plugin_warnings();
    }

    #[test]
    fn test_deserializer_impl_errors_on_bad_known_payload() {
        let payload = r#"[{"Dummy": 1}]"#;
        assert!(PluginOptionsDeserializerImpl.deserialize(payload).is_err());
    }
}
