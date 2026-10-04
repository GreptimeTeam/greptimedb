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

use common_options::plugin_options::{PluginOptionsDeserializer, PluginOptionsSerializer};
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
            common_options::plugin_options::filter_known_plugin_options(values)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, OnceLock};

    use super::*;

    // The dropped-tag buffer is process-global, so tests that touch it run serialized.
    fn lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    fn filter(values: Vec<Value>) -> Result<Vec<PluginOptions>, serde_json::Error> {
        common_options::plugin_options::filter_known_plugin_options(values)
    }

    #[test]
    fn test_drops_unknown_keeps_known() {
        let _g = lock();
        // `null` is the serialized form of the unit-struct `DummyOptions`.
        let payload = r#"[
            {"Dummy": null},
            {"SomeEnterpriseThing": {"foo": "bar", "num": 5}},
            {"AnotherUnknown": {}}
        ]"#;
        let values: Vec<Value> = serde_json::from_str(payload).unwrap();
        let kept = filter(values).unwrap();
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
        let _g = lock();
        // Privacy (#5): only the tag is recorded, never the raw payload.
        let values: Vec<Value> =
            serde_json::from_str(r#"[{"SecretPlugin": {"token": "hunter2"}}]"#).unwrap();
        filter(values).unwrap();
        let recorded = common_options::plugin_options::take_dropped_plugin_warnings();
        assert_eq!(recorded, vec!["SecretPlugin".to_string()]);
    }

    #[test]
    fn test_known_variant_with_malformed_payload_errors() {
        let _g = lock();
        // Regression (#3): a known variant with a bad payload must NOT be
        // silently dropped.
        for bad in [r#"[{"Dummy": 1}]"#, r#"[{"Dummy": {"x": 1}}]"#] {
            let values: Vec<Value> = serde_json::from_str(bad).unwrap();
            let err = filter(values).unwrap_err();
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
        let _g = lock();
        // Not a recognizable plugin entry: surface the error.
        let values: Vec<Value> = serde_json::from_str(r#"[123]"#).unwrap();
        assert!(filter(values).is_err());
    }

    #[test]
    fn test_deserializer_impl_is_lenient_for_unknown() {
        let _g = lock();
        let payload = r#"[{"Dummy": null}, {"UnknownPlugin": {"x": 1}}]"#;
        let kept = PluginOptionsDeserializerImpl.deserialize(payload).unwrap();
        assert_eq!(kept, vec![PluginOptions::Dummy(DummyOptions)]);
        common_options::plugin_options::take_dropped_plugin_warnings();
    }

    #[test]
    fn test_deserializer_impl_errors_on_bad_known_payload() {
        let _g = lock();
        let payload = r#"[{"Dummy": 1}]"#;
        assert!(PluginOptionsDeserializerImpl.deserialize(payload).is_err());
    }
}
