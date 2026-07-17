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
            Ok(filter_known_plugin_options(values))
        }
    }
}

/// Keeps only the plugin options that the current build recognizes, dropping the
/// unrecognized ones with a warning.
///
/// An unrecognized option usually belongs to a plugin that is not compiled into
/// the current build (for example, an enterprise plugin option seen by an
/// open-source build). Rather than aborting startup, the unknown option is
/// ignored with a warning so a shared config file can be used across builds.
fn filter_known_plugin_options(values: Vec<Value>) -> Vec<PluginOptions> {
    values
        .into_iter()
        .filter_map(
            |value| match serde_json::from_value::<PluginOptions>(value.clone()) {
                Ok(options) => Some(options),
                Err(err) => {
                    warn!(
                        "Ignoring unrecognized plugin option in the config: {err}. \
                     This usually means the option belongs to a plugin that is not \
                     compiled into this build. Raw value: {value}"
                    );
                    None
                }
            },
        )
        .collect()
}

/// A serde `deserialize_with` helper that parses the `plugins` field leniently:
/// unrecognized plugin options are dropped with a warning instead of failing the
/// (de)serialization. Works for both the TOML config file path (via the `config`
/// crate) and plain JSON sources.
pub fn deserialize_plugin_options<'de, D>(deserializer: D) -> Result<Vec<PluginOptions>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let values = Vec::<Value>::deserialize(deserializer)?;
    Ok(filter_known_plugin_options(values))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_known_drops_unknown_with_known_kept() {
        // `null` is the serialized form of the unit-struct `DummyOptions`.
        let payload = r#"[
            {"Dummy": null},
            {"SomeEnterpriseThing": {"foo": "bar", "num": 5}},
            {"AnotherUnknown": {}}
        ]"#;
        let values: Vec<Value> = serde_json::from_str(payload).unwrap();
        let kept = filter_known_plugin_options(values);
        assert_eq!(kept, vec![PluginOptions::Dummy(DummyOptions)]);
    }

    #[test]
    fn test_deserializer_impl_is_lenient() {
        let payload = r#"[{"Dummy": null}, {"UnknownPlugin": {"x": 1}}]"#;
        let kept = PluginOptionsDeserializerImpl.deserialize(payload).unwrap();
        assert_eq!(kept, vec![PluginOptions::Dummy(DummyOptions)]);
    }

    #[test]
    fn test_deserialize_plugin_options_via_json() {
        // Simulate what serde_json would feed into `deserialize_plugin_options`.
        let json = r#"[{"Dummy": null}, {"Bogus": {}}]"#;
        let mut deserializer = serde_json::Deserializer::from_str(json);
        let kept = deserialize_plugin_options(&mut deserializer).unwrap();
        assert_eq!(kept, vec![PluginOptions::Dummy(DummyOptions)]);
    }
}
