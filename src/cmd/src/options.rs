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

use clap::Parser;
use common_config::Configurable;
use common_options::plugin_options::PluginOptionsDeserializer;
use common_runtime::global::RuntimeOptions;
use plugins::PluginOptions;
use plugins::options::PluginOptionsDeserializerImpl;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Parser, Default, Debug, Clone)]
pub struct GlobalOptions {
    #[clap(long, value_name = "LOG_DIR")]
    #[arg(global = true)]
    pub log_dir: Option<String>,

    #[clap(long, value_name = "LOG_LEVEL")]
    #[arg(global = true)]
    pub log_level: Option<String>,

    #[cfg(feature = "tokio-console")]
    #[clap(long, value_name = "TOKIO_CONSOLE_ADDR")]
    #[arg(global = true)]
    pub tokio_console_addr: Option<String>,
}

// TODO(LFC): Move logging and tracing options into global options, like the runtime options.
/// All the options of GreptimeDB.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct GreptimeOptions<T> {
    /// The runtime options.
    pub runtime: RuntimeOptions,
    /// The plugin options.
    ///
    /// Deserialized leniently via [`deserialize_plugin_options`]: plugin
    /// options not recognized by the current build are dropped (with a deferred
    /// warning) instead of aborting startup, while malformed payloads for known
    /// variants still error.
    #[serde(default, deserialize_with = "deserialize_plugin_options")]
    pub plugins: Vec<PluginOptions>,

    /// The options of each component (like Datanode or Standalone) of GreptimeDB.
    #[serde(flatten)]
    pub component: T,
}

impl<T> Configurable for GreptimeOptions<T>
where
    T: Configurable,
{
    fn env_list_keys() -> Option<&'static [&'static str]> {
        T::env_list_keys()
    }
}

/// A serde `deserialize_with` helper that parses the `plugins` field leniently.
///
/// It delegates to the plugin crate's deserializer (`PluginOptionsDeserializerImpl`),
/// which knows the recognized variants: unknown options are dropped while
/// malformed known options still error. Keeping the variant-aware logic in the
/// `plugins` crate avoids duplicating it and avoids introducing a new symbol on
/// the replaceable `plugins` crate.
fn deserialize_plugin_options<'de, D>(deserializer: D) -> Result<Vec<PluginOptions>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let values = Vec::<Value>::deserialize(deserializer)?;
    if values.is_empty() {
        return Ok(vec![]);
    }
    let payload = serde_json::to_string(&values).map_err(serde::de::Error::custom)?;
    PluginOptionsDeserializerImpl
        .deserialize(&payload)
        .map_err(serde::de::Error::custom)
}

/// Emits the plugin options that were dropped during config loading (because
/// they were not recognized by the current build) as warnings.
///
/// Config loading runs before the global tracing subscriber is installed, so
/// the warnings are buffered and must be flushed once logging is initialized.
/// Each server's `build` method calls this right after initializing logging.
pub(crate) fn flush_dropped_plugin_warnings() {
    let tags = common_options::plugin_options::take_dropped_plugin_warnings();
    for tag in tags {
        common_telemetry::warn!(
            "Ignoring unrecognized plugin option `{tag}` in the config; \
             it likely belongs to a plugin that is not compiled into this build."
        );
    }
}
