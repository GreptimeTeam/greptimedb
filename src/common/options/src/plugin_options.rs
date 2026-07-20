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
