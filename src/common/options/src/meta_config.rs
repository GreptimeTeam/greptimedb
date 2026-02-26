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

use std::sync::Arc;

/// A trait for serializing Metasrv config to a JSON string.
/// So it can be used in the metasrv's crate instead of depending on the plugins' crate.
pub trait PluginOptionsSerializer: Send + Sync {
    fn serialize(&self) -> Result<Vec<String>, serde_json::Error>;
}

pub type PluginOptionsSerializerRef = Arc<dyn PluginOptionsSerializer>;
