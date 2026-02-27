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

use common_options::plugin_options::PluginOptionsSerializer;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DummyOptions;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PluginOptions {
    Dummy(DummyOptions),
}

#[allow(unused)]
pub struct PluginOptionsList(pub Vec<PluginOptions>);

impl PluginOptionsSerializer for PluginOptionsList {
    fn serialize(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&self.0)
    }
}
