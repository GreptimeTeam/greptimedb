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

/// A trait for serializing Metasrv config to a JSON string.
/// So it can be used in the metasrv's crate instead of depending on the plugins' crate.
pub trait MetasrvConfigSerializer {
    fn to_wrapper_str(&self) -> Result<MetasrvConfigWrapper, serde_json::Error>;
}

/// A wrapper for the serialized Metasrv config.
/// Avoid type collision for String in the plugins.
#[derive(Debug, Clone, PartialEq)]
pub struct MetasrvConfigWrapper(pub String);
