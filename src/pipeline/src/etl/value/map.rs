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

use crate::etl::value::Value;
use crate::PipelineMap;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Map {
    pub values: PipelineMap,
}

impl Map {
    pub fn one(key: impl Into<String>, value: Value) -> Map {
        let mut map = Map::default();
        map.insert(key, value);
        map
    }

    pub fn insert(&mut self, key: impl Into<String>, value: Value) {
        self.values.insert(key.into(), value);
    }

    pub fn extend(&mut self, Map { values }: Map) {
        self.values.extend(values);
    }
}

impl From<PipelineMap> for Map {
    fn from(values: PipelineMap) -> Self {
        Self { values }
    }
}

impl std::ops::Deref for Map {
    type Target = PipelineMap;

    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

impl std::ops::DerefMut for Map {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.values
    }
}

impl std::fmt::Display for Map {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let values = self
            .values
            .iter()
            .map(|(k, v)| format!("{}: {}", k, v))
            .collect::<Vec<String>>()
            .join(", ");
        write!(f, "{{{}}}", values)
    }
}
