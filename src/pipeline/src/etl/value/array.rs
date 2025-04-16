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

use crate::error::{Error, Result};
use crate::etl::value::Value;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Array {
    pub values: Vec<Value>,
}

impl Array {
    pub fn new() -> Self {
        Array { values: vec![] }
    }
}

impl std::fmt::Display for Array {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let values = self
            .values
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<String>>()
            .join(", ");
        write!(f, "[{}]", values)
    }
}

impl std::ops::Deref for Array {
    type Target = Vec<Value>;

    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

impl std::ops::DerefMut for Array {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.values
    }
}

impl IntoIterator for Array {
    type Item = Value;

    type IntoIter = std::vec::IntoIter<Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

impl From<Vec<Value>> for Array {
    fn from(values: Vec<Value>) -> Self {
        Array { values }
    }
}

impl TryFrom<Vec<serde_json::Value>> for Array {
    type Error = Error;

    fn try_from(value: Vec<serde_json::Value>) -> Result<Self> {
        let values = value
            .into_iter()
            .map(|v| v.try_into())
            .collect::<Result<Vec<_>>>()?;
        Ok(Array { values })
    }
}
