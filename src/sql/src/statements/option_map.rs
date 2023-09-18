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

mod visit;
mod visit_mut;

use std::borrow::Borrow;
use std::collections::HashMap;
use std::iter::FromIterator;

/// Options hashmap.
/// Because the trait `Visit` and `VisitMut` is not implemented for `HashMap<String, String>`, we have to wrap it and implement them by ourself.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct OptionMap {
    pub map: HashMap<String, String>,
}

impl OptionMap {
    pub fn insert(&mut self, k: String, v: String) {
        self.map.insert(k, v);
    }

    pub fn get(&self, k: &str) -> Option<&String> {
        self.map.get(k)
    }
}

impl From<HashMap<String, String>> for OptionMap {
    fn from(map: HashMap<String, String>) -> Self {
        Self { map }
    }
}

impl AsRef<HashMap<String, String>> for OptionMap {
    fn as_ref(&self) -> &HashMap<String, String> {
        &self.map
    }
}

impl Borrow<HashMap<String, String>> for OptionMap {
    fn borrow(&self) -> &HashMap<String, String> {
        &self.map
    }
}

impl FromIterator<(String, String)> for OptionMap {
    fn from_iter<I: IntoIterator<Item = (String, String)>>(iter: I) -> Self {
        Self {
            map: iter.into_iter().collect(),
        }
    }
}
