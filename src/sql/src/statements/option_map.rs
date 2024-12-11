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

use std::collections::{BTreeMap, HashMap};
use std::ops::ControlFlow;

use common_base::secrets::{ExposeSecret, ExposeSecretMut, SecretString};
use serde::{Deserialize, Serialize};
use sqlparser::ast::{Visit, VisitMut, Visitor, VisitorMut};

const REDACTED_OPTIONS: [&str; 2] = ["access_key_id", "secret_access_key"];

/// Options hashmap.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct OptionMap {
    options: BTreeMap<String, String>,
    #[serde(skip_serializing)]
    secrets: BTreeMap<String, SecretString>,
}

impl OptionMap {
    pub fn insert(&mut self, k: String, v: String) {
        if REDACTED_OPTIONS.contains(&k.as_str()) {
            self.secrets.insert(k, SecretString::new(Box::new(v)));
        } else {
            self.options.insert(k, v);
        }
    }

    pub fn get(&self, k: &str) -> Option<&String> {
        if let Some(value) = self.options.get(k) {
            Some(value)
        } else if let Some(value) = self.secrets.get(k) {
            Some(value.expose_secret())
        } else {
            None
        }
    }

    pub fn is_empty(&self) -> bool {
        self.options.is_empty() && self.secrets.is_empty()
    }

    pub fn len(&self) -> usize {
        self.options.len() + self.secrets.len()
    }

    pub fn to_str_map(&self) -> HashMap<&str, &str> {
        let mut map = HashMap::with_capacity(self.len());
        map.extend(self.options.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        map.extend(
            self.secrets
                .iter()
                .map(|(k, v)| (k.as_str(), v.expose_secret().as_str())),
        );
        map
    }

    pub fn into_map(self) -> HashMap<String, String> {
        let mut map = HashMap::with_capacity(self.len());
        map.extend(self.options);
        map.extend(
            self.secrets
                .into_iter()
                .map(|(k, v)| (k, v.expose_secret().to_string())),
        );
        map
    }

    pub fn kv_pairs(&self) -> Vec<String> {
        let mut result = Vec::with_capacity(self.options.len() + self.secrets.len());
        for (k, v) in self.options.iter() {
            if k.contains(".") {
                result.push(format!("'{k}' = '{}'", v.escape_default()));
            } else {
                result.push(format!("{k} = '{}'", v.escape_default()));
            }
        }
        for (k, _) in self.secrets.iter() {
            if k.contains(".") {
                result.push(format!("'{k}' = '******'"));
            } else {
                result.push(format!("{k} = '******'"));
            }
        }
        result
    }
}

impl From<HashMap<String, String>> for OptionMap {
    fn from(value: HashMap<String, String>) -> Self {
        let mut result = OptionMap::default();
        for (k, v) in value.into_iter() {
            result.insert(k, v);
        }
        result
    }
}

impl PartialEq for OptionMap {
    fn eq(&self, other: &Self) -> bool {
        if self.options.ne(&other.options) {
            return false;
        }

        if self.secrets.len() != other.secrets.len() {
            return false;
        }

        self.secrets.iter().all(|(key, value)| {
            other
                .secrets
                .get(key)
                .map_or(false, |v| value.expose_secret() == v.expose_secret())
        })
    }
}

impl Eq for OptionMap {}

impl Visit for OptionMap {
    fn visit<V: Visitor>(&self, visitor: &mut V) -> ControlFlow<V::Break> {
        for (k, v) in &self.options {
            k.visit(visitor)?;
            v.visit(visitor)?;
        }
        for (k, v) in &self.secrets {
            k.visit(visitor)?;
            v.expose_secret().visit(visitor)?;
        }
        ControlFlow::Continue(())
    }
}

impl VisitMut for OptionMap {
    fn visit<V: VisitorMut>(&mut self, visitor: &mut V) -> ControlFlow<V::Break> {
        for (_, v) in self.options.iter_mut() {
            v.visit(visitor)?;
        }
        for (_, v) in self.secrets.iter_mut() {
            v.expose_secret_mut().visit(visitor)?;
        }
        ControlFlow::Continue(())
    }
}
