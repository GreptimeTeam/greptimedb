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
use either::Either;
use serde::Serialize;
use sqlparser::ast::{Visit, VisitMut, Visitor, VisitorMut};

use crate::util::OptionValue;

const REDACTED_OPTIONS: [&str; 2] = ["access_key_id", "secret_access_key"];

/// Options hashmap.
#[derive(Clone, Debug, Default, Serialize)]
pub struct OptionMap {
    options: BTreeMap<String, OptionValue>,
    #[serde(skip_serializing)]
    secrets: BTreeMap<String, SecretString>,
}

impl OptionMap {
    pub fn new<I: IntoIterator<Item = (String, OptionValue)>>(options: I) -> Self {
        let (secrets, options): (Vec<_>, Vec<_>) = options
            .into_iter()
            .partition(|(k, _)| REDACTED_OPTIONS.contains(&k.as_str()));
        Self {
            options: options.into_iter().collect(),
            secrets: secrets
                .into_iter()
                .filter_map(|(k, v)| {
                    v.as_string()
                        .map(|v| (k, SecretString::new(Box::new(v.to_string()))))
                })
                .collect(),
        }
    }

    pub fn insert(&mut self, k: String, v: String) {
        if REDACTED_OPTIONS.contains(&k.as_str()) {
            self.secrets.insert(k, SecretString::new(Box::new(v)));
        } else {
            self.options.insert(k, v.into());
        }
    }

    pub fn insert_options(&mut self, key: &str, value: OptionValue) {
        if REDACTED_OPTIONS.contains(&key) {
            self.secrets.insert(
                key.to_string(),
                SecretString::new(Box::new(value.to_string())),
            );
        } else {
            self.options.insert(key.to_string(), value);
        }
    }

    pub fn get(&self, k: &str) -> Option<&str> {
        if let Some(value) = self.options.get(k) {
            value.as_string()
        } else if let Some(value) = self.secrets.get(k) {
            Some(value.expose_secret())
        } else {
            None
        }
    }

    pub fn value(&self, k: &str) -> Option<&OptionValue> {
        self.options.get(k)
    }

    pub fn is_empty(&self) -> bool {
        self.options.is_empty() && self.secrets.is_empty()
    }

    pub fn len(&self) -> usize {
        self.options.len() + self.secrets.len()
    }

    pub fn to_str_map(&self) -> HashMap<&str, &str> {
        let mut map = HashMap::with_capacity(self.len());
        map.extend(
            self.options
                .iter()
                .filter_map(|(k, v)| v.as_string().map(|v| (k.as_str(), v))),
        );
        map.extend(
            self.secrets
                .iter()
                .map(|(k, v)| (k.as_str(), v.expose_secret().as_str())),
        );
        map
    }

    pub fn into_map(self) -> HashMap<String, String> {
        let mut map = HashMap::with_capacity(self.len());
        map.extend(
            self.options
                .into_iter()
                .filter_map(|(k, v)| v.as_string().map(|v| (k, v.to_string()))),
        );
        map.extend(
            self.secrets
                .into_iter()
                .map(|(k, v)| (k, v.expose_secret().clone())),
        );
        map
    }

    pub fn kv_pairs(&self) -> Vec<String> {
        let mut result = Vec::with_capacity(self.options.len() + self.secrets.len());
        for (k, v) in self
            .options
            .iter()
            .filter_map(|(k, v)| v.as_string().map(|v| (k, v)))
        {
            if k.contains(".") {
                result.push(format!("'{k}' = '{}'", v.escape_debug()));
            } else {
                result.push(format!("{k} = '{}'", v.escape_debug()));
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

    pub fn entries(&self) -> impl Iterator<Item = (&str, Either<&OptionValue, &str>)> {
        let options = self
            .options
            .iter()
            .map(|(k, v)| (k.as_str(), Either::Left(v)));
        let secrets = self
            .secrets
            .keys()
            .map(|k| (k.as_str(), Either::Right("******")));
        std::iter::chain(options, secrets)
    }
}

impl<I: IntoIterator<Item = (String, String)>> From<I> for OptionMap {
    fn from(value: I) -> Self {
        let mut result = OptionMap::default();
        for (k, v) in value {
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
                .is_some_and(|v| value.expose_secret() == v.expose_secret())
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

#[cfg(test)]
mod tests {
    use crate::statements::OptionMap;

    #[test]
    fn test_format() {
        let mut map = OptionMap::default();
        map.insert("comment".to_string(), "中文comment".to_string());
        assert_eq!("comment = '中文comment'", map.kv_pairs()[0]);

        let mut map = OptionMap::default();
        map.insert("a.b".to_string(), "中文comment".to_string());
        assert_eq!("'a.b' = '中文comment'", map.kv_pairs()[0]);

        let mut map = OptionMap::default();
        map.insert("a.b".to_string(), "中文comment\n".to_string());
        assert_eq!("'a.b' = '中文comment\\n'", map.kv_pairs()[0]);
    }
}
