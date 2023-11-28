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

use std::collections::HashMap;
use std::fmt::Display;

use serde::{Deserialize, Serialize};
use store_api::storage::{RegionNumber, TableId};

/// The name of a wal meta key.
#[derive(Debug)]
pub enum KeyName {
    /// Kafka topic.
    KafkaTopic,
}

impl Display for KeyName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name_str = match self {
            KeyName::KafkaTopic => "kafka_topic",
        };
        f.write_str(name_str)
    }
}

/// The key to identify a wal meta.
pub struct RegionWalMetaKey {
    table_id: TableId,
    region_number: RegionNumber,
    name: KeyName,
}

impl RegionWalMetaKey {
    /// Construct a wal meta key with the given table id, region number, and key name.
    pub fn new(table_id: TableId, region_number: RegionNumber, name: KeyName) -> Self {
        Self {
            table_id,
            region_number,
            name,
        }
    }
}

impl ToString for RegionWalMetaKey {
    fn to_string(&self) -> String {
        format!("{}/{}/{}", self.table_id, self.region_number, self.name)
    }
}

/// A region's unique wal metadata.
// FIXME(niebayes): Shall String or RegionWalMetaKey be used as the key type?
#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct RegionWalMeta(HashMap<String, String>);

impl RegionWalMeta {
    pub fn with_metas<T>(metas: T) -> Self
    where
        T: IntoIterator<Item = (String, String)>,
    {
        Self(HashMap::from_iter(metas))
    }

    /// Gets the value associated with the given wal meta key.
    pub fn get(&self, key: &RegionWalMetaKey) -> Option<&String> {
        self.0.get(&key.to_string())
    }

    /// Inserts a key-value pair with the key represented as a wal meta key while the value a string.
    /// This insertion always succeeds since the given key-value pair will override the existing one.
    pub fn insert(&mut self, key: RegionWalMetaKey, value: String) {
        self.0.insert(key.to_string(), value);
    }
}
