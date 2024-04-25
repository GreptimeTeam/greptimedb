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

pub mod client_manager;
pub mod consumer;
pub mod log_store;
pub mod producer;

use std::fmt::Display;
use std::mem::size_of;

use serde::{Deserialize, Serialize};
use store_api::logstore::entry::{Entry, Id as EntryId};
use store_api::logstore::namespace::Namespace;

use crate::error::Error;

/// Kafka Namespace implementation.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct NamespaceImpl {
    pub region_id: u64,
    pub topic: String,
}

impl Namespace for NamespaceImpl {
    fn id(&self) -> u64 {
        self.region_id
    }
}

impl Display for NamespaceImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[topic: {}, region: {}]", self.topic, self.region_id)
    }
}

/// Kafka Entry implementation.
#[derive(Debug, PartialEq, Clone)]
pub struct EntryImpl {
    /// Entry payload.
    pub data: Vec<u8>,
    /// The logical entry id.
    pub id: EntryId,
    /// The namespace used to identify and isolate log entries from different regions.
    pub ns: NamespaceImpl,
}

impl Entry for EntryImpl {
    type Error = Error;
    type Namespace = NamespaceImpl;

    fn data(&self) -> &[u8] {
        &self.data
    }

    fn id(&self) -> EntryId {
        self.id
    }

    fn namespace(&self) -> Self::Namespace {
        self.ns.clone()
    }

    fn estimated_size(&self) -> usize {
        size_of::<Self>() + self.data.capacity() * size_of::<u8>() + self.ns.topic.capacity()
    }
}

impl Display for EntryImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Entry [ns: {}, id: {}, data_len: {}]",
            self.ns,
            self.id,
            self.data.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use store_api::logstore::entry::Entry;

    use crate::kafka::{EntryImpl, NamespaceImpl};

    #[test]
    fn test_estimated_size() {
        let entry = EntryImpl {
            data: Vec::with_capacity(100),
            id: 0,
            ns: NamespaceImpl {
                region_id: 0,
                topic: String::with_capacity(10),
            },
        };
        let expected = size_of::<EntryImpl>() + 100 * size_of::<u8>() + 10;
        let got = entry.estimated_size();
        assert_eq!(expected, got);
    }
}
