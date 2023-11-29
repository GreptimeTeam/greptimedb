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

pub mod log_store;
mod topic_client_manager;
use std::collections::BTreeMap;

use common_config::wal::kafka::KafkaTopic as Topic;
use rskafka::record::Record;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use store_api::logstore::entry::{Entry, Id as EntryId};
use store_api::logstore::namespace::Namespace;

use crate::error::{
    DeserEntryMetaSnafu, Error, MissingEntryMetaSnafu, MissingRecordValueSnafu, Result,
    SerEntryMetaSnafu,
};

const ENTRY_META_KEY: &str = "entry_meta";

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct NamespaceImpl {
    region_id: u64,
    topic: Topic,
}

impl NamespaceImpl {
    fn new(region_id: u64, topic: Topic) -> Self {
        Self { region_id, topic }
    }

    fn region_id(&self) -> u64 {
        self.region_id
    }

    fn topic(&self) -> &Topic {
        &self.topic
    }
}

impl Namespace for NamespaceImpl {
    fn id(&self) -> u64 {
        self.region_id
    }
}

pub struct EntryImpl {
    /// Entry payload.
    data: Vec<u8>,
    /// The logical entry id.
    id: EntryId,
    /// The namespace used to identify and isolate log entries from different regions.
    ns: NamespaceImpl,
}

impl EntryImpl {
    fn new(data: Vec<u8>, entry_id: EntryId, ns: NamespaceImpl) -> Self {
        Self {
            data,
            id: entry_id,
            ns,
        }
    }
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
}

/// The `EntryMeta` is used to group entry metadata during serialization and deserialization.
#[derive(Serialize, Deserialize)]
struct EntryMeta {
    entry_id: EntryId,
    region_id: u64,
    topic: String,
}

impl TryInto<Record> for EntryImpl {
    type Error = Error;

    fn try_into(self) -> Result<Record> {
        let entry_meta = EntryMeta {
            entry_id: self.id,
            region_id: self.ns.region_id(),
            topic: self.ns.topic,
        };
        let raw_entry_meta = serde_json::to_vec(&entry_meta).context(SerEntryMetaSnafu)?;

        Ok(Record {
            key: None,
            value: Some(self.data),
            headers: BTreeMap::from([(ENTRY_META_KEY.to_string(), raw_entry_meta)]),
            timestamp: rskafka::chrono::Utc::now(),
        })
    }
}

impl TryFrom<Record> for EntryImpl {
    type Error = Error;

    fn try_from(record: Record) -> Result<Self> {
        let value = record.value.as_ref().context(MissingRecordValueSnafu {
            record: record.clone(),
        })?;

        let raw_entry_meta = record
            .headers
            .get(ENTRY_META_KEY)
            .context(MissingEntryMetaSnafu {
                record: record.clone(),
            })?;
        let entry_meta = serde_json::from_slice(raw_entry_meta).context(DeserEntryMetaSnafu {
            record: record.clone(),
        })?;

        let EntryMeta {
            entry_id,
            region_id,
            topic,
        } = entry_meta;

        Ok(Self {
            data: value.clone(),
            id: entry_id,
            ns: NamespaceImpl::new(region_id, topic),
        })
    }
}
