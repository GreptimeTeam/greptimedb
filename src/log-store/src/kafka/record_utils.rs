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

use std::collections::BTreeMap;

use common_config::wal::KafkaWalTopic as Topic;
use rskafka::record::Record;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};

use crate::error::{DecodeKeySnafu, EncodeKeySnafu, MissingKeySnafu, MissingValueSnafu, Result};
use crate::kafka::{EntryId, EntryImpl, NamespaceImpl};

/// The key of a record.
/// An rskafka record consists of key, value, headers, and datetime. The value of a record
/// is the entry data. Either of the key or the headers can be chosen to store the entry metadata
/// including topic, region id, and entry id. Currently, the entry metadata is stored in the key.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct RecordKey {
    topic: Topic,
    region_id: u64,
    entry_id: EntryId,
}

// When writing to a region, a wal entry is constructed from all mutations on the region.
// I.e., a wal entry is itself a log batch and hence no need to group multiple entries into a record.
// That's why the mapping between entries and records are one-one.
impl TryInto<Record> for EntryImpl {
    type Error = crate::error::Error;

    fn try_into(self) -> Result<Record> {
        let key = RecordKey {
            topic: self.ns.topic,
            region_id: self.ns.region_id,
            entry_id: self.id,
        };
        let raw_key = serde_json::to_vec(&key).context(EncodeKeySnafu { key })?;

        Ok(Record {
            key: Some(raw_key),
            value: Some(self.data),
            headers: BTreeMap::default(),
            timestamp: rskafka::chrono::Utc::now(),
        })
    }
}

impl TryFrom<Record> for EntryImpl {
    type Error = crate::error::Error;

    fn try_from(record: Record) -> Result<Self> {
        let raw_key = record.key.context(MissingKeySnafu)?;
        let key: RecordKey = serde_json::from_slice(&raw_key).context(DecodeKeySnafu)?;
        let data = record.value.context(MissingValueSnafu)?;

        Ok(Self {
            id: key.entry_id,
            ns: NamespaceImpl::new(key.region_id, key.topic),
            data,
        })
    }
}
