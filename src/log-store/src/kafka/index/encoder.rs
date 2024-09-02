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

use std::collections::{BTreeSet, HashMap};
use std::sync::Mutex;

use delta_encoding::{DeltaDecoderExt, DeltaEncoderExt};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::logstore::provider::KafkaProvider;
use store_api::storage::RegionId;

use crate::error::{self, Result};
use crate::kafka::index::collector::RegionIndexes;

/// Converts a [`RegionIndexes`] instance into a [`DeltaEncodedRegionIndexes`].
///
/// This conversion encodes the index values using delta encoding to reduce storage space.
impl From<&RegionIndexes> for DeltaEncodedRegionIndexes {
    fn from(value: &RegionIndexes) -> Self {
        let mut regions = HashMap::with_capacity(value.regions.len());
        for (region_id, indexes) in value.regions.iter() {
            let indexes = indexes.iter().copied().deltas().collect();
            regions.insert(*region_id, indexes);
        }
        Self {
            regions,
            last_index: value.latest_entry_id,
        }
    }
}

/// Represents the delta-encoded version of region indexes for efficient storage.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct DeltaEncodedRegionIndexes {
    regions: HashMap<RegionId, Vec<u64>>,
    last_index: u64,
}

impl DeltaEncodedRegionIndexes {
    /// Retrieves the original (decoded) index values for a given region.
    pub(crate) fn region(&self, region_id: RegionId) -> Option<BTreeSet<u64>> {
        let decoded = self
            .regions
            .get(&region_id)
            .map(|delta| delta.iter().copied().original().collect::<BTreeSet<_>>());

        decoded
    }

    /// Retrieves the last index.
    pub(crate) fn last_index(&self) -> u64 {
        self.last_index
    }
}

pub trait IndexEncoder: Send + Sync {
    fn encode(&self, provider: &KafkaProvider, region_index: &RegionIndexes);

    fn finish(&self) -> Result<Vec<u8>>;
}

/// [`DatanodeWalIndexes`] structure holds the WAL indexes for a datanode.
#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct DatanodeWalIndexes(HashMap<String, DeltaEncodedRegionIndexes>);

impl DatanodeWalIndexes {
    fn insert(&mut self, topic: String, region_index: &RegionIndexes) {
        self.0.insert(topic, region_index.into());
    }

    fn encode(&mut self) -> Result<Vec<u8>> {
        let value = serde_json::to_vec(&self.0).context(error::EncodeJsonSnafu);
        self.0.clear();
        value
    }

    pub(crate) fn decode(byte: &[u8]) -> Result<Self> {
        serde_json::from_slice(byte).context(error::DecodeJsonSnafu)
    }

    /// Retrieves the delta encoded region indexes for a given `provider`.
    pub(crate) fn provider(&self, provider: &KafkaProvider) -> Option<&DeltaEncodedRegionIndexes> {
        self.0.get(&provider.topic)
    }
}

/// [`JsonIndexEncoder`] encodes the [`RegionIndexes`]s into JSON format.
#[derive(Debug, Default)]
pub(crate) struct JsonIndexEncoder {
    buf: Mutex<DatanodeWalIndexes>,
}

impl IndexEncoder for JsonIndexEncoder {
    fn encode(&self, provider: &KafkaProvider, region_index: &RegionIndexes) {
        self.buf
            .lock()
            .unwrap()
            .insert(provider.topic.to_string(), region_index);
    }

    fn finish(&self) -> Result<Vec<u8>> {
        let mut buf = self.buf.lock().unwrap();
        buf.encode()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};

    use store_api::logstore::provider::KafkaProvider;
    use store_api::storage::RegionId;

    use super::{DatanodeWalIndexes, IndexEncoder, JsonIndexEncoder};
    use crate::kafka::index::collector::RegionIndexes;

    #[test]
    fn test_json_index_encoder() {
        let encoder = JsonIndexEncoder::default();
        let topic_1 = KafkaProvider::new("my_topic_1".to_string());
        let region_1_indexes = BTreeSet::from([1u64, 2, 4, 5, 20]);
        let region_2_indexes = BTreeSet::from([4u64, 12, 43, 54, 75]);
        encoder.encode(
            &topic_1,
            &RegionIndexes {
                regions: HashMap::from([
                    (RegionId::new(1, 1), region_1_indexes.clone()),
                    (RegionId::new(1, 2), region_2_indexes.clone()),
                ]),
                latest_entry_id: 1024,
            },
        );
        let topic_2 = KafkaProvider::new("my_topic_2".to_string());
        encoder.encode(
            &topic_2,
            &RegionIndexes {
                regions: HashMap::from([
                    (
                        RegionId::new(1, 1),
                        BTreeSet::from([1024u64, 1025, 1026, 1028, 2048]),
                    ),
                    (RegionId::new(1, 2), BTreeSet::from([1512])),
                ]),
                latest_entry_id: 2048,
            },
        );

        let bytes = encoder.finish().unwrap();
        let datanode_index = DatanodeWalIndexes::decode(&bytes).unwrap();
        assert_eq!(
            datanode_index
                .provider(&topic_1)
                .unwrap()
                .region(RegionId::new(1, 1))
                .unwrap(),
            region_1_indexes,
        );
        assert_eq!(
            datanode_index
                .provider(&topic_1)
                .unwrap()
                .region(RegionId::new(1, 2))
                .unwrap(),
            region_2_indexes,
        );
        assert!(datanode_index
            .provider(&KafkaProvider::new("my_topic_3".to_string()))
            .is_none());
    }
}
