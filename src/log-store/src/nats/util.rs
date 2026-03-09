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

//! Shared helpers for the NATS JetStream WAL.

use store_api::logstore::entry::{Entry, MultiplePartEntry, MultiplePartHeader, NaiveEntry};
use store_api::logstore::provider::Provider;
use store_api::storage::RegionId;

use crate::nats::record::ESTIMATED_META_SIZE;

/// Builds an [`Entry`] from raw bytes, splitting into `MultiplePart` if the
/// data exceeds `max_data_size`.
///
/// This mirrors `kafka::log_store::build_entry`.
pub(crate) fn build_nats_entry(
    data: Vec<u8>,
    entry_id: u64,
    region_id: RegionId,
    provider: &Provider,
    max_batch_bytes: usize,
) -> Entry {
    let max_data_size = max_batch_bytes.saturating_sub(ESTIMATED_META_SIZE);
    if data.len() <= max_data_size {
        Entry::Naive(NaiveEntry {
            provider: provider.clone(),
            region_id,
            entry_id,
            data,
        })
    } else {
        let parts: Vec<Vec<u8>> = data.chunks(max_data_size).map(|s| s.to_vec()).collect();
        let num_parts = parts.len();

        let mut headers = Vec::with_capacity(num_parts);
        headers.push(MultiplePartHeader::First);
        headers.extend((1..num_parts - 1).map(MultiplePartHeader::Middle));
        headers.push(MultiplePartHeader::Last);

        Entry::MultiplePart(MultiplePartEntry {
            provider: provider.clone(),
            region_id,
            entry_id,
            headers,
            parts,
        })
    }
}

#[cfg(test)]
mod tests {
    use store_api::logstore::entry::{Entry, MultiplePartHeader};
    use store_api::logstore::provider::Provider;
    use store_api::storage::RegionId;

    use super::*;

    #[test]
    fn test_build_nats_entry_naive() {
        let provider = Provider::nats_provider("greptimedb_wal_subject.0".to_string());
        let region_id = RegionId::new(1, 1);
        let entry = build_nats_entry(vec![1u8; 100], 1, region_id, &provider, 1024);
        assert!(matches!(entry, Entry::Naive(_)));
    }

    #[test]
    fn test_build_nats_entry_multipart() {
        let provider = Provider::nats_provider("greptimedb_wal_subject.0".to_string());
        let region_id = RegionId::new(1, 1);
        // max_batch_bytes = 300, max_data_size = 300 - 256 = 44
        let entry = build_nats_entry(vec![1u8; 100], 1, region_id, &provider, 300);
        let mp = entry.into_multiple_part_entry().unwrap();
        assert_eq!(mp.headers[0], MultiplePartHeader::First);
        assert_eq!(*mp.headers.last().unwrap(), MultiplePartHeader::Last);
        assert!(mp.parts.len() >= 3);
    }
}
