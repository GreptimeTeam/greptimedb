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

use std::ops::Range;
use std::time::Instant;

use bytes::Bytes;
use common_telemetry::trace;
use object_store::ObjectStore;

const FETCH_PARALLELISM: usize = 8;
pub(crate) const MERGE_GAP: usize = 512 * 1024;

/// Asynchronously fetches byte ranges from an object store.
///
/// * `FETCH_PARALLELISM` - The number of concurrent fetch operations.
/// * `MERGE_GAP` - The maximum gap size (in bytes) to merge small byte ranges for optimized fetching.
pub async fn fetch_byte_ranges(
    file_path: &str,
    object_store: ObjectStore,
    ranges: &[Range<u64>],
) -> object_store::Result<Vec<Bytes>> {
    let total_size = ranges.iter().map(|r| r.end - r.start).sum::<u64>();
    let start = Instant::now();

    let result = object_store
        .reader_with(file_path)
        .concurrent(FETCH_PARALLELISM)
        .gap(MERGE_GAP)
        .await?
        .fetch(ranges.to_vec())
        .await?
        .into_iter()
        .map(|buf| buf.to_bytes())
        .collect::<Vec<_>>();

    trace!(
        "Fetch {} bytes from '{}' in object store, cost: {:?}",
        total_size,
        file_path,
        start.elapsed()
    );

    Ok(result)
}
