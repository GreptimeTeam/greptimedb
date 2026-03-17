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

use std::time::Duration;

use chrono::Duration as ChronoDuration;

use super::manifest::{ChunkMeta, TimeRange};

pub fn generate_chunks(time_range: &TimeRange, window: Duration) -> Vec<ChunkMeta> {
    let (Some(start), Some(end)) = (time_range.start, time_range.end) else {
        return vec![ChunkMeta::new(1, time_range.clone())];
    };

    if start >= end {
        return Vec::new();
    }

    let window = match ChronoDuration::from_std(window) {
        Ok(window) if window > ChronoDuration::zero() => window,
        _ => return vec![ChunkMeta::new(1, time_range.clone())],
    };

    let mut chunks = Vec::new();
    let mut cursor = start;
    let mut id = 1;

    while cursor < end {
        let next = cursor + window;
        let next = if next < end { next } else { end };
        chunks.push(ChunkMeta::new(id, TimeRange::new(Some(cursor), Some(next))));
        id += 1;
        cursor = next;
    }

    chunks
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::*;

    #[test]
    fn test_generate_chunks_unbounded() {
        let range = TimeRange::unbounded();
        let chunks = generate_chunks(&range, Duration::from_secs(3600));
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].time_range, range);
    }

    #[test]
    fn test_generate_chunks_split() {
        let start = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2025, 1, 1, 3, 0, 0).unwrap();
        let range = TimeRange::new(Some(start), Some(end));

        let chunks = generate_chunks(&range, Duration::from_secs(3600));
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].time_range.start, Some(start));
        assert_eq!(
            chunks[2].time_range.end,
            Some(Utc.with_ymd_and_hms(2025, 1, 1, 3, 0, 0).unwrap())
        );
    }

    #[test]
    fn test_generate_chunks_empty_range() {
        let start = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let range = TimeRange::new(Some(start), Some(start));
        let chunks = generate_chunks(&range, Duration::from_secs(3600));
        assert!(chunks.is_empty());
    }
}
