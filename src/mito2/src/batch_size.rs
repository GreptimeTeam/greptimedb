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

//! Utilities for choosing an execution batch size from source statistics.

use crate::sst::parquet::DEFAULT_READ_BATCH_SIZE;

/// Approximate decoded bytes to produce in one batch.
pub(crate) const TARGET_BATCH_BYTES: usize = 64 * 1024 * 1024;

/// Estimates a batch size from `(num_rows, estimated_bytes)` pairs.
pub(crate) fn estimate_batch_size(sources: impl IntoIterator<Item = (u64, u64)>) -> usize {
    let max_row_width = sources
        .into_iter()
        .filter_map(|(rows, bytes)| estimate_row_width(rows, bytes))
        .max();

    let Some(row_width) = max_row_width else {
        return DEFAULT_READ_BATCH_SIZE;
    };

    (TARGET_BATCH_BYTES as u64 / row_width).clamp(1, DEFAULT_READ_BATCH_SIZE as u64) as usize
}

/// Returns the ceiling of the estimated bytes per row.
fn estimate_row_width(rows: u64, bytes: u64) -> Option<u64> {
    if rows == 0 || bytes == 0 {
        return None;
    }

    Some(bytes / rows + u64::from(!bytes.is_multiple_of(rows)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_estimate_batch_size_without_stats() {
        assert_eq!(DEFAULT_READ_BATCH_SIZE, estimate_batch_size([]));
        assert_eq!(
            DEFAULT_READ_BATCH_SIZE,
            estimate_batch_size([(0, 100), (100, 0)])
        );
    }

    #[test]
    fn test_estimate_batch_size_for_narrow_and_wide_rows() {
        assert_eq!(DEFAULT_READ_BATCH_SIZE, estimate_batch_size([(100, 100)]));
        assert_eq!(256, estimate_batch_size([(1, 256 * 1024)]));
        assert_eq!(1, estimate_batch_size([(1, TARGET_BATCH_BYTES as u64 + 1)]));
    }

    #[test]
    fn test_estimate_batch_size_uses_widest_known_source() {
        assert_eq!(
            256,
            estimate_batch_size([(100, 0), (100, 100), (4, 1024 * 1024)])
        );
    }

    #[test]
    fn test_estimate_batch_size_saturates() {
        assert_eq!(
            DEFAULT_READ_BATCH_SIZE,
            estimate_batch_size([(u64::MAX, u64::MAX)])
        );
        assert_eq!(1, estimate_batch_size([(1, u64::MAX)]));
        assert_eq!(1, estimate_batch_size([(2, u64::MAX)]));
    }
}
