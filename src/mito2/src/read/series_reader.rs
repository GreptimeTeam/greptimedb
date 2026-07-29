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

//! Shared types for range-based metric series reads.

#[allow(dead_code)]
const TSID_DOMAIN_END: u128 = 1u128 << u64::BITS;

/// A stable partition of the TSID integer domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SeriesRange {
    start: u128,
    end: u128,
}

#[allow(dead_code)]
impl SeriesRange {
    pub(crate) fn new(partition: usize, partitions: usize) -> Option<Self> {
        if partitions == 0 || partition >= partitions {
            return None;
        }

        let partitions = partitions as u128;
        let partition = partition as u128;
        let boundary = |partition: u128| {
            let numerator = partition * TSID_DOMAIN_END;
            numerator / partitions + u128::from(!numerator.is_multiple_of(partitions))
        };
        Some(Self {
            start: boundary(partition),
            end: boundary(partition + 1),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn series_ranges_cover_the_tsid_domain() {
        let ranges = (0..3)
            .map(|partition| SeriesRange::new(partition, 3).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(0, ranges[0].start);
        assert_eq!(TSID_DOMAIN_END, ranges[2].end);
        assert_eq!(ranges[0].end, ranges[1].start);
        assert_eq!(ranges[1].end, ranges[2].start);
    }
}
