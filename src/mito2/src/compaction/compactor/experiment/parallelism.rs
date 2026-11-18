// Copyright 2023-2026 GrepTime Inc.
//
// This file is part of the GreptimeDB Enterprise Edition and is licensed under
// the GreptimeDB Enterprise License. You may not use this file except in
// compliance with that license. A copy of the license is available at the root
// of this repository in the file LICENSE-ENTERPRISE.
//
// Unless required by applicable law or agreed to in writing, this software is
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied.

//! Experimental concurrency knobs; unset values retain automatic parallelism 1.
use std::num::NonZeroUsize;
use std::sync::LazyLock;
use store_api::storage::RegionId;

fn configured(name: &str, fallback: usize) -> usize {
    match std::env::var(name) {
        Ok(value) => value
            .parse::<NonZeroUsize>()
            .expect("compaction parallelism must be positive")
            .get(),
        Err(_) => fallback,
    }
}

static AUTOMATIC: LazyLock<usize> =
    LazyLock::new(|| configured("TESTBED_AUTO_COMPACTION_PARALLELISM", 1));
static SHARED: LazyLock<usize> =
    LazyLock::new(|| configured("TESTBED_SHARED_SCAN_PARALLELISM", *AUTOMATIC));

pub(crate) fn automatic_parallelism() -> usize {
    *AUTOMATIC
}

pub(crate) fn merge_parallelism(region: RegionId, shared: bool, ordinary: usize) -> usize {
    select(region.region_number(), shared, ordinary, *SHARED)
}

fn select(_number: u32, shared: bool, ordinary: usize, scanner: usize) -> usize {
    // Every participant holds an execution slot from ready to publication.
    // Followers must admit the same number of batches as the scanning Region.
    if shared { scanner } else { ordinary }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_shared_participants_get_scanner_parallelism() {
        for number in 0..12 {
            assert_eq!(16, select(number, true, 8, 16));
            assert_eq!(24, select(number, true, 8, 24));
        }
    }

    #[test]
    fn ordinary_l1_keeps_its_own_limit_on_every_region() {
        for number in 0..12 {
            assert_eq!(8, select(number, false, 8, 16));
            assert_eq!(8, select(number, false, 8, 24));
            assert_eq!(4, select(number, false, 4, 32));
        }
    }

    #[test]
    fn defaults_keep_previous_execution_width() {
        for shared in [false, true] {
            for number in 0..12 {
                assert_eq!(1, select(number, shared, 1, 1));
            }
        }
    }
}
