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

use std::ops::DerefMut;

use common_meta::ddl::DetectingRegion;
use dashmap::mapref::multiple::RefMulti;
use dashmap::DashMap;

use crate::failure_detector::{PhiAccrualFailureDetector, PhiAccrualFailureDetectorOptions};

/// Detects the region failures.
pub(crate) struct RegionFailureDetector {
    options: PhiAccrualFailureDetectorOptions,
    detectors: DashMap<DetectingRegion, PhiAccrualFailureDetector>,
}

pub(crate) struct FailureDetectorEntry<'a> {
    e: RefMulti<'a, DetectingRegion, PhiAccrualFailureDetector>,
}

impl FailureDetectorEntry<'_> {
    pub(crate) fn region_ident(&self) -> &DetectingRegion {
        self.e.key()
    }

    pub(crate) fn failure_detector(&self) -> &PhiAccrualFailureDetector {
        self.e.value()
    }
}

impl RegionFailureDetector {
    pub(crate) fn new(options: PhiAccrualFailureDetectorOptions) -> Self {
        Self {
            options,
            detectors: DashMap::new(),
        }
    }

    /// Returns [`PhiAccrualFailureDetector`] of the specific [`DetectingRegion`].
    pub(crate) fn region_failure_detector(
        &self,
        detecting_region: DetectingRegion,
    ) -> impl DerefMut<Target = PhiAccrualFailureDetector> + '_ {
        self.detectors
            .entry(detecting_region)
            .or_insert_with(|| PhiAccrualFailureDetector::from_options(self.options))
    }

    /// Returns A mutable reference to the [`PhiAccrualFailureDetector`] for the specified [`DetectingRegion`].
    /// If a detector already exists for the region, it is returned. Otherwise, a new
    /// detector is created and initialized with the provided timestamp.
    pub(crate) fn maybe_init_region_failure_detector(
        &self,
        detecting_region: DetectingRegion,
        ts_millis: i64,
    ) -> impl DerefMut<Target = PhiAccrualFailureDetector> + '_ {
        self.detectors.entry(detecting_region).or_insert_with(|| {
            let mut detector = PhiAccrualFailureDetector::from_options(self.options);
            detector.heartbeat(ts_millis);
            detector
        })
    }

    /// Returns a [FailureDetectorEntry] iterator.
    pub(crate) fn iter(&self) -> impl Iterator<Item = FailureDetectorEntry> + '_ {
        self.detectors
            .iter()
            .map(move |e| FailureDetectorEntry { e })
    }

    /// Removes the specific [PhiAccrualFailureDetector] if exists.
    pub(crate) fn remove(&self, region: &DetectingRegion) {
        self.detectors.remove(region);
    }

    /// Removes all [PhiAccrualFailureDetector]s.
    pub(crate) fn clear(&self) {
        self.detectors.clear()
    }

    /// Returns true if the specific [`DetectingRegion`] exists.
    #[cfg(test)]
    pub(crate) fn contains(&self, region: &DetectingRegion) -> bool {
        self.detectors.contains_key(region)
    }

    /// Returns the length
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.detectors.len()
    }

    /// Returns true if it's empty
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.detectors.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn dump(&self) -> RegionFailureDetector {
        let mut m = DashMap::with_capacity(self.detectors.len());
        m.extend(self.detectors.iter().map(|x| (*x.key(), x.value().clone())));
        Self {
            detectors: m,
            options: self.options,
        }
    }
}

#[cfg(test)]
mod tests {

    use store_api::storage::RegionId;

    use super::*;

    #[test]
    fn test_default_failure_detector_container() {
        let container = RegionFailureDetector::new(Default::default());
        let detecting_region = (2, RegionId::new(1, 1));
        let _ = container.region_failure_detector(detecting_region);
        assert!(container.contains(&detecting_region));

        {
            let mut iter = container.iter();
            let _ = iter.next().unwrap();
            assert!(iter.next().is_none());
        }

        container.clear();
        assert!(container.is_empty());
    }
}
