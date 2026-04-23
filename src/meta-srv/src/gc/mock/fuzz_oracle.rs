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

use std::collections::{HashMap, HashSet};

use store_api::storage::{FileId, RegionId};

use crate::gc::Region2Peers;

#[derive(Debug, Clone)]
pub(super) struct Phase2MockFuzzEvidence {
    pub(super) seed: u64,
    pub(super) action_index: usize,
    pub(super) deleted_files: HashMap<RegionId, Vec<FileId>>,
    pub(super) need_retry_regions: HashSet<RegionId>,
    pub(super) processed_regions: HashSet<RegionId>,
    pub(super) reachable_files: HashSet<FileId>,
    pub(super) protected_files: HashSet<FileId>,
    pub(super) route_overrides: Region2Peers,
    pub(super) allowed_override_regions: HashSet<RegionId>,
    pub(super) expected_target_regions: HashSet<RegionId>,
}

impl Phase2MockFuzzEvidence {
    pub(super) fn concise_summary(&self) -> String {
        format!(
            "seed={} action_index={} deleted_regions={} deleted_files={} need_retry_regions={:?} processed_regions={:?}",
            self.seed,
            self.action_index,
            self.deleted_files.len(),
            self.deleted_files.values().map(Vec::len).sum::<usize>(),
            self.need_retry_regions,
            self.processed_regions,
        )
    }
}

pub(super) fn validate_phase2_mock_evidence(
    evidence: &Phase2MockFuzzEvidence,
) -> Result<(), String> {
    let deleted_file_set: HashSet<_> = evidence.deleted_files.values().flatten().copied().collect();

    let reachable_overlap: HashSet<_> = deleted_file_set
        .intersection(&evidence.reachable_files)
        .copied()
        .collect();
    if !reachable_overlap.is_empty() {
        return Err(format!(
            "oracle violation: deleted files intersect reachable set; overlap={reachable_overlap:?}; {}",
            evidence.concise_summary(),
        ));
    }

    let protected_overlap: HashSet<_> = deleted_file_set
        .intersection(&evidence.protected_files)
        .copied()
        .collect();
    if !protected_overlap.is_empty() {
        return Err(format!(
            "oracle violation: deleted files intersect protected set; overlap={protected_overlap:?}; {}",
            evidence.concise_summary(),
        ));
    }

    let unexpected_override_regions: Vec<_> = evidence
        .route_overrides
        .keys()
        .copied()
        .filter(|region_id| !evidence.allowed_override_regions.contains(region_id))
        .collect();
    if !unexpected_override_regions.is_empty() {
        return Err(format!(
            "oracle violation: route overrides escaped allowed scope; unexpected_regions={unexpected_override_regions:?}; {}",
            evidence.concise_summary(),
        ));
    }

    let deleted_regions: HashSet<_> = evidence.deleted_files.keys().copied().collect();
    let lost_processed_regions: Vec<_> = deleted_regions
        .difference(&evidence.processed_regions)
        .copied()
        .collect();
    if !lost_processed_regions.is_empty() {
        return Err(format!(
            "oracle violation: deleted regions missing from processed bookkeeping; missing={lost_processed_regions:?}; {}",
            evidence.concise_summary(),
        ));
    }

    let accounted_regions: HashSet<_> = evidence
        .processed_regions
        .union(&evidence.need_retry_regions)
        .copied()
        .collect();
    let unaccounted_targets: Vec<_> = evidence
        .expected_target_regions
        .difference(&accounted_regions)
        .copied()
        .collect();
    if !unaccounted_targets.is_empty() {
        return Err(format!(
            "oracle violation: target regions were neither processed nor marked for retry; missing={unaccounted_targets:?}; {}",
            evidence.concise_summary(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    fn file_id(id: &str) -> FileId {
        FileId::from_str(id).unwrap()
    }

    fn base_evidence() -> Phase2MockFuzzEvidence {
        let region_id = RegionId::new(42, 1);
        let deleted = file_id("00000000-0000-0000-0000-000000000042");

        Phase2MockFuzzEvidence {
            seed: 7,
            action_index: 3,
            deleted_files: HashMap::from([(region_id, vec![deleted])]),
            need_retry_regions: HashSet::new(),
            processed_regions: [region_id].into(),
            reachable_files: HashSet::new(),
            protected_files: HashSet::new(),
            route_overrides: HashMap::new(),
            allowed_override_regions: HashSet::new(),
            expected_target_regions: [region_id].into(),
        }
    }

    #[test]
    fn test_phase2_oracle_rejects_protected_overlap() {
        let mut evidence = base_evidence();
        evidence.protected_files = evidence.deleted_files.values().flatten().copied().collect();

        let error = validate_phase2_mock_evidence(&evidence).unwrap_err();
        assert!(error.contains("intersect protected set"));
    }

    #[test]
    fn test_phase2_oracle_rejects_unexpected_route_override_scope() {
        let mut evidence = base_evidence();
        let dropped_region = RegionId::new(42, 2);
        evidence
            .route_overrides
            .insert(dropped_region, Default::default());

        let error = validate_phase2_mock_evidence(&evidence).unwrap_err();
        assert!(error.contains("route overrides escaped allowed scope"));
    }

    #[test]
    fn test_phase2_oracle_rejects_missing_processed_bookkeeping() {
        let mut evidence = base_evidence();
        evidence.processed_regions.clear();

        let error = validate_phase2_mock_evidence(&evidence).unwrap_err();
        assert!(error.contains("missing from processed bookkeeping"));
    }
}
