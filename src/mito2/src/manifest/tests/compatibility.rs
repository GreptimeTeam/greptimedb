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

use serde_json::json;
use store_api::storage::RegionId;

use crate::manifest::action::{
    RegionCheckpoint, RegionManifest, RegionMetaAction, RegionMetaActionList, TruncateKind,
};
use crate::sst::file::FileMeta;

const EDIT: &str =
    include_str!("../../../../../tests/compatibility/manifest/table_compaction/legacy-edit.json");
const MANIFEST: &str = include_str!(
    "../../../../../tests/compatibility/manifest/table_compaction/legacy-manifest.json"
);

#[test]
fn test_file_only_manifest_compatibility() {
    let mut json: serde_json::Value = serde_json::from_str(EDIT).unwrap();
    json["actions"][0]["Edit"]["future_core_option"] = json!(true);
    let actions = RegionMetaActionList::decode(&serde_json::to_vec(&json).unwrap()).unwrap();
    let RegionMetaAction::Edit(edit) = &actions.actions[0] else {
        panic!("expected edit")
    };
    assert_eq!(
        vec![0, 1],
        edit.files_to_add
            .iter()
            .map(|f| f.level)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        vec![7, 8],
        edit.files_to_add
            .iter()
            .map(|f| f.sequence.unwrap().get())
            .collect::<Vec<_>>()
    );
    assert_eq!(Some(8), edit.committed_sequence);
    // The fixture predates `substituted_up_to`. An absent cut means no row of the file was
    // substituted, which is what an older writer's output means.
    assert_eq!(
        vec![None, None],
        edit.files_to_add
            .iter()
            .map(|f| f.substituted_up_to)
            .collect::<Vec<_>>()
    );
    // A cut is optional on the wire: encoding a file without one stays readable by an
    // older writer, and a zero cut (a column that never carried a per-row number) survives
    // the round trip.
    let encoded = serde_json::to_value(&edit.files_to_add[0]).unwrap();
    assert!(encoded.get("substituted_up_to").is_none());
    for cut in [0, 7] {
        let mut file = encoded.clone();
        file["substituted_up_to"] = json!(cut);
        let decoded: FileMeta = serde_json::from_value(file).unwrap();
        assert_eq!(Some(cut), decoded.substituted_up_to);
    }
    let encoded = String::from_utf8(actions.encode().unwrap()).unwrap();
    assert!(!encoded.contains("table_compaction") && !encoded.contains("extensions"));
    let mut manifest: serde_json::Value = serde_json::from_str(MANIFEST).unwrap();
    manifest["future_core_option"] = json!(true);
    let checkpoint = json!({"last_version":12,"compacted_actions":1,"checkpoint":manifest});
    let decoded = RegionCheckpoint::decode(&serde_json::to_vec(&checkpoint).unwrap()).unwrap();
    let manifest = decoded.checkpoint.as_ref().unwrap();
    assert_eq!(12, manifest.manifest_version);
    assert_eq!(5, manifest.flushed_entry_id);
    assert_eq!(8, manifest.flushed_sequence);
    let encoded = String::from_utf8(decoded.encode().unwrap()).unwrap();
    assert!(!encoded.contains("table_compaction") && !encoded.contains("extensions"));
    for level in [0, 1, 2, 3, 255] {
        let mut file = serde_json::to_value(&edit.files_to_add[0]).unwrap();
        file["level"] = level.into();
        assert_eq!(
            crate::sst::file::is_supported_level(level),
            serde_json::from_value::<FileMeta>(file).is_ok(),
            "level {level}"
        );
    }
}

/// The fixture test above only asserted `FileMeta`. The actions that carry it aged with the
/// same format, so decode the legacy bytes into the current action types, assert the fields
/// those types still keep, and round-trip them: an older writer's manifest must stay readable
/// here, and what this build writes must stay readable to it.
#[test]
fn test_legacy_edit_truncate_and_manifest_round_trip() {
    let actions = RegionMetaActionList::decode(EDIT.as_bytes()).unwrap();
    let RegionMetaAction::Edit(edit) = &actions.actions[0] else {
        panic!("expected edit")
    };
    assert_eq!(Some(5), edit.flushed_entry_id);
    assert_eq!(Some(8), edit.flushed_sequence);
    assert_eq!(Some(8), edit.committed_sequence);
    assert_eq!(Some(Duration::from_secs(3600)), edit.compaction_time_window);
    assert_eq!(None, edit.timestamp_ms);
    assert!(edit.files_to_remove.is_empty());
    // WAL recovery postdates the fixture: an absent flatten decodes to "nothing to change"
    // rather than to evidence this build silently drops.
    assert_eq!(None, edit.wal_recovery.pending_tails);
    assert_eq!(None, edit.wal_recovery.handoff_floor);
    assert_eq!(
        actions,
        RegionMetaActionList::decode(&actions.encode().unwrap()).unwrap()
    );

    // A truncate of the same era: the untagged kind is told apart by its fields, and the
    // entry id / sequence pair is the only thing a truncate-all records.
    let legacy_truncate = json!({"actions": [{"Truncate": {
        "region_id": 4294967297u64,
        "truncated_entry_id": 5,
        "truncated_sequence": 8
    }}]});
    let decoded =
        RegionMetaActionList::decode(&serde_json::to_vec(&legacy_truncate).unwrap()).unwrap();
    let RegionMetaAction::Truncate(truncate) = &decoded.actions[0] else {
        panic!("expected truncate")
    };
    assert_eq!(RegionId::new(1, 1), truncate.region_id);
    assert_eq!(None, truncate.timestamp_ms);
    assert_eq!(
        TruncateKind::All {
            truncated_entry_id: 5,
            truncated_sequence: 8,
        },
        truncate.kind
    );
    assert_eq!(
        decoded,
        RegionMetaActionList::decode(&decoded.encode().unwrap()).unwrap()
    );

    // A partial truncate lists the files it removes, and the substituted cut a file states
    // survives the round trip like every other `FileMeta` field.
    let mut legacy_file = serde_json::from_str::<serde_json::Value>(EDIT).unwrap()["actions"][0]
        ["Edit"]["files_to_add"][1]
        .clone();
    legacy_file["substituted_up_to"] = json!(6);
    let partial_truncate = json!({"actions": [{"Truncate": {
        "region_id": 4294967297u64,
        "files_to_remove": [legacy_file]
    }}]});
    let decoded =
        RegionMetaActionList::decode(&serde_json::to_vec(&partial_truncate).unwrap()).unwrap();
    let RegionMetaAction::Truncate(truncate) = &decoded.actions[0] else {
        panic!("expected truncate")
    };
    let TruncateKind::Partial { files_to_remove } = &truncate.kind else {
        panic!("expected partial truncate")
    };
    assert_eq!(1, files_to_remove.len());
    assert_eq!(Some(6), files_to_remove[0].substituted_up_to);
    assert_eq!(Some(8), files_to_remove[0].sequence.map(|seq| seq.get()));
    assert_eq!(
        decoded,
        RegionMetaActionList::decode(&decoded.encode().unwrap()).unwrap()
    );

    // The manifest is decoded on its own, not only as a checkpoint payload: the entry id and
    // the sequence it states are what recovery starts from.
    let manifest = RegionManifest::decode(MANIFEST.as_bytes()).unwrap();
    assert_eq!(12, manifest.manifest_version);
    assert_eq!(5, manifest.flushed_entry_id);
    assert_eq!(8, manifest.flushed_sequence);
    assert_eq!(Some(8), manifest.committed_sequence);
    assert_eq!(None, manifest.truncated_entry_id);
    assert_eq!(
        Some(Duration::from_secs(3600)),
        manifest.compaction_time_window
    );
    assert!(manifest.files.is_empty());
    assert_eq!(
        manifest,
        RegionManifest::decode(&serde_json::to_vec(&manifest).unwrap()).unwrap()
    );
}

#[rstest::rstest]
#[case("table_compaction")]
#[case("extensions")]
#[test]
fn test_staging_manifest_prototype_state_is_rejected(#[case] field: &str) {
    // Staging blobs are parsed as standalone manifests, so they need the same gate
    // as checkpoints and deltas instead of silently dropping recovery evidence.
    assert!(RegionManifest::decode(MANIFEST.as_bytes()).is_ok());
    let mut manifest: serde_json::Value = serde_json::from_str(MANIFEST).unwrap();
    manifest[field] = json!({"legacy_state": true});
    assert!(matches!(
        RegionManifest::decode(&serde_json::to_vec(&manifest).unwrap()),
        Err(crate::error::Error::UnsupportedOperation { .. })
    ));
}

#[rstest::rstest]
#[case("table_compaction")]
#[case("extensions")]
#[test]
fn test_prototype_task_state_is_not_silently_discarded(#[case] field: &str) {
    let mut actions: serde_json::Value = serde_json::from_str(EDIT).unwrap();
    actions["actions"][0]["Edit"][field] = json!({"legacy_state":true});
    assert!(matches!(
        RegionMetaActionList::decode(&serde_json::to_vec(&actions).unwrap()),
        Err(crate::error::Error::UnsupportedOperation { .. })
    ));
    let mut manifest: serde_json::Value = serde_json::from_str(MANIFEST).unwrap();
    manifest[field] = json!({"legacy_state":true});
    let checkpoint = json!({"last_version":12,"compacted_actions":1,"checkpoint":manifest});
    assert!(matches!(
        RegionCheckpoint::decode(&serde_json::to_vec(&checkpoint).unwrap()),
        Err(crate::error::Error::UnsupportedOperation { .. })
    ));
}
