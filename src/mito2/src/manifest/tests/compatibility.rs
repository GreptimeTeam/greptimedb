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

use serde_json::json;

use crate::manifest::action::{
    RegionCheckpoint, RegionManifest, RegionMetaAction, RegionMetaActionList,
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
            level < 2 || (cfg!(feature = "enterprise") && level == 2),
            serde_json::from_value::<FileMeta>(file).is_ok(),
            "level {level}"
        );
    }
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
