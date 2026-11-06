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

const LEGACY_EDIT: &str =
    include_str!("../../../../../tests/compatibility/manifest/table_compaction/legacy-edit.json");
const LEGACY_MANIFEST: &str = include_str!(
    "../../../../../tests/compatibility/manifest/table_compaction/legacy-manifest.json"
);

#[test]
fn test_legacy_manifest_compatibility() {
    let mut json: serde_json::Value = serde_json::from_str(LEGACY_EDIT).unwrap();
    // Optional core fields must not be mistaken for required extension state.
    json["actions"][0]["Edit"]["future_core_option"] = json!(true);
    let actions = RegionMetaActionList::decode(&serde_json::to_vec(&json).unwrap()).unwrap();
    let RegionMetaAction::Edit(edit) = &actions.actions[0] else {
        panic!("expected edit")
    };
    assert_eq!(
        vec![0, 1],
        edit.files_to_add
            .iter()
            .map(|file| file.level)
            .collect::<Vec<_>>()
    );
    assert_eq!(Some(8), edit.committed_sequence);
    assert_eq!(
        vec![7, 8],
        edit.files_to_add
            .iter()
            .map(|file| file.sequence.unwrap().get())
            .collect::<Vec<_>>()
    );
    let encoded = String::from_utf8(actions.encode().unwrap()).unwrap();
    assert!(!encoded.contains("table_compaction") && !encoded.contains("extensions"));
    let mut json: serde_json::Value = serde_json::from_str(LEGACY_MANIFEST).unwrap();
    json["future_core_option"] = json!(true);
    let manifest: RegionManifest = serde_json::from_value(json).unwrap();
    assert!(manifest.extensions.is_empty());
    assert_eq!(12, manifest.manifest_version);
    assert_eq!(5, manifest.flushed_entry_id);
    assert_eq!(8, manifest.flushed_sequence);
    let encoded = serde_json::to_string(&manifest).unwrap();
    assert!(!encoded.contains("table_compaction") && !encoded.contains("extensions"));
    for level in [0, 1, 2, 3, 255] {
        let mut file = serde_json::to_value(&edit.files_to_add[0]).unwrap();
        file["level"] = level.into();
        let decoded = serde_json::from_value::<FileMeta>(file);
        assert_eq!(
            level < 2 || (cfg!(feature = "enterprise") && level == 2),
            decoded.is_ok(),
            "level {level}"
        );
    }
}

#[rstest::rstest]
#[case::future_extension("future_extension", json!({"floor": 7}), json!({"set_floor": 7}), false)]
#[case::table_compaction("table_compaction", json!({"registrations": {}, "publications": {}, "coordinator_epoch": 3, "min_request_epoch": 7}), json!({"cleanup": {"min_request_epoch": 7, "max_receipts": 1, "coordinator_epoch": 3}}), cfg!(feature = "enterprise"))]
#[test]
fn test_extension_wire_capability(
    #[case] name: &str,
    #[case] state: serde_json::Value,
    #[case] operation: serde_json::Value,
    #[case] supported: bool,
) {
    let mut manifest: serde_json::Value = serde_json::from_str(LEGACY_MANIFEST).unwrap();
    if name == "table_compaction" {
        manifest[name] = state;
    } else {
        manifest["extensions"][name] = state;
    }
    let checkpoint = json!({"last_version": 12, "compacted_actions": 1, "checkpoint": manifest});
    let decoded = RegionCheckpoint::decode(&serde_json::to_vec(&checkpoint).unwrap());
    assert_eq!(supported, decoded.is_ok(), "{decoded:?}");
    if let Ok(decoded) = decoded {
        let encoded: serde_json::Value =
            serde_json::from_slice(&decoded.encode().unwrap()).unwrap();
        assert_eq!(checkpoint["checkpoint"][name], encoded["checkpoint"][name]);
        assert!(encoded["checkpoint"].get("extensions").is_none());
    }
    let mut actions: serde_json::Value = serde_json::from_str(LEGACY_EDIT).unwrap();
    if name == "table_compaction" {
        actions["actions"][0]["Edit"][name] = operation;
    } else {
        actions["actions"][0]["Edit"]["extensions"][name] = operation;
    }
    let decoded = RegionMetaActionList::decode(&serde_json::to_vec(&actions).unwrap());
    assert_eq!(supported, decoded.is_ok(), "{decoded:?}");
    if let Ok(decoded) = decoded {
        let encoded: serde_json::Value =
            serde_json::from_slice(&decoded.encode().unwrap()).unwrap();
        assert_eq!(
            actions["actions"][0]["Edit"][name],
            encoded["actions"][0]["Edit"][name]
        );
        assert!(encoded["actions"][0]["Edit"].get("extensions").is_none());
    }
}
