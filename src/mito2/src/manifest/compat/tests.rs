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

use crate::manifest::action::{RegionEdit, RegionMetaAction};

#[test]
fn test_edit_action_tag_matches_serialized_enum() {
    let encoded = serde_json::to_value(RegionMetaAction::Edit(RegionEdit::default())).unwrap();
    let object = encoded.as_object().unwrap();
    assert_eq!(1, object.len(), "action must stay externally tagged");
    assert!(
        object.contains_key(super::EDIT_ACTION_TAG),
        "the delta guard reads raw JSON by tag; keep EDIT_ACTION_TAG in sync with RegionMetaAction"
    );
}
