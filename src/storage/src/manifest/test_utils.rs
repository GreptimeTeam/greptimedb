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

use datatypes::type_id::LogicalTypeId;
use store_api::storage::SequenceNumber;
use uuid::Uuid;

use crate::manifest::action::*;
use crate::metadata::RegionMetadata;
use crate::sst::FileMeta;
use crate::test_util::descriptor_util::RegionDescBuilder;

pub fn build_region_meta() -> RegionMetadata {
    let region_name = "region-0";
    let desc = RegionDescBuilder::new(region_name)
        .id(0)
        .push_key_column(("k1", LogicalTypeId::Int32, false))
        .push_value_column(("v1", LogicalTypeId::Float32, true))
        .build();
    desc.try_into().unwrap()
}

pub fn build_region_edit(
    sequence: SequenceNumber,
    files_to_add: &[&Uuid],
    files_to_remove: &[&Uuid],
) -> RegionEdit {
    RegionEdit {
        region_version: 0,
        flushed_sequence: Some(sequence),
        files_to_add: files_to_add
            .iter()
            .map(|f| FileMeta {
                region_id: 0,
                file_id: f.clone().clone(),
                time_range: None,
                level: 0,
            })
            .collect(),
        files_to_remove: files_to_remove
            .iter()
            .map(|f| FileMeta {
                region_id: 0,
                file_id: f.clone().clone(),
                time_range: None,
                level: 0,
            })
            .collect(),
    }
}
