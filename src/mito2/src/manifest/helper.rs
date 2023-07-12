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

use serde::Serialize;
use store_api::manifest::action::ProtocolVersion;
use store_api::manifest::ManifestVersion;

use crate::error::Result;
use crate::manifest::action::RegionCheckpoint;

pub const NEWLINE: &[u8] = b"\n";

pub fn encode_actions<T: Serialize>(
    prev_version: ManifestVersion,
    actions: &[T],
) -> Result<Vec<u8>> {
    todo!()
}

pub fn encode_checkpoint(snasphot: &RegionCheckpoint) -> Result<Vec<u8>> {
    todo!()
}

pub fn decode_checkpoint(bs: &[u8], reader_version: ProtocolVersion) -> Result<RegionCheckpoint> {
    todo!()
}
