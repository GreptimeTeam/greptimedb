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

//! Metadata of mito regions.

use std::sync::Arc;

use crate::error::Error;
use crate::worker::request::CreateRequest;

/// Static metadata of a region.
#[derive(Debug)]
pub(crate) struct RegionMetadata {}

pub(crate) type RegionMetadataRef = Arc<RegionMetadata>;

impl TryFrom<CreateRequest> for RegionMetadata {
    type Error = Error;

    fn try_from(_value: CreateRequest) -> Result<Self, Self::Error> {
        todo!()
    }
}
