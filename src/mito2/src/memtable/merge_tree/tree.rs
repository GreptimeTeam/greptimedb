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

//! Implementation of the memtable merge tree.

use std::sync::{Arc, RwLock};

use store_api::metadata::RegionMetadataRef;

use crate::error::Result;
use crate::memtable::merge_tree::mutable::MutablePart;
use crate::memtable::merge_tree::MergeTreeConfig;
use crate::memtable::KeyValues;

/// The merge tree.
pub(crate) struct MergeTree {
    /// Metadata of the region.
    pub(crate) metadata: RegionMetadataRef,
    /// Mutable part of the tree.
    mutable: RwLock<MutablePart>,
}

pub(crate) type MergeTreeRef = Arc<MergeTree>;

impl MergeTree {
    /// Creates a new merge tree.
    pub(crate) fn new(metadata: RegionMetadataRef, config: &MergeTreeConfig) -> MergeTree {
        MergeTree {
            metadata,
            mutable: RwLock::new(MutablePart::new(&config)),
        }
    }

    /// Write key-values into the tree.
    #[allow(unused)]
    pub(crate) fn write(&self, _kvs: &KeyValues) -> Result<()> {
        todo!()
    }
}
