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
use crate::memtable::merge_tree::mutable::{MutablePart, WriteMetrics};
use crate::memtable::merge_tree::MergeTreeConfig;
use crate::memtable::KeyValues;
use crate::row_converter::{McmpRowCodec, SortField};

/// The merge tree.
pub(crate) struct MergeTree {
    /// Metadata of the region.
    pub(crate) metadata: RegionMetadataRef,
    /// Primary key codec.
    row_codec: McmpRowCodec,
    /// Mutable part of the tree.
    mutable: RwLock<MutablePart>,
}

pub(crate) type MergeTreeRef = Arc<MergeTree>;

impl MergeTree {
    /// Creates a new merge tree.
    pub(crate) fn new(metadata: RegionMetadataRef, config: &MergeTreeConfig) -> MergeTree {
        let row_codec = McmpRowCodec::new(
            metadata
                .primary_key_columns()
                .map(|c| SortField::new(c.column_schema.data_type.clone()))
                .collect(),
        );
        MergeTree {
            metadata,
            row_codec,
            mutable: RwLock::new(MutablePart::new(&config)),
        }
    }

    /// Write key-values into the tree.
    pub(crate) fn write(&self, kvs: &KeyValues, metrics: &mut WriteMetrics) -> Result<()> {
        let mut part = self.mutable.write().unwrap();
        part.write(&self.metadata, &self.row_codec, kvs, metrics)
    }
}
