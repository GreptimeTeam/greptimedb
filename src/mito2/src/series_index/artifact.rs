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

//! Immutable, potentially multi-object format outputs and reader-owned lifetimes.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde::{Deserialize, Serialize};
use store_api::storage::FileId;

use crate::series_index::purger::IndexFilePurger;
use crate::sst::file::RegionFileId;

/// Persisted artifact identifiers. Concrete codecs are added with their implementations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum ArtifactFormat {
    #[serde(rename = "parquet")]
    Parquet,
    #[serde(rename = "columnar-series")]
    ColumnarSeries,
    #[cfg(test)]
    #[serde(rename = "fake-samples")]
    FakeSamples,
    #[cfg(test)]
    #[serde(rename = "fake-index")]
    FakeIndex,
}

/// One immutable object produced by a backend build.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ArtifactObject {
    pub(crate) path: String,
    pub(crate) file_size: u64,
}

/// Identifies the exact bytes produced by one backend build.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ArtifactMetadata {
    pub(crate) format: ArtifactFormat,
    pub(crate) generation: FileId,
    /// Every object owned by this build, including metadata and auxiliary files.
    pub(crate) objects: Vec<ArtifactObject>,
    pub(crate) file_size: u64,
}

/// One samples artifact derived from an immutable Parquet SST.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SamplesEntry {
    pub(crate) file_id: FileId,
    pub(crate) artifact: ArtifactMetadata,
}

#[derive(Debug, Clone)]
pub(crate) struct SamplesFileHandle(Arc<SamplesFileInner>);

#[derive(Debug)]
struct SamplesFileInner {
    file_id: RegionFileId,
    entry: SamplesEntry,
    retired: AtomicBool,
    purger: IndexFilePurger,
}

impl SamplesFileHandle {
    pub(crate) fn new(file_id: RegionFileId, entry: SamplesEntry, purger: IndexFilePurger) -> Self {
        Self(Arc::new(SamplesFileInner {
            file_id,
            entry,
            retired: AtomicBool::new(false),
            purger,
        }))
    }

    pub(crate) fn entry(&self) -> &SamplesEntry {
        &self.0.entry
    }

    pub(crate) fn mark_deleted(&self) {
        self.0.retired.store(true, Ordering::Release);
    }
}

impl Drop for SamplesFileInner {
    fn drop(&mut self) {
        if self.retired.load(Ordering::Acquire) {
            self.purger.purge_objects(
                self.file_id,
                self.entry
                    .artifact
                    .objects
                    .iter()
                    .map(|object| object.path.clone())
                    .collect(),
            );
        }
    }
}
