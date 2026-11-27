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

use serde::{Deserialize, Serialize};
use store_api::storage::FileId;

use crate::sst::file::RegionFileId;

/// Persisted artifact identifiers. Concrete codecs are added with their implementations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum ArtifactFormat {
    #[serde(rename = "parquet")]
    Parquet,
    #[serde(rename = "columnar-series")]
    ColumnarSeries,
    #[serde(rename = "columnar-samples")]
    ColumnarSamples,
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

/// One samples artifact derived from immutable Parquet SSTs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SamplesEntry {
    pub(crate) file_id: FileId,
    pub(crate) artifact: ArtifactMetadata,
}
#[derive(Debug, Clone)]
pub(crate) struct SamplesFileHandle(Arc<SamplesFileInner>);
#[derive(Debug)]
struct SamplesFileInner {
    file: crate::sst::file::FileHandle,
    entry: SamplesEntry,
    manifest_path: String,
}
impl SamplesFileHandle {
    #[cfg(test)]
    pub(crate) fn new(file_id: RegionFileId, entry: SamplesEntry) -> Self {
        let manifest_path = format!(
            "{}manifest.json",
            crate::series_index::samples::samples_prefix(
                file_id.region_id().as_u64(),
                entry.artifact.generation
            )
        );
        let file = crate::sst::file::FileHandle::new(
            crate::sst::file::FileMeta {
                region_id: file_id.region_id(),
                file_id: file_id.file_id(),
                ..Default::default()
            },
            crate::test_util::new_noop_file_purger(),
        );
        Self(Arc::new(SamplesFileInner {
            file,
            entry,
            manifest_path,
        }))
    }

    pub(crate) fn from_narthex(
        file: crate::sst::file::FileHandle,
        region_dir: &str,
    ) -> crate::error::Result<Self> {
        let crate::sst::file::FileFormat::NarthexSamples(_) = &file.meta_ref().format else {
            return crate::error::UnsupportedOperationSnafu {
                err_msg: "Expected narthex samples file",
            }
            .fail();
        };
        let objects = file
            .meta_ref()
            .narthex_objects(region_dir)
            .expect("matched Narthex samples");
        let file_size = file.meta_ref().file_size;
        let file_id = file.file_id();
        Ok(Self(Arc::new(SamplesFileInner {
            manifest_path: format!(
                "{}manifest.json",
                crate::sst::location::narthex_file_dir(region_dir, file_id.file_id())
            ),
            file,
            entry: SamplesEntry {
                file_id: file_id.file_id(),
                artifact: ArtifactMetadata {
                    format: ArtifactFormat::ColumnarSamples,
                    generation: file_id.file_id(),
                    objects,
                    file_size,
                },
            },
        })))
    }

    pub(crate) fn file_id(&self) -> RegionFileId {
        self.0.file.file_id()
    }
    pub(crate) fn manifest_path(&self) -> String {
        self.0.manifest_path.clone()
    }
    pub(crate) fn entry(&self) -> &SamplesEntry {
        &self.0.entry
    }
}
