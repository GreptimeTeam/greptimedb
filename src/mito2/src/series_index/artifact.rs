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

//! Immutable, potentially multi-object format outputs.

use serde::{Deserialize, Serialize};
use store_api::storage::FileId;

/// Persisted artifact identifiers. Concrete codecs are added with their implementations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum ArtifactFormat {
    #[serde(rename = "columnar-series")]
    ColumnarSeries,
    #[serde(rename = "columnar-samples")]
    ColumnarSamples,
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
