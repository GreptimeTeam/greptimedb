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

use serde::{Deserialize, Serialize};

/// Metadata for directory in puffin file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirMetadata {
    pub files: Vec<DirFileMetadata>,
}

/// Metadata for file in directory in puffin file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirFileMetadata {
    /// The relative path of the file in the directory.
    pub relative_path: String,

    /// The file is stored as a blob in the puffin file.
    /// `blob_index` is the index of the blob in the puffin file.
    pub blob_index: usize,

    /// The key of the blob in the puffin file.
    pub key: String,
}
