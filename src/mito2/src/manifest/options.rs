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

//! Options for [RegionManifestManager](crate::manifest::manager::RegionManifestManager).

use common_datasource::compression::CompressionType;
use object_store::ObjectStore;

#[derive(Debug, Clone)]
pub struct RegionManifestOptions {
    manifest_dir: String,
    object_store: ObjectStore,
    compress_type: CompressionType,
    /// Interval of version ([ManifestVersion](store_api::manifest::ManifestVersion)) between two checkpoints
    checkpoint_interval: u64,
}
