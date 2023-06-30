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

use async_trait::async_trait;
use common_error::ext::ErrorExt;

use crate::manifest::ManifestVersion;

#[async_trait]
pub trait LogIterator: Send + Sync {
    type Error: ErrorExt + Send + Sync;

    async fn next_log(&mut self) -> Result<Option<(ManifestVersion, Vec<u8>)>, Self::Error>;
}

#[async_trait]
pub trait ManifestLogStorage {
    type Error: ErrorExt + Send + Sync;
    type Iter: LogIterator<Error = Self::Error>;

    /// Scan the logs in [start, end)
    async fn scan(
        &self,
        start: ManifestVersion,
        end: ManifestVersion,
    ) -> Result<Self::Iter, Self::Error>;

    /// Delete logs and checkpoints which version is less than specified `end`.
    /// Keep the last checkpoint and remaining logs if `keep_last_checkpoint` is true.
    ///
    /// Returns the delete logs number.
    async fn delete_until(
        &self,
        end: ManifestVersion,
        keep_last_checkpoint: bool,
    ) -> Result<usize, Self::Error>;

    /// Delete all logs and checkpoints, and remove the manifest directory.
    /// The delta file corresponding to the `remove_action_version` will be deleted along with the manifest directory at the end.
    async fn delete_all(&self, remove_action_version: ManifestVersion) -> Result<(), Self::Error>;

    /// Save a log
    async fn save(&self, version: ManifestVersion, bytes: &[u8]) -> Result<(), Self::Error>;

    /// Delete logs in [start, end) and ignore checkpoints.
    async fn delete(&self, start: ManifestVersion, end: ManifestVersion)
        -> Result<(), Self::Error>;

    /// Save a checkpoint.
    async fn save_checkpoint(
        &self,
        version: ManifestVersion,
        bytes: &[u8],
    ) -> Result<(), Self::Error>;

    /// Load the latest checkpoint
    async fn load_last_checkpoint(&self)
        -> Result<Option<(ManifestVersion, Vec<u8>)>, Self::Error>;
    /// Delete the checkpoint by version
    async fn delete_checkpoint(&self, version: ManifestVersion) -> Result<(), Self::Error>;

    /// Load the checkpoint by version
    async fn load_checkpoint(
        &self,
        version: ManifestVersion,
    ) -> Result<Option<(ManifestVersion, Vec<u8>)>, Self::Error>;
}
