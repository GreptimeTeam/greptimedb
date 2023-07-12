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

use std::sync::Arc;

use common_datasource::compression::CompressionType;
use object_store::ObjectStore;
use store_api::manifest::action::{ProtocolAction, ProtocolVersion};
use store_api::manifest::ManifestVersion;

use crate::error::Result;
use crate::manifest::action::{MetaActionIteratorImpl, RegionCheckpoint, RegionMetaActionList};
use crate::manifest::region::RegionManifestCheckpointer;
use crate::manifest::storage::ManifestObjectStore;

// rewrite note:
// trait Checkpoint -> struct RegionCheckpoint
// trait MetaAction -> struct RegionMetaActionList
// trait MetaActionIterator -> struct MetaActionIteratorImpl
// struct ManifestImpl -> RegionManifest
#[derive(Clone, Debug)]
pub struct RegionManifest {}

impl RegionManifest {
    // from impl ManifestImpl

    pub fn new() -> Self {
        todo!()
    }

    pub fn create(
        _manifest_dir: &str,
        _object_store: ObjectStore,
        _compress_type: CompressionType,
    ) -> Self {
        todo!()
    }

    pub(crate) fn checkpointer(&self) -> Option<RegionManifestCheckpointer> {
        todo!()
    }

    pub(crate) fn set_last_checkpoint_version(&self, _version: ManifestVersion) {
        todo!()
    }

    /// Update inner state.
    pub fn update_state(&self, _version: ManifestVersion, _protocol: Option<ProtocolAction>) {
        todo!()
    }

    pub(crate) async fn save_checkpoint(&self, checkpoint: &RegionCheckpoint) -> Result<()> {
        todo!()
    }

    pub(crate) async fn may_do_checkpoint(&self, version: ManifestVersion) -> Result<()> {
        todo!()
    }

    pub(crate) fn manifest_store(&self) -> &Arc<ManifestObjectStore> {
        todo!()
    }

    // from Manifest

    pub async fn update(&self, action_list: RegionMetaActionList) -> Result<ManifestVersion> {
        todo!()
    }

    pub async fn scan(
        &self,
        start: ManifestVersion,
        end: ManifestVersion,
    ) -> Result<MetaActionIteratorImpl> {
        todo!()
    }

    pub async fn do_checkpoint(&self) -> Result<Option<RegionCheckpoint>> {
        todo!()
    }

    pub async fn last_checkpoint(&self) -> Result<Option<RegionCheckpoint>> {
        todo!()
    }

    pub async fn start(&self) -> Result<()> {
        todo!()
    }

    pub async fn stop(&self) -> Result<()> {
        todo!()
    }

    // from Checkpoint

    /// Set a protocol action into checkpoint
    pub fn set_protocol(&mut self, _action: ProtocolAction) {
        todo!()
    }

    /// The last compacted action's version of checkpoint
    pub fn last_version(&self) -> ManifestVersion {
        todo!()
    }

    /// Encode this checkpoint into a byte vector
    pub fn encode(&self) -> Result<Vec<u8>> {
        todo!()
    }

    pub fn decode(_bytes: &[u8], _reader_version: ProtocolVersion) -> Result<Self> {
        todo!()
    }
}
