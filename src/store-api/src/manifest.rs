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

//! metadata service
pub mod action;
mod storage;

use async_trait::async_trait;
use common_error::ext::ErrorExt;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::manifest::action::{ProtocolAction, ProtocolVersion};
pub use crate::manifest::storage::*;

pub type ManifestVersion = u64;
pub const MIN_VERSION: u64 = 0;
pub const MAX_VERSION: u64 = u64::MAX;

/// The action to alter metadata
pub trait MetaAction: Serialize + DeserializeOwned + Send + Sync + Clone + std::fmt::Debug {
    type Error: ErrorExt + Send + Sync;

    /// Set a protocol action into meta action
    fn set_protocol(&mut self, action: ProtocolAction);

    /// Set previous valid manifest version.
    fn set_prev_version(&mut self, version: ManifestVersion);

    /// Encode this action into a byte vector
    fn encode(&self) -> Result<Vec<u8>, Self::Error>;

    /// Decode self from byte slice with reader protocol version,
    /// return error when reader version is not supported.
    fn decode(
        bs: &[u8],
        reader_version: ProtocolVersion,
    ) -> Result<(Self, Option<ProtocolAction>), Self::Error>;
}
/// The checkpoint by checkpoint
pub trait Checkpoint: Send + Sync + Clone + std::fmt::Debug {
    type Error: ErrorExt + Send + Sync;

    /// Set a protocol action into checkpoint
    fn set_protocol(&mut self, action: ProtocolAction);

    /// The last compacted action's version of checkpoint
    fn last_version(&self) -> ManifestVersion;

    /// Encode this checkpoint into a byte vector
    fn encode(&self) -> Result<Vec<u8>, Self::Error>;

    /// Decode self from byte slice with reader protocol version,
    /// return error when reader version is not supported.
    fn decode(bs: &[u8], reader_version: ProtocolVersion) -> Result<Self, Self::Error>;
}

#[async_trait]
pub trait MetaActionIterator {
    type MetaAction: MetaAction;
    type Error: ErrorExt + Send + Sync;

    async fn next_action(
        &mut self,
    ) -> Result<Option<(ManifestVersion, Self::MetaAction)>, Self::Error>;
}

/// Manifest service
#[async_trait]
pub trait Manifest: Send + Sync + Clone + 'static {
    type Error: ErrorExt + Send + Sync;
    type MetaAction: MetaAction;
    type MetaActionIterator: MetaActionIterator<Error = Self::Error, MetaAction = Self::MetaAction>;
    type Checkpoint: Checkpoint;

    /// Update metadata by the action
    async fn update(&self, action: Self::MetaAction) -> Result<ManifestVersion, Self::Error>;

    /// Scan actions which version in range [start, end)
    async fn scan(
        &self,
        start: ManifestVersion,
        end: ManifestVersion,
    ) -> Result<Self::MetaActionIterator, Self::Error>;

    /// Do a checkpoint, it will create a checkpoint and compact actions.
    async fn do_checkpoint(&self) -> Result<Option<Self::Checkpoint>, Self::Error>;

    /// Returns the last success checkpoint
    async fn last_checkpoint(&self) -> Result<Option<Self::Checkpoint>, Self::Error>;

    /// Returns the last(or latest) manifest version.
    fn last_version(&self) -> ManifestVersion;

    /// Start the service
    async fn start(&self) -> Result<(), Self::Error> {
        Ok(())
    }
    /// Stop the service
    async fn stop(&self) -> Result<(), Self::Error> {
        Ok(())
    }
}
