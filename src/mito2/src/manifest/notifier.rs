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

use store_api::manifest::ManifestVersion;
use strum::AsRefStr;

use crate::error::Result;
use crate::region::MitoRegion;

pub(crate) type ManifestChangeNotifierRef = Arc<dyn ManifestChangeNotifier>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsRefStr)]
pub(crate) enum ManifestChangeEvent {
    Compcation,
    Flush,
    RegionEdit,
    RegionChange,
    Truncate,
}

/// Manifest change notifier used to notify the manifest change to the downstream.
#[async_trait::async_trait]
pub(crate) trait ManifestChangeNotifier: Send + Sync {
    async fn notify(&self, region: &MitoRegion, manifest_version: ManifestVersion) -> Result<()>;
}

/// Noop manifest change notifier.
pub(crate) struct NoopManifestChangeNotifier;

#[async_trait::async_trait]
impl ManifestChangeNotifier for NoopManifestChangeNotifier {
    async fn notify(&self, _region: &MitoRegion, _manifest_version: ManifestVersion) -> Result<()> {
        Ok(())
    }
}
