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

use std::any::Any;

use async_trait::async_trait;
use store_api::manifest::{MetaAction, Snapshot};

use crate::error::{Error, Result};
use crate::manifest::ManifestImpl;

#[async_trait]
pub trait Checkpointer: Send + Sync + std::fmt::Debug {
    type Snapshot: Snapshot<Error = Error>;
    type MetaAction: MetaAction<Error = Error>;

    /// Try to do a checkpoint, return the snapshot if successes.
    async fn do_checkpoint(
        &self,
        manifest: &ManifestImpl<Self::Snapshot, Self::MetaAction>,
    ) -> Result<Option<Self::Snapshot>>;

    fn as_any(&self) -> &dyn Any;
}
