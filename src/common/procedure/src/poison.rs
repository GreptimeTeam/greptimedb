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

use async_trait::async_trait;
use strum::{AsRefStr, Display, EnumString};

use crate::error::Result;
use crate::procedure::PoisonKey;
use crate::ProcedureId;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, AsRefStr, EnumString, Display)]
#[strum(serialize_all = "snake_case")]
pub enum ResourceType {
    Table,
}

pub type PoisonManagerRef = Arc<dyn PoisonManager>;

/// Poison manager for procedure.
///
/// This trait is used to manage the state of operations on resources, particularly
/// when an operation encounters an unrecoverable error, potentially leading to
/// metadata inconsistency. In such cases, manual intervention is required to
/// resolve the issue before any further operations can be performed on the resource.
///
/// ## Behavior:
/// - **Insertion**: When an operation begins on a resource, a "poison" key is inserted
///   into the state store to indicate the operation is in progress.
/// - **Deletion**: If the operation completes successfully or
///   other cases can ensure the resource is in a consistent state, the poison key is removed
///   from the state store, indicating the resource is in a consistent state.
/// - **Failure Handling**:
///   - If the operation fails or other cases may lead to metadata inconsistency,
///     the poison key remains in the state store.
///   - The presence of this key indicates that the resource has encountered an
///     unrecoverable error and the metadata may be inconsistent.
///   - New operations on the same resource are rejected until the resource is
///     manually recovered and the poison key is removed.
#[async_trait]
pub trait PoisonManager: Send + Sync {
    async fn set_poison(&self, key: &PoisonKey, procedure_id: ProcedureId) -> Result<()>;

    async fn delete_poison(&self, key: &PoisonKey, procedure_id: ProcedureId) -> Result<()>;
}
