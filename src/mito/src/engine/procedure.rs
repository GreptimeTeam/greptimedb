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

mod create;

use std::sync::Arc;

use common_procedure::ProcedureManager;
pub(crate) use create::CreateMitoTable;
use store_api::storage::StorageEngine;

use crate::engine::MitoEngineInner;

/// Register all procedure loaders to the procedure manager.
///
/// # Panics
/// Panics on error.
pub(crate) fn register_procedure_loaders<S: StorageEngine>(
    engine_inner: Arc<MitoEngineInner<S>>,
    procedure_manager: &dyn ProcedureManager,
) {
    // The procedure names are expected to be unique, so we just panic on error.
    CreateMitoTable::register_loader(engine_inner, procedure_manager);
}
