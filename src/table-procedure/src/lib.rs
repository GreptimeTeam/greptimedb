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

//! Procedures for table operations.

mod alter;
mod create;
mod drop;
pub mod error;
mod truncate;

pub use alter::AlterTableProcedure;
use catalog::CatalogManagerRef;
use common_procedure::ProcedureManager;
pub use create::CreateTableProcedure;
pub use drop::DropTableProcedure;
use table::engine::{TableEngineProcedureRef, TableEngineRef};
pub use truncate::TruncateTableProcedure;

/// Register all procedure loaders to the procedure manager.
///
/// # Panics
/// Panics on error.
#[allow(clippy::items_after_test_module)]
pub fn register_procedure_loaders(
    catalog_manager: CatalogManagerRef,
    engine_procedure: TableEngineProcedureRef,
    table_engine: TableEngineRef,
    procedure_manager: &dyn ProcedureManager,
) {
    CreateTableProcedure::register_loader(
        catalog_manager.clone(),
        engine_procedure.clone(),
        table_engine,
        procedure_manager,
    );
    AlterTableProcedure::register_loader(
        catalog_manager.clone(),
        engine_procedure.clone(),
        procedure_manager,
    );
    DropTableProcedure::register_loader(
        catalog_manager.clone(),
        engine_procedure.clone(),
        procedure_manager,
    );
    TruncateTableProcedure::register_loader(catalog_manager, engine_procedure, procedure_manager)
}

#[cfg(test)]
mod test_util;
