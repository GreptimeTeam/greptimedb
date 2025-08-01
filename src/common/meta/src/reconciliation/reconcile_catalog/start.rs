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

use common_procedure::{Context as ProcedureContext, Status};
use serde::{Deserialize, Serialize};
use snafu::ensure;

use crate::error::{self, Result};
use crate::key::catalog_name::CatalogNameKey;
use crate::reconciliation::reconcile_catalog::reconcile_databases::ReconcileDatabases;
use crate::reconciliation::reconcile_catalog::{ReconcileCatalogContext, State};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ReconcileCatalogStart;

#[async_trait::async_trait]
#[typetag::serde]
impl State for ReconcileCatalogStart {
    async fn next(
        &mut self,
        ctx: &mut ReconcileCatalogContext,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let exists = ctx
            .table_metadata_manager
            .catalog_manager()
            .exists(CatalogNameKey {
                catalog: &ctx.persistent_ctx.catalog,
            })
            .await?;

        ensure!(
            exists,
            error::CatalogNotFoundSnafu {
                catalog: &ctx.persistent_ctx.catalog
            },
        );

        Ok((Box::new(ReconcileDatabases), Status::executing(true)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
