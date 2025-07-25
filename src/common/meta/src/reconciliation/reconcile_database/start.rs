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
use crate::key::schema_name::SchemaNameKey;
use crate::reconciliation::reconcile_database::reconcile_tables::ReconcileTables;
use crate::reconciliation::reconcile_database::{ReconcileDatabaseContext, State};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ReconcileDatabaseStart;

#[async_trait::async_trait]
#[typetag::serde]
impl State for ReconcileDatabaseStart {
    async fn next(
        &mut self,
        ctx: &mut ReconcileDatabaseContext,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let exists = ctx
            .table_metadata_manager
            .schema_manager()
            .exists(SchemaNameKey {
                catalog: &ctx.persistent_ctx.catalog,
                schema: &ctx.persistent_ctx.schema,
            })
            .await?;

        ensure!(
            exists,
            error::SchemaNotFoundSnafu {
                table_schema: &ctx.persistent_ctx.schema,
            },
        );

        Ok((Box::new(ReconcileTables), Status::executing(true)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
