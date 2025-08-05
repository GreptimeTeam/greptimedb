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
use common_telemetry::info;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::reconciliation::reconcile_catalog::{ReconcileCatalogContext, State};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ReconcileCatalogEnd;

#[async_trait::async_trait]
#[typetag::serde]
impl State for ReconcileCatalogEnd {
    async fn next(
        &mut self,
        ctx: &mut ReconcileCatalogContext,
        procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        info!(
            "Catalog reconciliation completed. catalog: {}, procedure_id: {}, metrics: {}, elapsed: {:?}",
            ctx.persistent_ctx.catalog,
            procedure_ctx.procedure_id,
            ctx.volatile_ctx.metrics,
            ctx.volatile_ctx.start_time.elapsed()
        );
        Ok((Box::new(ReconcileCatalogEnd), Status::done()))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
