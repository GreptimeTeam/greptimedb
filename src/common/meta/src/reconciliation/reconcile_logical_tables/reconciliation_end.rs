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
use crate::reconciliation::reconcile_logical_tables::{ReconcileLogicalTablesContext, State};

#[derive(Debug, Serialize, Deserialize)]
pub struct ReconciliationEnd;

#[async_trait::async_trait]
#[typetag::serde]
impl State for ReconciliationEnd {
    async fn next(
        &mut self,
        ctx: &mut ReconcileLogicalTablesContext,
        procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let table_id = ctx.table_id();
        let table_name = ctx.table_name();
        let metrics = ctx.metrics();

        info!(
            "Logical tables reconciliation completed. logical tables: {:?}, physical_table_id: {}, table_name: {}, procedure_id: {}, metrics: {}",
            ctx.persistent_ctx.logical_table_ids,
            table_id,
            table_name,
            procedure_ctx.procedure_id,
            metrics
        );
        Ok((Box::new(ReconciliationEnd), Status::done()))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
