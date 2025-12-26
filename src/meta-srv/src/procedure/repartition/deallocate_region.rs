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

use crate::error::Result;
use crate::procedure::repartition::repartition_end::RepartitionEnd;
use crate::procedure::repartition::{Context, State};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeallocateRegion;

#[async_trait::async_trait]
#[typetag::serde]
impl State for DeallocateRegion {
    async fn next(
        &mut self,
        ctx: &mut Context,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let region_to_deallocate = ctx
            .persistent_ctx
            .plans
            .iter()
            .map(|p| p.pending_deallocate_region_ids.len())
            .sum::<usize>();
        if region_to_deallocate == 0 {
            return Ok((Box::new(RepartitionEnd), Status::done()));
        }

        // TODO(weny): deallocate regions.
        todo!()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
