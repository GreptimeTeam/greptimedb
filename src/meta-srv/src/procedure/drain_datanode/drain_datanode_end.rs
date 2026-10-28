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
use crate::procedure::drain_datanode::{Context, State};

/// Finishes the procedure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DrainDatanodeEnd;

#[async_trait::async_trait]
#[typetag::serde]
impl State for DrainDatanodeEnd {
    async fn next(
        &mut self,
        _ctx: &mut Context,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        Ok((Box::new(DrainDatanodeEnd), Status::done()))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::procedure::drain_datanode::test_util::{TestEnv, new_persistent_context};
    use crate::procedure::region_migration::test_util::TestingEnv;

    #[tokio::test]
    async fn test_drain_datanode_end_transitions_to_done() {
        let env = TestEnv::new().await;
        let mut ctx = env.context(new_persistent_context(Some(2)));
        let procedure_ctx = TestingEnv::procedure_context();

        let (next, status) = DrainDatanodeEnd
            .next(&mut ctx, &procedure_ctx)
            .await
            .unwrap();

        assert!(next.as_any().downcast_ref::<DrainDatanodeEnd>().is_some());
        assert!(matches!(status, Status::Done { output: None }));
    }

    #[tokio::test]
    async fn test_drain_datanode_end_finishes() {
        let env = TestEnv::new().await;
        let mut ctx = env.context(new_persistent_context(Some(2)));
        let procedure_ctx = TestingEnv::procedure_context();

        let (next, status) = DrainDatanodeEnd
            .next(&mut ctx, &procedure_ctx)
            .await
            .unwrap();

        assert!(next.as_any().downcast_ref::<DrainDatanodeEnd>().is_some());
        assert!(matches!(status, Status::Done { .. }));
    }
}
