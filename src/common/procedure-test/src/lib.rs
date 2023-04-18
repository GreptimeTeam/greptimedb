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

//! Test utilities for procedures.

use std::sync::Arc;

use async_trait::async_trait;
use common_procedure::{
    BoxedProcedure, Context, ContextProvider, ProcedureId, ProcedureState, Result, Status,
};

/// A Mock [ContextProvider] that always return [ProcedureState::Done].
struct MockContextProvider {}

#[async_trait]
impl ContextProvider for MockContextProvider {
    async fn procedure_state(&self, _procedure_id: ProcedureId) -> Result<Option<ProcedureState>> {
        Ok(Some(ProcedureState::Done))
    }
}

/// Executes a procedure until it returns [Status::Done].
///
/// # Panics
/// Panics if the `procedure` has subprocedure to execute.
pub async fn execute_procedure_until_done(procedure: &mut BoxedProcedure) {
    let ctx = Context {
        procedure_id: ProcedureId::random(),
        provider: Arc::new(MockContextProvider {}),
    };

    loop {
        match procedure.execute(&ctx).await.unwrap() {
            Status::Executing { .. } => (),
            Status::Suspended { subprocedures, .. } => assert!(
                subprocedures.is_empty(),
                "Executing subprocedure is unsupported"
            ),
            Status::Done => break,
        }
    }
}
