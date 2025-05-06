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

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use common_procedure::local::{acquire_dynamic_key_lock, DynamicKeyLockGuard};
use common_procedure::rwlock::KeyRwLock;
use common_procedure::store::poison_store::PoisonStore;
use common_procedure::test_util::InMemoryPoisonStore;
use common_procedure::{
    Context, ContextProvider, Output, PoisonKey, Procedure, ProcedureId, ProcedureState,
    ProcedureWithId, Result, Status, StringKey,
};

/// A Mock [ContextProvider].
#[derive(Default)]
pub struct MockContextProvider {
    states: HashMap<ProcedureId, ProcedureState>,
    poison_manager: InMemoryPoisonStore,
    dynamic_key_lock: Arc<KeyRwLock<String>>,
}

impl MockContextProvider {
    /// Returns a new provider.
    pub fn new(states: HashMap<ProcedureId, ProcedureState>) -> MockContextProvider {
        MockContextProvider {
            states,
            poison_manager: InMemoryPoisonStore::default(),
            dynamic_key_lock: Arc::new(KeyRwLock::new()),
        }
    }

    /// Returns a reference to the poison manager.
    pub fn poison_manager(&self) -> &InMemoryPoisonStore {
        &self.poison_manager
    }
}

#[async_trait]
impl ContextProvider for MockContextProvider {
    async fn procedure_state(&self, procedure_id: ProcedureId) -> Result<Option<ProcedureState>> {
        Ok(self.states.get(&procedure_id).cloned())
    }

    async fn try_put_poison(&self, key: &PoisonKey, procedure_id: ProcedureId) -> Result<()> {
        self.poison_manager
            .try_put_poison(key.to_string(), procedure_id.to_string())
            .await
    }

    async fn acquire_lock(&self, key: &StringKey) -> DynamicKeyLockGuard {
        acquire_dynamic_key_lock(&self.dynamic_key_lock, key).await
    }
}

/// Executes a procedure until it returns [Status::Done].
///
/// # Panics
/// Panics if the `procedure` has subprocedure to execute.
pub async fn execute_procedure_until_done(procedure: &mut dyn Procedure) -> Option<Output> {
    let ctx = Context {
        procedure_id: ProcedureId::random(),
        provider: Arc::new(MockContextProvider::default()),
    };

    loop {
        match procedure.execute(&ctx).await.unwrap() {
            Status::Executing { .. } => (),
            Status::Suspended { subprocedures, .. } => assert!(
                subprocedures.is_empty(),
                "Executing subprocedure is unsupported"
            ),
            Status::Done { output } => return output,
            Status::Poisoned { .. } => return None,
        }
    }
}

/// Executes a procedure once.
///
/// Returns whether the procedure is done.
pub async fn execute_procedure_once(
    procedure_id: ProcedureId,
    provider: MockContextProvider,
    procedure: &mut dyn Procedure,
) -> bool {
    let ctx = Context {
        procedure_id,
        provider: Arc::new(provider),
    };

    match procedure.execute(&ctx).await.unwrap() {
        Status::Executing { .. } => false,
        Status::Suspended { subprocedures, .. } => {
            assert!(
                subprocedures.is_empty(),
                "Executing subprocedure is unsupported"
            );
            false
        }
        Status::Done { .. } => true,
        Status::Poisoned { .. } => false,
    }
}

/// Executes a procedure until it returns [Status::Suspended] or [Status::Done].
///
/// Returns `Some` if it returns [Status::Suspended] or `None` if it returns [Status::Done].
pub async fn execute_until_suspended_or_done(
    procedure_id: ProcedureId,
    provider: MockContextProvider,
    procedure: &mut dyn Procedure,
) -> Option<Vec<ProcedureWithId>> {
    let ctx = Context {
        procedure_id,
        provider: Arc::new(provider),
    };

    loop {
        match procedure.execute(&ctx).await.unwrap() {
            Status::Executing { .. } => (),
            Status::Suspended { subprocedures, .. } => return Some(subprocedures),
            Status::Done { .. } => break,
            Status::Poisoned { .. } => unreachable!(),
        }
    }

    None
}

pub fn new_test_procedure_context() -> Context {
    Context {
        procedure_id: ProcedureId::random(),
        provider: Arc::new(MockContextProvider::default()),
    }
}

pub async fn execute_procedure_until<P: Procedure>(procedure: &mut P, until: impl Fn(&P) -> bool) {
    let mut reached = false;
    let context = new_test_procedure_context();
    while !matches!(
        procedure.execute(&context).await.unwrap(),
        Status::Done { .. }
    ) {
        if until(procedure) {
            reached = true;
            break;
        }
    }
    assert!(
        reached,
        "procedure '{}' did not reach the expected state",
        procedure.type_name()
    );
}
