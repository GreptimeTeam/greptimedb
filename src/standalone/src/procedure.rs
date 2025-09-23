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

use std::sync::Arc;

use common_meta::key::runtime_switch::RuntimeSwitchManager;
use common_meta::kv_backend::KvBackendRef;
use common_meta::state_store::KvStateStore;
use common_procedure::ProcedureManagerRef;
use common_procedure::local::{LocalManager, ManagerConfig};
use common_procedure::options::ProcedureConfig;

/// Builds the procedure manager.
pub fn build_procedure_manager(
    kv_backend: KvBackendRef,
    procedure_config: ProcedureConfig,
) -> ProcedureManagerRef {
    let kv_state_store = Arc::new(KvStateStore::new(kv_backend.clone()));

    let manager_config = ManagerConfig {
        max_retry_times: procedure_config.max_retry_times,
        retry_delay: procedure_config.retry_delay,
        max_running_procedures: procedure_config.max_running_procedures,
        ..Default::default()
    };
    let runtime_switch_manager = Arc::new(RuntimeSwitchManager::new(kv_backend));
    Arc::new(LocalManager::new(
        manager_config,
        kv_state_store.clone(),
        kv_state_store,
        Some(runtime_switch_manager),
        None,
    ))
}
