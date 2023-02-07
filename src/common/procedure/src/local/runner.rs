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

use common_telemetry::logging;

use crate::local::{ManagerContext, ProcedureMetaRef};
use crate::store::ProcedureStore;
use crate::BoxedProcedure;

pub(crate) struct Runner {
    pub(crate) meta: ProcedureMetaRef,
    pub(crate) procedure: BoxedProcedure,
    pub(crate) manager_ctx: Arc<ManagerContext>,
    pub(crate) step: u32,
    pub(crate) store: ProcedureStore,
}

impl Runner {
    /// Run the procedure.
    pub(crate) async fn run(self) {
        logging::info!(
            "Runner {}-{} starts",
            self.procedure.type_name(),
            self.meta.id
        );
        // We use the lock key in ProcedureMeta as it considers locks inherited from
        // its parent.
        let lock_key = self.meta.lock_key.clone();

        // TODO(yingwen): Support multiple lock keys.
        // Acquire lock if necessary.
        if let Some(key) = &lock_key {
            self.manager_ctx
                .lock_map
                .acquire_lock(key.key(), self.meta.clone())
                .await;
        }

        // TODO(yingwen): Execute the procedure.

        if let Some(key) = &lock_key {
            self.manager_ctx
                .lock_map
                .release_lock(key.key(), self.meta.id);
        }
        // We can't remove the metadata of the procedure now as users and its parent might
        // need to query its state.
        // TODO(yingwen): 1. Add TTL to the metadata; 2. Only keep state in the procedure store
        // so we don't need to always store the metadata in memory after the procedure is done.

        logging::info!(
            "Runner {}-{} exits",
            self.procedure.type_name(),
            self.meta.id
        );
    }
}
