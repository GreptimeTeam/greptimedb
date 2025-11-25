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

use async_trait::async_trait;
use common_meta::error::{self, Result};
use common_meta::leadership_notifier::LeadershipChangeListener;
use common_procedure::ProcedureManagerRef;
use snafu::ResultExt;

pub mod region_migration;
pub mod repartition;
#[cfg(any(test, feature = "testing"))]
pub mod test_util;
#[cfg(test)]
mod tests;
pub mod utils;
pub mod wal_prune;

#[derive(Clone)]
pub struct ProcedureManagerListenerAdapter(pub ProcedureManagerRef);

#[async_trait]
impl LeadershipChangeListener for ProcedureManagerListenerAdapter {
    fn name(&self) -> &str {
        "ProcedureManager"
    }

    async fn on_leader_start(&self) -> Result<()> {
        self.0
            .start()
            .await
            .context(error::StartProcedureManagerSnafu)
    }

    async fn on_leader_stop(&self) -> Result<()> {
        self.0
            .stop()
            .await
            .context(error::StopProcedureManagerSnafu)
    }
}
