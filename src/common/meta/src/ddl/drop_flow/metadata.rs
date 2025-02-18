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

use common_catalog::format_full_flow_name;
use snafu::{ensure, OptionExt};

use crate::ddl::drop_flow::DropFlowProcedure;
use crate::error::{self, Result};

impl DropFlowProcedure {
    /// Fetches the flow info.
    pub(crate) async fn fill_flow_metadata(&mut self) -> Result<()> {
        let catalog_name = &self.data.task.catalog_name;
        let flow_name = &self.data.task.flow_name;
        let flow_info_value = self
            .context
            .flow_metadata_manager
            .flow_info_manager()
            .get(self.data.task.flow_id)
            .await?
            .with_context(|| error::FlowNotFoundSnafu {
                flow_name: format_full_flow_name(catalog_name, flow_name),
            })?;

        let flow_route_values = self
            .context
            .flow_metadata_manager
            .flow_route_manager()
            .routes(self.data.task.flow_id)
            .await?
            .into_iter()
            .map(|(_, value)| value)
            .collect::<Vec<_>>();
        ensure!(
            !flow_route_values.is_empty(),
            error::FlowRouteNotFoundSnafu {
                flow_name: format_full_flow_name(catalog_name, flow_name),
            }
        );
        self.data.flow_info_value = Some(flow_info_value);
        self.data.flow_route_values = flow_route_values;

        Ok(())
    }
}
