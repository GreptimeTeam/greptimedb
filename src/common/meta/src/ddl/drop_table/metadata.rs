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

use common_catalog::format_full_table_name;
use snafu::OptionExt;
use store_api::metric_engine_consts::METRIC_ENGINE_NAME;

use crate::ddl::drop_table::DropTableProcedure;
use crate::error::{self, Result};

impl DropTableProcedure {
    /// Fetches the table info and physical table route.
    pub(crate) async fn fill_table_metadata(&mut self) -> Result<()> {
        let task = &self.data.task;
        let (physical_table_id, physical_table_route_value) = self
            .context
            .table_metadata_manager
            .table_route_manager()
            .get_physical_table_route(task.table_id)
            .await?;

        self.data.physical_region_routes = physical_table_route_value.region_routes;
        self.data.physical_table_id = Some(physical_table_id);

        if physical_table_id == self.data.table_id() {
            let table_info_value = self
                .context
                .table_metadata_manager
                .table_info_manager()
                .get(task.table_id)
                .await?
                .with_context(|| error::TableInfoNotFoundSnafu {
                    table: format_full_table_name(&task.catalog, &task.schema, &task.table),
                })?
                .into_inner();

            let engine = table_info_value.table_info.meta.engine;
            // rollback only if dropping the metric physical table fails
            self.data.allow_rollback = engine.as_str() == METRIC_ENGINE_NAME
        }

        Ok(())
    }
}
