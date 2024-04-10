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
use snafu::{ensure, OptionExt};

use crate::ddl::drop_table::DropTableProcedure;
use crate::error::{self, Result};
use crate::key::datanode_table::DatanodeTableKey;
use crate::rpc::router::region_distribution;

impl DropTableProcedure {
    /// Fetches the table info and table route.
    pub(crate) async fn fill_table_metadata(&mut self) -> Result<()> {
        let task = &self.data.task;
        let table_info_value = self
            .context
            .table_metadata_manager
            .table_info_manager()
            .get(task.table_id)
            .await?
            .with_context(|| error::TableInfoNotFoundSnafu {
                table: format_full_table_name(&task.catalog, &task.schema, &task.table),
            })?;
        let (_, physical_table_route_value) = self
            .context
            .table_metadata_manager
            .table_route_manager()
            .get_physical_table_route(task.table_id)
            .await?;
        let table_route_value = self
            .context
            .table_metadata_manager
            .table_route_manager()
            .table_route_storage()
            .get_raw(task.table_id)
            .await?
            .context(error::TableRouteNotFoundSnafu {
                table_id: task.table_id,
            })?;

        let distributions = region_distribution(&physical_table_route_value.region_routes);
        let datanode_table_keys = distributions
            .into_keys()
            .map(|datanode_id| DatanodeTableKey::new(datanode_id, task.table_id))
            .collect::<Vec<_>>();
        let datanode_table_values = self
            .context
            .table_metadata_manager
            .datanode_table_manager()
            .batch_get(&datanode_table_keys)
            .await?;
        ensure!(
            datanode_table_keys.len() == datanode_table_values.len(),
            error::UnexpectedSnafu {
                err_msg: format!(
                    "Dropping table: {}, num({}) of datanode table values should equal num({}) of keys",
                    task.table_id,
                    datanode_table_values.len(),
                    datanode_table_keys.len()
                )
            }
        );

        self.data.region_routes = physical_table_route_value.region_routes;
        self.data.table_info_value = Some(table_info_value);
        self.data.table_route_value = Some(table_route_value);
        self.data.datanode_table_value = datanode_table_keys
            .into_iter()
            .zip(datanode_table_values)
            .collect();

        Ok(())
    }
}
