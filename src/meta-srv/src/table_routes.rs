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

use api::v1::meta::TableRouteValue;
use common_meta::key::table_info::TableInfoValue;
use common_meta::key::TableMetadataManagerRef;
use common_meta::rpc::router::{Table, TableRoute};
use snafu::{OptionExt, ResultExt};
use table::metadata::TableId;

use crate::error::{self, Result, TableMetadataManagerSnafu, TableRouteNotFoundSnafu};
use crate::metasrv::Context;

pub(crate) async fn fetch_table(
    table_metadata_manager: &TableMetadataManagerRef,
    table_id: TableId,
) -> Result<Option<(TableInfoValue, TableRouteValue)>> {
    let (table_info, table_route) = table_metadata_manager
        .get_full_table_info(table_id)
        .await
        .context(TableMetadataManagerSnafu)?;

    if let Some(table_info) = table_info {
        let table_route = table_route
            .context(TableRouteNotFoundSnafu { table_id })?
            .into_inner();

        let table = Table {
            id: table_id as u64,
            table_name: table_info.table_name(),
            table_schema: vec![],
        };
        let table_route = TableRoute::new(table, table_route.region_routes);
        let table_route_value = table_route
            .try_into()
            .context(error::TableRouteConversionSnafu)?;

        Ok(Some((table_info.into_inner(), table_route_value)))
    } else {
        Ok(None)
    }
}

pub(crate) async fn fetch_tables(
    ctx: &Context,
    table_ids: Vec<TableId>,
) -> Result<Vec<(TableInfoValue, TableRouteValue)>> {
    let table_metadata_manager = &ctx.table_metadata_manager;

    let mut tables = vec![];
    // Maybe we can optimize the for loop in the future, but in general,
    // there won't be many keys, in fact, there is usually just one.
    for table_id in table_ids {
        if let Some(x) = fetch_table(table_metadata_manager, table_id).await? {
            tables.push(x);
        }
    }

    Ok(tables)
}

#[cfg(test)]
pub(crate) mod tests {
    use chrono::DateTime;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, MITO_ENGINE};
    use common_meta::key::TableMetadataManagerRef;
    use common_meta::peer::Peer;
    use common_meta::rpc::router::{Region, RegionRoute};
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, RawSchema};
    use table::metadata::{RawTableInfo, RawTableMeta, TableIdent, TableType};
    use table::requests::TableOptions;

    pub(crate) async fn prepare_table_region_and_info_value(
        table_metadata_manager: &TableMetadataManagerRef,
        table: &str,
    ) {
        let table_info = RawTableInfo {
            ident: TableIdent::new(1),
            name: table.to_string(),
            desc: None,
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            meta: RawTableMeta {
                schema: RawSchema::new(vec![ColumnSchema::new(
                    "a",
                    ConcreteDataType::string_datatype(),
                    true,
                )]),
                primary_key_indices: vec![],
                value_indices: vec![],
                engine: MITO_ENGINE.to_string(),
                next_column_id: 1,
                region_numbers: vec![1, 2, 3, 4],
                options: TableOptions::default(),
                created_on: DateTime::default(),
                partition_key_indices: vec![],
            },
            table_type: TableType::Base,
        };

        let region_route_factory = |region_id: u64, peer: u64| RegionRoute {
            region: Region {
                id: region_id.into(),
                ..Default::default()
            },
            leader_peer: Some(Peer {
                id: peer,
                addr: String::new(),
            }),
            follower_peers: vec![],
        };

        // Region distribution:
        // Datanode => Regions
        // 1 => 1, 2
        // 2 => 3
        // 3 => 4
        let region_routes = vec![
            region_route_factory(1, 1),
            region_route_factory(2, 1),
            region_route_factory(3, 2),
            region_route_factory(4, 3),
        ];
        table_metadata_manager
            .create_table_metadata(table_info, region_routes)
            .await
            .unwrap();
    }
}
