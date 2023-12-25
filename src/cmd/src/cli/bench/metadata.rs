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

use std::time::Instant;

use common_meta::key::table_route::TableRouteValue;
use common_meta::key::TableMetadataManagerRef;
use common_meta::table_name::TableName;

use crate::cli::bench::{
    bench_self_recorded, create_region_routes, create_region_wal_options, create_table_info,
};

pub struct TableMetadataBencher {
    table_metadata_manager: TableMetadataManagerRef,
    count: u32,
}

impl TableMetadataBencher {
    pub fn new(table_metadata_manager: TableMetadataManagerRef, count: u32) -> Self {
        Self {
            table_metadata_manager,
            count,
        }
    }

    pub async fn bench_create(&self) {
        let desc = format!(
            "TableMetadataBencher: creating {} table metadata",
            self.count
        );
        bench_self_recorded(
            &desc,
            |i| async move {
                let table_name = format!("bench_table_name_{}", i);
                let table_name = TableName::new("bench_catalog", "bench_schema", table_name);
                let table_info = create_table_info(i, table_name);

                let regions: Vec<_> = (0..64).collect();
                let region_routes = create_region_routes(regions.clone());
                let region_wal_options = create_region_wal_options(regions);

                let start = Instant::now();

                self.table_metadata_manager
                    .create_table_metadata(
                        table_info,
                        TableRouteValue::physical(region_routes),
                        region_wal_options,
                    )
                    .await
                    .unwrap();

                start.elapsed()
            },
            self.count,
        )
        .await;
    }

    pub async fn bench_get(&self) {
        let desc = format!(
            "TableMetadataBencher: getting {} table info and region routes",
            self.count
        );

        bench_self_recorded(
            &desc,
            |i| async move {
                let start = Instant::now();
                self.table_metadata_manager
                    .get_full_table_info(i)
                    .await
                    .unwrap();

                start.elapsed()
            },
            self.count,
        )
        .await;
    }

    pub async fn bench_delete(&self) {
        let desc = format!(
            "TableMetadataBencher: deleting {} table metadata",
            self.count
        );

        bench_self_recorded(
            &desc,
            |i| async move {
                let (table_info, table_route) = self
                    .table_metadata_manager
                    .get_full_table_info(i)
                    .await
                    .unwrap();
                let start = Instant::now();
                let _ = self
                    .table_metadata_manager
                    .delete_table_metadata(&table_info.unwrap(), &table_route.unwrap())
                    .await;
                start.elapsed()
            },
            self.count,
        )
        .await;
    }

    pub async fn bench_rename(&self) {
        let desc = format!("TableMetadataBencher: renaming {} table", self.count);

        bench_self_recorded(
            &desc,
            |i| async move {
                let (table_info, _) = self
                    .table_metadata_manager
                    .get_full_table_info(i)
                    .await
                    .unwrap();

                let new_table_name = format!("renamed_{}", i);

                let start = Instant::now();
                let _ = self
                    .table_metadata_manager
                    .rename_table(table_info.unwrap(), new_table_name)
                    .await;

                start.elapsed()
            },
            self.count,
        )
        .await;
    }
}
