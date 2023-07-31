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

use common_meta::key::table_info::TableInfoManager;
use common_meta::table_name::TableName;

use super::{bench, bench_self_recorded, create_table_info};

pub struct TableInfoBencher<'a> {
    table_info_manager: &'a TableInfoManager,
    count: u32,
}

impl<'a> TableInfoBencher<'a> {
    pub fn new(table_info_manager: &'a TableInfoManager, count: u32) -> Self {
        Self {
            table_info_manager,
            count,
        }
    }

    pub async fn start(&self) {
        self.bench_create().await;
        self.bench_get().await;
        self.bench_compare_and_put().await;
        self.bench_remove().await;
    }

    async fn bench_create(&self) {
        let desc = format!("TableInfoBencher: create {} table infos", self.count);
        bench(
            &desc,
            |i| async move {
                let table_name = format!("bench_table_name_{}", i);
                let table_name = TableName::new("bench_catalog", "bench_schema", table_name);
                let table_info = create_table_info(i, table_name);
                self.table_info_manager
                    .create(i, &table_info)
                    .await
                    .unwrap();
            },
            self.count,
        )
        .await;
    }

    async fn bench_get(&self) {
        let desc = format!("TableInfoBencher: get {} table infos", self.count);
        bench(
            &desc,
            |i| async move {
                assert!(self.table_info_manager.get(i).await.unwrap().is_some());
            },
            self.count,
        )
        .await;
    }

    async fn bench_compare_and_put(&self) {
        let desc = format!(
            "TableInfoBencher: compare_and_put {} table infos",
            self.count
        );
        bench_self_recorded(
            &desc,
            |i| async move {
                let table_info_value = self.table_info_manager.get(i).await.unwrap().unwrap();

                let mut new_table_info = table_info_value.table_info.clone();
                new_table_info.ident.version += 1;

                let start = Instant::now();

                self.table_info_manager
                    .compare_and_put(i, Some(table_info_value), new_table_info)
                    .await
                    .unwrap()
                    .unwrap();

                start.elapsed()
            },
            self.count,
        )
        .await;
    }

    async fn bench_remove(&self) {
        let desc = format!("TableInfoBencher: remove {} table infos", self.count);
        bench(
            &desc,
            |i| async move {
                self.table_info_manager.remove(i).await.unwrap();
            },
            self.count,
        )
        .await;
    }
}
