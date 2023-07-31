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

use common_meta::key::table_region::TableRegionManager;

use super::{bench, bench_self_recorded, create_region_distribution};

pub struct TableRegionBencher<'a> {
    table_region_manager: &'a TableRegionManager,
    count: u32,
}

impl<'a> TableRegionBencher<'a> {
    pub fn new(table_region_manager: &'a TableRegionManager, count: u32) -> Self {
        Self {
            table_region_manager,
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
        let desc = format!("TableRegionBencher: create {} table regions", self.count);
        bench_self_recorded(
            &desc,
            |i| async move {
                let region_distribution = create_region_distribution();

                let start = Instant::now();

                self.table_region_manager
                    .create(i, &region_distribution)
                    .await
                    .unwrap();

                start.elapsed()
            },
            self.count,
        )
        .await;
    }

    async fn bench_get(&self) {
        let desc = format!("TableRegionBencher: get {} table regions", self.count);
        bench(
            &desc,
            |i| async move {
                assert!(self.table_region_manager.get(i).await.unwrap().is_some());
            },
            self.count,
        )
        .await;
    }

    async fn bench_compare_and_put(&self) {
        let desc = format!(
            "TableRegionBencher: compare_and_put {} table regions",
            self.count
        );
        bench_self_recorded(
            &desc,
            |i| async move {
                let table_region_value = self.table_region_manager.get(i).await.unwrap().unwrap();

                let new_region_distribution = create_region_distribution();

                let start = Instant::now();

                self.table_region_manager
                    .compare_and_put(i, Some(table_region_value), new_region_distribution)
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
        let desc = format!("TableRegionBencher: remove {} table regions", self.count);
        bench(
            &desc,
            |i| async move {
                assert!(self.table_region_manager.remove(i).await.unwrap().is_some());
            },
            self.count,
        )
        .await;
    }
}
