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

use common_meta::key::datanode_table::{DatanodeTableKey, DatanodeTableManager};

use super::bench;

pub struct DatanodeTableBencher<'a> {
    datanode_table_manager: &'a DatanodeTableManager,
    count: u32,
}

impl<'a> DatanodeTableBencher<'a> {
    pub fn new(datanode_table_manager: &'a DatanodeTableManager, count: u32) -> Self {
        Self {
            datanode_table_manager,
            count,
        }
    }

    pub async fn start(&self) {
        self.bench_create().await;
        self.bench_get().await;
        self.bench_move_region().await;
        self.bench_tables().await;
        self.bench_remove().await;
    }

    async fn bench_create(&self) {
        let desc = format!(
            "DatanodeTableBencher: create {} datanode table keys",
            self.count
        );
        bench(
            &desc,
            |i| async move {
                self.datanode_table_manager
                    .create(1, i, vec![1, 2, 3, 4])
                    .await
                    .unwrap();
            },
            self.count,
        )
        .await;
    }

    async fn bench_get(&self) {
        let desc = format!(
            "DatanodeTableBencher: get {} datanode table keys",
            self.count
        );
        bench(
            &desc,
            |i| async move {
                let key = DatanodeTableKey::new(1, i);
                assert!(self
                    .datanode_table_manager
                    .get(&key)
                    .await
                    .unwrap()
                    .is_some());
            },
            self.count,
        )
        .await;
    }

    async fn bench_move_region(&self) {
        let desc = format!(
            "DatanodeTableBencher: move {} datanode table regions",
            self.count
        );
        bench(
            &desc,
            |i| async move {
                self.datanode_table_manager
                    .move_region(1, 2, i, 1)
                    .await
                    .unwrap();
            },
            self.count,
        )
        .await;
    }

    async fn bench_tables(&self) {
        let desc = format!(
            "DatanodeTableBencher: list {} datanode table keys",
            self.count
        );
        bench(
            &desc,
            |_| async move {
                assert!(!self
                    .datanode_table_manager
                    .tables(1)
                    .await
                    .unwrap()
                    .is_empty());
            },
            self.count,
        )
        .await;
    }

    async fn bench_remove(&self) {
        let desc = format!(
            "DatanodeTableBencher: remove {} datanode table keys",
            self.count
        );
        bench(
            &desc,
            |i| async move {
                self.datanode_table_manager.remove(1, i).await.unwrap();
            },
            self.count,
        )
        .await;
    }
}
