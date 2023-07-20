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

use common_meta::key::table_name::{TableNameKey, TableNameManager};

use super::bench;

pub struct TableNameBencher<'a> {
    table_name_manager: &'a TableNameManager,
    count: u32,
}

impl<'a> TableNameBencher<'a> {
    pub fn new(table_name_manager: &'a TableNameManager, count: u32) -> Self {
        Self {
            table_name_manager,
            count,
        }
    }

    pub async fn start(&self) {
        self.bench_create().await;
        self.bench_rename().await;
        self.bench_get().await;
        self.bench_tables().await;
        self.bench_remove().await;
    }

    async fn bench_create(&self) {
        let desc = format!("TableNameBencher: create {} table names", self.count);
        bench(
            &desc,
            |i| async move {
                let table_name = format!("bench_table_name_{}", i);
                let table_name_key = create_table_name_key(&table_name);
                self.table_name_manager
                    .create(&table_name_key, i)
                    .await
                    .unwrap();
            },
            self.count,
        )
        .await;
    }

    async fn bench_rename(&self) {
        let desc = format!("TableNameBencher: rename {} table names", self.count);
        bench(
            &desc,
            |i| async move {
                let table_name = format!("bench_table_name_{}", i);
                let new_table_name = format!("bench_table_name_new_{}", i);
                let table_name_key = create_table_name_key(&table_name);
                self.table_name_manager
                    .rename(table_name_key, i, &new_table_name)
                    .await
                    .unwrap();
            },
            self.count,
        )
        .await;
    }

    async fn bench_get(&self) {
        let desc = format!("TableNameBencher: get {} table names", self.count);
        bench(
            &desc,
            |i| async move {
                let table_name = format!("bench_table_name_new_{}", i);
                let table_name_key = create_table_name_key(&table_name);
                assert!(self
                    .table_name_manager
                    .get(table_name_key)
                    .await
                    .unwrap()
                    .is_some());
            },
            self.count,
        )
        .await;
    }

    async fn bench_tables(&self) {
        let desc = format!("TableNameBencher: list all {} table names", self.count);
        bench(
            &desc,
            |_| async move {
                assert!(!self
                    .table_name_manager
                    .tables("bench_catalog", "bench_schema")
                    .await
                    .unwrap()
                    .is_empty());
            },
            self.count,
        )
        .await;
    }

    async fn bench_remove(&self) {
        let desc = format!("TableNameBencher: remove {} table names", self.count);
        bench(
            &desc,
            |i| async move {
                let table_name = format!("bench_table_name_new_{}", i);
                let table_name_key = create_table_name_key(&table_name);
                self.table_name_manager
                    .remove(table_name_key)
                    .await
                    .unwrap();
            },
            self.count,
        )
        .await;
    }
}

fn create_table_name_key(table_name: &str) -> TableNameKey {
    TableNameKey::new("bench_catalog", "bench_schema", table_name)
}
