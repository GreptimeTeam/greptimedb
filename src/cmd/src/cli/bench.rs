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

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use clap::Parser;
use common_meta::key::{TableMetadataManager, TableMetadataManagerRef};
use common_meta::kv_backend::etcd::EtcdStore;
use common_meta::peer::Peer;
use common_meta::rpc::router::{Region, RegionRoute};
use common_meta::table_name::TableName;
use common_telemetry::info;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, RawSchema};
use rand::Rng;
use store_api::storage::RegionNumber;
use table::metadata::{RawTableInfo, RawTableMeta, TableId, TableIdent, TableType};

use self::metadata::TableMetadataBencher;
use crate::cli::{Instance, Tool};
use crate::error::Result;

mod metadata;

async fn bench_self_recorded<F, Fut>(desc: &str, f: F, count: u32)
where
    F: Fn(u32) -> Fut,
    Fut: Future<Output = Duration>,
{
    let mut total = Duration::default();

    for i in 1..=count {
        total += f(i).await;
    }

    let cost = total.as_millis() as f64 / count as f64;
    info!("{desc}, average operation cost: {cost:.2} ms");
}

#[derive(Debug, Default, Parser)]
pub struct BenchTableMetadataCommand {
    #[clap(long)]
    etcd_addr: String,
    #[clap(long)]
    count: u32,
}

impl BenchTableMetadataCommand {
    pub async fn build(&self) -> Result<Instance> {
        let etcd_store = EtcdStore::with_endpoints([&self.etcd_addr]).await.unwrap();

        let table_metadata_manager = Arc::new(TableMetadataManager::new(etcd_store));

        let tool = BenchTableMetadata {
            table_metadata_manager,
            count: self.count,
        };
        Ok(Instance::new(Box::new(tool)))
    }
}

struct BenchTableMetadata {
    table_metadata_manager: TableMetadataManagerRef,
    count: u32,
}

#[async_trait]
impl Tool for BenchTableMetadata {
    async fn do_work(&self) -> Result<()> {
        let bencher = TableMetadataBencher::new(self.table_metadata_manager.clone(), self.count);
        bencher.bench_create().await;
        bencher.bench_get().await;
        bencher.bench_rename().await;
        bencher.bench_delete().await;
        Ok(())
    }
}

fn create_table_info(table_id: TableId, table_name: TableName) -> RawTableInfo {
    let columns = 100;
    let mut column_schemas = Vec::with_capacity(columns);
    column_schemas.push(
        ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            true,
        )
        .with_time_index(true),
    );

    for i in 1..columns {
        let column_name = format!("my_column_{i}");
        column_schemas.push(ColumnSchema::new(
            column_name,
            ConcreteDataType::string_datatype(),
            true,
        ));
    }

    let meta = RawTableMeta {
        schema: RawSchema::new(column_schemas),
        engine: "mito".to_string(),
        created_on: chrono::DateTime::default(),
        primary_key_indices: vec![],
        next_column_id: columns as u32 + 1,
        value_indices: vec![],
        options: Default::default(),
        region_numbers: (1..=100).collect(),
        partition_key_indices: vec![],
    };

    RawTableInfo {
        ident: TableIdent {
            table_id,
            version: 1,
        },
        name: table_name.table_name,
        desc: Some("blah".to_string()),
        catalog_name: table_name.catalog_name,
        schema_name: table_name.schema_name,
        meta,
        table_type: TableType::Base,
    }
}

fn create_region_routes(regions: Vec<RegionNumber>) -> Vec<RegionRoute> {
    let mut region_routes = Vec::with_capacity(100);
    let mut rng = rand::thread_rng();

    for region_id in regions.into_iter().map(u64::from) {
        region_routes.push(RegionRoute {
            region: Region {
                id: region_id.into(),
                name: String::new(),
                partition: None,
                attrs: BTreeMap::new(),
            },
            leader_peer: Some(Peer {
                id: rng.gen_range(0..10),
                addr: String::new(),
            }),
            follower_peers: vec![],
            leader_status: None,
        });
    }

    region_routes
}

fn create_region_wal_options(regions: Vec<RegionNumber>) -> HashMap<RegionNumber, String> {
    // TODO(niebayes): construct region wal options for benchmark.
    let _ = regions;
    HashMap::default()
}
