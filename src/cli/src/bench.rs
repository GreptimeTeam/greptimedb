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
use common_error::ext::BoxedError;
use common_meta::key::{TableMetadataManager, TableMetadataManagerRef};
use common_meta::kv_backend::etcd::EtcdStore;
use common_meta::kv_backend::memory::MemoryKvBackend;
#[cfg(feature = "mysql_kvbackend")]
use common_meta::kv_backend::rds::MySqlStore;
#[cfg(feature = "pg_kvbackend")]
use common_meta::kv_backend::rds::PgStore;
use common_meta::peer::Peer;
use common_meta::rpc::router::{Region, RegionRoute};
use common_telemetry::info;
use common_wal::options::WalOptions;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema};
use rand::Rng;
use store_api::storage::RegionNumber;
use table::metadata::{TableId, TableIdent, TableInfo, TableMeta, TableType};
use table::table_name::TableName;

use self::metadata::TableMetadataBencher;
use crate::Tool;

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

/// Command to benchmark table metadata operations.
#[derive(Debug, Default, Parser)]
pub struct BenchTableMetadataCommand {
    #[clap(long)]
    etcd_addr: Option<String>,
    #[cfg(feature = "pg_kvbackend")]
    #[clap(long)]
    postgres_addr: Option<String>,
    #[cfg(feature = "pg_kvbackend")]
    #[clap(long)]
    postgres_schema: Option<String>,
    #[cfg(feature = "mysql_kvbackend")]
    #[clap(long)]
    mysql_addr: Option<String>,
    #[clap(long)]
    count: u32,
}

impl BenchTableMetadataCommand {
    pub async fn build(&self) -> std::result::Result<Box<dyn Tool>, BoxedError> {
        let kv_backend = if let Some(etcd_addr) = &self.etcd_addr {
            info!("Using etcd as kv backend");
            EtcdStore::with_endpoints([etcd_addr], 128).await.unwrap()
        } else {
            Arc::new(MemoryKvBackend::new())
        };

        #[cfg(feature = "pg_kvbackend")]
        let kv_backend = if let Some(postgres_addr) = &self.postgres_addr {
            info!("Using postgres as kv backend");
            PgStore::with_url(postgres_addr, "greptime_metakv", 128)
                .await
                .unwrap()
        } else {
            kv_backend
        };

        #[cfg(feature = "mysql_kvbackend")]
        let kv_backend = if let Some(mysql_addr) = &self.mysql_addr {
            info!("Using mysql as kv backend");
            MySqlStore::with_url(mysql_addr, "greptime_metakv", 128)
                .await
                .unwrap()
        } else {
            kv_backend
        };

        let table_metadata_manager = Arc::new(TableMetadataManager::new(kv_backend));

        let tool = BenchTableMetadata {
            table_metadata_manager,
            count: self.count,
        };
        Ok(Box::new(tool))
    }
}

struct BenchTableMetadata {
    table_metadata_manager: TableMetadataManagerRef,
    count: u32,
}

#[async_trait]
impl Tool for BenchTableMetadata {
    async fn do_work(&self) -> std::result::Result<(), BoxedError> {
        let bencher = TableMetadataBencher::new(self.table_metadata_manager.clone(), self.count);
        bencher.bench_create().await;
        bencher.bench_get().await;
        bencher.bench_rename().await;
        bencher.bench_delete().await;
        Ok(())
    }
}

fn create_table_info(table_id: TableId, table_name: TableName) -> TableInfo {
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

    let meta = TableMeta {
        schema: Arc::new(Schema::new(column_schemas)),
        engine: "mito".to_string(),
        created_on: chrono::DateTime::default(),
        updated_on: chrono::DateTime::default(),
        primary_key_indices: vec![],
        next_column_id: columns as u32 + 1,
        value_indices: vec![],
        options: Default::default(),
        partition_key_indices: vec![],
        column_ids: vec![],
    };

    TableInfo {
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
    let mut rng = rand::rng();

    for region_id in regions.into_iter().map(u64::from) {
        region_routes.push(RegionRoute {
            region: Region {
                id: region_id.into(),
                name: String::new(),
                attrs: BTreeMap::new(),
                partition_expr: Default::default(),
            },
            leader_peer: Some(Peer {
                id: rng.random_range(0..10),
                addr: String::new(),
            }),
            follower_peers: vec![],
            leader_state: None,
            leader_down_since: None,
            write_route_policy: None,
        });
    }

    region_routes
}

fn create_region_wal_options(regions: Vec<RegionNumber>) -> HashMap<RegionNumber, WalOptions> {
    // TODO(niebayes): construct region wal options for benchmark.
    let _ = regions;
    HashMap::default()
}
