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

mod datanode_table;
mod table_info;
mod table_name;
mod table_region;

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use clap::Parser;
use common_meta::key::table_region::RegionDistribution;
use common_meta::key::{TableMetadataManager, TableMetadataManagerRef};
use common_meta::table_name::TableName;
use common_telemetry::info;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, RawSchema};
use meta_srv::service::store::etcd::EtcdStore;
use meta_srv::service::store::kv::KvBackendAdapter;
use rand::prelude::SliceRandom;
use table::metadata::{RawTableInfo, RawTableMeta, TableId, TableIdent, TableType};

use crate::cli::bench::datanode_table::DatanodeTableBencher;
use crate::cli::bench::table_info::TableInfoBencher;
use crate::cli::bench::table_name::TableNameBencher;
use crate::cli::bench::table_region::TableRegionBencher;
use crate::cli::{Instance, Tool};
use crate::error::Result;

async fn bench<F, Fut>(desc: &str, f: F, count: u32)
where
    F: Fn(u32) -> Fut,
    Fut: Future<Output = ()>,
{
    let mut total = Duration::default();

    for i in 1..=count {
        let start = Instant::now();

        f(i).await;

        total += start.elapsed();
    }

    let cost = total.as_millis() as f64 / count as f64;
    info!("{desc}, average operation cost: {cost:.2} ms");
}

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

        let table_metadata_manager = Arc::new(TableMetadataManager::new(KvBackendAdapter::wrap(
            etcd_store,
        )));

        let tool = BenchTableMetadata {
            table_metadata_manager,
            count: self.count,
        };
        Ok(Instance::Tool(Box::new(tool)))
    }
}

struct BenchTableMetadata {
    table_metadata_manager: TableMetadataManagerRef,
    count: u32,
}

#[async_trait]
impl Tool for BenchTableMetadata {
    async fn do_work(&self) -> Result<()> {
        info!("Start benching table name manager ...");
        TableNameBencher::new(self.table_metadata_manager.table_name_manager(), self.count)
            .start()
            .await;

        info!("Start benching table info manager ...");
        TableInfoBencher::new(self.table_metadata_manager.table_info_manager(), self.count)
            .start()
            .await;

        info!("Start benching table region manager ...");
        TableRegionBencher::new(
            self.table_metadata_manager.table_region_manager(),
            self.count,
        )
        .start()
        .await;

        info!("Start benching datanode table manager ...");
        DatanodeTableBencher::new(
            self.table_metadata_manager.datanode_table_manager(),
            self.count,
        )
        .start()
        .await;
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
        engine_options: Default::default(),
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

fn create_region_distribution() -> RegionDistribution {
    let mut regions = (1..=100).collect::<Vec<u32>>();
    regions.shuffle(&mut rand::thread_rng());

    let mut region_distribution = RegionDistribution::new();
    for datanode_id in 0..10 {
        region_distribution.insert(
            datanode_id as u64,
            regions[datanode_id * 10..(datanode_id + 1) * 10].to_vec(),
        );
    }
    region_distribution
}
