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

use async_trait::async_trait;
use clap::Parser;
use common_meta::helper::TableGlobalValue;
use common_meta::key::datanode_table::{DatanodeTableKey, DatanodeTableValue};
use common_meta::key::table_info::{TableInfoKey, TableInfoValue};
use common_meta::key::table_name::{TableNameKey, TableNameValue};
use common_meta::key::table_region::{RegionDistribution, TableRegionKey, TableRegionValue};
use common_meta::key::TableMetaKey;
use common_meta::rpc::store::{
    BatchDeleteRequest, BatchPutRequest, PutRequest, RangeRequest, RangeResponse,
};
use common_meta::util::get_prefix_end_key;
use common_telemetry::info;
use etcd_client::Client;
use meta_srv::service::store::etcd::EtcdStore;
use meta_srv::service::store::kv::KvStoreRef;
use snafu::ResultExt;

use crate::cli::{Instance, Tool};
use crate::error::{ConnectEtcdSnafu, Result};

#[derive(Debug, Default, Parser)]
pub struct UpgradeCommand {
    #[clap(long)]
    etcd_addr: String,
    #[clap(long)]
    dryrun: bool,
}

impl UpgradeCommand {
    pub async fn build(&self) -> Result<Instance> {
        let client = Client::connect([&self.etcd_addr], None)
            .await
            .context(ConnectEtcdSnafu {
                etcd_addr: &self.etcd_addr,
            })?;
        let tool = MigrateTableMetadata {
            etcd_store: EtcdStore::with_etcd_client(client),
            dryrun: self.dryrun,
        };
        Ok(Instance::Tool(Box::new(tool)))
    }
}

struct MigrateTableMetadata {
    etcd_store: KvStoreRef,
    dryrun: bool,
}

#[async_trait]
impl Tool for MigrateTableMetadata {
    async fn do_work(&self) -> Result<()> {
        let mut key = b"__tg".to_vec();
        let range_end = get_prefix_end_key(&key);

        let mut processed_keys = 0;
        loop {
            info!("Start scanning key from: {}", String::from_utf8_lossy(&key));

            let req = RangeRequest::new()
                .with_range(key, range_end.clone())
                .with_limit(1000);
            let resp = self.etcd_store.range(req).await.unwrap();
            for kv in resp.kvs.iter() {
                let key = String::from_utf8_lossy(kv.key());
                let value = TableGlobalValue::from_bytes(kv.value())
                    .unwrap_or_else(|e| panic!("table global value is corrupted: {e}, key: {key}"));

                self.create_table_name_key(&value).await;

                self.create_datanode_table_keys(&value).await;

                self.split_table_global_value(&key, value).await;
            }

            self.delete_migrated_keys(&resp).await;

            processed_keys += resp.kvs.len();

            if resp.more {
                key = get_prefix_end_key(resp.kvs.last().unwrap().key());
            } else {
                break;
            }
        }
        info!("Total migrated TableGlobalKeys: {processed_keys}");
        Ok(())
    }
}

impl MigrateTableMetadata {
    async fn delete_migrated_keys(&self, resp: &RangeResponse) {
        info!("Deleting {} TableGlobalKeys", resp.kvs.len());
        let req = BatchDeleteRequest {
            keys: resp.kvs.iter().map(|kv| kv.key().to_vec()).collect(),
            prev_kv: false,
        };
        if self.dryrun {
            info!("Dryrun: do nothing");
        } else {
            self.etcd_store.batch_delete(req).await.unwrap();
        }
    }

    async fn split_table_global_value(&self, key: &str, value: TableGlobalValue) {
        let table_id = value.table_id();
        let region_distribution: RegionDistribution = value.regions_id_map.into_iter().collect();

        let table_info_key = TableInfoKey::new(table_id);
        let table_info_value = TableInfoValue::new(value.table_info);

        let table_region_key = TableRegionKey::new(table_id);
        let table_region_value = TableRegionValue::new(region_distribution);

        info!("Splitting TableGlobalKey '{key}' into '{table_info_key}' and '{table_region_key}'");

        if self.dryrun {
            info!("Dryrun: do nothing");
        } else {
            self.etcd_store
                .batch_put(
                    BatchPutRequest::new()
                        .add_kv(
                            table_info_key.as_raw_key(),
                            table_info_value.try_as_raw_value().unwrap(),
                        )
                        .add_kv(
                            table_region_key.as_raw_key(),
                            table_region_value.try_as_raw_value().unwrap(),
                        ),
                )
                .await
                .unwrap();
        }
    }

    async fn create_table_name_key(&self, value: &TableGlobalValue) {
        let table_info = &value.table_info;
        let table_id = value.table_id();

        let table_name_key = TableNameKey::new(
            &table_info.catalog_name,
            &table_info.schema_name,
            &table_info.name,
        );
        let table_name_value = TableNameValue::new(table_id);

        info!("Creating '{table_name_key}' => {table_id}");

        if self.dryrun {
            info!("Dryrun: do nothing");
        } else {
            self.etcd_store
                .put(
                    PutRequest::new()
                        .with_key(table_name_key.as_raw_key())
                        .with_value(table_name_value.try_as_raw_value().unwrap()),
                )
                .await
                .unwrap();
        }
    }

    async fn create_datanode_table_keys(&self, value: &TableGlobalValue) {
        let table_id = value.table_id();
        let region_distribution: RegionDistribution =
            value.regions_id_map.clone().into_iter().collect();

        let datanode_table_kvs = region_distribution
            .into_iter()
            .map(|(datanode_id, regions)| {
                let k = DatanodeTableKey::new(datanode_id, table_id);
                info!("Creating DatanodeTableKey '{k}' => {regions:?}");
                (k, DatanodeTableValue::new(table_id, regions))
            })
            .collect::<Vec<_>>();

        if self.dryrun {
            info!("Dryrun: do nothing");
        } else {
            let mut req = BatchPutRequest::new();
            for (key, value) in datanode_table_kvs {
                req = req.add_kv(key.as_raw_key(), value.try_as_raw_value().unwrap());
            }
            self.etcd_store.batch_put(req).await.unwrap();
        }
    }
}
