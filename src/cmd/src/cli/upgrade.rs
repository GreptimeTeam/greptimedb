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

use std::sync::Arc;

use async_trait::async_trait;
use clap::Parser;
use client::api::v1::meta::TableRouteValue;
use common_meta::error as MetaError;
use common_meta::helper::{CatalogKey as v1CatalogKey, SchemaKey as v1SchemaKey, TableGlobalValue};
use common_meta::key::catalog_name::{CatalogNameKey, CatalogNameValue};
use common_meta::key::datanode_table::{DatanodeTableKey, DatanodeTableValue};
use common_meta::key::schema_name::{SchemaNameKey, SchemaNameValue};
use common_meta::key::table_info::{TableInfoKey, TableInfoValue};
use common_meta::key::table_name::{TableNameKey, TableNameValue};
use common_meta::key::table_region::{RegionDistribution, TableRegionKey, TableRegionValue};
use common_meta::key::table_route::{NextTableRouteKey, TableRouteValue as NextTableRouteValue};
use common_meta::key::TableMetaKey;
use common_meta::range_stream::PaginationStream;
use common_meta::rpc::router::TableRoute;
use common_meta::rpc::store::{BatchDeleteRequest, BatchPutRequest, PutRequest, RangeRequest};
use common_meta::rpc::KeyValue;
use common_meta::util::get_prefix_end_key;
use common_telemetry::info;
use etcd_client::Client;
use futures::TryStreamExt;
use meta_srv::service::store::etcd::EtcdStore;
use meta_srv::service::store::kv::{KvBackendAdapter, KvStoreRef};
use prost::Message;
use snafu::ResultExt;

use crate::cli::{Instance, Tool};
use crate::error::{self, ConnectEtcdSnafu, Result};

#[derive(Debug, Default, Parser)]
pub struct UpgradeCommand {
    #[clap(long)]
    etcd_addr: String,
    #[clap(long)]
    dryrun: bool,

    #[clap(long)]
    skip_table_global_keys: bool,
    #[clap(long)]
    skip_catalog_keys: bool,
    #[clap(long)]
    skip_schema_keys: bool,
    #[clap(long)]
    skip_table_route_keys: bool,
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
            skip_catalog_keys: self.skip_catalog_keys,
            skip_table_global_keys: self.skip_table_global_keys,
            skip_schema_keys: self.skip_schema_keys,
            skip_table_route_keys: self.skip_table_route_keys,
        };
        Ok(Instance::Tool(Box::new(tool)))
    }
}

struct MigrateTableMetadata {
    etcd_store: KvStoreRef,
    dryrun: bool,

    skip_table_global_keys: bool,

    skip_catalog_keys: bool,

    skip_schema_keys: bool,

    skip_table_route_keys: bool,
}

#[async_trait]
impl Tool for MigrateTableMetadata {
    // migrates database's metadata from 0.3 to 0.4.
    async fn do_work(&self) -> Result<()> {
        if !self.skip_table_global_keys {
            self.migrate_table_global_values().await?;
        }
        if !self.skip_catalog_keys {
            self.migrate_catalog_keys().await?;
        }
        if !self.skip_schema_keys {
            self.migrate_schema_keys().await?;
        }
        if !self.skip_table_route_keys {
            self.migrate_table_route_keys().await?;
        }
        Ok(())
    }
}

const PAGE_SIZE: usize = 1000;

impl MigrateTableMetadata {
    async fn migrate_table_route_keys(&self) -> Result<()> {
        let key = b"__meta_table_route".to_vec();
        let range_end = get_prefix_end_key(&key);
        let mut keys = Vec::new();
        info!("Start scanning key from: {}", String::from_utf8_lossy(&key));

        let mut stream = PaginationStream::new(
            KvBackendAdapter::wrap(self.etcd_store.clone()),
            RangeRequest::new().with_range(key, range_end),
            PAGE_SIZE,
            Arc::new(|kv: KeyValue| {
                let value =
                    TableRouteValue::decode(&kv.value[..]).context(MetaError::DecodeProtoSnafu)?;
                Ok((kv.key, value))
            }),
        );

        while let Some((key, value)) = stream.try_next().await.context(error::IterStreamSnafu)? {
            self.migrate_table_route_key(value).await?;
            keys.push(key);
        }

        info!("Total migrated TableRouteKeys: {}", keys.len());
        self.delete_migrated_keys(keys).await;

        Ok(())
    }

    async fn migrate_table_route_key(&self, value: TableRouteValue) -> Result<()> {
        let table_route = TableRoute::try_from_raw(
            &value.peers,
            value.table_route.expect("expected table_route"),
        )
        .unwrap();

        let new_table_value = NextTableRouteValue::new(table_route.region_routes);

        let new_key = NextTableRouteKey::new(table_route.table.id as u32);
        info!("Creating '{new_key}'");

        if self.dryrun {
            info!("Dryrun: do nothing");
        } else {
            self.etcd_store
                .put(
                    PutRequest::new()
                        .with_key(new_key.as_raw_key())
                        .with_value(new_table_value.try_as_raw_value().unwrap()),
                )
                .await
                .unwrap();
        }

        Ok(())
    }

    async fn migrate_schema_keys(&self) -> Result<()> {
        // The schema key prefix.
        let key = b"__s".to_vec();
        let range_end = get_prefix_end_key(&key);

        let mut keys = Vec::new();
        info!("Start scanning key from: {}", String::from_utf8_lossy(&key));
        let mut stream = PaginationStream::new(
            KvBackendAdapter::wrap(self.etcd_store.clone()),
            RangeRequest::new().with_range(key, range_end),
            PAGE_SIZE,
            Arc::new(|kv: KeyValue| {
                let key_str =
                    std::str::from_utf8(&kv.key).context(MetaError::ConvertRawKeySnafu)?;
                let key = v1SchemaKey::parse(key_str)
                    .unwrap_or_else(|e| panic!("schema key is corrupted: {e}, key: {key_str}"));

                Ok((key, ()))
            }),
        );
        while let Some((key, _)) = stream.try_next().await.context(error::IterStreamSnafu)? {
            let _ = self.migrate_schema_key(&key).await;
            keys.push(key.to_string().as_bytes().to_vec());
        }
        info!("Total migrated SchemaKeys: {}", keys.len());
        self.delete_migrated_keys(keys).await;

        Ok(())
    }

    async fn migrate_schema_key(&self, key: &v1SchemaKey) -> Result<()> {
        let new_key = SchemaNameKey::new(&key.catalog_name, &key.schema_name);
        let schema_name_value = SchemaNameValue;

        info!("Creating '{new_key}'");

        if self.dryrun {
            info!("Dryrun: do nothing");
        } else {
            self.etcd_store
                .put(
                    PutRequest::new()
                        .with_key(new_key.as_raw_key())
                        .with_value(schema_name_value.try_as_raw_value().unwrap()),
                )
                .await
                .unwrap();
        }

        Ok(())
    }

    async fn migrate_catalog_keys(&self) -> Result<()> {
        // The catalog key prefix.
        let key = b"__c".to_vec();
        let range_end = get_prefix_end_key(&key);

        let mut keys = Vec::new();
        info!("Start scanning key from: {}", String::from_utf8_lossy(&key));
        let mut stream = PaginationStream::new(
            KvBackendAdapter::wrap(self.etcd_store.clone()),
            RangeRequest::new().with_range(key, range_end),
            PAGE_SIZE,
            Arc::new(|kv: KeyValue| {
                let key_str =
                    std::str::from_utf8(&kv.key).context(MetaError::ConvertRawKeySnafu)?;
                let key = v1CatalogKey::parse(key_str)
                    .unwrap_or_else(|e| panic!("catalog key is corrupted: {e}, key: {key_str}"));

                Ok((key, ()))
            }),
        );
        while let Some((key, _)) = stream.try_next().await.context(error::IterStreamSnafu)? {
            let _ = self.migrate_catalog_key(&key).await;
            keys.push(key.to_string().as_bytes().to_vec());
        }
        info!("Total migrated CatalogKeys: {}", keys.len());
        self.delete_migrated_keys(keys).await;

        Ok(())
    }

    async fn migrate_catalog_key(&self, key: &v1CatalogKey) {
        let new_key = CatalogNameKey::new(&key.catalog_name);
        let catalog_name_value = CatalogNameValue;

        info!("Creating '{new_key}'");

        if self.dryrun {
            info!("Dryrun: do nothing");
        } else {
            self.etcd_store
                .put(
                    PutRequest::new()
                        .with_key(new_key.as_raw_key())
                        .with_value(catalog_name_value.try_as_raw_value().unwrap()),
                )
                .await
                .unwrap();
        }
    }

    async fn migrate_table_global_values(&self) -> Result<()> {
        let key = b"__tg".to_vec();
        let range_end = get_prefix_end_key(&key);

        let mut keys = Vec::new();

        info!("Start scanning key from: {}", String::from_utf8_lossy(&key));
        let mut stream = PaginationStream::new(
            KvBackendAdapter::wrap(self.etcd_store.clone()),
            RangeRequest::new().with_range(key, range_end.clone()),
            PAGE_SIZE,
            Arc::new(|kv: KeyValue| {
                let key = String::from_utf8_lossy(kv.key()).to_string();
                let value = TableGlobalValue::from_bytes(kv.value())
                    .unwrap_or_else(|e| panic!("table global value is corrupted: {e}, key: {key}"));

                Ok((key, value))
            }),
        );
        while let Some((key, value)) = stream.try_next().await.context(error::IterStreamSnafu)? {
            self.create_table_name_key(&value).await;

            self.create_datanode_table_keys(&value).await;

            self.split_table_global_value(&key, value).await;

            keys.push(key.as_bytes().to_vec());
        }

        info!("Total migrated TableGlobalKeys: {}", keys.len());
        self.delete_migrated_keys(keys).await;

        Ok(())
    }

    async fn delete_migrated_keys(&self, keys: Vec<Vec<u8>>) {
        for keys in keys.chunks(PAGE_SIZE) {
            info!("Deleting {} TableGlobalKeys", keys.len());
            let req = BatchDeleteRequest {
                keys: keys.to_vec(),
                prev_kv: false,
            };
            if self.dryrun {
                info!("Dryrun: do nothing");
            } else {
                self.etcd_store.batch_delete(req).await.unwrap();
            }
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
