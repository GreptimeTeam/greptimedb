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

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use clap::Parser;
use client::api::v1::meta::TableRouteValue;
use common_meta::ddl::utils::region_storage_path;
use common_meta::error as MetaError;
use common_meta::key::catalog_name::{CatalogNameKey, CatalogNameValue};
use common_meta::key::datanode_table::{DatanodeTableKey, DatanodeTableValue, RegionInfo};
use common_meta::key::schema_name::{SchemaNameKey, SchemaNameValue};
use common_meta::key::table_info::{TableInfoKey, TableInfoValue};
use common_meta::key::table_name::{TableNameKey, TableNameValue};
use common_meta::key::table_region::{TableRegionKey, TableRegionValue};
use common_meta::key::table_route::{TableRouteKey, TableRouteValue as NextTableRouteValue};
use common_meta::key::{RegionDistribution, TableMetaKey, TableMetaValue};
use common_meta::kv_backend::etcd::EtcdStore;
use common_meta::kv_backend::KvBackendRef;
use common_meta::range_stream::PaginationStream;
use common_meta::rpc::router::TableRoute;
use common_meta::rpc::store::{BatchDeleteRequest, BatchPutRequest, PutRequest, RangeRequest};
use common_meta::rpc::KeyValue;
use common_meta::util::get_prefix_end_key;
use common_telemetry::info;
use etcd_client::Client;
use futures::TryStreamExt;
use prost::Message;
use snafu::ResultExt;
use v1_helper::{CatalogKey as v1CatalogKey, SchemaKey as v1SchemaKey, TableGlobalValue};

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
        Ok(Instance::new(Box::new(tool)))
    }
}

struct MigrateTableMetadata {
    etcd_store: KvBackendRef,
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
            self.etcd_store.clone(),
            RangeRequest::new().with_range(key, range_end),
            PAGE_SIZE,
            Arc::new(|kv: KeyValue| {
                let value =
                    TableRouteValue::decode(&kv.value[..]).context(MetaError::DecodeProtoSnafu)?;
                Ok((kv.key, value))
            }),
        );

        while let Some((key, value)) = stream.try_next().await.context(error::IterStreamSnafu)? {
            let table_id = self.migrate_table_route_key(value).await?;
            keys.push(key);
            keys.push(TableRegionKey::new(table_id).as_raw_key())
        }

        info!("Total migrated TableRouteKeys: {}", keys.len() / 2);
        self.delete_migrated_keys(keys).await;

        Ok(())
    }

    async fn migrate_table_route_key(&self, value: TableRouteValue) -> Result<u32> {
        let table_route = TableRoute::try_from_raw(
            &value.peers,
            value.table_route.expect("expected table_route"),
        )
        .unwrap();

        let new_table_value = NextTableRouteValue::physical(table_route.region_routes);

        let table_id = table_route.table.id as u32;
        let new_key = TableRouteKey::new(table_id);
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

        Ok(table_id)
    }

    async fn migrate_schema_keys(&self) -> Result<()> {
        // The schema key prefix.
        let key = b"__s".to_vec();
        let range_end = get_prefix_end_key(&key);

        let mut keys = Vec::new();
        info!("Start scanning key from: {}", String::from_utf8_lossy(&key));
        let mut stream = PaginationStream::new(
            self.etcd_store.clone(),
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
        let schema_name_value = SchemaNameValue::default();

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
            self.etcd_store.clone(),
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
            self.etcd_store.clone(),
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
            info!("Deleting {} keys", keys.len());
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
        let engine = value.table_info.meta.engine.as_str();
        let region_storage_path = region_storage_path(
            &value.table_info.catalog_name,
            &value.table_info.schema_name,
        );
        let region_distribution: RegionDistribution =
            value.regions_id_map.clone().into_iter().collect();

        // TODO(niebayes): properly fetch or construct wal options.
        let region_wal_options = HashMap::default();

        let datanode_table_kvs = region_distribution
            .into_iter()
            .map(|(datanode_id, regions)| {
                let k = DatanodeTableKey::new(datanode_id, table_id);
                info!("Creating DatanodeTableKey '{k}' => {regions:?}");
                (
                    k,
                    DatanodeTableValue::new(
                        table_id,
                        regions,
                        RegionInfo {
                            engine: engine.to_string(),
                            region_storage_path: region_storage_path.clone(),
                            region_options: (&value.table_info.meta.options).into(),
                            region_wal_options: region_wal_options.clone(),
                        },
                    ),
                )
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

#[deprecated(since = "0.4.0", note = "Used for migrate old version(v0.3) metadata")]
mod v1_helper {
    use std::collections::HashMap;
    use std::fmt::{Display, Formatter};

    use err::{DeserializeCatalogEntryValueSnafu, Error, InvalidCatalogSnafu};
    use lazy_static::lazy_static;
    use regex::Regex;
    use serde::{Deserialize, Serialize};
    use snafu::{ensure, OptionExt, ResultExt};
    use table::metadata::{RawTableInfo, TableId};

    pub const CATALOG_KEY_PREFIX: &str = "__c";
    pub const SCHEMA_KEY_PREFIX: &str = "__s";

    /// The pattern of a valid catalog, schema or table name.
    const NAME_PATTERN: &str = "[a-zA-Z_:][a-zA-Z0-9_:]*";

    lazy_static! {
        static ref CATALOG_KEY_PATTERN: Regex =
            Regex::new(&format!("^{CATALOG_KEY_PREFIX}-({NAME_PATTERN})$")).unwrap();
    }

    lazy_static! {
        static ref SCHEMA_KEY_PATTERN: Regex = Regex::new(&format!(
            "^{SCHEMA_KEY_PREFIX}-({NAME_PATTERN})-({NAME_PATTERN})$"
        ))
        .unwrap();
    }

    /// Table global info contains necessary info for a datanode to create table regions, including
    /// table id, table meta(schema...), region id allocation across datanodes.
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub struct TableGlobalValue {
        /// Id of datanode that created the global table info kv. only for debugging.
        pub node_id: u64,
        /// Allocation of region ids across all datanodes.
        pub regions_id_map: HashMap<u64, Vec<u32>>,
        pub table_info: RawTableInfo,
    }

    impl TableGlobalValue {
        pub fn table_id(&self) -> TableId {
            self.table_info.ident.table_id
        }
    }

    pub struct CatalogKey {
        pub catalog_name: String,
    }

    impl Display for CatalogKey {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.write_str(CATALOG_KEY_PREFIX)?;
            f.write_str("-")?;
            f.write_str(&self.catalog_name)
        }
    }

    impl CatalogKey {
        pub fn parse(s: impl AsRef<str>) -> Result<Self, Error> {
            let key = s.as_ref();
            let captures = CATALOG_KEY_PATTERN
                .captures(key)
                .context(InvalidCatalogSnafu { key })?;
            ensure!(captures.len() == 2, InvalidCatalogSnafu { key });
            Ok(Self {
                catalog_name: captures[1].to_string(),
            })
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct CatalogValue;

    pub struct SchemaKey {
        pub catalog_name: String,
        pub schema_name: String,
    }

    impl Display for SchemaKey {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.write_str(SCHEMA_KEY_PREFIX)?;
            f.write_str("-")?;
            f.write_str(&self.catalog_name)?;
            f.write_str("-")?;
            f.write_str(&self.schema_name)
        }
    }

    impl SchemaKey {
        pub fn parse(s: impl AsRef<str>) -> Result<Self, Error> {
            let key = s.as_ref();
            let captures = SCHEMA_KEY_PATTERN
                .captures(key)
                .context(InvalidCatalogSnafu { key })?;
            ensure!(captures.len() == 3, InvalidCatalogSnafu { key });
            Ok(Self {
                catalog_name: captures[1].to_string(),
                schema_name: captures[2].to_string(),
            })
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct SchemaValue;

    macro_rules! define_catalog_value {
    ( $($val_ty: ty), *) => {
            $(
                impl $val_ty {
                    pub fn parse(s: impl AsRef<str>) -> Result<Self, Error> {
                        serde_json::from_str(s.as_ref())
                            .context(DeserializeCatalogEntryValueSnafu { raw: s.as_ref() })
                    }

                    pub fn from_bytes(bytes: impl AsRef<[u8]>) -> Result<Self, Error> {
                         Self::parse(&String::from_utf8_lossy(bytes.as_ref()))
                    }
                }
            )*
        }
    }

    define_catalog_value!(TableGlobalValue);

    mod err {
        use snafu::{Location, Snafu};

        #[derive(Debug, Snafu)]
        #[snafu(visibility(pub))]
        pub enum Error {
            #[snafu(display("Invalid catalog info: {}", key))]
            InvalidCatalog { key: String, location: Location },

            #[snafu(display("Failed to deserialize catalog entry value: {}", raw))]
            DeserializeCatalogEntryValue {
                raw: String,
                location: Location,
                source: serde_json::error::Error,
            },
        }
    }
}
