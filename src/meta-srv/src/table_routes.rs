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

use api::v1::meta::{PutRequest, TableRouteValue};
use catalog::helper::{TableGlobalKey, TableGlobalValue};
use common_meta::key::TableRouteKey;
use common_meta::rpc::store::{BatchGetRequest, BatchGetResponse};
use snafu::{OptionExt, ResultExt};

use crate::error::{
    ConvertProtoDataSnafu, DecodeTableRouteSnafu, InvalidCatalogValueSnafu, Result,
    TableRouteNotFoundSnafu,
};
use crate::service::store::ext::KvStoreExt;
use crate::service::store::kv::KvStoreRef;

pub async fn get_table_global_value(
    kv_store: &KvStoreRef,
    key: &TableGlobalKey,
) -> Result<Option<TableGlobalValue>> {
    let kv = kv_store.get(key.to_raw_key()).await?;
    kv.map(|kv| TableGlobalValue::from_bytes(kv.value).context(InvalidCatalogValueSnafu))
        .transpose()
}

pub(crate) async fn batch_get_table_global_value(
    kv_store: &KvStoreRef,
    keys: Vec<&TableGlobalKey>,
) -> Result<HashMap<TableGlobalKey, Option<TableGlobalValue>>> {
    let req = BatchGetRequest {
        keys: keys.iter().map(|x| x.to_raw_key()).collect::<Vec<_>>(),
    };
    let mut resp: BatchGetResponse = kv_store
        .batch_get(req.into())
        .await?
        .try_into()
        .context(ConvertProtoDataSnafu)?;

    let kvs = resp.take_kvs();
    let mut result = HashMap::with_capacity(kvs.len());
    for kv in kvs {
        let key = TableGlobalKey::try_from_raw_key(kv.key()).context(InvalidCatalogValueSnafu)?;
        let value = TableGlobalValue::from_bytes(kv.value()).context(InvalidCatalogValueSnafu)?;
        let _ = result.insert(key, Some(value));
    }

    for key in keys {
        if !result.contains_key(key) {
            let _ = result.insert(key.clone(), None);
        }
    }
    Ok(result)
}

pub(crate) async fn put_table_global_value(
    kv_store: &KvStoreRef,
    key: &TableGlobalKey,
    value: &TableGlobalValue,
) -> Result<()> {
    let req = PutRequest {
        header: None,
        key: key.to_raw_key(),
        value: value.as_bytes().context(InvalidCatalogValueSnafu)?,
        prev_kv: false,
    };
    let _ = kv_store.put(req).await;
    Ok(())
}

pub(crate) async fn get_table_route_value(
    kv_store: &KvStoreRef,
    key: &TableRouteKey<'_>,
) -> Result<TableRouteValue> {
    let kv = kv_store
        .get(key.key().into_bytes())
        .await?
        .with_context(|| TableRouteNotFoundSnafu { key: key.key() })?;
    kv.value
        .as_slice()
        .try_into()
        .context(DecodeTableRouteSnafu)
}

pub(crate) async fn put_table_route_value(
    kv_store: &KvStoreRef,
    key: &TableRouteKey<'_>,
    value: TableRouteValue,
) -> Result<()> {
    let req = PutRequest {
        header: None,
        key: key.key().into_bytes(),
        value: value.into(),
        prev_kv: false,
    };
    let _ = kv_store.put(req).await?;
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use api::v1::meta::{Peer, Region, RegionRoute, Table, TableName, TableRoute};
    use chrono::DateTime;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, MITO_ENGINE};
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, RawSchema};
    use table::metadata::{RawTableInfo, RawTableMeta, TableIdent, TableType};
    use table::requests::TableOptions;

    use super::*;
    use crate::error;
    use crate::service::store::memory::MemStore;

    pub(crate) async fn prepare_table_global_value(
        kv_store: &KvStoreRef,
        table: &str,
    ) -> (TableGlobalKey, TableGlobalValue) {
        // Region distribution:
        // Datanode => Regions
        // 1 => 1, 2
        // 2 => 3
        // 3 => 4
        let regions_id_map = HashMap::from([(1, vec![1, 2]), (2, vec![3]), (3, vec![4])]);

        let key = TableGlobalKey {
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: table.to_string(),
        };
        let value = TableGlobalValue {
            node_id: 1,
            regions_id_map,
            table_info: RawTableInfo {
                ident: TableIdent::new(1),
                name: table.to_string(),
                desc: None,
                catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                schema_name: DEFAULT_SCHEMA_NAME.to_string(),
                meta: RawTableMeta {
                    schema: RawSchema::new(vec![ColumnSchema::new(
                        "a",
                        ConcreteDataType::string_datatype(),
                        true,
                    )]),
                    primary_key_indices: vec![],
                    value_indices: vec![],
                    engine: MITO_ENGINE.to_string(),
                    next_column_id: 1,
                    region_numbers: vec![1, 2, 3, 4],
                    engine_options: HashMap::new(),
                    options: TableOptions::default(),
                    created_on: DateTime::default(),
                },
                table_type: TableType::Base,
            },
        };
        put_table_global_value(kv_store, &key, &value)
            .await
            .unwrap();
        (key, value)
    }

    pub(crate) async fn prepare_table_route_value<'a>(
        kv_store: &'a KvStoreRef,
        table: &'a str,
    ) -> (TableRouteKey<'a>, TableRouteValue) {
        let key = TableRouteKey {
            table_id: 1,
            catalog_name: DEFAULT_CATALOG_NAME,
            schema_name: DEFAULT_SCHEMA_NAME,
            table_name: table,
        };

        let peers = (1..=3)
            .map(|id| Peer {
                id,
                addr: "".to_string(),
            })
            .collect::<Vec<_>>();

        // region routes:
        // region number => leader node
        // 1 => 1
        // 2 => 1
        // 3 => 2
        // 4 => 3
        let region_routes = vec![
            new_region_route(1, &peers, 1),
            new_region_route(2, &peers, 1),
            new_region_route(3, &peers, 2),
            new_region_route(4, &peers, 3),
        ];
        let table_route = TableRoute {
            table: Some(Table {
                id: 1,
                table_name: Some(TableName {
                    catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                    schema_name: DEFAULT_SCHEMA_NAME.to_string(),
                    table_name: table.to_string(),
                }),
                table_schema: vec![],
            }),
            region_routes,
        };
        let value = TableRouteValue {
            peers,
            table_route: Some(table_route),
        };
        put_table_route_value(kv_store, &key, value.clone())
            .await
            .unwrap();
        (key, value)
    }

    pub(crate) fn new_region_route(
        region_number: u64,
        peers: &[Peer],
        leader_node: u64,
    ) -> RegionRoute {
        let region = Region {
            id: region_number,
            name: "".to_string(),
            partition: None,
            attrs: HashMap::new(),
        };
        let leader_peer_index = peers
            .iter()
            .enumerate()
            .find_map(|(i, peer)| {
                if peer.id == leader_node {
                    Some(i as u64)
                } else {
                    None
                }
            })
            .unwrap();
        RegionRoute {
            region: Some(region),
            leader_peer_index,
            follower_peer_indexes: vec![],
        }
    }

    #[tokio::test]
    async fn test_put_and_get_table_global_value() {
        let kv_store = Arc::new(MemStore::new()) as _;

        let not_exist_key = TableGlobalKey {
            catalog_name: "not_exist_catalog".to_string(),
            schema_name: "not_exist_schema".to_string(),
            table_name: "not_exist_table".to_string(),
        };
        assert!(get_table_global_value(&kv_store, &not_exist_key)
            .await
            .unwrap()
            .is_none());

        let (key, value) = prepare_table_global_value(&kv_store, "my_table").await;
        let actual = get_table_global_value(&kv_store, &key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(actual, value);

        let keys = vec![&not_exist_key, &key];
        let result = batch_get_table_global_value(&kv_store, keys).await.unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.get(&not_exist_key).unwrap().is_none());
        assert_eq!(result.get(&key).unwrap().as_ref().unwrap(), &value);
    }

    #[tokio::test]
    async fn test_put_and_get_table_route_value() {
        let kv_store = Arc::new(MemStore::new()) as _;

        let key = TableRouteKey {
            table_id: 1,
            catalog_name: "not_exist_catalog",
            schema_name: "not_exist_schema",
            table_name: "not_exist_table",
        };
        assert!(matches!(
            get_table_route_value(&kv_store, &key).await.unwrap_err(),
            error::Error::TableRouteNotFound { .. }
        ));

        let (key, value) = prepare_table_route_value(&kv_store, "my_table").await;
        let actual = get_table_route_value(&kv_store, &key).await.unwrap();
        assert_eq!(actual, value);
    }
}
