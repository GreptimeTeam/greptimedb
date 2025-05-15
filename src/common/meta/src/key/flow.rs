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

pub mod flow_info;
pub(crate) mod flow_name;
pub(crate) mod flow_route;
pub mod flow_state;
mod flownode_addr_helper;
pub(crate) mod flownode_flow;
pub(crate) mod table_flow;
use std::ops::Deref;
use std::sync::Arc;

use common_telemetry::info;
use flow_route::{FlowRouteKey, FlowRouteManager, FlowRouteValue};
use snafu::{ensure, OptionExt};
use table_flow::TableFlowValue;

use self::flow_info::{FlowInfoKey, FlowInfoValue};
use self::flow_name::FlowNameKey;
use self::flownode_flow::FlownodeFlowKey;
use self::table_flow::TableFlowKey;
use crate::ensure_values;
use crate::error::{self, Result};
use crate::key::flow::flow_info::FlowInfoManager;
use crate::key::flow::flow_name::FlowNameManager;
use crate::key::flow::flow_state::FlowStateManager;
use crate::key::flow::flownode_flow::FlownodeFlowManager;
pub use crate::key::flow::table_flow::{TableFlowManager, TableFlowManagerRef};
use crate::key::txn_helper::TxnOpGetResponseSet;
use crate::key::{DeserializedValueWithBytes, FlowId, FlowPartitionId, MetadataKey};
use crate::kv_backend::txn::Txn;
use crate::kv_backend::KvBackendRef;
use crate::rpc::store::BatchDeleteRequest;

/// The key of `__flow/` scope.
#[derive(Debug, PartialEq)]
pub struct FlowScoped<T> {
    inner: T,
}

impl<T> Deref for FlowScoped<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> FlowScoped<T> {
    const PREFIX: &'static str = "__flow/";

    /// Returns a new [FlowScoped] key.
    pub fn new(inner: T) -> FlowScoped<T> {
        Self { inner }
    }
}

impl<'a, T: MetadataKey<'a, T>> MetadataKey<'a, FlowScoped<T>> for FlowScoped<T> {
    fn to_bytes(&self) -> Vec<u8> {
        let prefix = FlowScoped::<T>::PREFIX.as_bytes();
        let inner = self.inner.to_bytes();
        let mut bytes = Vec::with_capacity(prefix.len() + inner.len());
        bytes.extend(prefix);
        bytes.extend(inner);
        bytes
    }

    fn from_bytes(bytes: &'a [u8]) -> Result<FlowScoped<T>> {
        let prefix = FlowScoped::<T>::PREFIX.as_bytes();
        ensure!(
            bytes.starts_with(prefix),
            error::MismatchPrefixSnafu {
                prefix: String::from_utf8_lossy(prefix),
                key: String::from_utf8_lossy(bytes),
            }
        );
        let inner = T::from_bytes(&bytes[prefix.len()..])?;
        Ok(FlowScoped { inner })
    }
}

pub type FlowMetadataManagerRef = Arc<FlowMetadataManager>;

/// The manager of metadata, provides ability to:
/// - Create metadata of the flow.
/// - Retrieve metadata of the flow.
/// - Delete metadata of the flow.
pub struct FlowMetadataManager {
    flow_info_manager: FlowInfoManager,
    flow_route_manager: FlowRouteManager,
    flownode_flow_manager: FlownodeFlowManager,
    table_flow_manager: TableFlowManager,
    flow_name_manager: FlowNameManager,
    /// only metasrv have access to itself's memory backend, so for other case it should be None
    flow_state_manager: Option<FlowStateManager>,
    kv_backend: KvBackendRef,
}

impl FlowMetadataManager {
    /// Returns a new [`FlowMetadataManager`].
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self {
            flow_info_manager: FlowInfoManager::new(kv_backend.clone()),
            flow_route_manager: FlowRouteManager::new(kv_backend.clone()),
            flow_name_manager: FlowNameManager::new(kv_backend.clone()),
            flownode_flow_manager: FlownodeFlowManager::new(kv_backend.clone()),
            table_flow_manager: TableFlowManager::new(kv_backend.clone()),
            flow_state_manager: None,
            kv_backend,
        }
    }

    /// Returns the [`FlowNameManager`].
    pub fn flow_name_manager(&self) -> &FlowNameManager {
        &self.flow_name_manager
    }

    pub fn flow_state_manager(&self) -> Option<&FlowStateManager> {
        self.flow_state_manager.as_ref()
    }

    /// Returns the [`FlowInfoManager`].
    pub fn flow_info_manager(&self) -> &FlowInfoManager {
        &self.flow_info_manager
    }

    /// Returns the [`FlowRouteManager`].
    pub fn flow_route_manager(&self) -> &FlowRouteManager {
        &self.flow_route_manager
    }

    /// Returns the [`FlownodeFlowManager`].
    pub fn flownode_flow_manager(&self) -> &FlownodeFlowManager {
        &self.flownode_flow_manager
    }

    /// Returns the [`TableFlowManager`].
    pub fn table_flow_manager(&self) -> &TableFlowManager {
        &self.table_flow_manager
    }

    /// Creates metadata for flow and returns an error if different metadata exists.
    pub async fn create_flow_metadata(
        &self,
        flow_id: FlowId,
        flow_info: FlowInfoValue,
        flow_routes: Vec<(FlowPartitionId, FlowRouteValue)>,
    ) -> Result<()> {
        let (create_flow_flow_name_txn, on_create_flow_flow_name_failure) = self
            .flow_name_manager
            .build_create_txn(&flow_info.catalog_name, &flow_info.flow_name, flow_id)?;

        let (create_flow_txn, on_create_flow_failure) = self
            .flow_info_manager
            .build_create_txn(flow_id, &flow_info)?;

        let create_flow_routes_txn = self
            .flow_route_manager
            .build_create_txn(flow_id, flow_routes.clone())?;

        let create_flownode_flow_txn = self
            .flownode_flow_manager
            .build_create_txn(flow_id, flow_info.flownode_ids().clone());

        let create_table_flow_txn = self.table_flow_manager.build_create_txn(
            flow_id,
            flow_routes
                .into_iter()
                .map(|(partition_id, route)| (partition_id, TableFlowValue { peer: route.peer }))
                .collect(),
            flow_info.source_table_ids(),
        )?;

        let txn = Txn::merge_all(vec![
            create_flow_flow_name_txn,
            create_flow_txn,
            create_flow_routes_txn,
            create_flownode_flow_txn,
            create_table_flow_txn,
        ]);
        info!(
            "Creating flow {}.{}({}), with {} txn operations",
            flow_info.catalog_name,
            flow_info.flow_name,
            flow_id,
            txn.max_operations()
        );

        let mut resp = self.kv_backend.txn(txn).await?;
        if !resp.succeeded {
            let mut set = TxnOpGetResponseSet::from(&mut resp.responses);
            let remote_flow_flow_name =
                on_create_flow_flow_name_failure(&mut set)?.with_context(|| {
                    error::UnexpectedSnafu {
                        err_msg: format!(
                    "Reads the empty flow name in comparing operation of the creating flow, flow_id: {flow_id}"
                ),
                    }
                })?;

            if remote_flow_flow_name.flow_id() != flow_id {
                info!(
                    "Trying to create flow {}.{}({}), but flow({}) already exists",
                    flow_info.catalog_name,
                    flow_info.flow_name,
                    flow_id,
                    remote_flow_flow_name.flow_id()
                );

                return error::FlowAlreadyExistsSnafu {
                    flow_name: format!("{}.{}", flow_info.catalog_name, flow_info.flow_name),
                }
                .fail();
            }

            let remote_flow =
                on_create_flow_failure(&mut set)?.with_context(|| error::UnexpectedSnafu {
                    err_msg: format!(
                        "Reads the empty flow in comparing operation of creating flow, flow_id: {flow_id}"
                    ),
                })?;
            let op_name = "creating flow";
            ensure_values!(*remote_flow, flow_info, op_name);
        }

        Ok(())
    }

    /// Update metadata for flow and returns an error if old metadata IS NOT exists.
    pub async fn update_flow_metadata(
        &self,
        flow_id: FlowId,
        current_flow_info: &DeserializedValueWithBytes<FlowInfoValue>,
        new_flow_info: &FlowInfoValue,
        flow_routes: Vec<(FlowPartitionId, FlowRouteValue)>,
    ) -> Result<()> {
        let (update_flow_flow_name_txn, on_create_flow_flow_name_failure) =
            self.flow_name_manager.build_update_txn(
                &new_flow_info.catalog_name,
                &new_flow_info.flow_name,
                flow_id,
            )?;

        let (update_flow_txn, on_create_flow_failure) =
            self.flow_info_manager
                .build_update_txn(flow_id, current_flow_info, new_flow_info)?;

        let update_flow_routes_txn = self.flow_route_manager.build_update_txn(
            flow_id,
            current_flow_info,
            flow_routes.clone(),
        )?;

        let update_flownode_flow_txn = self.flownode_flow_manager.build_update_txn(
            flow_id,
            current_flow_info,
            new_flow_info.flownode_ids().clone(),
        );

        let update_table_flow_txn = self.table_flow_manager.build_update_txn(
            flow_id,
            current_flow_info,
            flow_routes
                .into_iter()
                .map(|(partition_id, route)| (partition_id, TableFlowValue { peer: route.peer }))
                .collect(),
            new_flow_info.source_table_ids(),
        )?;

        let txn = Txn::merge_all(vec![
            update_flow_flow_name_txn,
            update_flow_txn,
            update_flow_routes_txn,
            update_flownode_flow_txn,
            update_table_flow_txn,
        ]);
        info!(
            "Creating flow {}.{}({}), with {} txn operations",
            new_flow_info.catalog_name,
            new_flow_info.flow_name,
            flow_id,
            txn.max_operations()
        );

        let mut resp = self.kv_backend.txn(txn).await?;
        if !resp.succeeded {
            let mut set = TxnOpGetResponseSet::from(&mut resp.responses);
            let remote_flow_flow_name =
                on_create_flow_flow_name_failure(&mut set)?.with_context(|| {
                    error::UnexpectedSnafu {
                        err_msg: format!(
                        "Reads the empty flow name in comparing operation of the updating flow, flow_id: {flow_id}"
                    ),
                    }
                })?;

            if remote_flow_flow_name.flow_id() != flow_id {
                info!(
                    "Trying to updating flow {}.{}({}), but flow({}) already exists with a different flow id",
                    new_flow_info.catalog_name,
                    new_flow_info.flow_name,
                    flow_id,
                    remote_flow_flow_name.flow_id()
                );

                return error::UnexpectedSnafu {
                    err_msg: format!(
                        "Reads different flow id when updating flow({2}.{3}), prev flow id = {0}, updating with flow id = {1}",
                        remote_flow_flow_name.flow_id(),
                        flow_id,
                        new_flow_info.catalog_name,
                        new_flow_info.flow_name,
                    ),
                }.fail();
            }

            let remote_flow =
                on_create_flow_failure(&mut set)?.with_context(|| error::UnexpectedSnafu {
                    err_msg: format!(
                        "Reads the empty flow in comparing operation of the updating flow, flow_id: {flow_id}"
                    ),
                })?;
            let op_name = "updating flow";
            ensure_values!(*remote_flow, new_flow_info.clone(), op_name);
        }

        Ok(())
    }

    fn flow_metadata_keys(&self, flow_id: FlowId, flow_value: &FlowInfoValue) -> Vec<Vec<u8>> {
        let source_table_ids = flow_value.source_table_ids();
        let mut keys =
            Vec::with_capacity(2 + flow_value.flownode_ids.len() * (source_table_ids.len() + 2));
        // Builds flow name key
        let flow_name = FlowNameKey::new(&flow_value.catalog_name, &flow_value.flow_name);
        keys.push(flow_name.to_bytes());

        // Builds flow value key
        let flow_info_key = FlowInfoKey::new(flow_id);
        keys.push(flow_info_key.to_bytes());

        // Builds flownode flow keys & table flow keys
        flow_value
            .flownode_ids
            .iter()
            .for_each(|(&partition_id, &flownode_id)| {
                keys.push(FlownodeFlowKey::new(flownode_id, flow_id, partition_id).to_bytes());
                keys.push(FlowRouteKey::new(flow_id, partition_id).to_bytes());
                source_table_ids.iter().for_each(|&table_id| {
                    keys.push(
                        TableFlowKey::new(table_id, flownode_id, flow_id, partition_id).to_bytes(),
                    );
                })
            });
        keys
    }

    /// Deletes metadata for table **permanently**.
    pub async fn destroy_flow_metadata(
        &self,
        flow_id: FlowId,
        flow_value: &FlowInfoValue,
    ) -> Result<()> {
        let keys = self.flow_metadata_keys(flow_id, flow_value);
        let _ = self
            .kv_backend
            .batch_delete(BatchDeleteRequest::new().with_keys(keys))
            .await?;
        Ok(())
    }
}

impl std::fmt::Debug for FlowMetadataManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlowMetadataManager").finish()
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use futures::TryStreamExt;
    use table::metadata::TableId;
    use table::table_name::TableName;

    use super::*;
    use crate::key::flow::table_flow::TableFlowKey;
    use crate::key::FlowPartitionId;
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::peer::Peer;
    use crate::FlownodeId;

    #[derive(Debug)]
    struct MockKey {
        inner: Vec<u8>,
    }

    impl<'a> MetadataKey<'a, MockKey> for MockKey {
        fn to_bytes(&self) -> Vec<u8> {
            self.inner.clone()
        }

        fn from_bytes(bytes: &'a [u8]) -> Result<MockKey> {
            Ok(MockKey {
                inner: bytes.to_vec(),
            })
        }
    }

    #[test]
    fn test_flow_scoped_to_bytes() {
        let key = FlowScoped::new(MockKey {
            inner: b"hi".to_vec(),
        });
        assert_eq!(b"__flow/hi".to_vec(), key.to_bytes());
    }

    #[test]
    fn test_flow_scoped_from_bytes() {
        let bytes = b"__flow/hi";
        let key = FlowScoped::<MockKey>::from_bytes(bytes).unwrap();
        assert_eq!(key.inner.inner, b"hi".to_vec());
    }

    #[test]
    fn test_flow_scoped_from_bytes_mismatch() {
        let bytes = b"__table/hi";
        let err = FlowScoped::<MockKey>::from_bytes(bytes).unwrap_err();
        assert_matches!(err, error::Error::MismatchPrefix { .. });
    }

    fn test_flow_info_value(
        flow_name: &str,
        flownode_ids: BTreeMap<FlowPartitionId, FlownodeId>,
        source_table_ids: Vec<TableId>,
    ) -> FlowInfoValue {
        let catalog_name = "greptime";
        let sink_table_name = TableName {
            catalog_name: catalog_name.to_string(),
            schema_name: "my_schema".to_string(),
            table_name: "sink_table".to_string(),
        };
        FlowInfoValue {
            catalog_name: catalog_name.to_string(),
            query_context: None,
            flow_name: flow_name.to_string(),
            source_table_ids,
            sink_table_name,
            flownode_ids,
            raw_sql: "raw".to_string(),
            expire_after: Some(300),
            comment: "hi".to_string(),
            options: Default::default(),
            created_time: chrono::Utc::now(),
            updated_time: chrono::Utc::now(),
        }
    }

    #[tokio::test]
    async fn test_create_flow_metadata() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let flow_metadata_manager = FlowMetadataManager::new(mem_kv.clone());
        let flow_id = 10;
        let flow_value = test_flow_info_value(
            "flow",
            [(0, 1u64), (1, 2u64)].into(),
            vec![1024, 1025, 1026],
        );
        let flow_routes = vec![
            (
                1u32,
                FlowRouteValue {
                    peer: Peer::empty(1),
                },
            ),
            (
                2,
                FlowRouteValue {
                    peer: Peer::empty(2),
                },
            ),
        ];
        flow_metadata_manager
            .create_flow_metadata(flow_id, flow_value.clone(), flow_routes.clone())
            .await
            .unwrap();
        // Creates again.
        flow_metadata_manager
            .create_flow_metadata(flow_id, flow_value.clone(), flow_routes.clone())
            .await
            .unwrap();
        let got = flow_metadata_manager
            .flow_info_manager()
            .get(flow_id)
            .await
            .unwrap()
            .unwrap();
        let routes = flow_metadata_manager
            .flow_route_manager()
            .routes(flow_id)
            .await
            .unwrap();
        assert_eq!(
            routes,
            vec![
                (
                    FlowRouteKey::new(flow_id, 1),
                    FlowRouteValue {
                        peer: Peer::empty(1),
                    },
                ),
                (
                    FlowRouteKey::new(flow_id, 2),
                    FlowRouteValue {
                        peer: Peer::empty(2),
                    },
                ),
            ]
        );
        assert_eq!(got, flow_value);
        let flows = flow_metadata_manager
            .flownode_flow_manager()
            .flows(1)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(flows, vec![(flow_id, 0)]);
        for table_id in [1024, 1025, 1026] {
            let nodes = flow_metadata_manager
                .table_flow_manager()
                .flows(table_id)
                .await
                .unwrap();
            assert_eq!(
                nodes,
                vec![
                    (
                        TableFlowKey::new(table_id, 1, flow_id, 1),
                        TableFlowValue {
                            peer: Peer::empty(1)
                        }
                    ),
                    (
                        TableFlowKey::new(table_id, 2, flow_id, 2),
                        TableFlowValue {
                            peer: Peer::empty(2)
                        }
                    )
                ]
            );
        }
    }

    #[tokio::test]
    async fn test_create_flow_metadata_flow_exists_err() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let flow_metadata_manager = FlowMetadataManager::new(mem_kv);
        let flow_id = 10;
        let flow_value = test_flow_info_value("flow", [(0, 1u64)].into(), vec![1024, 1025, 1026]);
        let flow_routes = vec![
            (
                1u32,
                FlowRouteValue {
                    peer: Peer::empty(1),
                },
            ),
            (
                2,
                FlowRouteValue {
                    peer: Peer::empty(2),
                },
            ),
        ];
        flow_metadata_manager
            .create_flow_metadata(flow_id, flow_value.clone(), flow_routes.clone())
            .await
            .unwrap();
        // Creates again
        let err = flow_metadata_manager
            .create_flow_metadata(flow_id + 1, flow_value, flow_routes.clone())
            .await
            .unwrap_err();
        assert_matches!(err, error::Error::FlowAlreadyExists { .. });
    }

    #[tokio::test]
    async fn test_create_flow_metadata_unexpected_err() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let flow_metadata_manager = FlowMetadataManager::new(mem_kv);
        let flow_id = 10;
        let catalog_name = "greptime";
        let flow_value = test_flow_info_value("flow", [(0, 1u64)].into(), vec![1024, 1025, 1026]);
        let flow_routes = vec![
            (
                1u32,
                FlowRouteValue {
                    peer: Peer::empty(1),
                },
            ),
            (
                2,
                FlowRouteValue {
                    peer: Peer::empty(2),
                },
            ),
        ];
        flow_metadata_manager
            .create_flow_metadata(flow_id, flow_value.clone(), flow_routes.clone())
            .await
            .unwrap();
        // Creates again.
        let another_sink_table_name = TableName {
            catalog_name: catalog_name.to_string(),
            schema_name: "my_schema".to_string(),
            table_name: "another_sink_table".to_string(),
        };
        let flow_value = FlowInfoValue {
            catalog_name: "greptime".to_string(),
            query_context: None,
            flow_name: "flow".to_string(),
            source_table_ids: vec![1024, 1025, 1026],
            sink_table_name: another_sink_table_name,
            flownode_ids: [(0, 1u64)].into(),
            raw_sql: "raw".to_string(),
            expire_after: Some(300),
            comment: "hi".to_string(),
            options: Default::default(),
            created_time: chrono::Utc::now(),
            updated_time: chrono::Utc::now(),
        };
        let err = flow_metadata_manager
            .create_flow_metadata(flow_id, flow_value, flow_routes.clone())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Reads the different value"));
    }

    #[tokio::test]
    async fn test_destroy_flow_metadata() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let flow_metadata_manager = FlowMetadataManager::new(mem_kv.clone());
        let flow_id = 10;
        let flow_value = test_flow_info_value("flow", [(0, 1u64)].into(), vec![1024, 1025, 1026]);
        let flow_routes = vec![(
            0u32,
            FlowRouteValue {
                peer: Peer::empty(1),
            },
        )];
        flow_metadata_manager
            .create_flow_metadata(flow_id, flow_value.clone(), flow_routes.clone())
            .await
            .unwrap();

        flow_metadata_manager
            .destroy_flow_metadata(flow_id, &flow_value)
            .await
            .unwrap();
        // Destroys again
        flow_metadata_manager
            .destroy_flow_metadata(flow_id, &flow_value)
            .await
            .unwrap();
        // Ensures all keys are deleted
        assert!(mem_kv.is_empty())
    }

    #[tokio::test]
    async fn test_update_flow_metadata() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let flow_metadata_manager = FlowMetadataManager::new(mem_kv.clone());
        let flow_id = 10;
        let flow_value = test_flow_info_value(
            "flow",
            [(0, 1u64), (1, 2u64)].into(),
            vec![1024, 1025, 1026],
        );
        let flow_routes = vec![
            (
                1u32,
                FlowRouteValue {
                    peer: Peer::empty(1),
                },
            ),
            (
                2,
                FlowRouteValue {
                    peer: Peer::empty(2),
                },
            ),
        ];
        flow_metadata_manager
            .create_flow_metadata(flow_id, flow_value.clone(), flow_routes.clone())
            .await
            .unwrap();

        let new_flow_value = {
            let mut tmp = flow_value.clone();
            tmp.raw_sql = "new".to_string();
            tmp
        };

        // Update flow instead
        flow_metadata_manager
            .update_flow_metadata(
                flow_id,
                &DeserializedValueWithBytes::from_inner(flow_value.clone()),
                &new_flow_value,
                flow_routes.clone(),
            )
            .await
            .unwrap();

        let got = flow_metadata_manager
            .flow_info_manager()
            .get(flow_id)
            .await
            .unwrap()
            .unwrap();
        let routes = flow_metadata_manager
            .flow_route_manager()
            .routes(flow_id)
            .await
            .unwrap();
        assert_eq!(
            routes,
            vec![
                (
                    FlowRouteKey::new(flow_id, 1),
                    FlowRouteValue {
                        peer: Peer::empty(1),
                    },
                ),
                (
                    FlowRouteKey::new(flow_id, 2),
                    FlowRouteValue {
                        peer: Peer::empty(2),
                    },
                ),
            ]
        );
        assert_eq!(got, new_flow_value);
        let flows = flow_metadata_manager
            .flownode_flow_manager()
            .flows(1)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(flows, vec![(flow_id, 0)]);
        for table_id in [1024, 1025, 1026] {
            let nodes = flow_metadata_manager
                .table_flow_manager()
                .flows(table_id)
                .await
                .unwrap();
            assert_eq!(
                nodes,
                vec![
                    (
                        TableFlowKey::new(table_id, 1, flow_id, 1),
                        TableFlowValue {
                            peer: Peer::empty(1)
                        }
                    ),
                    (
                        TableFlowKey::new(table_id, 2, flow_id, 2),
                        TableFlowValue {
                            peer: Peer::empty(2)
                        }
                    )
                ]
            );
        }
    }

    #[tokio::test]
    async fn test_update_flow_metadata_diff_flownode() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let flow_metadata_manager = FlowMetadataManager::new(mem_kv.clone());
        let flow_id = 10;
        let flow_value = test_flow_info_value(
            "flow",
            [(0u32, 1u64), (1u32, 2u64)].into(),
            vec![1024, 1025, 1026],
        );
        let flow_routes = vec![
            (
                0u32,
                FlowRouteValue {
                    peer: Peer::empty(1),
                },
            ),
            (
                1,
                FlowRouteValue {
                    peer: Peer::empty(2),
                },
            ),
        ];
        flow_metadata_manager
            .create_flow_metadata(flow_id, flow_value.clone(), flow_routes.clone())
            .await
            .unwrap();

        let new_flow_value = {
            let mut tmp = flow_value.clone();
            tmp.raw_sql = "new".to_string();
            // move to different flownodes
            tmp.flownode_ids = [(0, 3u64), (1, 4u64)].into();
            tmp
        };
        let new_flow_routes = vec![
            (
                0u32,
                FlowRouteValue {
                    peer: Peer::empty(3),
                },
            ),
            (
                1,
                FlowRouteValue {
                    peer: Peer::empty(4),
                },
            ),
        ];

        // Update flow instead
        flow_metadata_manager
            .update_flow_metadata(
                flow_id,
                &DeserializedValueWithBytes::from_inner(flow_value.clone()),
                &new_flow_value,
                new_flow_routes.clone(),
            )
            .await
            .unwrap();

        let got = flow_metadata_manager
            .flow_info_manager()
            .get(flow_id)
            .await
            .unwrap()
            .unwrap();
        let routes = flow_metadata_manager
            .flow_route_manager()
            .routes(flow_id)
            .await
            .unwrap();
        assert_eq!(
            routes,
            vec![
                (
                    FlowRouteKey::new(flow_id, 0),
                    FlowRouteValue {
                        peer: Peer::empty(3),
                    },
                ),
                (
                    FlowRouteKey::new(flow_id, 1),
                    FlowRouteValue {
                        peer: Peer::empty(4),
                    },
                ),
            ]
        );
        assert_eq!(got, new_flow_value);

        let flows = flow_metadata_manager
            .flownode_flow_manager()
            .flows(1)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        // should moved to different flownode
        assert_eq!(flows, vec![]);

        let flows = flow_metadata_manager
            .flownode_flow_manager()
            .flows(3)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(flows, vec![(flow_id, 0)]);

        for table_id in [1024, 1025, 1026] {
            let nodes = flow_metadata_manager
                .table_flow_manager()
                .flows(table_id)
                .await
                .unwrap();
            assert_eq!(
                nodes,
                vec![
                    (
                        TableFlowKey::new(table_id, 3, flow_id, 0),
                        TableFlowValue {
                            peer: Peer::empty(3)
                        }
                    ),
                    (
                        TableFlowKey::new(table_id, 4, flow_id, 1),
                        TableFlowValue {
                            peer: Peer::empty(4)
                        }
                    )
                ]
            );
        }
    }

    #[tokio::test]
    async fn test_update_flow_metadata_flow_replace_diff_id_err() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let flow_metadata_manager = FlowMetadataManager::new(mem_kv);
        let flow_id = 10;
        let flow_value = test_flow_info_value("flow", [(0, 1u64)].into(), vec![1024, 1025, 1026]);
        let flow_routes = vec![
            (
                1u32,
                FlowRouteValue {
                    peer: Peer::empty(1),
                },
            ),
            (
                2,
                FlowRouteValue {
                    peer: Peer::empty(2),
                },
            ),
        ];
        flow_metadata_manager
            .create_flow_metadata(flow_id, flow_value.clone(), flow_routes.clone())
            .await
            .unwrap();
        // update again with same flow id
        flow_metadata_manager
            .update_flow_metadata(
                flow_id,
                &DeserializedValueWithBytes::from_inner(flow_value.clone()),
                &flow_value,
                flow_routes.clone(),
            )
            .await
            .unwrap();
        // update again with wrong flow id, expected error
        let err = flow_metadata_manager
            .update_flow_metadata(
                flow_id + 1,
                &DeserializedValueWithBytes::from_inner(flow_value.clone()),
                &flow_value,
                flow_routes,
            )
            .await
            .unwrap_err();
        assert_matches!(err, error::Error::Unexpected { .. });
        assert!(err
            .to_string()
            .contains("Reads different flow id when updating flow"));
    }

    #[tokio::test]
    async fn test_update_flow_metadata_unexpected_err_prev_value_diff() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let flow_metadata_manager = FlowMetadataManager::new(mem_kv);
        let flow_id = 10;
        let catalog_name = "greptime";
        let flow_value = test_flow_info_value("flow", [(0, 1u64)].into(), vec![1024, 1025, 1026]);
        let flow_routes = vec![
            (
                1u32,
                FlowRouteValue {
                    peer: Peer::empty(1),
                },
            ),
            (
                2,
                FlowRouteValue {
                    peer: Peer::empty(2),
                },
            ),
        ];
        flow_metadata_manager
            .create_flow_metadata(flow_id, flow_value.clone(), flow_routes.clone())
            .await
            .unwrap();
        // Creates again.
        let another_sink_table_name = TableName {
            catalog_name: catalog_name.to_string(),
            schema_name: "my_schema".to_string(),
            table_name: "another_sink_table".to_string(),
        };
        let flow_value = FlowInfoValue {
            catalog_name: "greptime".to_string(),
            query_context: None,
            flow_name: "flow".to_string(),
            source_table_ids: vec![1024, 1025, 1026],
            sink_table_name: another_sink_table_name,
            flownode_ids: [(0, 1u64)].into(),
            raw_sql: "raw".to_string(),
            expire_after: Some(300),
            comment: "hi".to_string(),
            options: Default::default(),
            created_time: chrono::Utc::now(),
            updated_time: chrono::Utc::now(),
        };
        let err = flow_metadata_manager
            .update_flow_metadata(
                flow_id,
                &DeserializedValueWithBytes::from_inner(flow_value.clone()),
                &flow_value,
                flow_routes.clone(),
            )
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("Reads the different value"),
            "error: {:?}",
            err
        );
    }
}
