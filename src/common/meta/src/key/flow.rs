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

pub(crate) mod flow_info;
pub(crate) mod flow_name;
pub(crate) mod flownode_flow;
pub(crate) mod table_flow;

use std::ops::Deref;
use std::sync::Arc;

use common_telemetry::info;
use snafu::{ensure, OptionExt};

use self::flow_info::FlowInfoValue;
use crate::ensure_values;
use crate::error::{self, Result};
use crate::key::flow::flow_info::FlowInfoManager;
use crate::key::flow::flow_name::FlowNameManager;
use crate::key::flow::flownode_flow::FlownodeFlowManager;
use crate::key::flow::table_flow::TableFlowManager;
use crate::key::scope::MetaKey;
use crate::key::txn_helper::TxnOpGetResponseSet;
use crate::key::FlowId;
use crate::kv_backend::txn::Txn;
use crate::kv_backend::KvBackendRef;

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

    /// Returns a new [FlowTaskScoped] key.
    pub fn new(inner: T) -> FlowScoped<T> {
        Self { inner }
    }
}

impl<T: MetaKey<T>> MetaKey<FlowScoped<T>> for FlowScoped<T> {
    fn to_bytes(&self) -> Vec<u8> {
        let prefix = FlowScoped::<T>::PREFIX.as_bytes();
        let inner = self.inner.to_bytes();
        let mut bytes = Vec::with_capacity(prefix.len() + inner.len());
        bytes.extend(prefix);
        bytes.extend(inner);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<FlowScoped<T>> {
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
/// - Create metadata of the task.
/// - Retrieve metadata of the task.
/// - Delete metadata of the task.
pub struct FlowMetadataManager {
    flow_info_manager: FlowInfoManager,
    flownode_flow_manager: FlownodeFlowManager,
    table_flow_manager: TableFlowManager,
    flow_name_manager: FlowNameManager,
    kv_backend: KvBackendRef,
}

impl FlowMetadataManager {
    /// Returns a new [FlowTaskMetadataManager].
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self {
            flow_info_manager: FlowInfoManager::new(kv_backend.clone()),
            flow_name_manager: FlowNameManager::new(kv_backend.clone()),
            flownode_flow_manager: FlownodeFlowManager::new(kv_backend.clone()),
            table_flow_manager: TableFlowManager::new(kv_backend.clone()),
            kv_backend,
        }
    }

    /// Returns the [FlowNameManager].
    pub fn flow_name_manager(&self) -> &FlowNameManager {
        &self.flow_name_manager
    }

    /// Returns the [FlowTaskManager].
    pub fn flow_info_manager(&self) -> &FlowInfoManager {
        &self.flow_info_manager
    }

    /// Returns the [FlownodeFlowManager].
    pub fn flownode_flow_manager(&self) -> &FlownodeFlowManager {
        &self.flownode_flow_manager
    }

    /// Returns the [TableFlowManager].
    pub fn table_flow_manager(&self) -> &TableFlowManager {
        &self.table_flow_manager
    }

    /// Creates metadata for flow and returns an error if different metadata exists.
    pub async fn create_flow_metadata(
        &self,
        flow_id: FlowId,
        flow_value: FlowInfoValue,
    ) -> Result<()> {
        let (create_flow_flow_name_txn, on_create_flow_flow_name_failure) = self
            .flow_name_manager
            .build_create_txn(&flow_value.catalog_name, &flow_value.flow_name, flow_id)?;

        let (create_flow_txn, on_create_flow_failure) = self.flow_info_manager.build_create_txn(
            &flow_value.catalog_name,
            flow_id,
            &flow_value,
        )?;

        let create_flownode_task_txn = self.flownode_flow_manager.build_create_txn(
            &flow_value.catalog_name,
            flow_id,
            flow_value.flownode_ids().clone(),
        );

        let create_table_task_txn = self.table_flow_manager.build_create_txn(
            &flow_value.catalog_name,
            flow_id,
            flow_value.flownode_ids().clone(),
            flow_value.source_table_ids(),
        );

        let txn = Txn::merge_all(vec![
            create_flow_flow_name_txn,
            create_flow_txn,
            create_flownode_task_txn,
            create_table_task_txn,
        ]);
        info!(
            "Creating flow {}.{}({}), with {} txn operations",
            flow_value.catalog_name,
            flow_value.flow_name,
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
                    "Reads the empty flow name during the creating flow, flow_id: {flow_id}"
                ),
                    }
                })?;

            if remote_flow_flow_name.flow_id() != flow_id {
                info!(
                    "Trying to create flow {}.{}({}), but flow({}) already exists",
                    flow_value.catalog_name,
                    flow_value.flow_name,
                    flow_id,
                    remote_flow_flow_name.flow_id()
                );

                return error::TaskAlreadyExistsSnafu {
                    flow_name: format!("{}.{}", flow_value.catalog_name, flow_value.flow_name),
                }
                .fail();
            }

            let remote_flow =
                on_create_flow_failure(&mut set)?.with_context(|| error::UnexpectedSnafu {
                    err_msg: format!(
                        "Reads the empty flow during the creating flow, flow_id: {flow_id}"
                    ),
                })?;
            let op_name = "creating flow";
            ensure_values!(*remote_flow, flow_value, op_name);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::sync::Arc;

    use futures::TryStreamExt;

    use super::*;
    use crate::key::flow::table_flow::TableFlowKey;
    use crate::key::scope::CatalogScoped;
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::table_name::TableName;

    #[derive(Debug)]
    struct MockKey {
        inner: Vec<u8>,
    }

    impl MetaKey<MockKey> for MockKey {
        fn to_bytes(&self) -> Vec<u8> {
            self.inner.clone()
        }

        fn from_bytes(bytes: &[u8]) -> Result<MockKey> {
            Ok(MockKey {
                inner: bytes.to_vec(),
            })
        }
    }

    #[test]
    fn test_flow_scoped_to_bytes() {
        let key = FlowScoped::new(CatalogScoped::new(
            "my_catalog".to_string(),
            MockKey {
                inner: b"hi".to_vec(),
            },
        ));
        assert_eq!(b"__flow/my_catalog/hi".to_vec(), key.to_bytes());
    }

    #[test]
    fn test_flow_scoped_from_bytes() {
        let bytes = b"__flow/my_catalog/hi";
        let key = FlowScoped::<CatalogScoped<MockKey>>::from_bytes(bytes).unwrap();
        assert_eq!(key.catalog(), "my_catalog");
        assert_eq!(key.inner.inner, b"hi".to_vec());
    }

    #[test]
    fn test_flow_scoped_from_bytes_mismatch() {
        let bytes = b"__table/my_catalog/hi";
        let err = FlowScoped::<CatalogScoped<MockKey>>::from_bytes(bytes).unwrap_err();
        assert_matches!(err, error::Error::MismatchPrefix { .. });
    }

    #[tokio::test]
    async fn test_create_flow_metadata() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let flow_metadata_manager = FlowMetadataManager::new(mem_kv.clone());
        let task_id = 10;
        let catalog_name = "greptime";
        let sink_table_name = TableName {
            catalog_name: catalog_name.to_string(),
            schema_name: "my_schema".to_string(),
            table_name: "sink_table".to_string(),
        };
        let flow_value = FlowInfoValue {
            catalog_name: catalog_name.to_string(),
            flow_name: "task".to_string(),
            source_table_ids: vec![1024, 1025, 1026],
            sink_table_name,
            flownode_ids: [(0, 1u64)].into(),
            raw_sql: "raw".to_string(),
            expire_when: "expr".to_string(),
            comment: "hi".to_string(),
            options: Default::default(),
        };
        flow_metadata_manager
            .create_flow_metadata(task_id, flow_value.clone())
            .await
            .unwrap();
        // Creates again.
        flow_metadata_manager
            .create_flow_metadata(task_id, flow_value.clone())
            .await
            .unwrap();
        let got = flow_metadata_manager
            .flow_info_manager()
            .get(catalog_name, task_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got, flow_value);
        let tasks = flow_metadata_manager
            .flownode_flow_manager()
            .tasks(catalog_name, 1)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(tasks, vec![(task_id, 0)]);
        for table_id in [1024, 1025, 1026] {
            let nodes = flow_metadata_manager
                .table_flow_manager()
                .nodes(catalog_name, table_id)
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(
                nodes,
                vec![TableFlowKey::new(
                    catalog_name.to_string(),
                    table_id,
                    1,
                    task_id,
                    0
                )]
            );
        }
    }

    #[tokio::test]
    async fn test_create_table_metadata_task_exists_err() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let flow_metadata_manager = FlowMetadataManager::new(mem_kv);
        let task_id = 10;
        let catalog_name = "greptime";
        let sink_table_name = TableName {
            catalog_name: catalog_name.to_string(),
            schema_name: "my_schema".to_string(),
            table_name: "sink_table".to_string(),
        };
        let flow_value = FlowInfoValue {
            catalog_name: "greptime".to_string(),
            flow_name: "task".to_string(),
            source_table_ids: vec![1024, 1025, 1026],
            sink_table_name: sink_table_name.clone(),
            flownode_ids: [(0, 1u64)].into(),
            raw_sql: "raw".to_string(),
            expire_when: "expr".to_string(),
            comment: "hi".to_string(),
            options: Default::default(),
        };
        flow_metadata_manager
            .create_flow_metadata(task_id, flow_value.clone())
            .await
            .unwrap();
        // Creates again.
        let flow_value = FlowInfoValue {
            catalog_name: catalog_name.to_string(),
            flow_name: "task".to_string(),
            source_table_ids: vec![1024, 1025, 1026],
            sink_table_name,
            flownode_ids: [(0, 1u64)].into(),
            raw_sql: "raw".to_string(),
            expire_when: "expr".to_string(),
            comment: "hi".to_string(),
            options: Default::default(),
        };
        let err = flow_metadata_manager
            .create_flow_metadata(task_id + 1, flow_value)
            .await
            .unwrap_err();
        assert_matches!(err, error::Error::TaskAlreadyExists { .. });
    }

    #[tokio::test]
    async fn test_create_table_metadata_unexpected_err() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let flow_metadata_manager = FlowMetadataManager::new(mem_kv);
        let task_id = 10;
        let catalog_name = "greptime";
        let sink_table_name = TableName {
            catalog_name: catalog_name.to_string(),
            schema_name: "my_schema".to_string(),
            table_name: "sink_table".to_string(),
        };
        let flow_value = FlowInfoValue {
            catalog_name: "greptime".to_string(),
            flow_name: "task".to_string(),
            source_table_ids: vec![1024, 1025, 1026],
            sink_table_name: sink_table_name.clone(),
            flownode_ids: [(0, 1u64)].into(),
            raw_sql: "raw".to_string(),
            expire_when: "expr".to_string(),
            comment: "hi".to_string(),
            options: Default::default(),
        };
        flow_metadata_manager
            .create_flow_metadata(task_id, flow_value.clone())
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
            flow_name: "task".to_string(),
            source_table_ids: vec![1024, 1025, 1026],
            sink_table_name: another_sink_table_name,
            flownode_ids: [(0, 1u64)].into(),
            raw_sql: "raw".to_string(),
            expire_when: "expr".to_string(),
            comment: "hi".to_string(),
            options: Default::default(),
        };
        let err = flow_metadata_manager
            .create_flow_metadata(task_id, flow_value)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Reads the different value"));
    }
}
