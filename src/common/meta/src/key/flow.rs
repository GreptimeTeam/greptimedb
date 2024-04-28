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
pub(crate) mod flownode_task;
pub(crate) mod table_task;

use std::ops::Deref;
use std::sync::Arc;

use common_telemetry::info;
use snafu::{ensure, OptionExt};

use self::flow_info::FlowTaskValue;
use crate::ensure_values;
use crate::error::{self, Result};
use crate::key::flow::flow_info::FlowTaskManager;
use crate::key::flow::flow_name::FlowNameManager;
use crate::key::flow::flownode_task::FlownodeTaskManager;
use crate::key::flow::table_task::TableTaskManager;
use crate::key::scope::MetaKey;
use crate::key::txn_helper::TxnOpGetResponseSet;
use crate::key::FlowTaskId;
use crate::kv_backend::txn::Txn;
use crate::kv_backend::KvBackendRef;

/// The key of `__flow_task/` scope.
#[derive(Debug, PartialEq)]
pub struct FlowTaskScoped<T> {
    inner: T,
}

impl<T> Deref for FlowTaskScoped<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> FlowTaskScoped<T> {
    const PREFIX: &'static str = "__flow_task/";

    /// Returns a new [FlowTaskScoped] key.
    pub fn new(inner: T) -> FlowTaskScoped<T> {
        Self { inner }
    }
}

impl<T: MetaKey<T>> MetaKey<FlowTaskScoped<T>> for FlowTaskScoped<T> {
    fn to_bytes(&self) -> Vec<u8> {
        let prefix = FlowTaskScoped::<T>::PREFIX.as_bytes();
        let inner = self.inner.to_bytes();
        let mut bytes = Vec::with_capacity(prefix.len() + inner.len());
        bytes.extend(prefix);
        bytes.extend(inner);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<FlowTaskScoped<T>> {
        let prefix = FlowTaskScoped::<T>::PREFIX.as_bytes();
        ensure!(
            bytes.starts_with(prefix),
            error::MismatchPrefixSnafu {
                prefix: String::from_utf8_lossy(prefix),
                key: String::from_utf8_lossy(bytes),
            }
        );
        let inner = T::from_bytes(&bytes[prefix.len()..])?;
        Ok(FlowTaskScoped { inner })
    }
}

pub type FlowTaskMetadataManagerRef = Arc<FlowMetadataManager>;

/// The manager of metadata, provides ability to:
/// - Create metadata of the task.
/// - Retrieve metadata of the task.
/// - Delete metadata of the task.
pub struct FlowMetadataManager {
    flow_task_info_manager: FlowTaskManager,
    flownode_task_manager: FlownodeTaskManager,
    table_task_manager: TableTaskManager,
    flow_task_name_manager: FlowNameManager,
    kv_backend: KvBackendRef,
}

impl FlowMetadataManager {
    /// Returns a new [FlowTaskMetadataManager].
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self {
            flow_task_info_manager: FlowTaskManager::new(kv_backend.clone()),
            flow_task_name_manager: FlowNameManager::new(kv_backend.clone()),
            flownode_task_manager: FlownodeTaskManager::new(kv_backend.clone()),
            table_task_manager: TableTaskManager::new(kv_backend.clone()),
            kv_backend,
        }
    }

    /// Returns the [FlowNameManager].
    pub fn flow_task_name_manager(&self) -> &FlowNameManager {
        &self.flow_task_name_manager
    }

    /// Returns the [FlowTaskManager].
    pub fn flow_task_info_manager(&self) -> &FlowTaskManager {
        &self.flow_task_info_manager
    }

    /// Returns the [FlownodeTaskManager].
    pub fn flownode_task_manager(&self) -> &FlownodeTaskManager {
        &self.flownode_task_manager
    }

    /// Returns the [TableTaskManager].
    pub fn table_task_manager(&self) -> &TableTaskManager {
        &self.table_task_manager
    }

    /// Creates metadata for task and returns an error if different metadata exists.
    pub async fn create_flow_task_metadata(
        &self,
        flow_task_id: FlowTaskId,
        flow_task_value: FlowTaskValue,
    ) -> Result<()> {
        let (create_flow_task_name_txn, on_create_flow_task_name_failure) =
            self.flow_task_name_manager.build_create_txn(
                &flow_task_value.catalog_name,
                &flow_task_value.flow_name,
                flow_task_id,
            )?;

        let (create_flow_task_txn, on_create_flow_task_failure) =
            self.flow_task_info_manager.build_create_txn(
                &flow_task_value.catalog_name,
                flow_task_id,
                &flow_task_value,
            )?;

        let create_flownode_task_txn = self.flownode_task_manager.build_create_txn(
            &flow_task_value.catalog_name,
            flow_task_id,
            flow_task_value.flownode_ids().clone(),
        );

        let create_table_task_txn = self.table_task_manager.build_create_txn(
            &flow_task_value.catalog_name,
            flow_task_id,
            flow_task_value.flownode_ids().clone(),
            flow_task_value.source_table_ids(),
        );

        let txn = Txn::merge_all(vec![
            create_flow_task_name_txn,
            create_flow_task_txn,
            create_flownode_task_txn,
            create_table_task_txn,
        ]);
        info!(
            "Creating flow task {}.{}({}), with {} txn operations",
            flow_task_value.catalog_name,
            flow_task_value.flow_name,
            flow_task_id,
            txn.max_operations()
        );

        let mut resp = self.kv_backend.txn(txn).await?;
        if !resp.succeeded {
            let mut set = TxnOpGetResponseSet::from(&mut resp.responses);
            let remote_flow_task_name = on_create_flow_task_name_failure(&mut set)?
                .with_context(||error::UnexpectedSnafu {
                    err_msg: format!(
                    "Reads the empty flow task name during the creating flow task, flow_task_id: {flow_task_id}"
                ),
                })?;

            if remote_flow_task_name.flow_task_id() != flow_task_id {
                info!(
                    "Trying to create flow task {}.{}({}), but flow task({}) already exists",
                    flow_task_value.catalog_name,
                    flow_task_value.flow_name,
                    flow_task_id,
                    remote_flow_task_name.flow_task_id()
                );

                return error::TaskAlreadyExistsSnafu {
                    task_name: format!(
                        "{}.{}",
                        flow_task_value.catalog_name, flow_task_value.flow_name
                    ),
                }
                .fail();
            }

            let remote_flow_task = on_create_flow_task_failure(&mut set)?.with_context(|| {
                error::UnexpectedSnafu {
                    err_msg: format!(
                    "Reads the empty flow task during the creating flow task, flow_task_id: {flow_task_id}"
                ),
                }
            })?;
            let op_name = "creating flow task";
            ensure_values!(*remote_flow_task, flow_task_value, op_name);
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
    use crate::key::flow::table_task::TableTaskKey;
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
        let key = FlowTaskScoped::new(CatalogScoped::new(
            "my_catalog".to_string(),
            MockKey {
                inner: b"hi".to_vec(),
            },
        ));
        assert_eq!(b"__flow_task/my_catalog/hi".to_vec(), key.to_bytes());
    }

    #[test]
    fn test_flow_scoped_from_bytes() {
        let bytes = b"__flow_task/my_catalog/hi";
        let key = FlowTaskScoped::<CatalogScoped<MockKey>>::from_bytes(bytes).unwrap();
        assert_eq!(key.catalog(), "my_catalog");
        assert_eq!(key.inner.inner, b"hi".to_vec());
    }

    #[test]
    fn test_flow_scoped_from_bytes_mismatch() {
        let bytes = b"__table/my_catalog/hi";
        let err = FlowTaskScoped::<CatalogScoped<MockKey>>::from_bytes(bytes).unwrap_err();
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
        let flow_task_value = FlowTaskValue {
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
            .create_flow_task_metadata(task_id, flow_task_value.clone())
            .await
            .unwrap();
        // Creates again.
        flow_metadata_manager
            .create_flow_task_metadata(task_id, flow_task_value.clone())
            .await
            .unwrap();
        let got = flow_metadata_manager
            .flow_task_info_manager()
            .get(catalog_name, task_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got, flow_task_value);
        let tasks = flow_metadata_manager
            .flownode_task_manager()
            .tasks(catalog_name, 1)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(tasks, vec![(task_id, 0)]);
        for table_id in [1024, 1025, 1026] {
            let nodes = flow_metadata_manager
                .table_task_manager()
                .nodes(catalog_name, table_id)
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(
                nodes,
                vec![TableTaskKey::new(
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
        let flow_task_value = FlowTaskValue {
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
            .create_flow_task_metadata(task_id, flow_task_value.clone())
            .await
            .unwrap();
        // Creates again.
        let flow_task_value = FlowTaskValue {
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
            .create_flow_task_metadata(task_id + 1, flow_task_value)
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
        let flow_task_value = FlowTaskValue {
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
            .create_flow_task_metadata(task_id, flow_task_value.clone())
            .await
            .unwrap();
        // Creates again.
        let another_sink_table_name = TableName {
            catalog_name: catalog_name.to_string(),
            schema_name: "my_schema".to_string(),
            table_name: "another_sink_table".to_string(),
        };
        let flow_task_value = FlowTaskValue {
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
            .create_flow_task_metadata(task_id, flow_task_value)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Reads the different value"));
    }
}
