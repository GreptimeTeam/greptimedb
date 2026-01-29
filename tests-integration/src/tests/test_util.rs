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
use std::env;
use std::sync::Arc;

use async_trait::async_trait;
use client::OutputData;
use common_meta::DatanodeId;
use common_meta::kv_backend::KvBackendRef;
use common_meta::range_stream::{DEFAULT_PAGE_SIZE, PaginationStream};
use common_meta::rpc::KeyValue;
use common_meta::rpc::store::{BatchPutRequest, DeleteRangeRequest, RangeRequest};
use common_procedure::{ProcedureId, ProcedureManagerRef, watcher};
use common_query::Output;
use common_recordbatch::util;
use common_telemetry::tracing::info;
use common_telemetry::warn;
use common_test_util::find_workspace_path;
use common_wal::config::kafka::common::{KafkaConnectionConfig, KafkaTopicConfig};
use common_wal::config::kafka::{DatanodeKafkaConfig, MetasrvKafkaConfig};
use common_wal::config::{DatanodeWalConfig, MetasrvWalConfig};
use datanode::datanode::Datanode;
use frontend::instance::Instance;
use futures::TryStreamExt;
use meta_srv::metasrv::Metasrv;
use rstest_reuse::{self, template};
use servers::query_handler::sql::SqlQueryHandler;
use session::context::{QueryContext, QueryContextRef};

use crate::cluster::{GreptimeDbCluster, GreptimeDbClusterBuilder};
use crate::standalone::{GreptimeDbStandalone, GreptimeDbStandaloneBuilder};
use crate::test_util::StorageType;
use crate::tests::{MockDistributedInstance, create_distributed_instance};

#[async_trait::async_trait]
pub(crate) trait RebuildableMockInstance: MockInstance {
    // Rebuilds the instance and returns rebuilt frontend instance.
    async fn rebuild(&mut self);
}

#[async_trait]
pub trait MockInstance: Sync + Send {
    fn frontend(&self) -> Arc<Instance>;

    fn is_distributed_mode(&self) -> bool;
}

impl MockInstance for GreptimeDbStandalone {
    fn frontend(&self) -> Arc<Instance> {
        self.fe_instance().clone()
    }

    fn is_distributed_mode(&self) -> bool {
        false
    }
}

impl MockInstance for MockDistributedInstance {
    fn frontend(&self) -> Arc<Instance> {
        self.frontend()
    }

    fn is_distributed_mode(&self) -> bool {
        true
    }
}

/// For test purpose.
#[allow(clippy::large_enum_variant)]
pub(crate) enum MockInstanceBuilder {
    Standalone(GreptimeDbStandaloneBuilder),
    Distributed(GreptimeDbClusterBuilder),
}

pub(crate) enum MockInstanceImpl {
    Standalone(GreptimeDbStandalone),
    Distributed(GreptimeDbCluster),
}

impl MockInstanceImpl {
    pub(crate) fn metasrv(&self) -> &Arc<Metasrv> {
        match self {
            MockInstanceImpl::Standalone(_) => unreachable!(),
            MockInstanceImpl::Distributed(instance) => &instance.metasrv,
        }
    }

    pub(crate) fn datanodes(&self) -> &HashMap<DatanodeId, Datanode> {
        match self {
            MockInstanceImpl::Standalone(_) => unreachable!(),
            MockInstanceImpl::Distributed(instance) => &instance.datanode_instances,
        }
    }
}

impl MockInstance for MockInstanceImpl {
    fn frontend(&self) -> Arc<Instance> {
        match self {
            MockInstanceImpl::Standalone(instance) => instance.frontend(),
            MockInstanceImpl::Distributed(instance) => instance.fe_instance().clone(),
        }
    }

    fn is_distributed_mode(&self) -> bool {
        matches!(self, &MockInstanceImpl::Distributed(_))
    }
}

impl MockInstanceBuilder {
    async fn build(&self) -> MockInstanceImpl {
        match self {
            MockInstanceBuilder::Standalone(builder) => {
                MockInstanceImpl::Standalone(builder.build().await)
            }
            MockInstanceBuilder::Distributed(builder) => {
                MockInstanceImpl::Distributed(builder.build(false).await)
            }
        }
    }

    async fn rebuild(&self, instance: MockInstanceImpl) -> MockInstanceImpl {
        match self {
            MockInstanceBuilder::Standalone(builder) => {
                let MockInstanceImpl::Standalone(instance) = instance else {
                    unreachable!()
                };
                let GreptimeDbStandalone {
                    opts,
                    guard,
                    kv_backend,
                    procedure_manager,
                    ..
                } = instance;
                MockInstanceImpl::Standalone(
                    builder
                        .build_with(kv_backend, guard, opts, procedure_manager, false)
                        .await,
                )
            }
            MockInstanceBuilder::Distributed(builder) => {
                let MockInstanceImpl::Distributed(instance) = instance else {
                    unreachable!()
                };
                let GreptimeDbCluster {
                    guards,
                    datanode_options,
                    mut datanode_instances,
                    ..
                } = instance;
                for (id, instance) in datanode_instances.iter_mut() {
                    instance
                        .shutdown()
                        .await
                        .unwrap_or_else(|_| panic!("Failed to shutdown datanode {}", id));
                }

                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                MockInstanceImpl::Distributed(
                    builder.build_with(datanode_options, false, guards).await,
                )
            }
        }
    }
}

pub(crate) struct TestContext {
    instance: Option<MockInstanceImpl>,
    builder: MockInstanceBuilder,
}

impl TestContext {
    pub(crate) async fn new(builder: MockInstanceBuilder) -> Self {
        let instance = builder.build().await;

        Self {
            instance: Some(instance),
            builder,
        }
    }

    pub(crate) fn metasrv(&self) -> &Arc<Metasrv> {
        self.instance.as_ref().unwrap().metasrv()
    }

    pub(crate) fn frontend(&self) -> Arc<Instance> {
        self.instance.as_ref().unwrap().frontend()
    }

    pub(crate) fn datanodes(&self) -> &HashMap<DatanodeId, Datanode> {
        self.instance.as_ref().unwrap().datanodes()
    }
}

#[async_trait::async_trait]
impl RebuildableMockInstance for TestContext {
    async fn rebuild(&mut self) {
        info!("Rebuilding the instance");
        let instance = self.builder.rebuild(self.instance.take().unwrap()).await;
        self.instance = Some(instance);
    }
}

impl MockInstance for TestContext {
    fn frontend(&self) -> Arc<Instance> {
        self.instance.as_ref().unwrap().frontend()
    }

    fn is_distributed_mode(&self) -> bool {
        self.instance.as_ref().unwrap().is_distributed_mode()
    }
}

pub(crate) async fn standalone() -> Arc<dyn MockInstance> {
    let test_name = uuid::Uuid::new_v4().to_string();
    let instance = GreptimeDbStandaloneBuilder::new(&test_name).build().await;
    Arc::new(instance)
}

pub(crate) async fn distributed() -> Arc<dyn MockInstance> {
    let test_name = uuid::Uuid::new_v4().to_string();
    let instance = create_distributed_instance(&test_name).await;
    Arc::new(instance)
}

pub(crate) async fn standalone_with_multiple_object_stores() -> Arc<dyn MockInstance> {
    let _ = dotenv::dotenv();
    let test_name = uuid::Uuid::new_v4().to_string();
    let mut storage_types = StorageType::build_storage_types_based_on_env();
    // File is the default storage, remove it to avoid panic
    storage_types.retain(|x| *x != StorageType::File);

    let instance = GreptimeDbStandaloneBuilder::new(&test_name)
        .with_store_providers(storage_types)
        .build()
        .await;
    Arc::new(instance)
}

pub(crate) async fn distributed_with_multiple_object_stores() -> Arc<dyn MockInstance> {
    let _ = dotenv::dotenv();
    let test_name = uuid::Uuid::new_v4().to_string();
    let mut storage_types = StorageType::build_storage_types_based_on_env();
    // File is the default storage, remove it to avoid panic
    storage_types.retain(|x| *x != StorageType::File);

    let cluster = GreptimeDbClusterBuilder::new(&test_name)
        .await
        .with_store_providers(storage_types)
        .build(false)
        .await;
    Arc::new(MockDistributedInstance(cluster))
}

pub(crate) async fn standalone_with_kafka_wal() -> Option<Box<dyn RebuildableMockInstance>> {
    let _ = dotenv::dotenv();
    let endpoints = env::var("GT_KAFKA_ENDPOINTS").unwrap_or_default();
    common_telemetry::init_default_ut_logging();
    if endpoints.is_empty() {
        warn!("The endpoints is empty, skipping the test");
        return None;
    }

    let endpoints = endpoints
        .split(',')
        .map(|s| s.trim().to_string())
        .collect::<Vec<_>>();
    let test_name = uuid::Uuid::new_v4().to_string();
    let builder = GreptimeDbStandaloneBuilder::new(&test_name)
        .with_datanode_wal_config(DatanodeWalConfig::Kafka(DatanodeKafkaConfig {
            connection: KafkaConnectionConfig {
                broker_endpoints: endpoints.clone(),
                ..Default::default()
            },
            ..Default::default()
        }))
        .with_metasrv_wal_config(MetasrvWalConfig::Kafka(MetasrvKafkaConfig {
            connection: KafkaConnectionConfig {
                broker_endpoints: endpoints,
                ..Default::default()
            },
            kafka_topic: KafkaTopicConfig {
                topic_name_prefix: test_name.clone(),
                num_topics: 3,
                ..Default::default()
            },
            ..Default::default()
        }));
    let instance = TestContext::new(MockInstanceBuilder::Standalone(builder)).await;
    Some(Box::new(instance))
}

pub(crate) async fn distributed_with_kafka_wal() -> Option<Box<dyn RebuildableMockInstance>> {
    let _ = dotenv::dotenv();
    let endpoints = env::var("GT_KAFKA_ENDPOINTS").unwrap_or_default();
    common_telemetry::init_default_ut_logging();
    if endpoints.is_empty() {
        warn!("The endpoints is empty, skipping the test");
        return None;
    }

    let endpoints = endpoints
        .split(',')
        .map(|s| s.trim().to_string())
        .collect::<Vec<_>>();
    let test_name = uuid::Uuid::new_v4().to_string();
    let builder = GreptimeDbClusterBuilder::new(&test_name)
        .await
        .with_datanode_wal_config(DatanodeWalConfig::Kafka(DatanodeKafkaConfig {
            connection: KafkaConnectionConfig {
                broker_endpoints: endpoints.clone(),
                ..Default::default()
            },
            ..Default::default()
        }))
        .with_metasrv_wal_config(MetasrvWalConfig::Kafka(MetasrvKafkaConfig {
            connection: KafkaConnectionConfig {
                broker_endpoints: endpoints,
                ..Default::default()
            },
            kafka_topic: KafkaTopicConfig {
                topic_name_prefix: test_name.clone(),
                num_topics: 3,
                ..Default::default()
            },
            ..Default::default()
        }));
    let instance = TestContext::new(MockInstanceBuilder::Distributed(builder)).await;
    Some(Box::new(instance))
}

#[template]
#[rstest]
#[case::test_with_standalone(standalone_with_kafka_wal())]
#[case::test_with_distributed(distributed_with_kafka_wal())]
#[awt]
#[tokio::test(flavor = "multi_thread")]
pub(crate) fn both_instances_cases_with_kafka_wal(
    #[future]
    #[case]
    rebuildable_instance: Option<Box<dyn RebuildableMockInstance>>,
) {
}

#[template]
#[rstest]
#[case::test_with_standalone(standalone_with_multiple_object_stores())]
#[case::test_with_distributed(distributed_with_multiple_object_stores())]
#[awt]
#[tokio::test(flavor = "multi_thread")]
pub(crate) fn both_instances_cases_with_custom_storages(
    #[future]
    #[case]
    instance: Arc<dyn MockInstance>,
) {
}

#[template]
#[rstest]
#[case::test_with_standalone(standalone())]
#[case::test_with_distributed(distributed())]
#[awt]
#[tokio::test(flavor = "multi_thread")]
pub(crate) fn both_instances_cases(
    #[future]
    #[case]
    instance: Arc<dyn MockInstance>,
) {
}

#[template]
#[rstest]
#[case::test_with_standalone(standalone())]
#[awt]
#[tokio::test(flavor = "multi_thread")]
pub(crate) fn standalone_instance_case(
    #[future]
    #[case]
    instance: Arc<dyn MockInstance>,
) {
}

pub(crate) async fn check_unordered_output_stream(output: Output, expected: &str) {
    let sort_table = |table: &str| -> String {
        let replaced = table.replace("\\n", "\n");
        let mut lines = replaced.split('\n').collect::<Vec<_>>();
        lines.sort();
        lines
            .into_iter()
            .map(|s| s.to_string())
            .reduce(|acc, e| format!("{acc}\\n{e}"))
            .unwrap()
    };

    let recordbatches = match output.data {
        OutputData::Stream(stream) => util::collect_batches(stream).await.unwrap(),
        OutputData::RecordBatches(recordbatches) => recordbatches,
        _ => unreachable!(),
    };
    let pretty_print = sort_table(&recordbatches.pretty_print().unwrap());
    let expected = sort_table(expected);
    assert_eq!(
        pretty_print,
        expected,
        "\n{}",
        recordbatches.pretty_print().unwrap()
    );
}

pub fn prepare_path(p: &str) -> String {
    #[cfg(windows)]
    let p = {
        // We need unix style path even in the Windows, because the path is used in object-store, must
        // be delimited with '/'. Inside the object-store, it will be converted to file system needed
        // path in the end.
        p.replace('\\', "/")
    };

    p.to_string()
}

/// Find the testing file resource under workspace root to be used in object store.
pub fn find_testing_resource(path: &str) -> String {
    let p = find_workspace_path(path).display().to_string();

    prepare_path(&p)
}

pub async fn try_execute_sql(
    instance: &Arc<Instance>,
    sql: &str,
) -> servers::error::Result<Output> {
    try_execute_sql_with(instance, sql, QueryContext::arc()).await
}

pub async fn try_execute_sql_with(
    instance: &Arc<Instance>,
    sql: &str,
    query_ctx: QueryContextRef,
) -> servers::error::Result<Output> {
    instance.do_query(sql, query_ctx).await.remove(0)
}

/// Dump the kv backend to a vector of key-value pairs.
pub async fn dump_kvbackend(kv_backend: &KvBackendRef) -> Vec<(Vec<u8>, Vec<u8>)> {
    let req = RangeRequest::new().with_range(vec![0], vec![0]);
    let stream = PaginationStream::new(kv_backend.clone(), req, DEFAULT_PAGE_SIZE, |kv| {
        Ok((kv.key, kv.value))
    })
    .into_stream();
    stream.try_collect::<Vec<_>>().await.unwrap()
}

/// Clear the kv backend and restore the key-value pairs.
pub async fn restore_kvbackend(kv_backend: &KvBackendRef, keyvalues: Vec<(Vec<u8>, Vec<u8>)>) {
    // Clear the kv backend before restoring.
    let req = DeleteRangeRequest::new().with_range(vec![0], vec![0]);
    kv_backend.delete_range(req).await.unwrap();

    let mut req = BatchPutRequest::default();
    for (key, value) in keyvalues {
        req.kvs.push(KeyValue { key, value });
    }
    kv_backend.batch_put(req).await.unwrap();
}

/// Wait for the procedure to complete.
pub async fn wait_procedure(procedure_manager: &ProcedureManagerRef, procedure_id: ProcedureId) {
    let mut watcher = procedure_manager.procedure_watcher(procedure_id).unwrap();
    watcher::wait(&mut watcher).await.unwrap();
}
