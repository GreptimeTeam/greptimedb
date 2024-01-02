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

use std::env;
use std::sync::Arc;

use common_config::wal::KafkaConfig;
use common_config::WalConfig;
use common_meta::wal::kafka::KafkaConfig as MetaKafkaConfig;
use common_meta::wal::WalConfig as MetaWalConfig;
use common_query::Output;
use common_recordbatch::util;
use common_telemetry::warn;
use common_test_util::find_workspace_path;
use frontend::instance::Instance;
use rstest_reuse::{self, template};

use crate::cluster::GreptimeDbClusterBuilder;
use crate::standalone::{GreptimeDbStandalone, GreptimeDbStandaloneBuilder};
use crate::test_util::StorageType;
use crate::tests::{create_distributed_instance, MockDistributedInstance};

#[async_trait::async_trait]
pub(crate) trait RebuildableMockInstance: MockInstance {
    // Rebuilds the instance and returns rebuilt frontend instance.
    async fn rebuild(&mut self) -> Arc<Instance>;
}

pub(crate) trait MockInstance: Sync + Send {
    fn frontend(&self) -> Arc<Instance>;

    fn is_distributed_mode(&self) -> bool;
}

impl MockInstance for GreptimeDbStandalone {
    fn frontend(&self) -> Arc<Instance> {
        self.instance.clone()
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

impl MockInstanceBuilder {
    async fn build(&self) -> Arc<dyn MockInstance> {
        match self {
            MockInstanceBuilder::Standalone(builder) => Arc::new(builder.clone().build().await),
            MockInstanceBuilder::Distributed(builder) => {
                Arc::new(MockDistributedInstance(builder.clone().build().await))
            }
        }
    }
}

pub(crate) struct TestContext {
    instance: Arc<dyn MockInstance>,
    builder: MockInstanceBuilder,
}

impl TestContext {
    async fn new(builder: MockInstanceBuilder) -> Self {
        let instance = builder.build().await;

        Self { instance, builder }
    }
}

#[async_trait::async_trait]
impl RebuildableMockInstance for TestContext {
    async fn rebuild(&mut self) -> Arc<Instance> {
        let instance = self.builder.build().await;
        self.instance = instance;
        self.instance.frontend()
    }
}

impl MockInstance for TestContext {
    fn frontend(&self) -> Arc<Instance> {
        self.instance.frontend()
    }

    fn is_distributed_mode(&self) -> bool {
        self.instance.is_distributed_mode()
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
    let storage_types = StorageType::build_storage_types_based_on_env();
    let instance = GreptimeDbStandaloneBuilder::new(&test_name)
        .with_store_providers(storage_types)
        .build()
        .await;
    Arc::new(instance)
}

pub(crate) async fn distributed_with_multiple_object_stores() -> Arc<dyn MockInstance> {
    let _ = dotenv::dotenv();
    let test_name = uuid::Uuid::new_v4().to_string();
    let providers = StorageType::build_storage_types_based_on_env();
    let cluster = GreptimeDbClusterBuilder::new(&test_name)
        .await
        .with_store_providers(providers)
        .build()
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
        .with_wal_config(WalConfig::Kafka(KafkaConfig {
            broker_endpoints: endpoints.clone(),
            ..Default::default()
        }))
        .with_meta_wal_config(MetaWalConfig::Kafka(MetaKafkaConfig {
            broker_endpoints: endpoints,
            topic_name_prefix: test_name.to_string(),
            num_topics: 3,
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
        .with_wal_config(WalConfig::Kafka(KafkaConfig {
            broker_endpoints: endpoints.clone(),
            ..Default::default()
        }))
        .with_meta_wal_config(MetaWalConfig::Kafka(MetaKafkaConfig {
            broker_endpoints: endpoints,
            topic_name_prefix: test_name.to_string(),
            num_topics: 3,
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
    instance: Option<Box<dyn RebuildableMockInstance>>,
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

    let recordbatches = match output {
        Output::Stream(stream) => util::collect_batches(stream).await.unwrap(),
        Output::RecordBatches(recordbatches) => recordbatches,
        _ => unreachable!(),
    };
    let pretty_print = sort_table(&recordbatches.pretty_print().unwrap());
    let expected = sort_table(expected);
    assert_eq!(pretty_print, expected);
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
