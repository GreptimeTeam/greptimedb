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

mod instance_kafka_wal_test;
mod instance_test;
mod promql_test;
mod test_util;

use std::collections::HashMap;
use std::sync::Arc;

use common_meta::key::TableMetadataManagerRef;
use datanode::datanode::Datanode;
use frontend::instance::Instance;

use crate::cluster::{GreptimeDbCluster, GreptimeDbClusterBuilder};

pub struct MockDistributedInstance(GreptimeDbCluster);

impl MockDistributedInstance {
    pub fn frontend(&self) -> Arc<Instance> {
        self.0.frontend.clone()
    }

    pub fn datanodes(&self) -> &HashMap<u64, Datanode> {
        &self.0.datanode_instances
    }

    pub fn table_metadata_manager(&self) -> &TableMetadataManagerRef {
        self.0.meta_srv.table_metadata_manager()
    }
}

pub async fn create_distributed_instance(test_name: &str) -> MockDistributedInstance {
    let builder = GreptimeDbClusterBuilder::new(test_name).await;
    let cluster = builder.build().await;
    MockDistributedInstance(cluster)
}
