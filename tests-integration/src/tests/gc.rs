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

use meta_srv::gc::GcSchedulerOptions;
use mito2::gc::GcConfig;

use crate::cluster::GreptimeDbClusterBuilder;
use crate::tests::test_util::{MockInstanceBuilder, TestContext};

async fn distributed_with_gc() -> TestContext {
    common_telemetry::init_default_ut_logging();
    let test_name = uuid::Uuid::new_v4().to_string();
    let builder = GreptimeDbClusterBuilder::new(&test_name)
        .await
        .with_metasrv_gc_config(GcSchedulerOptions {
            enable: true,
            ..Default::default()
        })
        .with_datanode_gc_config(GcConfig {
            enable: true,
            ..Default::default()
        });
    TestContext::new(MockInstanceBuilder::Distributed(builder)).await
}

#[tokio::test]
async fn test_gc_basic() {
    let mut test_context = distributed_with_gc().await;

    // TODO: write some data, flush, repeat once, run compact_table, trigger_gc and verify data files are deleted.
}
