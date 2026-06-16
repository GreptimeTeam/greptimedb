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

use store_api::region_engine::RegionEngine;
use store_api::region_request::{RegionCloseRequest, RegionRequest};
use store_api::storage::RegionId;

use crate::config::MitoConfig;
use crate::test_util::{CreateRequestBuilder, TestEnv};

#[tokio::test]
async fn test_engine_close_region() {
    test_engine_close_region_with_format(false).await;
    test_engine_close_region_with_format(true).await;
}

#[tokio::test]
async fn test_engine_close_region_removes_region_metrics() {
    let mut env = TestEnv::with_prefix("close-metrics").await;
    let engine = env
        .create_engine_with_region_query_load_report(MitoConfig::default(), true)
        .await;

    let region_id = RegionId::new(9000, 1);
    let request = CreateRequestBuilder::new().build();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    {
        let region = engine.get_region(region_id).unwrap();
        region.written_bytes.set(1);
        let (_, query_cpu_time, query_scanned_bytes) = region.region_metrics();
        let query_cpu_time = query_cpu_time.unwrap();
        let query_scanned_bytes = query_scanned_bytes.unwrap();
        query_cpu_time.add(7);
        query_scanned_bytes.add(42);
    }

    engine
        .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
        .await
        .unwrap();
    assert!(!engine.is_region_exists(region_id));

    let metrics = prometheus::gather();
    assert_region_metric_missing(
        &metrics,
        "greptime_mito_region_written_bytes_since_open",
        region_id,
    );
    assert_region_metric_missing(&metrics, "greptime_mito_region_query_cpu_time", region_id);
    assert_region_metric_missing(
        &metrics,
        "greptime_mito_region_query_scanned_bytes",
        region_id,
    );
}

fn assert_region_metric_missing(
    metrics: &[prometheus::proto::MetricFamily],
    metric_name: &str,
    region_id: RegionId,
) {
    let exists = metrics
        .iter()
        .find(|metric| metric.name() == metric_name)
        .is_some_and(|metric| {
            metric.metric.iter().any(|metric| {
                metric.label.iter().any(|label| {
                    label.name() == "region_id" && label.value() == region_id.as_u64().to_string()
                })
            })
        });

    assert!(!exists, "metric {metric_name} still exists for {region_id}");
}

async fn test_engine_close_region_with_format(flat_format: bool) {
    let mut env = TestEnv::with_prefix("close").await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    // It's okay to close a region doesn't exist.
    engine
        .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
        .await
        .unwrap();

    let request = CreateRequestBuilder::new().build();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Close the created region.
    engine
        .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
        .await
        .unwrap();
    assert!(!engine.is_region_exists(region_id));

    // It's okay to close this region again.
    engine
        .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
        .await
        .unwrap();
}
