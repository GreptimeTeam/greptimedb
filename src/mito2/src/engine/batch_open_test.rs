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

use api::v1::Rows;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_recordbatch::RecordBatches;
use common_wal::options::{KafkaWalOptions, WalOptions, WAL_OPTIONS_KEY};
use futures::future::join_all;
use rstest::rstest;
use rstest_reuse::apply;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{RegionOpenRequest, RegionRequest};
use store_api::storage::{RegionId, ScanRequest};
use tokio::sync::Barrier;

use super::MitoEngine;
use crate::config::MitoConfig;
use crate::test_util::{
    build_rows, kafka_log_store_factory, multiple_log_store_factories,
    prepare_test_for_kafka_log_store, put_rows, raft_engine_log_store_factory, rows_schema,
    CreateRequestBuilder, LogStoreFactory, TestEnv,
};

#[apply(multiple_log_store_factories)]
async fn test_batch_open(factory: Option<LogStoreFactory>) {
    common_telemetry::init_default_ut_logging();
    let Some(factory) = factory else {
        return;
    };
    let mut env =
        TestEnv::with_prefix("open-batch-regions").with_log_store_factory(factory.clone());
    let engine = env.create_engine(MitoConfig::default()).await;
    let topic = prepare_test_for_kafka_log_store(&factory).await;

    let num_regions = 3u32;
    let barrier = Arc::new(Barrier::new(num_regions as usize));
    let region_dir = |region_id| format!("test/{region_id}");
    let mut tasks = vec![];
    for id in 1..=num_regions {
        let barrier = barrier.clone();
        let engine = engine.clone();
        let topic = topic.clone();
        tasks.push(async move {
            let region_id = RegionId::new(1, id);
            let request = CreateRequestBuilder::new()
                .region_dir(&region_dir(region_id))
                .kafka_topic(topic.clone())
                .build();
            let column_schemas = rows_schema(&request);
            engine
                .handle_request(region_id, RegionRequest::Create(request))
                .await
                .unwrap();
            barrier.wait().await;
            for i in 0..10 {
                let rows = Rows {
                    schema: column_schemas.clone(),
                    rows: build_rows(
                        (id as usize) * 100 + i as usize,
                        (id as usize) * 100 + i as usize + 1,
                    ),
                };
                put_rows(&engine, region_id, rows).await;
            }
        });
    }
    join_all(tasks).await;

    let assert_result = |engine: MitoEngine| async move {
        let region_id = RegionId::new(1, 1);
        let request = ScanRequest::default();
        let stream = engine.scan_to_stream(region_id, request).await.unwrap();
        let batches = RecordBatches::try_collect(stream).await.unwrap();
        let expected = "+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 100   | 100.0   | 1970-01-01T00:01:40 |
| 101   | 101.0   | 1970-01-01T00:01:41 |
| 102   | 102.0   | 1970-01-01T00:01:42 |
| 103   | 103.0   | 1970-01-01T00:01:43 |
| 104   | 104.0   | 1970-01-01T00:01:44 |
| 105   | 105.0   | 1970-01-01T00:01:45 |
| 106   | 106.0   | 1970-01-01T00:01:46 |
| 107   | 107.0   | 1970-01-01T00:01:47 |
| 108   | 108.0   | 1970-01-01T00:01:48 |
| 109   | 109.0   | 1970-01-01T00:01:49 |
+-------+---------+---------------------+";
        assert_eq!(expected, batches.pretty_print().unwrap());

        let region_id = RegionId::new(1, 2);
        let request = ScanRequest::default();
        let stream = engine.scan_to_stream(region_id, request).await.unwrap();
        let batches = RecordBatches::try_collect(stream).await.unwrap();
        let expected = "+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 200   | 200.0   | 1970-01-01T00:03:20 |
| 201   | 201.0   | 1970-01-01T00:03:21 |
| 202   | 202.0   | 1970-01-01T00:03:22 |
| 203   | 203.0   | 1970-01-01T00:03:23 |
| 204   | 204.0   | 1970-01-01T00:03:24 |
| 205   | 205.0   | 1970-01-01T00:03:25 |
| 206   | 206.0   | 1970-01-01T00:03:26 |
| 207   | 207.0   | 1970-01-01T00:03:27 |
| 208   | 208.0   | 1970-01-01T00:03:28 |
| 209   | 209.0   | 1970-01-01T00:03:29 |
+-------+---------+---------------------+";
        assert_eq!(expected, batches.pretty_print().unwrap());

        let region_id = RegionId::new(1, 3);
        let request = ScanRequest::default();
        let stream = engine.scan_to_stream(region_id, request).await.unwrap();
        let batches = RecordBatches::try_collect(stream).await.unwrap();
        let expected = "+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 300   | 300.0   | 1970-01-01T00:05:00 |
| 301   | 301.0   | 1970-01-01T00:05:01 |
| 302   | 302.0   | 1970-01-01T00:05:02 |
| 303   | 303.0   | 1970-01-01T00:05:03 |
| 304   | 304.0   | 1970-01-01T00:05:04 |
| 305   | 305.0   | 1970-01-01T00:05:05 |
| 306   | 306.0   | 1970-01-01T00:05:06 |
| 307   | 307.0   | 1970-01-01T00:05:07 |
| 308   | 308.0   | 1970-01-01T00:05:08 |
| 309   | 309.0   | 1970-01-01T00:05:09 |
+-------+---------+---------------------+";
        assert_eq!(expected, batches.pretty_print().unwrap());
    };
    assert_result(engine.clone()).await;

    let mut options = HashMap::new();
    if let Some(topic) = &topic {
        options.insert(
            WAL_OPTIONS_KEY.to_string(),
            serde_json::to_string(&WalOptions::Kafka(KafkaWalOptions {
                topic: topic.to_string(),
            }))
            .unwrap(),
        );
    };
    let mut requests = (1..=num_regions)
        .map(|id| {
            let region_id = RegionId::new(1, id);
            (
                region_id,
                RegionOpenRequest {
                    engine: String::new(),
                    region_dir: region_dir(region_id),
                    options: options.clone(),
                    skip_wal_replay: false,
                },
            )
        })
        .collect::<Vec<_>>();
    requests.push((
        RegionId::new(1, 4),
        RegionOpenRequest {
            engine: String::new(),
            region_dir: "no-exists".to_string(),
            options: options.clone(),
            skip_wal_replay: false,
        },
    ));

    // Reopen engine.
    let engine = env.reopen_engine(engine, MitoConfig::default()).await;
    let mut results = engine
        .handle_batch_open_requests(4, requests)
        .await
        .unwrap();
    let (_, result) = results.pop().unwrap();
    assert_eq!(
        result.unwrap_err().status_code(),
        StatusCode::RegionNotFound
    );
    for (_, result) in results {
        assert!(result.is_ok());
    }
    assert_result(engine.clone()).await;
}

#[apply(multiple_log_store_factories)]
async fn test_batch_open_err(factory: Option<LogStoreFactory>) {
    common_telemetry::init_default_ut_logging();
    let Some(factory) = factory else {
        return;
    };
    let mut env =
        TestEnv::with_prefix("open-batch-regions-err").with_log_store_factory(factory.clone());
    let engine = env.create_engine(MitoConfig::default()).await;
    let topic = prepare_test_for_kafka_log_store(&factory).await;
    let mut options = HashMap::new();
    if let Some(topic) = &topic {
        options.insert(
            WAL_OPTIONS_KEY.to_string(),
            serde_json::to_string(&WalOptions::Kafka(KafkaWalOptions {
                topic: topic.to_string(),
            }))
            .unwrap(),
        );
    };
    let num_regions = 3u32;
    let region_dir = "test".to_string();
    let requests = (1..=num_regions)
        .map(|id| {
            (
                RegionId::new(1, id),
                RegionOpenRequest {
                    engine: String::new(),
                    region_dir: region_dir.to_string(),
                    options: options.clone(),
                    skip_wal_replay: false,
                },
            )
        })
        .collect::<Vec<_>>();

    let results = engine
        .handle_batch_open_requests(3, requests)
        .await
        .unwrap();
    for (_, result) in results {
        assert_eq!(
            result.unwrap_err().status_code(),
            StatusCode::RegionNotFound
        );
    }
}
