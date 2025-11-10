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

use api::v1::Rows;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_recordbatch::RecordBatches;
use common_wal::options::{KafkaWalOptions, WAL_OPTIONS_KEY, WalOptions};
use rstest::rstest;
use rstest_reuse::apply;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{PathType, RegionOpenRequest, RegionRequest};
use store_api::storage::{RegionId, ScanRequest};

use super::MitoEngine;
use crate::config::MitoConfig;
use crate::test_util::{
    CreateRequestBuilder, LogStoreFactory, TestEnv, build_rows, kafka_log_store_factory,
    multiple_log_store_factories, prepare_test_for_kafka_log_store, put_rows,
    raft_engine_log_store_factory, rows_schema,
};

#[apply(multiple_log_store_factories)]
async fn test_batch_open(factory: Option<LogStoreFactory>) {
    test_batch_open_with_format(factory.clone(), false).await;
    test_batch_open_with_format(factory, true).await;
}

async fn test_batch_open_with_format(factory: Option<LogStoreFactory>, flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let Some(factory) = factory else {
        return;
    };
    let mut env = TestEnv::with_prefix("open-batch-regions")
        .await
        .with_log_store_factory(factory.clone());
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;
    let topic = prepare_test_for_kafka_log_store(&factory).await;

    let num_regions = 3u32;
    let table_dir_fn = |region_id| format!("test/{region_id}");
    let mut region_schema = HashMap::new();

    for id in 1..=num_regions {
        let engine = engine.clone();
        let topic = topic.clone();
        let region_id = RegionId::new(1, id);
        let request = CreateRequestBuilder::new()
            .table_dir(&table_dir_fn(region_id))
            .kafka_topic(topic.clone())
            .build();
        let column_schemas = rows_schema(&request);
        region_schema.insert(region_id, column_schemas);
        engine
            .handle_request(region_id, RegionRequest::Create(request))
            .await
            .unwrap();
    }

    for i in 0..10 {
        for region_number in 1..=num_regions {
            let region_id = RegionId::new(1, region_number);
            let rows = Rows {
                schema: region_schema[&region_id].clone(),
                rows: build_rows(
                    (region_number as usize) * 120 + i as usize,
                    (region_number as usize) * 120 + i as usize + 1,
                ),
            };
            put_rows(&engine, region_id, rows).await;
        }
    }

    let assert_result = |engine: MitoEngine| async move {
        for i in 1..num_regions {
            let region_id = RegionId::new(1, i);
            let request = ScanRequest::default();
            let stream = engine.scan_to_stream(region_id, request).await.unwrap();
            let batches = RecordBatches::try_collect(stream).await.unwrap();
            let mut expected = String::new();
            expected.push_str(
                "+-------+---------+---------------------+\n| tag_0 | field_0 | ts                  |\n+-------+---------+---------------------+\n",
            );
            for row in 0..10 {
                expected.push_str(&format!(
                    "| {}   | {}.0   | 1970-01-01T00:{:02}:{:02} |\n",
                    i * 120 + row,
                    i * 120 + row,
                    2 * i,
                    row
                ));
            }
            expected.push_str("+-------+---------+---------------------+");
            assert_eq!(expected, batches.pretty_print().unwrap());
        }
    };
    assert_result(engine.clone()).await;

    let mut options = HashMap::new();
    if let Some(topic) = &topic {
        options.insert(
            WAL_OPTIONS_KEY.to_string(),
            serde_json::to_string(&WalOptions::Kafka(KafkaWalOptions {
                topic: topic.clone(),
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
                    table_dir: table_dir_fn(region_id),
                    options: options.clone(),
                    skip_wal_replay: false,
                    path_type: PathType::Bare,
                    checkpoint: None,
                },
            )
        })
        .collect::<Vec<_>>();
    requests.push((
        RegionId::new(1, 4),
        RegionOpenRequest {
            engine: String::new(),
            table_dir: "no-exists".to_string(),
            options: options.clone(),
            skip_wal_replay: false,
            path_type: PathType::Bare,
            checkpoint: None,
        },
    ));

    // Reopen engine.
    let engine = env
        .reopen_engine(
            engine,
            MitoConfig {
                default_experimental_flat_format: flat_format,
                ..Default::default()
            },
        )
        .await;
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
    test_batch_open_err_with_format(factory.clone(), false).await;
    test_batch_open_err_with_format(factory, true).await;
}

async fn test_batch_open_err_with_format(factory: Option<LogStoreFactory>, flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let Some(factory) = factory else {
        return;
    };
    let mut env = TestEnv::with_prefix("open-batch-regions-err")
        .await
        .with_log_store_factory(factory.clone());
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;
    let topic = prepare_test_for_kafka_log_store(&factory).await;
    let mut options = HashMap::new();
    if let Some(topic) = &topic {
        options.insert(
            WAL_OPTIONS_KEY.to_string(),
            serde_json::to_string(&WalOptions::Kafka(KafkaWalOptions {
                topic: topic.clone(),
            }))
            .unwrap(),
        );
    };
    let num_regions = 3u32;
    let table_dir = "test".to_string();
    let requests = (1..=num_regions)
        .map(|id| {
            (
                RegionId::new(1, id),
                RegionOpenRequest {
                    engine: String::new(),
                    table_dir: table_dir.clone(),
                    options: options.clone(),
                    skip_wal_replay: false,
                    path_type: PathType::Bare,
                    checkpoint: None,
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
