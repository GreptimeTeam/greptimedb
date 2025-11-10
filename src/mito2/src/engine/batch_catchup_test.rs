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
use store_api::region_request::{PathType, RegionCatchupRequest, RegionOpenRequest, RegionRequest};
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::test_util::{
    CreateRequestBuilder, LogStoreFactory, TestEnv, build_rows, flush_region,
    kafka_log_store_factory, prepare_test_for_kafka_log_store, put_rows, rows_schema,
    single_kafka_log_store_factory,
};

#[apply(single_kafka_log_store_factory)]
async fn test_batch_catchup(factory: Option<LogStoreFactory>) {
    test_batch_catchup_with_format(factory.clone(), false).await;
    test_batch_catchup_with_format(factory, true).await;
}

async fn test_batch_catchup_with_format(factory: Option<LogStoreFactory>, flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let Some(factory) = factory else {
        return;
    };
    let mut env = TestEnv::with_prefix("catchup-batch-regions")
        .await
        .with_log_store_factory(factory.clone());
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    // Prepares 3 topics for 8 regions
    let num_topic = 3;
    let mut topics = vec![];
    for _ in 0..num_topic {
        let topic = prepare_test_for_kafka_log_store(&factory).await.unwrap();
        topics.push(topic);
    }

    let num_regions = 8u32;
    let table_dir_fn = |region_id| format!("test/{region_id}");
    let mut region_schema = HashMap::new();

    let get_topic_idx = |id| (id - 1) % num_topic;

    // Creates 8 regions and puts data into them
    for id in 1..=num_regions {
        let engine = engine.clone();
        let topic_idx = get_topic_idx(id);
        let topic = topics[topic_idx as usize].clone();
        let region_id = RegionId::new(1, id);
        let request = CreateRequestBuilder::new()
            .table_dir(&table_dir_fn(region_id))
            .kafka_topic(Some(topic))
            .build();
        let column_schemas = rows_schema(&request);
        region_schema.insert(region_id, column_schemas);
        engine
            .handle_request(region_id, RegionRequest::Create(request))
            .await
            .unwrap();
    }

    // Puts data into regions
    let rows = 30;
    for i in 0..rows {
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
            if i % region_number == 0 {
                flush_region(&engine, region_id, None).await;
            }
        }
    }

    let assert_result = |engine: MitoEngine| async move {
        for i in 1..=num_regions {
            let region_id = RegionId::new(1, i);
            let request = ScanRequest::default();
            let stream = engine.scan_to_stream(region_id, request).await.unwrap();
            let batches = RecordBatches::try_collect(stream).await.unwrap();
            let mut expected = String::new();
            expected.push_str(
                "+-------+---------+---------------------+\n| tag_0 | field_0 | ts                  |\n+-------+---------+---------------------+\n",
            );
            for row in 0..rows {
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

    let requests = (1..=num_regions)
        .map(|id| {
            let region_id = RegionId::new(1, id);
            let topic_idx = get_topic_idx(id);
            let topic = topics[topic_idx as usize].clone();
            let mut options = HashMap::new();
            options.insert(
                WAL_OPTIONS_KEY.to_string(),
                serde_json::to_string(&WalOptions::Kafka(KafkaWalOptions { topic })).unwrap(),
            );
            (
                region_id,
                RegionOpenRequest {
                    engine: String::new(),
                    table_dir: table_dir_fn(region_id),
                    options,
                    skip_wal_replay: true,
                    path_type: PathType::Bare,
                    checkpoint: None,
                },
            )
        })
        .collect::<Vec<_>>();

    let parallelism = 2;
    let results = engine
        .handle_batch_open_requests(parallelism, requests)
        .await
        .unwrap();
    for (_, result) in results {
        assert!(result.is_ok());
    }

    let requests = (1..=num_regions)
        .map(|id| {
            let region_id = RegionId::new(1, id);
            (
                region_id,
                RegionCatchupRequest {
                    set_writable: true,
                    entry_id: None,
                    metadata_entry_id: None,
                    location_id: None,
                    checkpoint: None,
                },
            )
        })
        .collect::<Vec<_>>();

    let results = engine
        .handle_batch_catchup_requests(parallelism, requests)
        .await
        .unwrap();
    for (_, result) in results {
        assert!(result.is_ok());
    }
    assert_result(engine.clone()).await;
}

#[apply(single_kafka_log_store_factory)]
async fn test_batch_catchup_err(factory: Option<LogStoreFactory>) {
    test_batch_catchup_err_with_format(factory.clone(), false).await;
    test_batch_catchup_err_with_format(factory, true).await;
}

async fn test_batch_catchup_err_with_format(factory: Option<LogStoreFactory>, flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let Some(factory) = factory else {
        return;
    };
    let mut env = TestEnv::with_prefix("catchup-regions-err")
        .await
        .with_log_store_factory(factory.clone());
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;
    let num_regions = 3u32;
    let requests = (1..num_regions)
        .map(|id| {
            let region_id = RegionId::new(1, id);
            (
                region_id,
                RegionCatchupRequest {
                    set_writable: true,
                    entry_id: None,
                    metadata_entry_id: None,
                    location_id: None,
                    checkpoint: None,
                },
            )
        })
        .collect::<Vec<_>>();

    let results = engine
        .handle_batch_catchup_requests(4, requests)
        .await
        .unwrap();
    for (_, result) in results {
        assert_eq!(
            result.unwrap_err().status_code(),
            StatusCode::RegionNotFound
        );
    }
}
