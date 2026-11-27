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

//! Behavioral coverage for series-index reconciliation.

use api::v1::helper::row;
use api::v1::value::ValueData;
use api::v1::{ColumnDataType, Rows, SemanticType, WriteHint};
use store_api::metric_engine_consts::PRIMARY_KEY_ENCODING;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{RegionPutRequest, RegionRequest};
use store_api::storage::RegionId;
use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::region::MitoRegionRef;
use crate::test_util::sst_util::new_sparse_primary_key;
use crate::test_util::{CreateRequestBuilder, TestEnv, flush_region, rows_schema};

pub(crate) async fn prepare_region_with_metadata(
    env: &mut TestEnv,
    timestamps: &[i64],
    config: MitoConfig,
    metadata: store_api::metadata::RegionMetadataRef,
) -> (MitoEngine, MitoRegionRef) {
    prepare_region_with_metadata_and_flush(env, timestamps, config, metadata, true).await
}

pub(crate) async fn prepare_region_with_metadata_and_flush(
    env: &mut TestEnv,
    timestamps: &[i64],
    config: MitoConfig,
    metadata: store_api::metadata::RegionMetadataRef,
    flush_each: bool,
) -> (MitoEngine, MitoRegionRef) {
    let engine = env.create_engine(config).await;
    let region_id = RegionId::new(1, 1);
    let mut request = CreateRequestBuilder::new().build();
    request.column_metadatas = metadata.column_metadatas.clone();
    request.primary_key = metadata.primary_key.clone();
    request
        .options
        .insert(PRIMARY_KEY_ENCODING.to_string(), "sparse".to_string());
    request
        .options
        .insert("memtable.type".to_string(), "bulk".to_string());
    request
        .options
        .insert("sst_format".to_string(), "flat".to_string());
    request
        .options
        .insert("compaction.type".to_string(), "twcs".to_string());
    request.options.insert(
        "compaction.twcs.time_window".to_string(),
        if flush_each { "100s" } else { "172800s" }.to_string(),
    );
    // Keep the source SSTs stable while tests reconcile multiple buckets.
    for window in ["active_window", "inactive_window"] {
        request.options.insert(
            format!("compaction.twcs.{window}.trigger_file_num"),
            "100".to_string(),
        );
    }
    let full_schema = rows_schema(&request);
    let mut pk_column = full_schema[0].clone();
    pk_column.column_name = PRIMARY_KEY_COLUMN_NAME.to_string();
    pk_column.datatype = ColumnDataType::Binary.into();
    pk_column.semantic_type = SemanticType::Tag.into();
    let schema = vec![pk_column, full_schema[5].clone(), full_schema[4].clone()];
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    // Flush completion can precede compaction scheduling. Keep background TTL
    // cleanup from racing tests that alter options after preparing the SSTs.
    let region = engine.get_region(region_id).unwrap();
    region.update_schedule_compaction_millis();
    for &ts in timestamps {
        engine
            .handle_request(
                region_id,
                RegionRequest::Put(RegionPutRequest {
                    skip_wal: false,
                    rows: Rows {
                        schema: schema.clone(),
                        rows: vec![row(vec![
                            ValueData::BinaryValue(new_sparse_primary_key(
                                &["a", "x"],
                                &metadata,
                                10,
                                0,
                            )),
                            ValueData::TimestampMillisecondValue(ts),
                            if metadata
                                .field_columns()
                                .next()
                                .unwrap()
                                .column_schema
                                .data_type
                                == datatypes::prelude::ConcreteDataType::float64_datatype()
                            {
                                ValueData::F64Value(1.0)
                            } else {
                                ValueData::U64Value(1)
                            },
                        ])],
                    },
                    hint: Some(WriteHint {
                        primary_key_encoding: api::v1::PrimaryKeyEncoding::Sparse.into(),
                    }),
                    partition_expr_version: None,
                }),
            )
            .await
            .unwrap();
        if flush_each {
            flush_region(&engine, region_id, None).await;
        }
    }
    if !flush_each {
        flush_region(&engine, region_id, None).await;
    }
    (engine, region)
}
