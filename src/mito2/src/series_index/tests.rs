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

//! Shared fixtures for series-index construction tests.

use std::sync::Arc;

use api::v1::helper::row;
use api::v1::value::ValueData;
use api::v1::{ColumnDataType, Rows, SemanticType, WriteHint};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metric_engine_consts::PRIMARY_KEY_ENCODING;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{RegionPutRequest, RegionRequest};
use store_api::storage::RegionId;
use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::region::MitoRegionRef;
use crate::test_util::sst_util::{new_sparse_primary_key, sst_region_metadata_with_encoding};
use crate::test_util::{CreateRequestBuilder, TestEnv, flush_region, rows_schema};

/// Builds real sparse SSTs; background maintenance is disabled so tests control publication.
pub(super) async fn prepare_region(env: &mut TestEnv) -> (MitoEngine, MitoRegionRef) {
    prepare_region_with_timestamps(env, &[1000, 2000, 3000, 4000]).await
}

async fn prepare_region_with_timestamps(
    env: &mut TestEnv,
    timestamps: &[i64],
) -> (MitoEngine, MitoRegionRef) {
    let engine = env.create_engine(MitoConfig::default()).await;
    let metadata = Arc::new(sst_region_metadata_with_encoding(
        PrimaryKeyEncoding::Sparse,
    ));
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
        "100s".to_string(),
    );
    // Keep the source SSTs stable while tests reconcile multiple buckets.
    request.options.insert(
        "compaction.twcs.trigger_file_num".to_string(),
        "100".to_string(),
    );
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
    for &ts in timestamps {
        engine
            .handle_request(
                region_id,
                RegionRequest::Put(RegionPutRequest {
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
                            ValueData::U64Value(1),
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
        flush_region(&engine, region_id, None).await;
    }
    let region = engine.get_region(region_id).unwrap();
    (engine, region)
}
