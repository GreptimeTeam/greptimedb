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

use std::path::Path;
use std::sync::Arc;

use api::v1::SemanticType;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use object_store::layers::mock::{
    BytesRange, Error as MockError, ErrorKind, MockLayer, MockLayerBuilder, Result as MockResult,
    RpRead, oio,
};
use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataBuilder};
use store_api::storage::RegionId;

/// Build a basic region metadata for testing.
/// It contains three columns:
/// - ts: timestamp millisecond, semantic type: `Timestamp`, column id: 45
/// - pk: string, semantic type: `Tag`, column id: 36
/// - val: float64, semantic type: `Field`, column id: 251
pub fn basic_region_metadata() -> RegionMetadata {
    let mut builder = RegionMetadataBuilder::new(RegionId::new(23, 33));
    builder
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: 45,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("pk", ConcreteDataType::string_datatype(), false),
            semantic_type: SemanticType::Tag,
            column_id: 36,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("val", ConcreteDataType::float64_datatype(), false),
            semantic_type: SemanticType::Field,
            column_id: 251,
        })
        .primary_key(vec![36]);
    builder.build().unwrap()
}

struct NotFoundReader;

impl oio::StreamRead for NotFoundReader {
    async fn open(&self, _: BytesRange) -> MockResult<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        Err(MockError::new(
            ErrorKind::NotFound,
            "mock listed manifest delta not found",
        ))
    }
}

pub(crate) fn fail_manifest_delta_reads_layer() -> MockLayer {
    MockLayerBuilder::default()
        .reader_factory(Arc::new(|path, _args, inner| {
            let file_name = path.rsplit('/').next().unwrap_or(path);
            if crate::manifest::storage::is_delta_file(file_name) {
                Box::new(oio::StreamReader::new(NotFoundReader))
            } else {
                inner
            }
        }))
        .build()
        .unwrap()
}

/// Measures stored delta and checkpoint bytes independently of the size tracker.
pub async fn manifest_dir_usage(path: impl AsRef<Path>) -> u64 {
    let mut entries = tokio::fs::read_dir(path).await.unwrap();
    let mut total = 0;
    while let Some(entry) = entries.next_entry().await.unwrap() {
        let name = entry.file_name();
        let name = name.to_str().unwrap();
        if crate::manifest::storage::is_delta_file(name)
            || crate::manifest::storage::is_checkpoint_file(name)
        {
            total += entry.metadata().await.unwrap().len();
        }
    }
    total
}

/// Independently measures the logical recovery set described by the persisted pointer.
/// Older physical files (including the previous checkpoint) are deliberately excluded.
pub async fn manifest_recovery_usage(path: impl AsRef<Path>) -> u64 {
    let path = path.as_ref();
    let checkpoint = match tokio::fs::read(path.join("_last_checkpoint")).await {
        Ok(bytes) => Some(
            serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()["version"]
                .as_u64()
                .unwrap(),
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => panic!("Cannot read checkpoint pointer: {error}"),
    };
    let mut entries = tokio::fs::read_dir(path).await.unwrap();
    let mut total = 0;
    while let Some(entry) = entries.next_entry().await.unwrap() {
        let name = entry.file_name();
        let Some((version, suffix)) = name.to_str().unwrap().split_once('.') else {
            continue;
        };
        let Ok(version) = version.parse::<u64>() else {
            continue;
        };
        let keep = if suffix.starts_with("json") {
            checkpoint.is_none_or(|checkpoint| version > checkpoint)
        } else if suffix.starts_with("checkpoint") {
            checkpoint == Some(version)
        } else {
            false
        };
        if keep {
            total += entry.metadata().await.unwrap().len();
        }
    }
    total
}
