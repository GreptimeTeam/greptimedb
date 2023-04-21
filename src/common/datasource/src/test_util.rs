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

use std::path::PathBuf;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use common_test_util::temp_dir::{create_temp_dir, TempDir};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::physical_plan::file_format::{FileScanConfig, FileStream};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use object_store::services::Fs;
use object_store::ObjectStore;

use crate::compression::CompressionType;
use crate::file_format::json::{stream_to_json, JsonOpener};

pub const TEST_BATCH_SIZE: usize = 100;

pub fn get_data_dir(path: &str) -> PathBuf {
    // https://doc.rust-lang.org/cargo/reference/environment-variables.html
    let dir = env!("CARGO_MANIFEST_DIR");

    PathBuf::from(dir).join(path)
}

pub fn format_schema(schema: Schema) -> Vec<String> {
    schema
        .fields()
        .iter()
        .map(|f| {
            format!(
                "{}: {:?}: {}",
                f.name(),
                f.data_type(),
                if f.is_nullable() { "NULL" } else { "NOT NULL" }
            )
        })
        .collect()
}

pub fn test_store(root: &str) -> ObjectStore {
    let mut builder = Fs::default();
    builder.root(root);

    ObjectStore::new(builder).unwrap().finish()
}

pub fn test_tmp_store(root: &str) -> (ObjectStore, TempDir) {
    let dir = create_temp_dir(root);

    let mut builder = Fs::default();
    builder.root("/");

    (ObjectStore::new(builder).unwrap().finish(), dir)
}

pub fn test_basic_schema() -> SchemaRef {
    let schema = Schema::new(vec![
        Field::new("num", DataType::Int64, false),
        Field::new("str", DataType::Utf8, false),
    ]);
    Arc::new(schema)
}

pub fn scan_config(file_schema: SchemaRef, limit: Option<usize>, filename: &str) -> FileScanConfig {
    FileScanConfig {
        object_store_url: ObjectStoreUrl::parse("empty://").unwrap(), // won't be used
        file_schema,
        file_groups: vec![vec![PartitionedFile::new(filename.to_string(), 10)]],
        statistics: Default::default(),
        projection: None,
        limit,
        table_partition_cols: vec![],
        output_ordering: None,
        infinite_source: false,
    }
}

pub async fn setup_stream_to_json_test(origin_path: &str, threshold: impl Fn(usize) -> usize) {
    let store = test_store("/");

    let schema = test_basic_schema();

    let json_opener = JsonOpener::new(
        100,
        schema.clone(),
        store.clone(),
        CompressionType::UNCOMPRESSED,
    );

    let size = store.read(origin_path).await.unwrap().len();

    let config = scan_config(schema.clone(), None, origin_path);

    let stream = FileStream::new(&config, 0, json_opener, &ExecutionPlanMetricsSet::new()).unwrap();

    let (tmp_store, dir) = test_tmp_store("test_stream_to_json");

    let output_path = format!("{}/{}", dir.path().display(), "output");

    stream_to_json(
        Box::pin(stream),
        tmp_store.clone(),
        output_path.clone(),
        threshold(size),
    )
    .await
    .unwrap();

    let written = tmp_store.read(&output_path).await.unwrap();
    let origin = store.read(origin_path).await.unwrap();

    // ignores `\n` for
    assert_eq!(
        String::from_utf8_lossy(&written).trim_end_matches('\n'),
        String::from_utf8_lossy(&origin).trim_end_matches('\n'),
    )
}
