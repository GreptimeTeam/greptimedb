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

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use common_test_util::temp_dir::{create_temp_dir, TempDir};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{
    CsvSource, FileGroup, FileScanConfig, FileScanConfigBuilder, FileSource, FileStream,
    JsonOpener, JsonSource,
};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use object_store::services::Fs;
use object_store::ObjectStore;

use crate::file_format::csv::stream_to_csv;
use crate::file_format::json::stream_to_json;
use crate::test_util;

pub const TEST_BATCH_SIZE: usize = 100;

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
    let builder = Fs::default();
    ObjectStore::new(builder.root(root)).unwrap().finish()
}

pub fn test_tmp_store(root: &str) -> (ObjectStore, TempDir) {
    let dir = create_temp_dir(root);

    let builder = Fs::default();
    (ObjectStore::new(builder.root("/")).unwrap().finish(), dir)
}

pub fn test_basic_schema() -> SchemaRef {
    let schema = Schema::new(vec![
        Field::new("num", DataType::Int64, false),
        Field::new("str", DataType::Utf8, false),
    ]);
    Arc::new(schema)
}

pub(crate) fn scan_config(
    file_schema: SchemaRef,
    limit: Option<usize>,
    filename: &str,
    file_source: Arc<dyn FileSource>,
) -> FileScanConfig {
    // object_store only recognize the Unix style path, so make it happy.
    let filename = &filename.replace('\\', "/");
    let file_group = FileGroup::new(vec![PartitionedFile::new(filename.to_string(), 4096)]);

    FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_schema, file_source)
        .with_file_group(file_group)
        .with_limit(limit)
        .build()
}

pub async fn setup_stream_to_json_test(origin_path: &str, threshold: impl Fn(usize) -> usize) {
    let store = test_store("/");

    let schema = test_basic_schema();

    let json_opener = JsonOpener::new(
        test_util::TEST_BATCH_SIZE,
        schema.clone(),
        FileCompressionType::UNCOMPRESSED,
        Arc::new(object_store_opendal::OpendalStore::new(store.clone())),
    );

    let size = store.read(origin_path).await.unwrap().len();

    let config = scan_config(schema, None, origin_path, Arc::new(JsonSource::new()));
    let stream = FileStream::new(
        &config,
        0,
        Arc::new(json_opener),
        &ExecutionPlanMetricsSet::new(),
    )
    .unwrap();

    let (tmp_store, dir) = test_tmp_store("test_stream_to_json");

    let output_path = format!("{}/{}", dir.path().display(), "output");

    assert!(stream_to_json(
        Box::pin(stream),
        tmp_store.clone(),
        &output_path,
        threshold(size),
        8
    )
    .await
    .is_ok());

    let written = tmp_store.read(&output_path).await.unwrap();
    let origin = store.read(origin_path).await.unwrap();
    assert_eq_lines(written.to_vec(), origin.to_vec());
}

pub async fn setup_stream_to_csv_test(origin_path: &str, threshold: impl Fn(usize) -> usize) {
    let store = test_store("/");

    let schema = test_basic_schema();

    let csv_source = CsvSource::new(true, b',', b'"')
        .with_schema(schema.clone())
        .with_batch_size(TEST_BATCH_SIZE);
    let config = scan_config(schema, None, origin_path, csv_source.clone());
    let size = store.read(origin_path).await.unwrap().len();

    let csv_opener = csv_source.create_file_opener(
        Arc::new(object_store_opendal::OpendalStore::new(store.clone())),
        &config,
        0,
    );
    let stream = FileStream::new(&config, 0, csv_opener, &ExecutionPlanMetricsSet::new()).unwrap();

    let (tmp_store, dir) = test_tmp_store("test_stream_to_csv");

    let output_path = format!("{}/{}", dir.path().display(), "output");

    assert!(stream_to_csv(
        Box::pin(stream),
        tmp_store.clone(),
        &output_path,
        threshold(size),
        8
    )
    .await
    .is_ok());

    let written = tmp_store.read(&output_path).await.unwrap();
    let origin = store.read(origin_path).await.unwrap();
    assert_eq_lines(written.to_vec(), origin.to_vec());
}

// Ignore the CRLF difference across operating systems.
fn assert_eq_lines(written: Vec<u8>, origin: Vec<u8>) {
    assert_eq!(
        String::from_utf8(written)
            .unwrap()
            .lines()
            .collect::<Vec<_>>(),
        String::from_utf8(origin)
            .unwrap()
            .lines()
            .collect::<Vec<_>>(),
    )
}
