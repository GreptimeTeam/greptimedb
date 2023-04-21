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
use std::vec;

use arrow_schema::SchemaRef;
use datafusion::assert_batches_eq;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::file_format::{FileOpener, FileScanConfig, FileStream, ParquetExec};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use futures::StreamExt;

use crate::compression::CompressionType;
use crate::file_format::csv::{CsvConfigBuilder, CsvOpener};
use crate::file_format::json::JsonOpener;
use crate::file_format::parquet::DefaultParquetFileReaderFactory;
use crate::test_util::{self, test_basic_schema, test_store};

fn scan_config(file_schema: SchemaRef, limit: Option<usize>, filename: &str) -> FileScanConfig {
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

struct Test<'a, T: FileOpener> {
    config: FileScanConfig,
    opener: T,
    expected: Vec<&'a str>,
}

impl<'a, T: FileOpener> Test<'a, T> {
    pub async fn run(self) {
        let result = FileStream::new(
            &self.config,
            0,
            self.opener,
            &ExecutionPlanMetricsSet::new(),
        )
        .unwrap()
        .map(|b| b.unwrap())
        .collect::<Vec<_>>()
        .await;

        assert_batches_eq!(self.expected, &result);
    }
}

#[tokio::test]
async fn test_json_opener() {
    let store = test_store("/");

    let schema = test_basic_schema();

    let json_opener = JsonOpener::new(
        100,
        schema.clone(),
        store.clone(),
        CompressionType::UNCOMPRESSED,
    );

    let path = &test_util::get_data_dir("tests/json/basic.json")
        .display()
        .to_string();
    let tests = [
        Test {
            config: scan_config(schema.clone(), None, path),
            opener: json_opener.clone(),
            expected: vec![
                "+-----+-------+",
                "| num | str   |",
                "+-----+-------+",
                "| 5   | test  |",
                "| 2   | hello |",
                "| 4   | foo   |",
                "+-----+-------+",
            ],
        },
        Test {
            config: scan_config(schema.clone(), Some(1), path),
            opener: json_opener.clone(),
            expected: vec![
                "+-----+------+",
                "| num | str  |",
                "+-----+------+",
                "| 5   | test |",
                "+-----+------+",
            ],
        },
    ];

    for test in tests {
        test.run().await;
    }
}

#[tokio::test]
async fn test_csv_opener() {
    let store = test_store("/");

    let schema = test_basic_schema();
    let path = &test_util::get_data_dir("tests/csv/basic.csv")
        .display()
        .to_string();
    let csv_conf = CsvConfigBuilder::default()
        .batch_size(test_util::TEST_BATCH_SIZE)
        .file_schema(schema.clone())
        .build()
        .unwrap();

    let csv_opener = CsvOpener::new(csv_conf, store, CompressionType::UNCOMPRESSED);

    let tests = [
        Test {
            config: scan_config(schema.clone(), None, path),
            opener: csv_opener.clone(),
            expected: vec![
                "+-----+-------+",
                "| num | str   |",
                "+-----+-------+",
                "| 5   | test  |",
                "| 2   | hello |",
                "| 4   | foo   |",
                "+-----+-------+",
            ],
        },
        Test {
            config: scan_config(schema.clone(), Some(1), path),
            opener: csv_opener.clone(),
            expected: vec![
                "+-----+------+",
                "| num | str  |",
                "+-----+------+",
                "| 5   | test |",
                "+-----+------+",
            ],
        },
    ];

    for test in tests {
        test.run().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_parquet_exec() {
    let store = test_store("/");

    let schema = test_basic_schema();

    let path = &test_util::get_data_dir("tests/parquet/basic.parquet")
        .display()
        .to_string();
    let base_config = scan_config(schema.clone(), None, path);

    let exec = ParquetExec::new(base_config, None, None)
        .with_parquet_file_reader_factory(Arc::new(DefaultParquetFileReaderFactory::new(store)));

    let ctx = SessionContext::new();

    let context = Arc::new(TaskContext::from(&ctx));

    // The stream batch size can be set by ctx.session_config.batch_size
    let result = exec
        .execute(0, context)
        .unwrap()
        .map(|b| b.unwrap())
        .collect::<Vec<_>>()
        .await;

    assert_batches_eq!(
        vec![
            "+-----+-------+",
            "| num | str   |",
            "+-----+-------+",
            "| 5   | test  |",
            "| 2   | hello |",
            "| 4   | foo   |",
            "+-----+-------+",
        ],
        &result
    );
}
