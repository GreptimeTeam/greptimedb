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

use std::assert_matches::assert_matches;
use std::collections::HashMap;
use std::sync::Arc;
use std::vec;

use common_test_util::find_workspace_path;
use datafusion::assert_batches_eq;
use datafusion::datasource::physical_plan::{FileOpener, FileScanConfig, FileStream, ParquetExec};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use futures::StreamExt;

use super::FORMAT_TYPE;
use crate::compression::CompressionType;
use crate::error;
use crate::file_format::csv::{CsvConfigBuilder, CsvOpener};
use crate::file_format::json::JsonOpener;
use crate::file_format::orc::{OrcFormat, OrcOpener};
use crate::file_format::parquet::DefaultParquetFileReaderFactory;
use crate::file_format::{FileFormat, Format};
use crate::test_util::{self, scan_config, test_basic_schema, test_store};

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
        CompressionType::Uncompressed,
    );

    let path = &find_workspace_path("/src/common/datasource/tests/json/basic.json")
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
    let path = &find_workspace_path("/src/common/datasource/tests/csv/basic.csv")
        .display()
        .to_string();
    let csv_conf = CsvConfigBuilder::default()
        .batch_size(test_util::TEST_BATCH_SIZE)
        .file_schema(schema.clone())
        .build()
        .unwrap();

    let csv_opener = CsvOpener::new(csv_conf, store, CompressionType::Uncompressed);

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

    let path = &find_workspace_path("/src/common/datasource/tests/parquet/basic.parquet")
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
        [
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

#[tokio::test]
async fn test_orc_opener() {
    let root = find_workspace_path("/src/common/datasource/tests/orc")
        .display()
        .to_string();
    let store = test_store(&root);
    let schema = OrcFormat.infer_schema(&store, "test.orc").await.unwrap();
    let schema = Arc::new(schema);

    let orc_opener = OrcOpener::new(store.clone(), schema.clone(), None);
    let path = "test.orc";

    let tests = [
        Test {
            config: scan_config(schema.clone(), None, path),
            opener: orc_opener.clone(),
            expected: vec![
            "+----------+-----+-------+------------+-----+-----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+----------------------------+-------------+",
            "| double_a | a   | b     | str_direct | d   | e   | f     | int_short_repeated | int_neg_short_repeated | int_delta | int_neg_delta | int_direct | int_neg_direct | bigint_direct | bigint_neg_direct | bigint_other | utf8_increase | utf8_decrease | timestamp_simple           | date_simple |",
            "+----------+-----+-------+------------+-----+-----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+----------------------------+-------------+",
            "| 1.0      | 1.0 | true  | a          | a   | ddd | aaaaa | 5                  | -5                     | 1         | 5             | 1          | -1             | 1             | -1                | 5            | a             | eeeee         | 2023-04-01T20:15:30.002    | 2023-04-01  |",
            "| 2.0      | 2.0 | false | cccccc     | bb  | cc  | bbbbb | 5                  | -5                     | 2         | 4             | 6          | -6             | 6             | -6                | -5           | bb            | dddd          | 2021-08-22T07:26:44.525777 | 2023-03-01  |",
            "| 3.0      |     |       |            |     |     |       |                    |                        |           |               |            |                |               |                   | 1            | ccc           | ccc           | 2023-01-01T00:00:00        | 2023-01-01  |",
            "| 4.0      | 4.0 | true  | ddd        | ccc | bb  | ccccc | 5                  | -5                     | 4         | 2             | 3          | -3             | 3             | -3                | 5            | dddd          | bb            | 2023-02-01T00:00:00        | 2023-02-01  |",
            "| 5.0      | 5.0 | false | ee         | ddd | a   | ddddd | 5                  | -5                     | 5         | 1             | 2          | -2             | 2             | -2                | 5            | eeeee         | a             | 2023-03-01T00:00:00        | 2023-03-01  |",
            "+----------+-----+-------+------------+-----+-----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+----------------------------+-------------+",
            ],
        },
        Test {
            config: scan_config(schema.clone(), Some(1), path),
            opener: orc_opener.clone(),
            expected: vec![
                "+----------+-----+------+------------+---+-----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+-------------------------+-------------+",
                "| double_a | a   | b    | str_direct | d | e   | f     | int_short_repeated | int_neg_short_repeated | int_delta | int_neg_delta | int_direct | int_neg_direct | bigint_direct | bigint_neg_direct | bigint_other | utf8_increase | utf8_decrease | timestamp_simple        | date_simple |",
                "+----------+-----+------+------------+---+-----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+-------------------------+-------------+",
                "| 1.0      | 1.0 | true | a          | a | ddd | aaaaa | 5                  | -5                     | 1         | 5             | 1          | -1             | 1             | -1                | 5            | a             | eeeee         | 2023-04-01T20:15:30.002 | 2023-04-01  |",
                "+----------+-----+------+------------+---+-----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+-------------------------+-------------+",
            ],
        },
    ];

    for test in tests {
        test.run().await;
    }
}

#[test]
fn test_format() {
    let value = [(FORMAT_TYPE.to_string(), "csv".to_string())]
        .into_iter()
        .collect::<HashMap<_, _>>();

    assert_matches!(Format::try_from(&value).unwrap(), Format::Csv(_));

    let value = [(FORMAT_TYPE.to_string(), "Parquet".to_string())]
        .into_iter()
        .collect::<HashMap<_, _>>();

    assert_matches!(Format::try_from(&value).unwrap(), Format::Parquet(_));

    let value = [(FORMAT_TYPE.to_string(), "JSON".to_string())]
        .into_iter()
        .collect::<HashMap<_, _>>();

    assert_matches!(Format::try_from(&value).unwrap(), Format::Json(_));

    let value = [(FORMAT_TYPE.to_string(), "ORC".to_string())]
        .into_iter()
        .collect::<HashMap<_, _>>();

    assert_matches!(Format::try_from(&value).unwrap(), Format::Orc(_));

    let value = [(FORMAT_TYPE.to_string(), "Foobar".to_string())]
        .into_iter()
        .collect::<HashMap<_, _>>();

    assert_matches!(
        Format::try_from(&value).unwrap_err(),
        error::Error::UnsupportedFormat { .. }
    );

    let value = HashMap::new();

    assert_matches!(Format::try_from(&value).unwrap(), Format::Parquet(_));
}
