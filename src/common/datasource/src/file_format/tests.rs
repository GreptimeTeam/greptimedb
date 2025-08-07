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
use datafusion::datasource::physical_plan::{
    CsvSource, FileScanConfig, FileSource, FileStream, JsonSource, ParquetSource,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_orc::OrcSource;
use futures::StreamExt;
use object_store::ObjectStore;

use super::FORMAT_TYPE;
use crate::file_format::parquet::DefaultParquetFileReaderFactory;
use crate::file_format::{FileFormat, Format, OrcFormat};
use crate::test_util::{scan_config, test_basic_schema, test_store};
use crate::{error, test_util};

struct Test<'a> {
    config: FileScanConfig,
    file_source: Arc<dyn FileSource>,
    expected: Vec<&'a str>,
}

impl Test<'_> {
    async fn run(self, store: &ObjectStore) {
        let store = Arc::new(object_store_opendal::OpendalStore::new(store.clone()));
        let file_opener = self.file_source.create_file_opener(store, &self.config, 0);

        let result = FileStream::new(
            &self.config,
            0,
            file_opener,
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
    let file_source = Arc::new(JsonSource::new()).with_batch_size(test_util::TEST_BATCH_SIZE);

    let path = &find_workspace_path("/src/common/datasource/tests/json/basic.json")
        .display()
        .to_string();
    let tests = [
        Test {
            config: scan_config(schema.clone(), None, path, file_source.clone()),
            file_source: file_source.clone(),
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
            config: scan_config(schema, Some(1), path, file_source.clone()),
            file_source,
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
        test.run(&store).await;
    }
}

#[tokio::test]
async fn test_csv_opener() {
    let store = test_store("/");
    let schema = test_basic_schema();
    let path = &find_workspace_path("/src/common/datasource/tests/csv/basic.csv")
        .display()
        .to_string();

    let file_source = CsvSource::new(true, b',', b'"')
        .with_batch_size(test_util::TEST_BATCH_SIZE)
        .with_schema(schema.clone());

    let tests = [
        Test {
            config: scan_config(schema.clone(), None, path, file_source.clone()),
            file_source: file_source.clone(),
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
            config: scan_config(schema, Some(1), path, file_source.clone()),
            file_source,
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
        test.run(&store).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_parquet_exec() {
    let store = test_store("/");

    let schema = test_basic_schema();

    let path = &find_workspace_path("/src/common/datasource/tests/parquet/basic.parquet")
        .display()
        .to_string();

    let parquet_source = ParquetSource::default()
        .with_parquet_file_reader_factory(Arc::new(DefaultParquetFileReaderFactory::new(store)));

    let config = scan_config(schema, None, path, Arc::new(parquet_source));
    let exec = DataSourceExec::from_data_source(config);
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
    let path = &find_workspace_path("/src/common/datasource/tests/orc/test.orc")
        .display()
        .to_string();

    let store = test_store("/");
    let schema = Arc::new(OrcFormat.infer_schema(&store, path).await.unwrap());
    let file_source = Arc::new(OrcSource::default());

    let tests = [
        Test {
            config: scan_config(schema.clone(), None, path, file_source.clone()),
            file_source: file_source.clone(),
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
            config: scan_config(schema.clone(), Some(1), path, file_source.clone()),
            file_source,
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
        test.run(&store).await;
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
