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

use client::Database;
use common_query::Output;
use common_recordbatch::util;

pub enum ExpectedOutput<'a> {
    AffectedRows(usize),
    QueryResult(&'a str),
}

pub async fn execute_and_check_output(db: &Database, sql: &str, expected: ExpectedOutput<'_>) {
    let output = db.sql(sql).await.unwrap();
    match (&output, expected) {
        (Output::AffectedRows(x), ExpectedOutput::AffectedRows(y)) => {
            assert_eq!(*x, y, "actual: \n{}", x)
        }
        (Output::RecordBatches(_), ExpectedOutput::QueryResult(x))
        | (Output::Stream(_, _), ExpectedOutput::QueryResult(x)) => {
            check_output_stream(output, x).await
        }
        _ => panic!(),
    }
}

pub async fn check_output_stream(output: Output, expected: &str) {
    let recordbatches = match output {
        Output::Stream(stream, _) => util::collect_batches(stream).await.unwrap(),
        Output::RecordBatches(recordbatches) => recordbatches,
        _ => unreachable!(),
    };
    let pretty_print = recordbatches.pretty_print().unwrap();
    assert_eq!(pretty_print, expected, "actual: \n{}", pretty_print);
}
