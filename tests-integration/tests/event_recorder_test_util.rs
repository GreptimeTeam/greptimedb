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
use std::time::Duration;

use client::OutputData;
use common_recordbatch::RecordBatches;
use datatypes::arrow::array::AsArray;
use frontend::instance::Instance;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::QueryContext;

const MAX_ATTEMPTS: usize = 60;
const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Asserts that the query eventually returns exactly one event.
pub(crate) async fn assert_single_event(instance: &Arc<Instance>, query: &str) {
    for _ in 0..MAX_ATTEMPTS {
        if event_count_is_one(instance, query).await {
            return;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }

    panic!("timed out waiting for event: {query}");
}

/// Returns the first non-null string from `column` once the query produces one.
pub(crate) async fn find_eventually_string(
    instance: &Arc<Instance>,
    query: &str,
    column: &str,
) -> String {
    for _ in 0..MAX_ATTEMPTS {
        if let Some(value) = query_first_string(instance, query, column).await {
            return value;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }

    panic!("timed out waiting for event query: {query}");
}

async fn event_count_is_one(instance: &Arc<Instance>, query: &str) -> bool {
    let Ok(output) = instance
        .do_query(query, QueryContext::arc())
        .await
        .remove(0)
    else {
        return false;
    };
    let OutputData::Stream(stream) = output.data else {
        unreachable!("event-count query must return a stream");
    };
    let Ok(batches) = RecordBatches::try_collect(stream).await else {
        return false;
    };
    let Ok(actual) = batches.pretty_print() else {
        return false;
    };

    actual
        == "\\
+-------------+
| event_count |
+-------------+
| 1           |
+-------------+"
}

async fn query_first_string(instance: &Arc<Instance>, query: &str, column: &str) -> Option<String> {
    let output = instance
        .do_query(query, QueryContext::arc())
        .await
        .remove(0)
        .ok()?;
    let OutputData::Stream(stream) = output.data else {
        unreachable!("event query must return a stream");
    };
    let batches = RecordBatches::try_collect(stream).await.ok()?;
    let batch = batches.take().into_iter().next()?;
    batch
        .column_by_name(column)?
        .as_string::<i32>()
        .iter()
        .next()
        .flatten()
        .map(ToString::to_string)
}
