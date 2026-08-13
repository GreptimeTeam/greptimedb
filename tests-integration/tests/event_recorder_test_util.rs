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
use datatypes::arrow::datatypes::{DataType as ArrowDataType, UInt32Type};
use frontend::instance::Instance;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::QueryContext;

const MAX_ATTEMPTS: usize = 60;
const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Asserts that submission and completion events record the expected actor.
pub(crate) async fn assert_procedure_actor(
    instance: &Arc<Instance>,
    procedure_id: &str,
    actor: Option<&str>,
) {
    let actor_predicate = actor.map_or_else(
        || "count(actor) = 0".to_string(),
        |actor| format!("count(*) = count(CASE WHEN actor = '{actor}' THEN 1 END)"),
    );
    for trigger in ["Submitted", "Succeeded"] {
        assert_eventually_eq(
            instance,
            &format!(
                "SELECT count(*) > 0 AND {actor_predicate} AS actor_matches FROM greptime_private.events WHERE procedure_id = '{procedure_id}' AND json_get_string(procedure_trigger, 'type') = '{trigger}'"
            ),
            "\
+---------------+
| actor_matches |
+---------------+
| true          |
+---------------+",
        )
        .await;
    }
}

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

/// Returns the first non-null u32 from `column` once the query produces one.
pub(crate) async fn find_eventually_u32(
    instance: &Arc<Instance>,
    query: &str,
    column: &str,
) -> u32 {
    for _ in 0..MAX_ATTEMPTS {
        if let Some(value) = query_first_u32(instance, query, column).await {
            return value;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }

    panic!("timed out waiting for event query: {query}");
}

/// Asserts that the query eventually returns the expected pretty-printed rows.
pub(crate) async fn assert_eventually_eq(instance: &Arc<Instance>, query: &str, expected: &str) {
    let mut last_actual = None;
    for _ in 0..MAX_ATTEMPTS {
        last_actual = query_pretty_print(instance, query).await;
        if last_actual.as_deref() == Some(expected) {
            return;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }

    panic!(
        "timed out waiting for event query: {query}\nexpected:\n{expected}\nactual:\n{}",
        last_actual.as_deref().unwrap_or("<query failed>")
    );
}

async fn event_count_is_one(instance: &Arc<Instance>, query: &str) -> bool {
    query_pretty_print(instance, query).await.as_deref()
        == Some(
            "\
+-------------+
| event_count |
+-------------+
| 1           |
+-------------+",
        )
}

async fn query_first_string(instance: &Arc<Instance>, query: &str, column: &str) -> Option<String> {
    let batches = query_record_batches(instance, query).await?;
    let batch = batches.take().into_iter().next()?;
    let column = batch.column_by_name(column)?;
    match column.data_type() {
        ArrowDataType::Utf8 => column
            .as_string::<i32>()
            .iter()
            .next()
            .flatten()
            .map(ToString::to_string),
        ArrowDataType::LargeUtf8 => column
            .as_string::<i64>()
            .iter()
            .next()
            .flatten()
            .map(ToString::to_string),
        ArrowDataType::Utf8View => column
            .as_string_view()
            .iter()
            .next()
            .flatten()
            .map(ToString::to_string),
        data_type => panic!("expected a string column, got {data_type:?}"),
    }
}

async fn query_first_u32(instance: &Arc<Instance>, query: &str, column: &str) -> Option<u32> {
    let batches = query_record_batches(instance, query).await?;
    let batch = batches.take().into_iter().next()?;
    batch
        .column_by_name(column)?
        .as_primitive::<UInt32Type>()
        .iter()
        .next()
        .flatten()
}

async fn query_pretty_print(instance: &Arc<Instance>, query: &str) -> Option<String> {
    query_record_batches(instance, query)
        .await?
        .pretty_print()
        .ok()
}

async fn query_record_batches(instance: &Arc<Instance>, query: &str) -> Option<RecordBatches> {
    let output = instance
        .do_query(query, QueryContext::arc())
        .await
        .remove(0)
        .ok()?;
    let OutputData::Stream(stream) = output.data else {
        return None;
    };
    RecordBatches::try_collect(stream).await.ok()
}
