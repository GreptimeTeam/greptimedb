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

use std::pin::Pin;
use std::task::{Context, Poll};

use common_recordbatch::{RecordBatch, RecordBatchStream, SendableRecordBatchStream};
use datatypes::schema::SchemaRef;
use futures::Stream;
use futures_util::ready;
use lazy_static::lazy_static;
use prometheus::*;

lazy_static! {
    pub static ref METRIC_PARSE_SQL_ELAPSED: Histogram = register_histogram!(
        "greptime_query_parse_sql_elapsed",
        "query parse sql elapsed"
    )
    .unwrap();
    pub static ref METRIC_PARSE_PROMQL_ELAPSED: Histogram = register_histogram!(
        "greptime_query_parse_promql_elapsed",
        "query parse promql elapsed"
    )
    .unwrap();
    pub static ref METRIC_OPTIMIZE_LOGICAL_ELAPSED: Histogram = register_histogram!(
        "greptime_query_optimize_logicalplan_elapsed",
        "query optimize logicalplan elapsed"
    )
    .unwrap();
    pub static ref METRIC_OPTIMIZE_PHYSICAL_ELAPSED: Histogram = register_histogram!(
        "greptime_query_optimize_physicalplan_elapsed",
        "query optimize physicalplan elapsed"
    )
    .unwrap();
    pub static ref METRIC_CREATE_PHYSICAL_ELAPSED: Histogram = register_histogram!(
        "greptime_query_create_physicalplan_elapsed",
        "query create physicalplan elapsed"
    )
    .unwrap();
    pub static ref METRIC_EXEC_PLAN_ELAPSED: Histogram = register_histogram!(
        "greptime_query_execute_plan_elapsed",
        "query execute plan elapsed"
    )
    .unwrap();
    pub static ref METRIC_MERGE_SCAN_POLL_ELAPSED: Histogram = register_histogram!(
        "greptime_query_merge_scan_poll_elapsed",
        "query merge scan poll elapsed"
    )
    .unwrap();
    pub static ref METRIC_MERGE_SCAN_REGIONS: Histogram = register_histogram!(
        "greptime_query_merge_scan_regions",
        "query merge scan regions"
    )
    .unwrap();
    pub static ref METRIC_MERGE_SCAN_ERRORS_TOTAL: IntCounter = register_int_counter!(
        "greptime_query_merge_scan_errors_total",
        "query merge scan errors total"
    )
    .unwrap();
}

/// A stream to call the callback once a RecordBatch stream is done.
pub struct OnDone<F> {
    stream: SendableRecordBatchStream,
    callback: Option<F>,
}

impl<F> OnDone<F> {
    /// Attaches a `callback` to invoke once the `stream` is terminated.
    pub fn new(stream: SendableRecordBatchStream, callback: F) -> Self {
        Self {
            stream,
            callback: Some(callback),
        }
    }
}

impl<F: FnOnce() + Unpin> RecordBatchStream for OnDone<F> {
    fn schema(&self) -> SchemaRef {
        self.stream.schema()
    }
}

impl<F: FnOnce() + Unpin> Stream for OnDone<F> {
    type Item = common_recordbatch::error::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(Pin::new(&mut self.stream).poll_next(cx)) {
            Some(rb) => Poll::Ready(Some(rb)),
            None => {
                if let Some(callback) = self.callback.take() {
                    callback();
                }
                Poll::Ready(None)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}
