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

//! Handles bulk insert requests.

use datatypes::arrow;
use store_api::logstore::LogStore;
use store_api::metadata::RegionMetadataRef;
use store_api::region_request::RegionBulkInsertsRequest;

use crate::error::InconsistentTimestampLengthSnafu;
use crate::memtable::bulk::part::BulkPart;
use crate::request::{OptionOutputTx, SenderBulkRequest};
use crate::worker::RegionWorkerLoop;
use crate::{error, metrics};

impl<S: LogStore> RegionWorkerLoop<S> {
    pub(crate) async fn handle_bulk_insert_batch(
        &mut self,
        region_metadata: RegionMetadataRef,
        request: RegionBulkInsertsRequest,
        pending_bulk_request: &mut Vec<SenderBulkRequest>,
        sender: OptionOutputTx,
    ) {
        let _timer = metrics::REGION_WORKER_HANDLE_WRITE_ELAPSED
            .with_label_values(&["process_bulk_req"])
            .start_timer();
        let batch = request.payload;
        if batch.num_rows() == 0 {
            sender.send(Ok(0));
            return;
        }

        let Some((ts_index, ts)) = batch
            .schema()
            .column_with_name(&region_metadata.time_index_column().column_schema.name)
            .map(|(index, _)| (index, batch.column(index)))
        else {
            sender.send(
                error::InvalidRequestSnafu {
                    region_id: region_metadata.region_id,
                    reason: format!(
                        "timestamp column `{}` not found",
                        region_metadata.time_index_column().column_schema.name
                    ),
                }
                .fail(),
            );
            return;
        };

        if batch.num_rows() != ts.len() {
            sender.send(
                InconsistentTimestampLengthSnafu {
                    expected: batch.num_rows(),
                    actual: ts.len(),
                }
                .fail(),
            );
            return;
        }

        // safety: ts data type must be a timestamp type.
        let (ts_primitive, _) = datatypes::timestamp::timestamp_array_to_primitive(ts).unwrap();

        // safety: we've checked ts.len() == batch.num_rows() and batch is not empty
        let min_ts = arrow::compute::min(&ts_primitive).unwrap();
        let max_ts = arrow::compute::max(&ts_primitive).unwrap();

        let part = BulkPart {
            batch,
            max_timestamp: max_ts,
            min_timestamp: min_ts,
            sequence: 0,
            timestamp_index: ts_index,
            raw_data: Some(request.raw_data),
        };
        pending_bulk_request.push(SenderBulkRequest {
            sender,
            request: part,
            region_id: request.region_id,
            region_metadata,
            partition_expr_version: request.partition_expr_version,
        });
    }
}
