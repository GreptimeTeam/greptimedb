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
use datatypes::arrow::array::{
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use datatypes::arrow::datatypes::{DataType, TimeUnit};
use store_api::logstore::LogStore;
use store_api::metadata::RegionMetadataRef;
use store_api::region_request::RegionBulkInsertsRequest;

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
        let num_rows = batch.num_rows();

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

        let DataType::Timestamp(unit, _) = ts.data_type() else {
            // safety: ts data type must be a timestamp type.
            unreachable!()
        };

        let (min_ts, max_ts) = match unit {
            TimeUnit::Second => {
                let ts = ts.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
                (
                    //safety: ts array must contain at least one row so this won't return None.
                    arrow::compute::min(ts).unwrap(),
                    arrow::compute::max(ts).unwrap(),
                )
            }

            TimeUnit::Millisecond => {
                let ts = ts
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();
                (
                    //safety: ts array must contain at least one row so this won't return None.
                    arrow::compute::min(ts).unwrap(),
                    arrow::compute::max(ts).unwrap(),
                )
            }
            TimeUnit::Microsecond => {
                let ts = ts
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                (
                    //safety: ts array must contain at least one row so this won't return None.
                    arrow::compute::min(ts).unwrap(),
                    arrow::compute::max(ts).unwrap(),
                )
            }
            TimeUnit::Nanosecond => {
                let ts = ts
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                (
                    //safety: ts array must contain at least one row so this won't return None.
                    arrow::compute::min(ts).unwrap(),
                    arrow::compute::max(ts).unwrap(),
                )
            }
        };

        let part = BulkPart {
            batch,
            num_rows,
            max_ts,
            min_ts,
            sequence: 0,
            timestamp_index: ts_index,
        };
        pending_bulk_request.push(SenderBulkRequest {
            sender,
            request: part,
            region_id: request.region_id,
            region_metadata,
        });
    }
}
