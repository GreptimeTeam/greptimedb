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

//! Handling write requests.

use crate::{worker::RegionWorkerLoop, request::{RegionRequest}};

impl<S> RegionWorkerLoop<S> {
    /// Takes and handles all write requests.
    ///
    /// # Panics
    /// Panics if `write_requests` contains a request whose body is a [WriteRequest].
    pub(crate) async fn handle_write_requests(&mut self, write_requests: Vec<RegionRequest>) {
        if write_requests.is_empty() {
            return;
        }

        // We need to check:
        // - region exists, if not, return error
        // - check whether the schema is compatible with region schema
        // - collect rows by region
        // - get sequence for each row

        // problem:
        // - column order in request may be different from table column order
        // - need to add missing column
        // - memtable may need a new struct for sequence and op type.

        todo!()
    }
}


// pb write message
// region id
// rows
// sequence
// op type

// /// Entry for a write request in [WriteRequestBatch].
// #[derive(Debug)]
// pub(crate) struct BatchEntry {
//     /// Result sender.
//     pub(crate) sender: Option<Sender<Result<()>>>,
//     /// A region write request.
//     pub(crate) request: WriteRequest,
// }

// /// Batch of write requests.
// #[derive(Debug, Default)]
// pub(crate) struct WriteRequestBatch {
//     /// Batched requests for each region.
//     pub(crate) requests: HashMap<RegionId, Vec<BatchEntry>>,
// }

// impl WriteRequestBatch {
//     /// Push a write request into the batch.
//     ///
//     /// # Panics
//     /// Panics if the request body isn't a [WriteRequest].
//     pub(crate) fn push(&mut self, request: RegionRequest) {
//         match request.body {
//             RequestBody::Write(write_req) => {
//                 self.requests.entry(write_req.region_id)
//                     .or_default()
//                     .push(BatchEntry { sender: request.sender, request: write_req, })
//             },
//             other => panic!("request is not a write request: {:?}", other),
//         }
//     }

//     /// Returns true if the batch is empty.
//     pub(crate) fn is_empty(&self) -> bool {
//         self.requests.is_empty()
//     }
// }

