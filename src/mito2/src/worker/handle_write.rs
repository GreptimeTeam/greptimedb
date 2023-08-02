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

use std::collections::HashMap;

use greptime_proto::v1::mito::Mutation;
use tokio::sync::oneshot::Sender;

use crate::error::{RegionNotFoundSnafu, Result};
use crate::region::version::VersionRef;
use crate::region::MitoRegionRef;
use crate::request::SenderWriteRequest;
use crate::worker::RegionWorkerLoop;

impl<S> RegionWorkerLoop<S> {
    /// Takes and handles all write requests.
    pub(crate) async fn handle_write_requests(&mut self, write_requests: Vec<SenderWriteRequest>) {
        if write_requests.is_empty() {
            return;
        }

        let mut region_ctxs = HashMap::new();
        for sender_req in write_requests {
            let region_id = sender_req.request.region_id;
            // Checks whether the region exists.
            if !region_ctxs.contains_key(&region_id) {
                let Some(region) = self.regions.get_region(region_id) else {
                    // No such region.
                    send_result(sender_req.sender, RegionNotFoundSnafu {
                        region_id,
                    }.fail());

                    continue;
                };

                // Initialize the context.
                region_ctxs.insert(region_id, RegionWriteCtx::new(region));
            }

            // Safety: Now we ensure the region exists.
            let region_ctx = region_ctxs.get_mut(&region_id).unwrap();

            // Checks request schema.
            if let Err(e) = sender_req
                .request
                .check_schema(&region_ctx.version.metadata)
            {
                send_result(sender_req.sender, Err(e));

                continue;
            }

            //
        }
        // We need to check:
        // - region exists, if not, return error
        // - check whether the schema is compatible with region schema. We should fill default value at this time.
        // - collect rows by region
        // - get sequence for each row

        // problem:
        // - column order in request may be different from table column order
        // - need to add missing column
        // - memtable may need a new struct for sequence and op type.

        todo!()
    }
}

/// Send result to the request.
fn send_result(sender: Option<Sender<Result<()>>>, res: Result<()>) {
    if let Some(sender) = sender {
        // Ignore send result.
        let _ = sender.send(res);
    }
}

/// Context to write to a region.
struct RegionWriteCtx {
    /// Region to write.
    region: MitoRegionRef,
    /// Version of the region while creating the context.
    version: VersionRef,
    /// Valid mutations.
    mutations: Vec<Mutation>,
    /// Result senders.
    ///
    /// The sender is 1:1 map to the mutation in `mutations`.
    senders: Vec<Option<Sender<Result<()>>>>,
}

impl RegionWriteCtx {
    /// Returns an empty context.
    fn new(region: MitoRegionRef) -> RegionWriteCtx {
        let version = region.version();
        RegionWriteCtx {
            region,
            version,
            mutations: Vec::new(),
            senders: Vec::new(),
        }
    }
}
