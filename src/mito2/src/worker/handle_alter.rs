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

//! Handling alter related requests.

use common_query::Output;
use common_telemetry::info;
use store_api::metadata::RegionMetadata;
use store_api::region_request::RegionAlterRequest;
use store_api::storage::RegionId;
use tokio::sync::oneshot;

use crate::error::{RegionNotFoundSnafu, Result};
use crate::region::version::Version;
use crate::worker::RegionWorkerLoop;

impl<S> RegionWorkerLoop<S> {
    pub(crate) async fn handle_alter_request(
        &mut self,
        region_id: RegionId,
        request: RegionAlterRequest,
        sender: Option<oneshot::Sender<Result<Output>>>,
    ) {
        let Some(region) = self.regions.get_region(region_id) else {
            if let Some(sender) = sender {
                let _ = sender.send(RegionNotFoundSnafu { region_id }.fail());
            }
            return;
        };

        info!("Try to alter region: {}, request: {:?}", region_id, request);

        // TODO(yingwen): We should remove immutable memtables on flush finished.
        let version = region.version();
        if !can_alter_directly(&version) {
            // We need to flush all memtables first.
            info!("Flush region: {} before alteration", region_id);

            todo!()
        }

        // Now we can alter the region directly.
        //

        unimplemented!()
    }
}

/// Checks whether all memtables are empty.
fn can_alter_directly(version: &Version) -> bool {
    unimplemented!()
}

/// Creates a metadata after applying the alter `request` to the old `metadata`.
///
/// Returns an error if the `request` is invalid.
fn metadata_after_alteration(
    metadata: &RegionMetadata,
    request: RegionAlterRequest,
) -> Result<RegionMetadata> {
    unimplemented!()
}
