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

//! Reclaims the temporary files that a crashed writer left in a region.

use std::sync::Arc;
use std::time::Duration;

use common_telemetry::info;
use object_store::temp_file::{sweep_temporary_files, writes_temporary_files};
use store_api::storage::RegionId;
use tokio::sync::Semaphore;

use crate::access_layer::{AccessLayer, AccessLayerRef};
use crate::manifest::storage::manifest_dir;
use crate::manifest::storage::staging::{staging_blob_path, staging_manifest_path};
use crate::sst::location::index_dir;

/// Returns the directories that `region_id` writes non-append files into.
///
/// Every one of them can hold the temporary file of a crashed writer, and a
/// list is not recursive, so the sweep has to name each directory.
pub(crate) fn region_write_dirs(access_layer: &AccessLayer, region_id: RegionId) -> Vec<String> {
    let region_dir = access_layer.build_region_dir(region_id);
    let manifest_path = manifest_dir(&region_dir);
    vec![
        region_dir.clone(),
        index_dir(&region_dir),
        manifest_path.clone(),
        staging_blob_path(&manifest_path),
        staging_manifest_path(&manifest_path),
    ]
}

/// Sweeps the temporary files of `region_id` in the background.
///
/// The caller is on the path that makes the region its writable leader, so
/// reclaiming leftovers must not slow it down or fail it: everything happens in
/// a spawned task that only logs.
///
/// `older_than` is the margin the caller takes from GC's
/// `unknown_file_lingering_time`, which guards the same thing: a file this old
/// is not one a writer is still filling.
pub(crate) fn spawn_region_sweep(
    access_layer: AccessLayerRef,
    region_id: RegionId,
    older_than: Duration,
    sweep_limiter: Arc<Semaphore>,
) {
    // No write to this store leaves a temporary file behind, so don't spend a
    // task on it: a sweep would still list every directory of the region to
    // find that out.
    if !writes_temporary_files(access_layer.object_store()) {
        return;
    }

    common_runtime::spawn_global(async move {
        // A burst of regions becoming writable must not run more sweeps at once
        // than the datanode allows GC jobs. Wait for a permit instead of giving
        // up, the sweep has no deadline.
        let Ok(_permit) = sweep_limiter.acquire_owned().await else {
            return;
        };

        let dirs = region_write_dirs(&access_layer, region_id);
        let deleted = sweep_temporary_files(access_layer.object_store(), &dirs, older_than).await;
        if deleted > 0 {
            info!(
                "Reclaimed {deleted} temporary file(s) of a crashed writer in region {region_id}"
            );
        }
    });
}

#[cfg(test)]
mod tests {
    use object_store::ObjectStore;
    use object_store::services::Memory;
    use store_api::region_request::PathType;

    use super::*;
    use crate::test_util::TestEnv;

    fn test_access_layer(env: &TestEnv) -> AccessLayer {
        AccessLayer::new(
            "test",
            PathType::Bare,
            ObjectStore::new(Memory::default()).unwrap(),
            env.get_puffin_manager(),
            env.get_intermediate_manager(),
        )
    }

    #[tokio::test]
    async fn test_region_write_dirs() {
        let env = TestEnv::new().await;
        let access_layer = test_access_layer(&env);
        let region_id = RegionId::new(1, 2);

        let dirs = region_write_dirs(&access_layer, region_id);

        assert_eq!(5, dirs.len());
        assert!(dirs[0].ends_with("1_0000000002/"));
        assert!(dirs[1].ends_with("1_0000000002/index/"));
        assert!(dirs[2].ends_with("1_0000000002/manifest/"));
        assert!(dirs[3].ends_with("1_0000000002/staging/blob/"));
        assert!(dirs[4].ends_with("1_0000000002/staging/manifest/"));
    }
}
