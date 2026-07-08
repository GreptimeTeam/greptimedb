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

use std::collections::{HashMap, HashSet};

use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use store_api::storage::RegionId;

use crate::config::MitoConfig;
use crate::test_util::TestEnv;

#[tokio::test]
async fn get_snapshot_of_file_refs_errors_on_missing_related_region() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let source_region = RegionId::new(1, 1);
    let missing_target_region = RegionId::new(1, 2);
    let related_regions = HashMap::from([(source_region, HashSet::from([missing_target_region]))]);

    let err = engine
        .get_snapshot_of_file_refs([], related_regions)
        .await
        .expect_err("missing related target region should fail file ref snapshot");

    assert_eq!(StatusCode::RegionNotFound, err.status_code());
}

#[tokio::test]
async fn get_snapshot_of_file_refs_skips_missing_file_handle_region() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let missing_region = RegionId::new(1, 1);
    let manifest = engine
        .get_snapshot_of_file_refs([missing_region], HashMap::new())
        .await
        .expect("missing file handle region should be skipped");

    assert!(manifest.file_refs.is_empty());
    assert!(manifest.manifest_version.is_empty());
    assert!(manifest.cross_region_refs.is_empty());
}
