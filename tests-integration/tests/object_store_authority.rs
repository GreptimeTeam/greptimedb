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

use common_meta::election::object_store::{AuthorityHead, HeadStore};
use object_store::ErrorKind;
use tests_integration::test_util::{StorageType, get_test_store_config};

/// Requires an explicitly configured S3-compatible endpoint (MinIO for PR1 qualification).
#[tokio::test]
async fn test_minio_head_storage() {
    if !StorageType::S3.test_on() {
        common_telemetry::init_default_ut_logging();
        common_telemetry::warn!(
            "NOT RUN: Authority storage qualification requires configured S3/MinIO"
        );
        return;
    }
    for name in [
        "GT_S3_BUCKET",
        "GT_S3_ACCESS_KEY_ID",
        "GT_S3_ACCESS_KEY",
        "GT_S3_REGION",
    ] {
        assert!(
            std::env::var_os(name).is_some(),
            "Missing {name}; this test requires real storage"
        );
    }
    let (config, _guard) = get_test_store_config(&StorageType::S3);
    let store = HeadStore::open(&config, "", "authority-test")
        .await
        .unwrap();
    assert_eq!(
        store
            .read_head()
            .await
            .unwrap_err()
            .authority_storage_error()
            .unwrap()
            .kind(),
        ErrorKind::NotFound
    );
    store.create_initial_head().await.unwrap();
    let initial = store.read_head().await.unwrap();
    assert_eq!(initial.head(), &AuthorityHead::initial());
    assert!(initial.metadata().last_modified().is_some());
    assert!(initial.metadata().etag().unwrap().starts_with('"'));
    let e = store.create_initial_head().await.unwrap_err();
    assert!(matches!(
        e.authority_storage_error().unwrap().kind(),
        ErrorKind::ConditionNotMatch | ErrorKind::AlreadyExists
    ));
    let next = initial.head().clone().with_next_write_seq().unwrap();
    store.replace_head(&initial, &next).await.unwrap();
    let current = store.read_head().await.unwrap();
    assert_eq!(current.head(), &next);
    assert!(current.metadata().last_modified().is_some());
    assert!(
        !store
            .same_version(initial.metadata(), current.metadata())
            .unwrap()
    );
    assert_eq!(
        store
            .replace_head(&initial, &next)
            .await
            .unwrap_err()
            .authority_storage_error()
            .unwrap()
            .kind(),
        ErrorKind::ConditionNotMatch
    );
    let after = store.read_head().await.unwrap();
    assert_eq!(after.head(), &next);
    assert!(
        store
            .same_version(after.metadata(), current.metadata())
            .unwrap()
    );
}
