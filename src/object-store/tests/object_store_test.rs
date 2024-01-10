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

use std::env;
use std::sync::Arc;

use anyhow::Result;
use common_telemetry::logging;
use common_test_util::temp_dir::create_temp_dir;
use object_store::layers::LruCacheLayer;
use object_store::services::{Fs, S3};
use object_store::test_util::TempFolder;
use object_store::{ObjectStore, ObjectStoreBuilder};
use opendal::raw::Accessor;
use opendal::services::{Azblob, Gcs, Oss};
use opendal::{EntryMode, Operator, OperatorBuilder};

async fn test_object_crud(store: &ObjectStore) -> Result<()> {
    // Create object handler.
    // Write data info object;
    let file_name = "test_file";
    assert!(store.read(file_name).await.is_err());

    store.write(file_name, "Hello, World!").await?;

    // Read data from object;
    let bs = store.read(file_name).await?;
    assert_eq!("Hello, World!", String::from_utf8(bs)?);

    // Read range from object;
    let bs = store.read_with(file_name).range(1..=11).await?;
    assert_eq!("ello, World", String::from_utf8(bs)?);

    // Get object's Metadata
    let meta = store.stat(file_name).await?;
    assert_eq!(EntryMode::FILE, meta.mode());
    assert_eq!(13, meta.content_length());

    // Delete object.
    store.delete(file_name).await.unwrap();
    assert!(store.read(file_name).await.is_err());
    Ok(())
}

async fn test_object_list(store: &ObjectStore) -> Result<()> {
    // Create  some object handlers.
    // Write something
    let p1 = "test_file1";
    let p2 = "test_file2";
    let p3 = "test_file3";
    store.write(p1, "Hello, object1!").await?;
    store.write(p2, "Hello, object2!").await?;
    store.write(p3, "Hello, object3!").await?;

    // List objects
    let entries = store.list("/").await?;
    assert_eq!(3, entries.len());

    store.delete(p1).await?;
    store.delete(p3).await?;

    // List objects again
    // Only o2 is exists
    let entries = store.list("/").await?;
    assert_eq!(1, entries.len());
    assert_eq!(p2, entries.first().unwrap().path());

    let content = store.read(p2).await?;
    assert_eq!("Hello, object2!", String::from_utf8(content)?);

    store.delete(p2).await?;
    let entries = store.list("/").await?;
    assert!(entries.is_empty());

    assert!(store.read(p1).await.is_err());
    assert!(store.read(p2).await.is_err());
    assert!(store.read(p3).await.is_err());

    Ok(())
}

#[tokio::test]
async fn test_fs_backend() -> Result<()> {
    let data_dir = create_temp_dir("test_fs_backend");
    let tmp_dir = create_temp_dir("test_fs_backend");
    let mut builder = Fs::default();
    let _ = builder
        .root(&data_dir.path().to_string_lossy())
        .atomic_write_dir(&tmp_dir.path().to_string_lossy());

    let store = ObjectStore::new(builder).unwrap().finish();

    test_object_crud(&store).await?;
    test_object_list(&store).await?;

    Ok(())
}

#[tokio::test]
async fn test_s3_backend() -> Result<()> {
    logging::init_default_ut_logging();
    if let Ok(bucket) = env::var("GT_S3_BUCKET") {
        if !bucket.is_empty() {
            logging::info!("Running s3 test.");

            let root = uuid::Uuid::new_v4().to_string();

            let mut builder = S3::default();
            let _ = builder
                .root(&root)
                .access_key_id(&env::var("GT_S3_ACCESS_KEY_ID")?)
                .secret_access_key(&env::var("GT_S3_ACCESS_KEY")?)
                .region(&env::var("GT_S3_REGION")?)
                .bucket(&bucket);

            let store = ObjectStore::new(builder).unwrap().finish();

            let guard = TempFolder::new(&store, "/");
            test_object_crud(&store).await?;
            test_object_list(&store).await?;
            guard.remove_all().await?;
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_oss_backend() -> Result<()> {
    logging::init_default_ut_logging();
    if let Ok(bucket) = env::var("GT_OSS_BUCKET") {
        if !bucket.is_empty() {
            logging::info!("Running oss test.");

            let root = uuid::Uuid::new_v4().to_string();

            let mut builder = Oss::default();
            let _ = builder
                .root(&root)
                .access_key_id(&env::var("GT_OSS_ACCESS_KEY_ID")?)
                .access_key_secret(&env::var("GT_OSS_ACCESS_KEY")?)
                .bucket(&bucket);

            let store = ObjectStore::new(builder).unwrap().finish();

            let guard = TempFolder::new(&store, "/");
            test_object_crud(&store).await?;
            test_object_list(&store).await?;
            guard.remove_all().await?;
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_azblob_backend() -> Result<()> {
    logging::init_default_ut_logging();
    if let Ok(container) = env::var("GT_AZBLOB_CONTAINER") {
        if !container.is_empty() {
            logging::info!("Running azblob test.");

            let root = uuid::Uuid::new_v4().to_string();

            let mut builder = Azblob::default();
            let _ = builder
                .root(&root)
                .account_name(&env::var("GT_AZBLOB_ACCOUNT_NAME")?)
                .account_key(&env::var("GT_AZBLOB_ACCOUNT_KEY")?)
                .container(&container);

            let store = ObjectStore::new(builder).unwrap().finish();

            let guard = TempFolder::new(&store, "/");
            test_object_crud(&store).await?;
            test_object_list(&store).await?;
            guard.remove_all().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_gcs_backend() -> Result<()> {
    logging::init_default_ut_logging();
    if let Ok(container) = env::var("GT_AZBLOB_CONTAINER") {
        if !container.is_empty() {
            logging::info!("Running azblob test.");

            let mut builder = Gcs::default();
            builder
                .root(&uuid::Uuid::new_v4().to_string())
                .bucket(&env::var("GT_GCS_BUCKET").unwrap())
                .scope(&env::var("GT_GCS_SCOPE").unwrap())
                .credential_path(&env::var("GT_GCS_CREDENTIAL_PATH").unwrap())
                .endpoint(&env::var("GT_GCS_ENDPOINT").unwrap());

            let store = ObjectStore::new(builder).unwrap().finish();

            let guard = TempFolder::new(&store, "/");
            test_object_crud(&store).await?;
            test_object_list(&store).await?;
            guard.remove_all().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_file_backend_with_lru_cache() -> Result<()> {
    logging::init_default_ut_logging();

    let data_dir = create_temp_dir("test_file_backend_with_lru_cache");
    let tmp_dir = create_temp_dir("test_file_backend_with_lru_cache");
    let mut builder = Fs::default();
    let _ = builder
        .root(&data_dir.path().to_string_lossy())
        .atomic_write_dir(&tmp_dir.path().to_string_lossy());

    let store = ObjectStore::new(builder).unwrap().finish();

    let cache_dir = create_temp_dir("test_file_backend_with_lru_cache");
    let cache_layer = {
        let mut builder = Fs::default();
        let _ = builder
            .root(&cache_dir.path().to_string_lossy())
            .atomic_write_dir(&cache_dir.path().to_string_lossy());
        let file_cache = Arc::new(builder.build().unwrap());

        LruCacheLayer::new(Arc::new(file_cache.clone()), 32)
            .await
            .unwrap()
    };

    let store = store.layer(cache_layer.clone());

    test_object_crud(&store).await?;
    test_object_list(&store).await?;

    assert_eq!(cache_layer.read_cache_stat().await, (0, 0));

    Ok(())
}

async fn assert_lru_cache<C: Accessor + Clone>(
    cache_layer: &LruCacheLayer<C>,
    file_names: &[&str],
) {
    for file_name in file_names {
        assert!(cache_layer.contains_file(file_name).await);
    }
}

async fn assert_cache_files(
    store: &Operator,
    file_names: &[&str],
    file_contents: &[&str],
) -> Result<()> {
    let objects = store.list("/").await?;

    // compare the cache file with the expected cache file; ignore orders
    for o in objects {
        let position = file_names.iter().position(|&x| x == o.name());
        assert!(position.is_some(), "file not found: {}", o.name());

        let position = position.unwrap();
        let bs = store.read(o.path()).await.unwrap();
        assert_eq!(
            file_contents[position],
            String::from_utf8(bs.clone())?,
            "file content not match: {}",
            o.name()
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_object_store_cache_policy() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    // create file storage
    let root_dir = create_temp_dir("test_object_store_cache_policy");
    let store = OperatorBuilder::new(
        Fs::default()
            .root(&root_dir.path().to_string_lossy())
            .atomic_write_dir(&root_dir.path().to_string_lossy())
            .build()
            .unwrap(),
    )
    .finish();

    // create file cache layer
    let cache_dir = create_temp_dir("test_object_store_cache_policy_cache");
    let atomic_temp_dir = create_temp_dir("test_object_store_cache_policy_cache_tmp");
    let mut builder = Fs::default();
    let _ = builder
        .root(&cache_dir.path().to_string_lossy())
        .atomic_write_dir(&atomic_temp_dir.path().to_string_lossy());
    let file_cache = Arc::new(builder.build().unwrap());
    let cache_store = OperatorBuilder::new(file_cache.clone()).finish();

    // create operator for cache dir to verify cache file
    let cache_layer = LruCacheLayer::new(Arc::new(file_cache.clone()), 38)
        .await
        .unwrap();
    let store = store.layer(cache_layer.clone());

    // create several object handler.
    // write data into object;
    let p1 = "test_file1";
    let p2 = "test_file2";
    store.write(p1, "Hello, object1!").await.unwrap();
    store.write(p2, "Hello, object2!").await.unwrap();

    // Try to read p1 and p2
    let _ = store.read_with(p1).range(0..).await?;
    let _ = store.read(p1).await?;
    let _ = store.read_with(p2).range(0..).await?;
    let _ = store.read_with(p2).range(7..).await?;
    let _ = store.read(p2).await?;

    assert_eq!(cache_layer.read_cache_stat().await, (3, 38));
    assert_cache_files(
        &cache_store,
        &[
            "6d29752bdc6e4d5ba5483b96615d6c48.cache-bytes=0-14",
            "ecfe0dce85de452eb0a325158e7bfb75.cache-bytes=7-14",
            "ecfe0dce85de452eb0a325158e7bfb75.cache-bytes=0-14",
        ],
        &["Hello, object1!", "object2!", "Hello, object2!"],
    )
    .await?;
    assert_lru_cache(
        &cache_layer,
        &[
            "6d29752bdc6e4d5ba5483b96615d6c48.cache-bytes=0-14",
            "ecfe0dce85de452eb0a325158e7bfb75.cache-bytes=7-14",
            "ecfe0dce85de452eb0a325158e7bfb75.cache-bytes=0-14",
        ],
    )
    .await;

    // Delete p2 file
    store.delete(p2).await.unwrap();

    assert_eq!(cache_layer.read_cache_stat().await, (1, 15));
    assert_cache_files(
        &cache_store,
        &["6d29752bdc6e4d5ba5483b96615d6c48.cache-bytes=0-14"],
        &["Hello, object1!"],
    )
    .await?;
    assert_lru_cache(
        &cache_layer,
        &["6d29752bdc6e4d5ba5483b96615d6c48.cache-bytes=0-14"],
    )
    .await;

    // Read the deleted file without a deterministic range size requires an extra `stat.`
    // Therefore, it won't go into the cache.
    assert!(store.read(p2).await.is_err());

    let p3 = "test_file3";
    store.write(p3, "Hello, object3!").await.unwrap();

    // Try to read p3
    let _ = store.read(p3).await.unwrap();
    let _ = store.read_with(p3).range(0..5).await.unwrap();

    assert_eq!(cache_layer.read_cache_stat().await, (3, 35));

    // However, The real open file happens after the reader is created.
    // The reader will throw an error during the reading
    // instead of returning `NotFound` during the reader creation.
    // The entry count is 4, because we have the p2 `NotFound` cache.
    assert!(store.read_with(p2).range(0..4).await.is_err());
    assert_eq!(cache_layer.read_cache_stat().await, (4, 35));

    assert_cache_files(
        &cache_store,
        &[
            "6d29752bdc6e4d5ba5483b96615d6c48.cache-bytes=0-14",
            "a8b1dc21e24bb55974e3e68acc77ed52.cache-bytes=0-14",
            "a8b1dc21e24bb55974e3e68acc77ed52.cache-bytes=0-4",
        ],
        &["Hello, object1!", "Hello, object3!", "Hello"],
    )
    .await?;
    assert_lru_cache(
        &cache_layer,
        &[
            "6d29752bdc6e4d5ba5483b96615d6c48.cache-bytes=0-14",
            "a8b1dc21e24bb55974e3e68acc77ed52.cache-bytes=0-14",
            "a8b1dc21e24bb55974e3e68acc77ed52.cache-bytes=0-4",
        ],
    )
    .await;

    // try to read p1, p2, p3
    let _ = store.read(p3).await.unwrap();
    let _ = store.read_with(p3).range(0..5).await.unwrap();
    assert!(store.read(p2).await.is_err());
    // Read p1 with range `1..` , the existing p1 with range `0..` must be evicted.
    let _ = store.read_with(p1).range(1..15).await.unwrap();
    assert_eq!(cache_layer.read_cache_stat().await, (4, 34));
    assert_cache_files(
        &cache_store,
        &[
            "6d29752bdc6e4d5ba5483b96615d6c48.cache-bytes=1-14",
            "a8b1dc21e24bb55974e3e68acc77ed52.cache-bytes=0-14",
            "a8b1dc21e24bb55974e3e68acc77ed52.cache-bytes=0-4",
        ],
        &["ello, object1!", "Hello, object3!", "Hello"],
    )
    .await?;
    assert_lru_cache(
        &cache_layer,
        &[
            "6d29752bdc6e4d5ba5483b96615d6c48.cache-bytes=1-14",
            "a8b1dc21e24bb55974e3e68acc77ed52.cache-bytes=0-14",
            "a8b1dc21e24bb55974e3e68acc77ed52.cache-bytes=0-4",
        ],
    )
    .await;

    let metric_text = common_telemetry::dump_metrics().unwrap();

    assert!(metric_text.contains("object_store_lru_cache_hit"));
    assert!(metric_text.contains("object_store_lru_cache_miss"));

    drop(cache_layer);
    // Test recover
    let cache_layer = LruCacheLayer::new(Arc::new(file_cache), 38).await.unwrap();

    // The p2 `NotFound` cache will not be recovered
    assert_eq!(cache_layer.read_cache_stat().await, (3, 34));
    assert_lru_cache(
        &cache_layer,
        &[
            "6d29752bdc6e4d5ba5483b96615d6c48.cache-bytes=1-14",
            "a8b1dc21e24bb55974e3e68acc77ed52.cache-bytes=0-14",
            "a8b1dc21e24bb55974e3e68acc77ed52.cache-bytes=0-4",
        ],
    )
    .await;

    Ok(())
}
