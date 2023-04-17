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
use common_telemetry::{logging, metric};
use common_test_util::temp_dir::create_temp_dir;
use object_store::cache_policy::LruCacheLayer;
use object_store::services::{Fs, S3};
use object_store::test_util::TempFolder;
use object_store::{util, ObjectStore, ObjectStoreBuilder};
use opendal::services::Oss;
use opendal::{EntryMode, Operator, OperatorBuilder};

async fn test_object_crud(store: &ObjectStore) -> Result<()> {
    // Create object handler.
    // Write data info object;
    let file_name = "test_file";
    store.write(file_name, "Hello, World!").await?;

    // Read data from object;
    let bs = store.read(file_name).await?;
    assert_eq!("Hello, World!", String::from_utf8(bs)?);

    // Read range from object;
    let bs = store.range_read(file_name, 1..=11).await?;
    assert_eq!("ello, World", String::from_utf8(bs)?);

    // Get object's Metadata
    let meta = store.stat(file_name).await?;
    assert_eq!(EntryMode::FILE, meta.mode());
    assert_eq!(13, meta.content_length());

    // Delete object.
    store.delete(file_name).await.unwrap();
    store.read(file_name).await.unwrap_err();
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
    let lister = store.list("/").await?;
    let entries = util::collect(lister).await?;
    assert_eq!(3, entries.len());

    store.delete(p1).await?;
    store.delete(p3).await?;

    // List objects again
    // Only o2 is exists
    let entries = util::collect(store.list("/").await?).await?;
    assert_eq!(1, entries.len());
    assert_eq!(p2, entries.get(0).unwrap().path());

    let content = store.read(p2).await?;
    assert_eq!("Hello, object2!", String::from_utf8(content)?);

    store.delete(p2).await?;
    let entries = util::collect(store.list("/").await?).await?;
    assert!(entries.is_empty());
    Ok(())
}

#[tokio::test]
async fn test_fs_backend() -> Result<()> {
    let data_dir = create_temp_dir("test_fs_backend");
    let tmp_dir = create_temp_dir("test_fs_backend");
    let mut builder = Fs::default();
    builder
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
            builder
                .root(&root)
                .access_key_id(&env::var("GT_S3_ACCESS_KEY_ID")?)
                .secret_access_key(&env::var("GT_S3_ACCESS_KEY")?)
                .bucket(&bucket);

            let store = ObjectStore::new(builder).unwrap().finish();

            let mut guard = TempFolder::new(&store, "/");
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
            builder
                .root(&root)
                .access_key_id(&env::var("GT_OSS_ACCESS_KEY_ID")?)
                .access_key_secret(&env::var("GT_OSS_ACCESS_KEY")?)
                .bucket(&bucket);

            let store = ObjectStore::new(builder).unwrap().finish();

            let mut guard = TempFolder::new(&store, "/");
            test_object_crud(&store).await?;
            test_object_list(&store).await?;
            guard.remove_all().await?;
        }
    }

    Ok(())
}

async fn assert_cache_files(
    store: &Operator,
    file_names: &[&str],
    file_contents: &[&str],
) -> Result<()> {
    let obs = store.list("/").await?;
    let objects = util::collect(obs).await?;

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
    common_telemetry::init_default_metrics_recorder();
    // create file storage
    let root_dir = create_temp_dir("test_fs_backend");
    let store = OperatorBuilder::new(
        Fs::default()
            .root(&root_dir.path().to_string_lossy())
            .atomic_write_dir(&root_dir.path().to_string_lossy())
            .build()
            .unwrap(),
    )
    .finish();

    // create file cache layer
    let cache_dir = create_temp_dir("test_fs_cache");
    let mut builder = Fs::default();
    builder
        .root(&cache_dir.path().to_string_lossy())
        .atomic_write_dir(&cache_dir.path().to_string_lossy());
    let cache_accessor = Arc::new(builder.build().unwrap());
    let cache_store = OperatorBuilder::new(cache_accessor.clone()).finish();

    // create operator for cache dir to verify cache file
    let store = store.layer(LruCacheLayer::new(Arc::new(cache_accessor), 3));

    // create several object handler.
    // write data into object;
    let p1 = "test_file1";
    let p2 = "test_file2";
    store.write(p1, "Hello, object1!").await.unwrap();
    store.write(p2, "Hello, object2!").await.unwrap();

    // create cache by read object
    store.range_read(p1, 0..).await?;
    store.read(p1).await?;
    store.range_read(p2, 0..).await?;
    store.read(p2).await?;

    assert_cache_files(
        &cache_store,
        &[
            "test_file1.cache-bytes=0-",
            "test_file2.cache-bytes=7-",
            "test_file2.cache-bytes=0-",
        ],
        &["Hello, object1!", "object2!", "Hello, object2!"],
    )
    .await?;

    store.delete(p2).await.unwrap();

    assert_cache_files(
        &cache_store,
        &["test_file1.cache-bytes=0-"],
        &["Hello, object1!"],
    )
    .await?;

    let p3 = "test_file3";
    store.write(p3, "Hello, object3!").await.unwrap();

    store.read(p3).await.unwrap();
    store.range_read(p3, 0..5).await.unwrap();

    assert_cache_files(
        &cache_store,
        &[
            "test_file1.cache-bytes=0-",
            "test_file3.cache-bytes=0-",
            "test_file3.cache-bytes=0-4",
        ],
        &["Hello, object1!", "Hello, object3!", "Hello"],
    )
    .await?;

    let handle = metric::try_handle().unwrap();
    let metric_text = handle.render();

    assert!(metric_text.contains("lru_cache_hit{lru_cache_name=\"test_file1\""));
    assert!(metric_text.contains("lru_cache_hit{lru_cache_name=\"test_file2\""));
    assert!(metric_text.contains("lru_cache_miss{lru_cache_name=\"test_file1\""));
    assert!(metric_text.contains("lru_cache_miss{lru_cache_name=\"test_file2\""));
    assert!(metric_text.contains("lru_cache_miss{lru_cache_name=\"test_file3\""));

    Ok(())
}
