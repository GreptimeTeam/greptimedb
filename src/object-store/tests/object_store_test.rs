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
use object_store::cache_policy::LruCacheLayer;
use object_store::services::{Fs, S3};
use object_store::test_util::TempFolder;
use object_store::{util, Object, ObjectLister, ObjectMode, ObjectStore, ObjectStoreBuilder};
use opendal::services::Oss;
use opendal::Operator;

async fn test_object_crud(store: &ObjectStore) -> Result<()> {
    // Create object handler.
    let object = store.object("test_file");

    // Write data info object;
    assert!(object.write("Hello, World!").await.is_ok());

    // Read data from object;
    let bs = object.read().await?;
    assert_eq!("Hello, World!", String::from_utf8(bs)?);

    // Read range from object;
    let bs = object.range_read(1..=11).await?;
    assert_eq!("ello, World", String::from_utf8(bs)?);

    // Get object's Metadata
    let meta = object.metadata().await?;
    assert_eq!("test_file", object.path());
    assert_eq!(ObjectMode::FILE, meta.mode());
    assert_eq!(13, meta.content_length());

    // Delete object.
    assert!(object.delete().await.is_ok());
    assert!(object.read().await.is_err());

    Ok(())
}

async fn test_object_list(store: &ObjectStore) -> Result<()> {
    // Create  some object handlers.
    let o1 = store.object("test_file1");
    let o2 = store.object("test_file2");
    let o3 = store.object("test_file3");

    // Write something
    assert!(o1.write("Hello, object1!").await.is_ok());
    assert!(o2.write("Hello, object2!").await.is_ok());
    assert!(o3.write("Hello, object3!").await.is_ok());

    // List objects
    let o: Object = store.object("/");
    let obs: ObjectLister = o.list().await?;
    let objects = util::collect(obs).await?;
    assert_eq!(3, objects.len());

    // Delete o1, o3
    assert!(o1.delete().await.is_ok());
    assert!(o3.delete().await.is_ok());

    // List obejcts again
    let objects = util::collect(o.list().await?).await?;
    assert_eq!(1, objects.len());

    // Only o2 is exists
    let o2 = &objects[0].clone();
    let bs = o2.read().await?;
    assert_eq!("Hello, object2!", String::from_utf8(bs)?);
    // Delete o2
    assert!(o2.delete().await.is_ok());

    let objects = util::collect(o.list().await?).await?;
    assert!(objects.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_fs_backend() -> Result<()> {
    let data_dir = create_temp_dir("test_fs_backend");
    let tmp_dir = create_temp_dir("test_fs_backend");
    let store = ObjectStore::new(
        Fs::default()
            .root(&data_dir.path().to_string_lossy())
            .atomic_write_dir(&tmp_dir.path().to_string_lossy())
            .build()?,
    )
    .finish();

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

            let accessor = S3::default()
                .root(&root)
                .access_key_id(&env::var("GT_S3_ACCESS_KEY_ID")?)
                .secret_access_key(&env::var("GT_S3_ACCESS_KEY")?)
                .bucket(&bucket)
                .build()?;

            let store = ObjectStore::new(accessor).finish();

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

            let accessor = Oss::default()
                .root(&root)
                .access_key_id(&env::var("GT_OSS_ACCESS_KEY_ID")?)
                .access_key_secret(&env::var("GT_OSS_ACCESS_KEY")?)
                .bucket(&bucket)
                .build()?;

            let store = ObjectStore::new(accessor).finish();

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
    let o = store.object("/");
    let obs = o.list().await?;
    let objects = util::collect(obs).await?;

    // compare the cache file with the expected cache file; ignore orders
    for o in objects {
        let position = file_names.iter().position(|&x| x == o.name());
        assert!(position.is_some(), "file not found: {}", o.name());

        let position = position.unwrap();
        let bs = o.read().await?;
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
    // create file storage
    let root_dir = create_temp_dir("test_fs_backend");
    let store = ObjectStore::new(
        Fs::default()
            .root(&root_dir.path().to_string_lossy())
            .atomic_write_dir(&root_dir.path().to_string_lossy())
            .build()?,
    );

    // create file cache layer
    let cache_dir = create_temp_dir("test_fs_cache");
    let cache_acc = Fs::default()
        .root(&cache_dir.path().to_string_lossy())
        .atomic_write_dir(&cache_dir.path().to_string_lossy())
        .build()?;
    let cache_store = ObjectStore::new(cache_acc.clone()).finish();
    // create operator for cache dir to verify cache file
    let store = store
        .layer(LruCacheLayer::new(Arc::new(cache_acc), 3))
        .finish();

    // create several object handler.
    let o1 = store.object("test_file1");
    let o2 = store.object("test_file2");

    // write data into object;
    assert!(o1.write("Hello, object1!").await.is_ok());
    assert!(o2.write("Hello, object2!").await.is_ok());

    // crate cache by read object
    o1.range_read(7..).await?;
    o1.read().await?;
    o2.range_read(7..).await?;
    o2.read().await?;

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

    assert!(o2.delete().await.is_ok());

    assert_cache_files(
        &cache_store,
        &["test_file1.cache-bytes=0-"],
        &["Hello, object1!"],
    )
    .await?;

    let o3 = store.object("test_file3");
    assert!(o3.write("Hello, object3!").await.is_ok());

    o3.read().await?;
    o3.range_read(0..5).await?;

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

    Ok(())
}
