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

use anyhow::Result;
use common_telemetry::logging;
use object_store::backend::{fs, s3};
use object_store::test_util::TempFolder;
use object_store::{util, Object, ObjectLister, ObjectMode, ObjectStore};
use opendal::services::oss;
use tempdir::TempDir;

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
    let data_dir = TempDir::new("test_fs_backend")?;
    let tmp_dir = TempDir::new("test_fs_backend")?;
    let store = ObjectStore::new(
        fs::Builder::default()
            .root(&data_dir.path().to_string_lossy())
            .atomic_write_dir(&tmp_dir.path().to_string_lossy())
            .build()?,
    );

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

            let accessor = s3::Builder::default()
                .root(&root)
                .access_key_id(&env::var("GT_S3_ACCESS_KEY_ID")?)
                .secret_access_key(&env::var("GT_S3_ACCESS_KEY")?)
                .bucket(&bucket)
                .build()?;

            let store = ObjectStore::new(accessor);

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

            let accessor = oss::Builder::default()
                .root(&root)
                .access_key_id(&env::var("GT_OSS_ACCESS_KEY_ID")?)
                .access_key_secret(&env::var("GT_OSS_ACCESS_KEY")?)
                .bucket(&bucket)
                .build()?;

            let store = ObjectStore::new(accessor);

            let mut guard = TempFolder::new(&store, "/");
            test_object_crud(&store).await?;
            test_object_list(&store).await?;
            guard.remove_all().await?;
        }
    }

    Ok(())
}
