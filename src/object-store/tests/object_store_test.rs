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
use common_telemetry::info;
use common_test_util::temp_dir::create_temp_dir;
use object_store::services::{Fs, S3};
use object_store::test_util::TempFolder;
use object_store::ObjectStore;
use opendal::EntryMode;
use opendal::services::{Azblob, Gcs, Oss};

async fn test_object_crud(store: &ObjectStore) -> Result<()> {
    // Create object handler.
    // Write data info object;
    let file_name = "test_file";
    assert!(store.read(file_name).await.is_err());

    store.write(file_name, "Hello, World!").await?;

    // Read data from object;
    let bs = store.read(file_name).await?;
    assert_eq!("Hello, World!", String::from_utf8(bs.to_vec())?);

    // Read range from object;
    let bs = store.read_with(file_name).range(1..=11).await?;
    assert_eq!("ello, World", String::from_utf8(bs.to_vec())?);

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
    let entries = store
        .list("/")
        .await?
        .into_iter()
        .filter(|x| x.metadata().mode() == EntryMode::FILE)
        .collect::<Vec<_>>();
    assert_eq!(3, entries.len());

    store.delete(p1).await?;
    store.delete(p3).await?;

    // List objects again
    // Only o2 and root exist
    let entries = store
        .list("/")
        .await?
        .into_iter()
        .filter(|x| x.metadata().mode() == EntryMode::FILE)
        .collect::<Vec<_>>();
    assert_eq!(1, entries.len());
    assert_eq!(p2, entries[0].path());

    let content = store.read(p2).await?;
    assert_eq!("Hello, object2!", String::from_utf8(content.to_vec())?);

    store.delete(p2).await?;
    let entries = store
        .list("/")
        .await?
        .into_iter()
        .filter(|x| x.metadata().mode() == EntryMode::FILE)
        .collect::<Vec<_>>();
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
    let builder = Fs::default()
        .root(&data_dir.path().to_string_lossy())
        .atomic_write_dir(&tmp_dir.path().to_string_lossy());

    let store = ObjectStore::new(builder).unwrap().finish();

    test_object_crud(&store).await?;
    test_object_list(&store).await?;

    Ok(())
}

#[tokio::test]
async fn test_s3_backend() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    if let Ok(bucket) = env::var("GT_S3_BUCKET")
        && !bucket.is_empty()
    {
        info!("Running s3 test.");

        let root = uuid::Uuid::new_v4().to_string();

        let builder = S3::default()
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

    Ok(())
}

#[tokio::test]
async fn test_oss_backend() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    if let Ok(bucket) = env::var("GT_OSS_BUCKET")
        && !bucket.is_empty()
    {
        info!("Running oss test.");

        let root = uuid::Uuid::new_v4().to_string();

        let builder = Oss::default()
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

    Ok(())
}

#[tokio::test]
async fn test_azblob_backend() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    if let Ok(container) = env::var("GT_AZBLOB_CONTAINER")
        && !container.is_empty()
    {
        info!("Running azblob test.");

        let root = uuid::Uuid::new_v4().to_string();

        let builder = Azblob::default()
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
    Ok(())
}

#[tokio::test]
async fn test_gcs_backend() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    if let Ok(container) = env::var("GT_AZBLOB_CONTAINER")
        && !container.is_empty()
    {
        info!("Running azblob test.");

        let builder = Gcs::default()
            .root(&uuid::Uuid::new_v4().to_string())
            .bucket(&env::var("GT_GCS_BUCKET").unwrap())
            .scope(&env::var("GT_GCS_SCOPE").unwrap())
            .credential_path(&env::var("GT_GCS_CREDENTIAL_PATH").unwrap())
            .credential(&env::var("GT_GCS_CREDENTIAL").unwrap())
            .endpoint(&env::var("GT_GCS_ENDPOINT").unwrap());

        let store = ObjectStore::new(builder).unwrap().finish();

        let guard = TempFolder::new(&store, "/");
        test_object_crud(&store).await?;
        test_object_list(&store).await?;
        guard.remove_all().await?;
    }
    Ok(())
}
