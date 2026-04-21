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
use arrow_object_store::path::Path;
use arrow_object_store::{ObjectStore as ArrowObjectStore, ObjectStoreExt};
use bytes::Bytes;
use common_telemetry::info;
use futures::TryStreamExt;
use object_store::ObjectStore;
use object_store::services::{Fs, S3};
use object_store::test_util::TempFolder;
use object_store_opendal::OpendalStore;
use opendal::EntryMode;
use opendal::services::{Azblob, Gcs, Memory, Oss};
use prometheus::{Encoder, TextEncoder};
use tempfile::TempDir;

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

async fn test_object_list_start_after(store: &ObjectStore) -> Result<()> {
    let scheme = store.info().scheme();
    // `start_after` is a service-level capability. Skip the checks when the
    // backend (e.g. the local Fs service) doesn't honor it natively — the
    // bound would be silently ignored and the full listing returned.
    if !store.info().native_capability().list_with_start_after {
        info!("Skip test_object_list_start_after: backend {scheme} lacks start_after support");
        return Ok(());
    }
    info!("Run test_object_list_start_after on backend {scheme}");

    let files = [
        "00000000000000000001.json",
        "00000000000000000002.json",
        "00000000000000000003.checkpoint",
        "00000000000000000003.json",
        "00000000000000000004.json",
    ];
    for name in files {
        store.write(name, "x").await?;
    }

    // Bare 20-digit bound: versions 1..=2 are skipped; version-3 deltas and
    // checkpoint are kept (their `.` suffix sorts after the bound).
    let lister = store
        .lister_with("/")
        .start_after("00000000000000000003")
        .await?;
    let mut got: Vec<String> = lister
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .filter(|e| e.metadata().mode() == EntryMode::FILE)
        .map(|e| e.name().to_string())
        .collect();
    got.sort();
    let mut expected = vec![
        "00000000000000000003.checkpoint".to_string(),
        "00000000000000000003.json".to_string(),
        "00000000000000000004.json".to_string(),
    ];
    expected.sort();
    assert_eq!(expected, got);

    // A bound that matches an existing name exactly excludes that name.
    let lister = store
        .lister_with("/")
        .start_after("00000000000000000003.json")
        .await?;
    let got: Vec<String> = lister
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .filter(|e| e.metadata().mode() == EntryMode::FILE)
        .map(|e| e.name().to_string())
        .collect();
    assert_eq!(vec!["00000000000000000004.json".to_string()], got);

    for name in files {
        store.delete(name).await?;
    }

    // OpenDAL resolves `start_after` against the operator root, not the
    // `lister_with` path. For a nested prefix like `manifest/`, the bound
    // must also embed that prefix — passing only the bare 20-digit name is
    // silently a no-op because the full keys start with `m...` > `0...`.
    let nested_files = [
        "manifest/00000000000000000001.json",
        "manifest/00000000000000000002.json",
        "manifest/00000000000000000003.checkpoint",
        "manifest/00000000000000000003.json",
        "manifest/00000000000000000004.json",
    ];
    for name in nested_files {
        store.write(name, "x").await?;
    }

    let lister = store
        .lister_with("manifest/")
        .start_after("manifest/00000000000000000003")
        .await?;
    let mut got: Vec<String> = lister
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .filter(|e| e.metadata().mode() == EntryMode::FILE)
        .map(|e| e.name().to_string())
        .collect();
    got.sort();
    let mut expected = vec![
        "00000000000000000003.checkpoint".to_string(),
        "00000000000000000003.json".to_string(),
        "00000000000000000004.json".to_string(),
    ];
    expected.sort();
    assert_eq!(expected, got);

    for name in nested_files {
        store.delete(name).await?;
    }
    Ok(())
}

fn assert_opendal_metrics() {
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    let text = String::from_utf8(buffer).unwrap();
    assert!(
        text.contains("opendal"),
        "Missing opendal metrics: {}",
        text
    );
}

fn create_temp_dir(prefix: &str) -> Result<TempDir> {
    Ok(tempfile::Builder::new().prefix(prefix).tempdir()?)
}

#[tokio::test]
async fn test_opendal_memory_smoke() -> Result<()> {
    let op = opendal::Operator::new(Memory::default())?.finish();
    let store: OpendalStore = OpendalStore::new(op);
    assert_eq!("memory", store.info().scheme());
    assert!(format!("{store}").contains("memory"));
    let store: Arc<dyn ArrowObjectStore> = Arc::new(store);
    let location = Path::from("smoke/test.txt");
    store
        .put(&location, Bytes::from_static(b"hello, memory").into())
        .await?;

    let content = store.get(&location).await?.bytes().await?;
    assert_eq!(content, Bytes::from_static(b"hello, memory"));

    let listed = store
        .list(Some(&Path::from("smoke")))
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].location, location);

    Ok(())
}

#[tokio::test]
async fn test_fs_backend() -> Result<()> {
    let data_dir = create_temp_dir("test_fs_backend")?;
    let tmp_dir = create_temp_dir("test_fs_backend")?;
    let builder = Fs::default()
        .root(&data_dir.path().to_string_lossy())
        .atomic_write_dir(&tmp_dir.path().to_string_lossy());

    let store = ObjectStore::new(builder).unwrap().finish();
    let store = object_store::util::with_instrument_layers(store, false);

    test_object_crud(&store).await?;
    test_object_list(&store).await?;
    test_object_list_start_after(&store).await?;

    assert_opendal_metrics();

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
        let store = object_store::util::with_instrument_layers(store, false);

        let guard = TempFolder::new(&store, "/");
        test_object_crud(&store).await?;
        test_object_list(&store).await?;
        test_object_list_start_after(&store).await?;
        assert_opendal_metrics();
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
        let store = object_store::util::with_instrument_layers(store, false);

        let guard = TempFolder::new(&store, "/");
        test_object_crud(&store).await?;
        test_object_list(&store).await?;
        test_object_list_start_after(&store).await?;
        assert_opendal_metrics();
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
        let store = object_store::util::with_instrument_layers(store, false);

        let guard = TempFolder::new(&store, "/");
        test_object_crud(&store).await?;
        test_object_list(&store).await?;
        test_object_list_start_after(&store).await?;
        assert_opendal_metrics();
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
        let store = object_store::util::with_instrument_layers(store, false);

        let guard = TempFolder::new(&store, "/");
        test_object_crud(&store).await?;
        test_object_list(&store).await?;
        test_object_list_start_after(&store).await?;
        assert_opendal_metrics();
        guard.remove_all().await?;
    }
    Ok(())
}
