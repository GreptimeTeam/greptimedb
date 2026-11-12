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

//! Tests for manifests published by conditional create.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use common_datasource::compression::CompressionType;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_test_util::temp_dir::{TempDir, create_temp_dir};
use object_store::ObjectStore;
use object_store::layers::CapabilityOverrideLayer;
use object_store::layers::mock::{
    Error as MockError, ErrorKind, MockLayer, MockLayerBuilder, Result as MockResult, oio,
};
use object_store::services::Fs;
use rstest::rstest;
use store_api::ManifestVersion;
use store_api::storage::FileId;

use crate::error::{Error, Result};
use crate::manifest::action::{RegionEdit, RegionMetaAction, RegionMetaActionList};
use crate::manifest::manager::{RegionManifestManager, RegionManifestOptions};
use crate::manifest::storage::{
    LAST_CHECKPOINT_FILE, ManifestObjectStore, delta_file, gen_path, is_checkpoint_file,
    is_delta_file,
};
use crate::manifest::tests::utils::{basic_region_metadata, fail_manifest_reads_layer};
use crate::sst::FormatType;
use crate::sst::file::FileMeta;
use crate::test_util::{DeltaFaults, nop_action};

fn temp_store() -> (TempDir, ObjectStore) {
    let dir = create_temp_dir("");
    let store = ObjectStore::new(Fs::default().root(dir.path().to_str().unwrap())).unwrap();
    (dir, store)
}

fn options(
    store: &ObjectStore,
    compress_type: CompressionType,
    checkpoint_distance: u64,
    conditional_publication: bool,
) -> RegionManifestOptions {
    RegionManifestOptions {
        manifest_dir: "/".to_string(),
        object_store: store.clone(),
        compress_type,
        checkpoint_distance,
        remove_file_options: Default::default(),
        manifest_cache: None,
        conditional_publication,
    }
}

async fn create_manager(options: RegionManifestOptions) -> RegionManifestManager {
    RegionManifestManager::new(
        Arc::new(basic_region_metadata()),
        0,
        options,
        FormatType::PrimaryKey,
        false,
        &Default::default(),
    )
    .await
    .unwrap()
}

async fn open_manager(options: RegionManifestOptions) -> RegionManifestManager {
    RegionManifestManager::open(options, &Default::default())
        .await
        .unwrap()
        .unwrap()
}

async fn commit(manager: &mut RegionManifestManager) -> Result<ManifestVersion> {
    manager
        .update_with_verification(nop_action(), false, true, || false)
        .await
}

fn delta_key(version: ManifestVersion) -> String {
    gen_path("/", &delta_file(version), CompressionType::Gzip)
}

fn conditional_store(store: &ObjectStore, compress_type: CompressionType) -> ManifestObjectStore {
    ManifestObjectStore::new("/", store.clone(), compress_type, Default::default(), None)
        .with_conditional_publication(true)
        .unwrap()
}

/// Returns a store that counts object writes and LIST requests.
fn counting_store() -> (TempDir, ObjectStore, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let (dir, store) = temp_store();
    let writes = Arc::new(AtomicUsize::new(0));
    let lists = Arc::new(AtomicUsize::new(0));
    let (observed_writes, observed_lists) = (writes.clone(), lists.clone());
    let layer = MockLayerBuilder::default()
        .writer_factory(Arc::new(move |_, _, inner| {
            observed_writes.fetch_add(1, Ordering::Relaxed);
            inner
        }))
        .lister_factory(Arc::new(move |_, _, inner| {
            observed_lists.fetch_add(1, Ordering::Relaxed);
            inner
        }))
        .build()
        .unwrap();
    (dir, store.layer(layer), writes, lists)
}

struct FailingLister(ErrorKind);

impl oio::List for FailingLister {
    async fn next(&mut self) -> MockResult<Option<oio::Entry>> {
        Err(MockError::new(self.0, "mock list failure"))
    }
}

fn failing_list_layer(kind: ErrorKind) -> MockLayer {
    MockLayerBuilder::default()
        .lister_factory(Arc::new(move |_, _, _| Box::new(FailingLister(kind))))
        .build()
        .unwrap()
}

fn assert_conflict(error: &Error) {
    assert!(
        matches!(error, Error::ManifestVersionConflict { .. }),
        "{error:?}"
    );
}

fn import(file_id: FileId) -> RegionMetaActionList {
    RegionMetaActionList::with_action(RegionMetaAction::Edit(RegionEdit {
        files_to_add: vec![FileMeta {
            file_id,
            ..Default::default()
        }],
        ..Default::default()
    }))
}

#[rstest]
#[case::conditional(true)]
#[case::legacy(false)]
#[tokio::test]
async fn test_ordinary_commit_writes_once_without_listing(#[case] conditional: bool) {
    let (_dir, store, writes, lists) = counting_store();
    let mut manager = create_manager(options(&store, CompressionType::Gzip, 0, conditional)).await;
    writes.store(0, Ordering::Relaxed);

    manager
        .update_with_verification(nop_action(), false, true, || !conditional)
        .await
        .unwrap();

    assert_eq!(1, writes.load(Ordering::Relaxed));
    assert_eq!(0, lists.load(Ordering::Relaxed));
}

#[tokio::test]
async fn test_commit_without_deadline_is_verified() {
    let (_dir, store, _writes, lists) = counting_store();
    let mut manager = create_manager(options(&store, CompressionType::Gzip, 1, true)).await;

    manager.update(nop_action(), false).await.unwrap();
    manager.wait_for_pending_checkpoint().await;

    assert_eq!(1, lists.load(Ordering::Relaxed));
    assert_eq!(1, manager.last_version());
    assert_eq!(0, manager.checkpointer().last_checkpoint_version());
}

#[rstest]
#[tokio::test]
async fn test_list_error_during_verification_conflicts(
    #[values(ErrorKind::Unexpected, ErrorKind::NotFound)] kind: ErrorKind,
) {
    let (_dir, store) = temp_store();
    let store = store.layer(failing_list_layer(kind));
    let mut manager = create_manager(options(&store, CompressionType::Gzip, 1, true)).await;

    let error = manager
        .update_with_verification(nop_action(), false, true, || true)
        .await
        .unwrap_err();

    assert_conflict(&error);
    assert!(error.to_string().contains("mock list failure"), "{error}");
    assert_eq!(0, manager.last_version());
    assert_eq!(0, manager.manifest().manifest_version);
    assert!(store.exists(&delta_key(1)).await.unwrap());
}

#[tokio::test]
async fn test_recovery_treats_missing_directory_as_no_checkpoint() {
    let (_dir, store) = temp_store();
    let store = store.layer(failing_list_layer(ErrorKind::NotFound));

    let checkpoint = conditional_store(&store, CompressionType::Gzip)
        .load_last_checkpoint()
        .await;

    assert!(checkpoint.unwrap().is_none());
}

#[tokio::test]
async fn test_compression_settings_share_one_name() {
    let (_dir, store) = temp_store();
    let mut stored = conditional_store(&store, CompressionType::Uncompressed);
    let mut compressed = conditional_store(&store, CompressionType::Gzip);
    let data = vec![b'a'; 1024];

    stored.save(1, &data).await.unwrap();
    let stored_object = store.read(&delta_key(1)).await.unwrap().to_vec();
    assert!(stored_object.len() > data.len());

    // A retry by a writer with the other setting succeeds and leaves the object alone.
    compressed.save(1, &data).await.unwrap();
    let retried_object = store.read(&delta_key(1)).await.unwrap().to_vec();
    assert_eq!(stored_object, retried_object);
    assert_eq!(stored_object.len() as u64, compressed.total_manifest_size());

    compressed.save(2, &data).await.unwrap();
    assert!(store.read(&delta_key(2)).await.unwrap().len() < data.len());
    assert_eq!(
        vec![(1, data.clone()), (2, data)],
        stored.fetch_manifests(1, 3).await.unwrap()
    );
}

#[rstest]
#[case::different_content(false, false)]
#[case::undecodable(true, false)]
#[case::unreadable(false, true)]
#[tokio::test]
async fn test_existing_delta_conflicts(#[case] undecodable: bool, #[case] unreadable: bool) {
    let (_dir, store) = temp_store();
    let existing = if undecodable {
        b"not gzip".to_vec()
    } else {
        let other = b"other".as_slice();
        CompressionType::Gzip.encode(other).await.unwrap()
    };
    store.write(&delta_key(1), existing.clone()).await.unwrap();
    let writer_store = if unreadable {
        store
            .clone()
            .layer(fail_manifest_reads_layer(is_delta_file))
    } else {
        store.clone()
    };

    let error = conditional_store(&writer_store, CompressionType::Gzip)
        .save(1, b"mine")
        .await
        .unwrap_err();

    assert_conflict(&error);
    assert!(error.may_have_persisted_manifest_update());
    assert_eq!(existing, store.read(&delta_key(1)).await.unwrap().to_vec());
}

#[tokio::test]
async fn test_failed_write_does_not_skip_version() {
    let (_dir, store) = temp_store();
    let mut manager = create_manager(options(&store, CompressionType::Gzip, 0, true)).await;
    let key = delta_key(1);
    store.write(&key, b"not gzip".to_vec()).await.unwrap();

    assert_conflict(&commit(&mut manager).await.unwrap_err());
    assert_eq!(0, manager.last_version());

    store.delete(&key).await.unwrap();
    assert_eq!(1, commit(&mut manager).await.unwrap());
    assert_eq!(1, manager.manifest().manifest_version);
}

#[tokio::test]
async fn test_stale_checkpoint_pointer_does_not_hide_newer_edits() {
    let (_dir, store) = temp_store();
    let pointer = format!("/{LAST_CHECKPOINT_FILE}");
    let region_options = options(&store, CompressionType::Uncompressed, 1, true);
    let mut manager = create_manager(region_options.clone()).await;
    commit(&mut manager).await.unwrap();
    manager.wait_for_pending_checkpoint().await;
    let stale_pointer = store.read(&pointer).await.unwrap().to_vec();
    for _ in 0..2 {
        commit(&mut manager).await.unwrap();
        manager.wait_for_pending_checkpoint().await;
    }
    manager
        .update_with_verification(nop_action(), false, false, || false)
        .await
        .unwrap();
    store.write(&pointer, stale_pointer).await.unwrap();

    let reopened = open_manager(region_options).await;

    assert_eq!(4, reopened.last_version());
    assert_eq!(3, reopened.checkpointer().last_checkpoint_version());
}

#[tokio::test]
async fn test_recovery_fails_when_listed_checkpoint_is_gone() {
    let (_dir, store) = temp_store();
    let compress_type = CompressionType::Gzip;
    let mut manager = create_manager(options(&store, compress_type, 1, true)).await;
    commit(&mut manager).await.unwrap();
    manager.wait_for_pending_checkpoint().await;
    let unreadable = store.layer(fail_manifest_reads_layer(is_checkpoint_file));

    let conditional = options(&unreadable, compress_type, 1, true);
    let error = RegionManifestManager::open(conditional, &Default::default())
        .await
        .unwrap_err();

    assert!(
        matches!(error, Error::ManifestCheckpointVanished { .. }),
        "{error:?}"
    );
    assert_eq!(StatusCode::StorageUnavailable, error.status_code());
    assert!(error.retry_hint().is_retryable());
}

#[tokio::test]
async fn test_backend_without_conditional_create_is_refused_at_open() {
    let (_dir, store) = temp_store();
    let compress_type = CompressionType::Gzip;
    create_manager(options(&store, compress_type, 0, false)).await;
    let unsupported = store.layer(CapabilityOverrideLayer::new(|mut capability| {
        capability.write_with_if_not_exists = false;
        capability
    }));

    let conditional = options(&unsupported, compress_type, 0, true);
    let error = RegionManifestManager::open(conditional, &Default::default())
        .await
        .unwrap_err();

    assert_eq!(StatusCode::Unsupported, error.status_code());
    open_manager(options(&unsupported, compress_type, 0, false)).await;
}

#[tokio::test]
async fn test_open_writes_nothing() {
    let (_dir, store, writes, _lists) = counting_store();
    let region_options = options(&store, CompressionType::Gzip, 0, true);
    create_manager(region_options.clone()).await;
    writes.store(0, Ordering::Relaxed);

    open_manager(region_options).await;

    assert_eq!(0, writes.load(Ordering::Relaxed));
}

#[rstest]
#[tokio::test]
async fn test_deltas_are_readable_across_modes(
    #[values(false, true)] written_conditionally: bool,
    #[values(CompressionType::Uncompressed, CompressionType::Gzip)] compress_type: CompressionType,
) {
    let (_dir, store) = temp_store();
    let mut writer = create_manager(options(&store, compress_type, 0, written_conditionally)).await;
    commit(&mut writer).await.unwrap();

    let mut reader = open_manager(options(&store, compress_type, 0, !written_conditionally)).await;

    assert_eq!(1, reader.last_version());
    assert_eq!(2, commit(&mut reader).await.unwrap());
}

#[tokio::test]
async fn test_staged_commit_retry_writes_same_content() {
    let (_dir, store) = temp_store();
    let faults = DeltaFaults::default();
    let store = store.layer(faults.layer());
    let mut manager = create_manager(options(&store, CompressionType::Gzip, 0, true)).await;
    let imported = FileId::random();
    manager.update(import(imported), true).await.unwrap();

    faults.lose_next_response();
    let error = manager.commit_staged(|| false).await.unwrap_err();
    assert!(error.may_have_persisted_manifest_update());
    assert_eq!(0, manager.last_version());

    // Until the kept commit is retried, only a retried import (empty for a target with no
    // files) is staged.
    manager.update(import(imported), true).await.unwrap();
    let empty = RegionMetaActionList::with_action(RegionMetaAction::Edit(Default::default()));
    manager.update(empty, true).await.unwrap();
    let refused = manager.update(import(FileId::random()), true).await;
    assert!(
        matches!(refused, Err(Error::RegionBusy { .. })),
        "{refused:?}"
    );

    // The retry writes the same content, so it recognizes its own landed write.
    assert_eq!(1, manager.commit_staged(|| false).await.unwrap().0);
    assert!(manager.staged_actions().is_none());
    assert!(manager.manifest().files.contains_key(&imported));
}
