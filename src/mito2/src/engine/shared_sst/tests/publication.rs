// Copyright 2023-2026 GrepTime Inc.
//
// This file is part of the GreptimeDB Enterprise Edition and is licensed under
// the GreptimeDB Enterprise License. You may not use this file except in
// compliance with that license. A copy of the license is available at the root
// of this repository in the file LICENSE-ENTERPRISE.
//
// Unless required by applicable law or agreed to in writing, this software is
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use common_base::Plugins;
use store_api::region_engine::RegionRole;
use tokio::sync::Notify;

use super::*;
use crate::engine::region_hook::{RegionHook, RegionHookRef};
use crate::manifest::action::{RegionMetaAction, RegionMetaActionList};

#[derive(Debug, Default)]
struct PublicationHook {
    pause: AtomicBool,
    entered: Notify,
    resume: Notify,
    verified: AtomicBool,
    published: AtomicBool,
    panic_after_commit: AtomicBool,
}

#[async_trait::async_trait]
impl RegionHook for PublicationHook {
    async fn on_shared_sst_files_verified(
        &self,
        _: RegionId,
        _: &RegionMetadataRef,
        files: &[VerifiedSharedSst<'_>],
    ) {
        for file in files {
            assert_eq!(
                file.file_meta.num_rows,
                file.footer.file_metadata().num_rows() as u64
            );
        }
        self.verified.store(true, Ordering::SeqCst);
        if self.pause.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.resume.notified().await;
        }
    }

    async fn on_manifest_updated(
        &self,
        _: RegionId,
        actions: &RegionMetaActionList,
        _: store_api::ManifestVersion,
    ) {
        if actions.actions.iter().any(|action| {
            matches!(action, RegionMetaAction::Edit(edit)
            if edit.files_to_remove.iter().any(|file| file.level == LEVEL_SHARED))
        }) {
            assert!(self.verified.load(Ordering::SeqCst));
            self.published.store(true, Ordering::SeqCst);
            assert!(
                !self.panic_after_commit.load(Ordering::SeqCst),
                "injected observer failure after durable commit"
            );
        }
    }
}

#[tokio::test]
async fn leadership_loss_during_output_verification_prevents_publication() {
    let hook = Arc::new(PublicationHook::default());
    let plugins = Plugins::new();
    plugins.insert(hook.clone() as RegionHookRef);
    let f = Fixture::with_plugins(&["a", "z"], plugins).await;
    f.register(0).await;
    f.register(1).await;
    let request = f.compute().await.remove(0);
    let region_id = request.metadata.region_id;
    let region = f.engine.find_region(region_id).unwrap();
    let before = region.manifest_ctx.manifest().await.manifest_version;
    hook.pause.store(true, Ordering::SeqCst);
    let demote = async {
        tokio::time::timeout(Duration::from_secs(5), hook.entered.notified())
            .await
            .unwrap();
        f.engine
            .set_region_role(region_id, RegionRole::Follower)
            .unwrap();
        hook.resume.notify_one();
    };
    let (applied, _) = tokio::join!(f.apply(request.clone()), demote);
    assert!(applied.is_err());
    assert!(!hook.published.load(Ordering::SeqCst));
    assert_eq!(
        before,
        region.manifest_ctx.manifest().await.manifest_version
    );
    f.engine
        .set_region_role(region_id, RegionRole::Leader)
        .unwrap();
    assert_eq!(SharedSstResponse::Applied, f.apply(request).await.unwrap());
    assert!(hook.published.load(Ordering::SeqCst));
}

#[tokio::test]
async fn committed_edit_reaches_worker_even_if_manifest_observer_panics() {
    let hook = Arc::new(PublicationHook::default());
    hook.panic_after_commit.store(true, Ordering::SeqCst);
    let plugins = Plugins::new();
    plugins.insert(hook.clone() as RegionHookRef);
    let f = Fixture::with_plugins(&["a", "z"], plugins).await;
    f.register(0).await;
    f.register(1).await;
    let before = f.query(0).await;
    let request = f.compute().await.remove(0);
    assert_eq!(SharedSstResponse::Applied, f.apply(request).await.unwrap());
    assert!(hook.published.load(Ordering::SeqCst));
    assert!(
        f.engine
            .shared_sst_snapshot(RegionId::new(1, 0))
            .await
            .unwrap()
            .files
            .is_empty()
    );
    assert_eq!(before, f.query(0).await);
}

#[tokio::test]
async fn output_footer_cannot_smuggle_newer_or_legacy_zero_versions() {
    let f = Fixture::new().await;
    f.register(0).await;
    f.register(1).await;
    let request = f.compute().await.remove(0);
    let original = f.query(0).await;
    for sequence in [0, 1000] {
        let mut forged = request.clone();
        let mut file =
            write_sst(&f.engine, request.metadata.clone(), &["a"], 1, 42, sequence).await;
        file.sequence = request.outputs[0].sequence;
        file.preserve_row_sequence = request.outputs[0].preserve_row_sequence;
        forged.outputs = vec![file];
        let error = f.apply(forged).await.unwrap_err().to_string();
        assert!(
            error.contains(if sequence == 0 {
                "zero output sequences"
            } else {
                "exceeds its inherited input bound"
            }),
            "{error}"
        );
        assert_eq!(original, f.query(0).await);
    }
}
