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

//! Single-region storage operations for shared SST compaction.
//!
//! The caller owns complete table registration, routing, input/output protection
//! and cross-region publication. These operations neither schedule work nor
//! authorize a table-wide transaction. They use ordinary manifest edits, not a
//! second task/receipt registry.

mod output;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use snafu::{OptionExt, ResultExt, ensure};
use store_api::ManifestVersion;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::RegionId;
use tokio::sync::oneshot;

use crate::engine::MitoEngine;
use crate::error::{RecvSnafu, RegionBusySnafu, RegionNotFoundSnafu, Result};
use crate::region::options::RegionOptions;
use crate::request::WorkerRequest;
use crate::sst::file::{FileMeta, LEVEL_SHARED};

/// A coherent manifest snapshot used to plan a conditional operation.
#[derive(Debug, Clone)]
pub struct SharedSstSnapshot {
    pub manifest_version: ManifestVersion,
    pub metadata: RegionMetadataRef,
    pub options: RegionOptions,
    pub files: Vec<FileMeta>,
}

/// Installs one new shared reference in this target's sequence domain.
///
/// Retries retain the original metadata and manifest version. An identical live
/// reference returns its original FileMeta without allocating another sequence.
/// If the reference is absent and the version changed, the request is rejected:
/// it may have already succeeded and been compacted/truncated. In particular,
/// an ambiguous retry must not refresh the version and re-admit the old source.
#[derive(Debug, Clone)]
pub struct RegisterSharedSst {
    pub metadata: RegionMetadataRef,
    pub manifest_version: ManifestVersion,
    /// Immutable uploaded source metadata, with LEVEL_SHARED and no sequence.
    pub file: FileMeta,
}

/// A fixed, lossless replacement for one target of a multi-region computation.
///
/// The caller supplies the snapshot used by the executor and holds its table
/// publication/DDL barrier. Ordinary writes may advance the manifest in the
/// meantime; only the selected inputs and schema/options are compared.
#[derive(Debug, Clone)]
pub struct ApplySharedCompaction {
    pub metadata: RegionMetadataRef,
    pub options: RegionOptions,
    pub inputs: Vec<FileMeta>,
    pub outputs: Vec<FileMeta>,
    pub window_start: i64,
    pub window_seconds: i64,
}

/// Operations supported by the single-region shared-file boundary.
#[derive(Debug)]
pub enum SharedSstOperation {
    Register(Box<RegisterSharedSst>),
    Apply(Box<ApplySharedCompaction>),
}

/// An output verified against its actual Parquet footer before publication.
/// Index creator statistics are deliberately not fabricated from FileMeta.
pub struct VerifiedSharedSst<'a> {
    pub file_meta: &'a FileMeta,
    pub footer: &'a parquet::file::metadata::ParquetMetaData,
}

/// Result acknowledged only after the worker has applied the manifest edit.
#[derive(Debug, PartialEq, Eq)]
pub enum SharedSstResponse {
    Registered(Box<FileMeta>),
    Applied,
    /// None of the expected inputs remain. No outputs were added.
    ///
    /// This does not assert that a previous output still exists, or that this
    /// particular computation removed the inputs. The publisher interprets it
    /// under its table barrier; it must never re-import those outputs.
    InputsGone,
}

#[derive(Debug)]
pub(crate) struct SharedSstRequest {
    pub(crate) region_id: RegionId,
    pub(crate) operation: SharedSstOperation,
    pub(crate) sender: oneshot::Sender<Result<SharedSstResponse>>,
}

#[derive(Debug)]
pub(crate) struct SharedSstFinished {
    pub(crate) region: crate::region::MitoRegionRef,
    pub(crate) sender: oneshot::Sender<Result<SharedSstResponse>>,
    pub(crate) result: Result<(crate::manifest::action::RegionEdit, SharedSstResponse)>,
}

impl MitoEngine {
    /// Captures the writable target's shared references and registration fence.
    pub async fn shared_sst_snapshot(&self, region_id: RegionId) -> Result<SharedSstSnapshot> {
        let region = self
            .find_region(region_id)
            .context(RegionNotFoundSnafu { region_id })?;
        let manager = region.manifest_ctx.manifest_manager.read().await;
        ensure!(
            region.is_writable() && !manager.is_stopped(),
            RegionBusySnafu { region_id }
        );
        let manifest = manager.manifest();
        let current = region.version_control.current();
        Ok(SharedSstSnapshot {
            manifest_version: manifest.manifest_version,
            metadata: Arc::clone(&manifest.metadata),
            options: current.version.options.clone(),
            files: manifest
                .files
                .values()
                .filter(|f| f.level == LEVEL_SHARED)
                .cloned()
                .collect(),
        })
    }

    /// Applies a trusted shared-file operation through the region worker.
    ///
    /// Busy targets reject rather than growing another request queue. Retry with
    /// the same operation; do not refresh a registration fence after an unknown
    /// outcome. A successful return includes the worker-visible version update.
    pub async fn handle_shared_sst(
        &self,
        region_id: RegionId,
        operation: SharedSstOperation,
    ) -> Result<SharedSstResponse> {
        let (sender, receiver) = oneshot::channel();
        self.inner
            .workers
            .submit_to_worker(
                region_id,
                WorkerRequest::SharedSst(Box::new(SharedSstRequest {
                    region_id,
                    operation,
                    sender,
                })),
            )
            .await?;
        receiver.await.context(RecvSnafu)?
    }
}

pub(crate) use output::verify_outputs;
