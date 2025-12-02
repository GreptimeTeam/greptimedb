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

use std::sync::Arc;
use std::time::Instant;

use common_telemetry::{info, warn};
use snafu::ensure;
use store_api::logstore::LogStore;

use crate::error::{self, Result};
use crate::region::MitoRegion;
use crate::region::opener::replay_memtable;
use crate::wal::Wal;
use crate::wal::entry_distributor::WalEntryReceiver;

pub struct RegionCatchupTask<S> {
    entry_receiver: Option<WalEntryReceiver>,
    region: Arc<MitoRegion>,
    replay_checkpoint_entry_id: Option<u64>,
    expected_last_entry_id: Option<u64>,
    allow_stale_entries: bool,
    location_id: Option<u64>,
    wal: Wal<S>,
}

impl<S: LogStore> RegionCatchupTask<S> {
    pub fn new(region: Arc<MitoRegion>, wal: Wal<S>, allow_stale_entries: bool) -> Self {
        Self {
            entry_receiver: None,
            region,
            replay_checkpoint_entry_id: None,
            expected_last_entry_id: None,
            allow_stale_entries,
            location_id: None,
            wal,
        }
    }

    /// Sets the location id.
    pub(crate) fn with_location_id(mut self, location_id: Option<u64>) -> Self {
        self.location_id = location_id;
        self
    }

    /// Sets the expected last entry id.
    pub(crate) fn with_expected_last_entry_id(
        mut self,
        expected_last_entry_id: Option<u64>,
    ) -> Self {
        self.expected_last_entry_id = expected_last_entry_id;
        self
    }

    /// Sets the entry receiver.
    pub(crate) fn with_entry_receiver(mut self, entry_receiver: Option<WalEntryReceiver>) -> Self {
        self.entry_receiver = entry_receiver;
        self
    }

    /// Sets the replay checkpoint entry id.
    pub(crate) fn with_replay_checkpoint_entry_id(
        mut self,
        replay_checkpoint_entry_id: Option<u64>,
    ) -> Self {
        self.replay_checkpoint_entry_id = replay_checkpoint_entry_id;
        self
    }

    pub async fn run(&mut self) -> Result<()> {
        if self.region.provider.is_remote_wal() {
            self.remote_wal_catchup().await
        } else {
            self.local_wal_catchup().await
        }
    }

    async fn remote_wal_catchup(&mut self) -> Result<()> {
        let flushed_entry_id = self.region.version_control.current().last_entry_id;
        let replay_from_entry_id = self
            .replay_checkpoint_entry_id
            .unwrap_or(flushed_entry_id)
            .max(flushed_entry_id);
        let region_id = self.region.region_id;
        info!(
            "Trying to replay memtable for region: {region_id}, provider: {:?}, replay from entry id: {replay_from_entry_id}, flushed entry id: {flushed_entry_id}",
            self.region.provider
        );
        let timer = Instant::now();
        let wal_entry_reader = self
            .entry_receiver
            .take()
            .map(|r| Box::new(r) as _)
            .unwrap_or_else(|| {
                self.wal
                    .wal_entry_reader(&self.region.provider, region_id, self.location_id)
            });
        let on_region_opened = self.wal.on_region_opened();
        let last_entry_id = replay_memtable(
            &self.region.provider,
            wal_entry_reader,
            region_id,
            replay_from_entry_id,
            &self.region.version_control,
            self.allow_stale_entries,
            on_region_opened,
        )
        .await?;
        info!(
            "Elapsed: {:?}, region: {region_id}, provider: {:?} catchup finished. replay from entry id: {replay_from_entry_id}, flushed entry id: {flushed_entry_id}, last entry id: {last_entry_id}, expected: {:?}.",
            timer.elapsed(),
            self.region.provider,
            self.expected_last_entry_id
        );
        if let Some(expected_last_entry_id) = self.expected_last_entry_id {
            ensure!(
                // The replayed last entry id may be greater than the `expected_last_entry_id`.
                last_entry_id >= expected_last_entry_id,
                error::UnexpectedSnafu {
                    reason: format!(
                        "Failed to catchup region {}, it was expected to replay to {}, but actually replayed to {}",
                        region_id, expected_last_entry_id, last_entry_id,
                    ),
                }
            )
        }
        Ok(())
    }

    async fn local_wal_catchup(&mut self) -> Result<()> {
        let version = self.region.version_control.current();
        let mut flushed_entry_id = version.last_entry_id;
        let region_id = self.region.region_id;
        let latest_entry_id = self
            .wal
            .store()
            .latest_entry_id(&self.region.provider)
            .unwrap_or_default();
        info!(
            "Skips to replay memtable for region: {}, flushed entry id: {}, latest entry id: {}",
            region_id, flushed_entry_id, latest_entry_id
        );

        if latest_entry_id > flushed_entry_id {
            warn!(
                "Found latest entry id is greater than flushed entry id, using latest entry id as flushed entry id, region: {}, latest entry id: {}, flushed entry id: {}",
                region_id, latest_entry_id, flushed_entry_id
            );
            flushed_entry_id = latest_entry_id;
            self.region.version_control.set_entry_id(flushed_entry_id);
        }
        let on_region_opened = self.wal.on_region_opened();
        on_region_opened(region_id, flushed_entry_id, &self.region.provider).await?;
        Ok(())
    }
}
