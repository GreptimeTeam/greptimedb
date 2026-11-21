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

use common_telemetry::warn;
use object_store::util;
use object_store::{ErrorKind, Metadata};
use snafu::{IntoError, ResultExt};
use uuid::Uuid;

use crate::election::object_store::{AuthorityHead, AuthorityState, HeadStore};
use crate::error::{self, Result};
use crate::metrics::METRIC_AUTHORITY_STORAGE_EVENTS;

impl HeadStore {
    pub(crate) fn check_capabilities(&self) -> Result<()> {
        let info = self.operator.info();
        let scheme = info.scheme();
        // TODO(alorun): Replace the allow-list with capability-based admission after
        // GCS passes real-endpoint generation-based conditional-write qualification.
        let reason = match scheme {
            "s3" | "azblob" => None,
            "gcs" => {
                Some("GCS generation-based conditional writes require real-endpoint validation")
            }
            "oss" => Some("OSS PutObject rejects If-Match and If-None-Match conditions"),
            "fs" | "memory" => {
                Some("backend lacks the required atomic conditional replacement semantics")
            }
            _ => Some("backend has not been qualified for Authority Head conditional replacement"),
        };
        if let Some(reason) = reason {
            return error::AuthorityCapabilitySnafu { scheme, reason }.fail();
        }
        let c = info.capability();
        if !(c.read
            && c.write
            && c.list
            && c.delete
            && c.write_with_if_not_exists
            && (c.write_with_if_version_match || c.write_with_if_match))
        {
            return error::AuthorityCapabilitySnafu {
                scheme,
                reason: "read/write/list/delete and atomic conditional create/replace are required",
            }
            .fail();
        }
        Ok(())
    }

    pub(crate) async fn probe_conditional_writes(&self) -> Result<()> {
        let path = util::join_path(&self.probe_prefix, &format!("{}.json", Uuid::new_v4()));
        let result = self.probe_at(&path).await;
        let cleanup = self.cleanup_probe(&path).await;
        if result.is_err() {
            METRIC_AUTHORITY_STORAGE_EVENTS
                .with_label_values(&["probe_failure"])
                .inc();
            if let Err(error) = cleanup {
                warn!(error; "Authority probe cleanup also failed");
            }
            return result;
        }
        cleanup
    }

    async fn probe_at(&self, path: &str) -> Result<()> {
        let initial = AuthorityHead::initial();
        self.create_at(path, &initial).await?;
        let first = self.read_at(path).await?;
        self.require_probe(first.head() == &initial, "initial contents differ")?;
        let next = initial.clone().with_next_write_seq()?;
        match self.create_at(path, &next).await {
            Err(error)
                if matches!(
                    error.authority_storage_error().map(|e| e.kind()),
                    Some(ErrorKind::ConditionNotMatch | ErrorKind::AlreadyExists)
                ) => {}
            Err(error) => return Err(error),
            Ok(()) => {
                return error::AuthorityProbeSnafu {
                    reason: "duplicate create was accepted",
                }
                .fail();
            }
        }
        let unchanged = self.read_at(path).await?;
        self.verify_probe(&unchanged, &initial, first.metadata(), true)?;
        self.replace_at(path, &first, &next).await?;
        let current = self.read_at(path).await?;
        self.verify_probe(&current, &next, first.metadata(), false)?;
        let stale = next.clone().with_next_write_seq()?;
        match self.replace_at(path, &first, &stale).await {
            Err(error)
                if error.authority_storage_error().map(|e| e.kind())
                    == Some(ErrorKind::ConditionNotMatch) => {}
            Err(error) => return Err(error),
            Ok(()) => {
                return error::AuthorityProbeSnafu {
                    reason: "stale replacement was accepted",
                }
                .fail();
            }
        }
        let unchanged = self.read_at(path).await?;
        self.verify_probe(&unchanged, &next, current.metadata(), true)?;
        // Exercise plain PUT and listing permission, without using listing for authority.
        self.operator
            .write(path, stale.encode()?)
            .await
            .context(error::AuthorityStorageSnafu {
                operation: "probe plain write",
            })?;
        let plain = self.read_at(path).await?;
        self.verify_probe(&plain, &stale, current.metadata(), false)?;
        self.operator
            .list(&self.probe_prefix)
            .await
            .context(error::AuthorityStorageSnafu {
                operation: "probe list",
            })?;
        Ok(())
    }

    fn verify_probe(
        &self,
        state: &AuthorityState,
        expected: &AuthorityHead,
        metadata: &Metadata,
        same: bool,
    ) -> Result<()> {
        self.require_probe(
            state.head() == expected && self.same_version(state.metadata(), metadata)? == same,
            "contents or condition identity differ from expected",
        )
    }

    fn require_probe(&self, valid: bool, reason: &'static str) -> Result<()> {
        if !valid {
            return error::AuthorityProbeSnafu { reason }.fail();
        }
        Ok(())
    }

    pub(crate) async fn cleanup_probe(&self, path: &str) -> Result<()> {
        self.operator.delete(path).await.map_err(|error| {
            METRIC_AUTHORITY_STORAGE_EVENTS
                .with_label_values(&["probe_cleanup_failure"])
                .inc();
            error::AuthorityStorageSnafu {
                operation: "probe cleanup",
            }
            .into_error(error)
        })
    }
}
