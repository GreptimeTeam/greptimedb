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

use common_error::ext::BoxedError;
use object_store::config::ObjectStoreConfig;
use object_store::factory;
use object_store::util;
use object_store::{ErrorKind, Metadata, ObjectStore};
use snafu::{IntoError, OptionExt, ResultExt};

use crate::election::object_store::AuthorityHead;
use crate::error::{self, Result};
use crate::metrics::METRIC_AUTHORITY_STORAGE_EVENTS;

/// A coherent read. Only this module can pair the payload and response metadata.
#[derive(Debug, Clone)]
pub struct AuthorityState {
    head: AuthorityHead,
    metadata: Metadata,
    scope: Arc<StoreScope>,
    path: String,
}

impl AuthorityState {
    /// Payload observed by this read.
    pub fn head(&self) -> &AuthorityHead {
        &self.head
    }
    /// Complete metadata returned by the same read.
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }
}

/// Protocol-specific storage. Writes are never transparently retried.
pub struct HeadStore {
    pub(crate) operator: ObjectStore,
    pub(crate) head_path: String,
    pub(crate) probe_prefix: String,
    scope: Arc<StoreScope>,
}

// A fresh allocation identifies the store instance that produced each state.
#[derive(Debug)]
struct StoreScope;

enum ConditionIdentity<'a> {
    Version(&'a str),
    Etag(&'a str),
}

impl HeadStore {
    /// Opens storage without creating or changing the official Head.
    ///
    /// Every call performs remote capability probes. Initialize once at node startup
    /// and share this instance. An `AuthorityState` is bound to the instance that read
    /// it; conditional replacement must use that same instance.
    ///
    /// `prefix` is relative to the configured storage root, which is applied once by
    /// the operator. An empty prefix is for an exclusively owned root. Clusters sharing
    /// a root must use non-empty, unique, stable prefixes. Path joining collapses
    /// repeated `/` separators and preserves at most one leading `/`; that leading `/`
    /// is still resolved within the operator root, not the host filesystem root.
    pub async fn open(config: &ObjectStoreConfig, data_home: &str, prefix: &str) -> Result<Self> {
        let operator = factory::new_raw_object_store(config, data_home)
            .await
            .context(error::AuthorityOpenSnafu)?;
        let store = Self::new(operator, prefix);
        store.check_capabilities()?;
        store.probe_conditional_writes().await?;
        Ok(store)
    }

    // The only Head operator layer assembly site, shared by open() and fault tests.
    // The input must be raw: never add RetryLayer or clone a general-purpose operator.
    fn new(operator: ObjectStore, prefix: &str) -> Self {
        let operator = util::with_instrument_layers(operator, false);
        Self {
            operator,
            head_path: util::join_path(prefix, "authority/head.json"),
            probe_prefix: util::join_dir(prefix, "probes"),
            scope: Arc::new(StoreScope),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(operator: ObjectStore, prefix: &str) -> Self {
        Self::new(operator, prefix)
    }

    /// Reads one full response and its metadata; absence requires operator recovery.
    pub async fn read_head(&self) -> Result<AuthorityState> {
        self.read_at(&self.head_path).await
    }

    /// Explicit create-only initialization. Normal reads never call this method.
    /// Read back with [`Self::read_head`] when payload and metadata are needed.
    pub async fn create_initial_head(&self) -> Result<()> {
        self.create_at(&self.head_path, &AuthorityHead::initial())
            .await
    }

    /// Replaces the observed version. Term and owner checks belong to the authority.
    /// Success confirms the PUT only; a subsequent [`Self::read_head`] reports its own result.
    pub async fn replace_head(
        &self,
        expected: &AuthorityState,
        next: &AuthorityHead,
    ) -> Result<()> {
        self.replace_at(&self.head_path, expected, next).await
    }

    /// Compares opaque conditional identities. Equality does not resolve a timed-out write.
    pub fn same_version(&self, left: &Metadata, right: &Metadata) -> Result<bool> {
        let left = self.identity(left)?;
        let right = self.identity(right)?;
        match (left, right) {
            (ConditionIdentity::Version(left), ConditionIdentity::Version(right))
            | (ConditionIdentity::Etag(left), ConditionIdentity::Etag(right)) => Ok(left == right),
            _ => error::AuthorityMetadataSnafu {
                reason: "incomparable condition identities",
            }
            .fail(),
        }
    }

    fn identity<'a>(&self, metadata: &'a Metadata) -> Result<ConditionIdentity<'a>> {
        let capability = self.operator.info().capability();
        if capability.write_with_if_version_match
            && let Some(version) = metadata.version()
        {
            if version.is_empty() {
                return error::AuthorityMetadataSnafu {
                    reason: "empty native condition identity",
                }
                .fail();
            }
            return Ok(ConditionIdentity::Version(version));
        }
        if capability.write_with_if_match
            && let Some(etag) = metadata.etag()
            && !etag.is_empty()
        {
            return Ok(ConditionIdentity::Etag(etag));
        }
        error::AuthorityMetadataSnafu {
            reason: "missing supported conditional identity",
        }
        .fail()
    }

    pub(crate) async fn read_at(&self, path: &str) -> Result<AuthorityState> {
        let read = async {
            let reader = self.operator.reader(path).await?;
            let body = reader.read(..).await?;
            Ok::<_, object_store::Error>((body, reader.metadata().cloned()))
        }
        .await;
        let (body, metadata) = read.map_err(|error| {
            if error.kind() == ErrorKind::NotFound {
                error::AuthorityMissingSnafu { path }.into_error(error)
            } else {
                error::AuthorityStorageSnafu { operation: "read" }.into_error(error)
            }
        })?;
        let metadata = metadata.context(error::AuthorityMetadataSnafu {
            reason: "read returned no metadata",
        })?;
        self.identity(&metadata)?;
        if metadata.last_modified().is_none() {
            return error::AuthorityMetadataSnafu {
                reason: "read returned no last_modified",
            }
            .fail();
        }
        let head = AuthorityHead::decode(&body.to_vec()).map_err(|source| {
            error::AuthorityReadHeadSnafu { path }.into_error(BoxedError::new(source))
        })?;
        Ok(AuthorityState {
            head,
            metadata,
            scope: self.scope.clone(),
            path: path.into(),
        })
    }

    pub(crate) async fn create_at(&self, path: &str, head: &AuthorityHead) -> Result<()> {
        self.write_result(
            self.operator
                .write_with(path, head.encode()?)
                .if_not_exists(true)
                .await,
        )
    }

    pub(crate) async fn replace_at(
        &self,
        path: &str,
        expected: &AuthorityState,
        next: &AuthorityHead,
    ) -> Result<()> {
        if !Arc::ptr_eq(&self.scope, &expected.scope) || expected.path != path {
            return error::AuthorityMetadataSnafu {
                reason: "state belongs to another storage instance or path",
            }
            .fail();
        }
        self.identity(&expected.metadata)?;
        self.write_result(
            self.operator
                .write_with(path, next.encode()?)
                .if_not_changed(&expected.metadata)
                .await,
        )
    }

    fn write_result(&self, result: object_store::Result<Metadata>) -> Result<()> {
        result.map(|_| ()).map_err(|error| {
            let outcome = match error.kind() {
                ErrorKind::ConditionNotMatch | ErrorKind::AlreadyExists | ErrorKind::Conflict => {
                    "conflict"
                }
                _ if error.is_temporary() || error.kind() == ErrorKind::Unexpected => {
                    "unknown_write"
                }
                _ => "write_error",
            };
            METRIC_AUTHORITY_STORAGE_EVENTS
                .with_label_values(&[outcome])
                .inc();
            error::AuthorityStorageSnafu { operation: "write" }.into_error(error)
        })
    }
}
