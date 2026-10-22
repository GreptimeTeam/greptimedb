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

//! Owned outputs shared by immutable columnar artifacts.

use bytes::Bytes;
use object_store::{ObjectStore, Writer};
use snafu::{IntoError, ResultExt};

use crate::error::{JoinSnafu, OpenDalSnafu, Result};
use crate::series_index::columnar::{invalid, require};
use crate::sst::{DEFAULT_WRITE_BUFFER_SIZE, DEFAULT_WRITE_CONCURRENCY};

/// Drop owns the live OpenDAL writer, including its private atomic-write temporary file.
pub(super) struct LiveFile {
    writer: Option<Writer>,
    store: ObjectStore,
    path: String,
}
impl Drop for LiveFile {
    fn drop(&mut self) {
        if let Some(mut writer) = self.writer.take() {
            let store = self.store.clone();
            let path = self.path.clone();
            common_runtime::spawn_compact(async move {
                if let Err(error) = writer.abort().await {
                    common_telemetry::warn!(error; "Aborting artifact output {path}");
                }
                if let Err(error) = store.delete(&path).await {
                    common_telemetry::warn!(error; "Deleting artifact output {path}");
                }
            });
        }
    }
}

pub(super) struct Outputs {
    pub(super) store: ObjectStore,
    pub(super) active: Option<LiveFile>,
    pub(super) paths: Vec<String>,
    pub(super) committed: bool,
}
impl Outputs {
    pub(super) async fn open(&mut self, path: String) -> Result<()> {
        require(self.active.is_none(), "artifact output already open")?;
        self.paths.push(path.clone());
        let store = self.store.clone();
        // The task owns opening: cancellation drops the returned LiveFile and aborts it.
        self.active = Some(
            common_runtime::spawn_compact(async move {
                let writer = store
                    .writer_with(&path)
                    .chunk(DEFAULT_WRITE_BUFFER_SIZE.as_bytes() as usize)
                    .concurrent(DEFAULT_WRITE_CONCURRENCY)
                    .await
                    .context(OpenDalSnafu)?;
                Ok::<_, crate::error::Error>(LiveFile {
                    writer: Some(writer),
                    store,
                    path,
                })
            })
            .await
            .context(JoinSnafu)??,
        );
        Ok(())
    }
    pub(super) async fn write(&mut self, bytes: Vec<u8>) -> Result<()> {
        self.active
            .as_mut()
            .and_then(|f| f.writer.as_mut())
            .ok_or_else(|| invalid("artifact output is not open"))?
            .write(Bytes::from(bytes))
            .await
            .context(OpenDalSnafu)?;
        Ok(())
    }
    pub(super) async fn close(&mut self) -> Result<()> {
        let file = self
            .active
            .as_mut()
            .ok_or_else(|| invalid("artifact output is not open"))?;
        file.writer
            .as_mut()
            .ok_or_else(|| invalid("artifact output is closed"))?
            .close()
            .await
            .context(OpenDalSnafu)?;
        file.writer = None;
        self.active = None;
        Ok(())
    }
    pub(super) async fn cleanup(&mut self) -> Result<()> {
        let mut first_error = None;
        if let Some(file) = self.active.as_mut()
            && let Some(writer) = file.writer.as_mut()
        {
            match writer.abort().await {
                Ok(()) => file.writer = None,
                Err(error) => first_error = Some(OpenDalSnafu.into_error(error)),
            }
        }
        self.active = None;
        for path in &self.paths {
            if let Err(error) = self.store.delete(path).await {
                first_error.get_or_insert_with(|| OpenDalSnafu.into_error(error));
            }
        }
        if let Some(error) = first_error {
            Err(error)
        } else {
            self.committed = true;
            Ok(())
        }
    }
}
impl Drop for Outputs {
    fn drop(&mut self) {
        if !self.committed {
            let mut owned = Self {
                store: self.store.clone(),
                active: self.active.take(),
                paths: std::mem::take(&mut self.paths),
                committed: true,
            };
            common_runtime::spawn_compact(async move {
                if let Err(error) = owned.cleanup().await {
                    common_telemetry::warn!(error; "Cleaning unpublished format artifact");
                }
            });
        }
    }
}
