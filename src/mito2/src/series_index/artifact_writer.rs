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

use crate::cache::write_cache::{NarthexUpload, WriteCache};
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
    pub(crate) upload: Option<NarthexUpload>,
}
impl Outputs {
    pub(super) async fn open(&mut self, path: String) -> Result<()> {
        require(self.active.is_none(), "artifact output already open")?;
        let path = if self.upload.is_some() {
            WriteCache::narthex_stage(&path)
        } else {
            path
        };
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
        if let Some(upload) = &mut self.upload {
            let path = file.path.strip_prefix(WriteCache::NARTHEX_STAGE).unwrap();
            upload.upload_closed(path).await?;
        }
        self.active = None;
        Ok(())
    }
    pub(crate) async fn commit(&mut self) {
        if self.upload.is_some()
            && let Some(dir) = self
                .paths
                .last()
                .and_then(|p| crate::sst::location::parent_dir(p))
        {
            let _ = self.store.delete(dir).await;
        }
        self.committed = true;
    }

    pub(super) async fn cleanup(&mut self) -> Result<()> {
        if let Some(upload) = &mut self.upload {
            upload.clean().await;
        }
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
        if first_error.is_none() {
            let dirs: std::collections::BTreeSet<_> = self
                .paths
                .iter()
                .filter_map(|path| crate::sst::location::parent_dir(path))
                .collect();
            for dir in dirs {
                if let Err(error) = self.store.delete(dir).await {
                    first_error.get_or_insert_with(|| OpenDalSnafu.into_error(error));
                }
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
                upload: self.upload.take(),
            };
            common_runtime::spawn_compact(async move {
                if let Err(error) = owned.cleanup().await {
                    common_telemetry::warn!(error; "Cleaning unpublished format artifact");
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_abort_removes_narthex_directory() {
        let root = common_test_util::temp_dir::create_temp_dir("narthex-abort");
        let store = ObjectStore::new(
            object_store::services::Fs::default().root(root.path().to_str().unwrap()),
        )
        .unwrap();
        let dir =
            crate::sst::location::narthex_file_dir("region/", store_api::storage::FileId::random());
        let mut outputs = Outputs {
            store: store.clone(),
            active: None,
            paths: Vec::new(),
            committed: false,
            upload: None,
        };
        outputs.open(format!("{dir}part-0.samples")).await.unwrap();
        outputs.write(vec![1, 2, 3]).await.unwrap();
        outputs.close().await.unwrap();
        assert!(root.path().join(&dir).is_dir());
        outputs.cleanup().await.unwrap();
        assert!(!root.path().join(&dir).exists());
    }
}
