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

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_telemetry::warn;
use puffin::puffin_manager::{PuffinWriter, PutOptions};
use snafu::{OptionExt, ResultExt};
use tantivy::indexer::NoMergePolicy;
use tantivy::schema::{STORED, Schema, TEXT};
use tantivy::store::{Compressor, ZstdCompressor};
use tantivy::{Index, IndexWriter, doc};

use crate::fulltext_index::create::FulltextIndexCreator;
use crate::fulltext_index::error::{
    ExternalSnafu, FinishedSnafu, IoSnafu, JoinSnafu, Result, SerializeToJsonSnafu, TantivySnafu,
};
use crate::fulltext_index::{Config, KEY_FULLTEXT_CONFIG};

pub const TEXT_FIELD_NAME: &str = "greptime_fulltext_text";
pub const ROWID_FIELD_NAME: &str = "greptime_fulltext_rowid";

/// `TantivyFulltextIndexCreator` is a fulltext index creator using tantivy.
pub struct TantivyFulltextIndexCreator {
    /// The tantivy index writer.
    writer: Option<IndexWriter>,
    /// Retained across awaits so abort can join a cancelled finish operation.
    finishing: Option<common_runtime::JoinHandle<Result<()>>>,

    /// The field for the text.
    text_field: tantivy::schema::Field,

    /// The field for the row id.
    rowid_field: tantivy::schema::Field,

    /// The current max row id.
    max_rowid: u64,

    /// Shared with blocking work so cancellation cannot unlink an active index.
    directory: Arc<TempIndexDirectory>,

    /// The configuration of the fulltext index.
    config: Config,
}

/// Owns an empty temporary directory, never a pre-existing index.
struct TempIndexDirectory {
    path: PathBuf,
    removed: AtomicBool,
}

impl TempIndexDirectory {
    fn create(path: PathBuf) -> Result<Arc<Self>> {
        std::fs::create_dir_all(&path).context(IoSnafu)?;
        if std::fs::read_dir(&path)
            .context(IoSnafu)?
            .next()
            .transpose()
            .context(IoSnafu)?
            .is_some()
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "Tantivy temporary directory must be empty",
            ))
            .context(IoSnafu);
        }
        Ok(Arc::new(Self {
            path,
            removed: AtomicBool::new(false),
        }))
    }
}

impl Drop for TempIndexDirectory {
    fn drop(&mut self) {
        if self.removed.load(Ordering::Relaxed) {
            return;
        }
        let path = self.path.clone();
        common_runtime::spawn_blocking_global(move || {
            if let Err(error) = std::fs::remove_dir_all(&path)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                warn!(error; "Failed to remove temporary Tantivy directory: {}", path.display());
            }
        });
    }
}

impl Drop for TantivyFulltextIndexCreator {
    fn drop(&mut self) {
        if let Some(writer) = self.writer.take() {
            let directory = self.directory.clone();
            // IndexWriter::drop joins its threads; neither that nor directory
            // removal may block the async task cancelling this creator.
            common_runtime::spawn_blocking_global(move || {
                drop(writer);
                drop(directory);
            });
        }
    }
}

impl TantivyFulltextIndexCreator {
    /// Creates a new `TantivyFulltextIndexCreator`.
    ///
    /// `path` must be an empty temporary directory. It is removed after the
    /// creator and any in-flight blocking work release it.
    pub async fn new(path: impl AsRef<Path>, config: Config, memory_limit: usize) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        // Keep creation and its guard in the same task: dropping the awaiting
        // future must not race mkdir against cleanup.
        common_runtime::spawn_blocking_global(move || Self::create(path, config, memory_limit))
            .await
            .context(JoinSnafu)?
    }

    fn create(path: PathBuf, config: Config, memory_limit: usize) -> Result<Self> {
        let directory = TempIndexDirectory::create(path)?;
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field(TEXT_FIELD_NAME, TEXT);
        let rowid_field = schema_builder.add_u64_field(ROWID_FIELD_NAME, STORED);
        let schema = schema_builder.build();

        let mut index = Index::create_in_dir(&directory.path, schema).context(TantivySnafu)?;
        index.settings_mut().docstore_compression = Compressor::Zstd(ZstdCompressor::default());
        index.set_tokenizers(config.build_tantivy_tokenizer());

        let memory_limit = Self::sanitize_memory_limit(memory_limit);

        // Use one thread to keep order of the row id.
        let writer = index
            .writer_with_num_threads(1, memory_limit)
            .context(TantivySnafu)?;
        writer.set_merge_policy(Box::new(NoMergePolicy));

        Ok(Self {
            writer: Some(writer),
            finishing: None,
            text_field,
            rowid_field,
            max_rowid: 0,
            directory,
            config,
        })
    }

    async fn finish_writer(&mut self) -> Result<()> {
        if self.finishing.is_none() {
            let writer = self.writer.take().context(FinishedSnafu)?;
            let directory = self.directory.clone();
            self.finishing = Some(common_runtime::spawn_blocking_global(move || {
                let _directory = directory;
                // Declare the writer after the lease so error unwinding also
                // drops/joins it before releasing the directory.
                let mut writer = writer;
                writer.commit().context(TantivySnafu)?;
                writer.wait_merging_threads().context(TantivySnafu)
            }));
        }
        let completed = self.finishing.as_mut().context(FinishedSnafu)?.await;
        self.finishing.take();
        completed.context(JoinSnafu)?
    }

    /// Effective per-column allocation, also used by compaction admission.
    pub fn sanitize_memory_limit(memory_limit: usize) -> usize {
        // Port from tantivy::indexer::index_writer::{MEMORY_BUDGET_NUM_BYTES_MIN, MEMORY_BUDGET_NUM_BYTES_MAX}
        const MARGIN_IN_BYTES: usize = 1_000_000;
        const MEMORY_BUDGET_NUM_BYTES_MIN: usize = ((MARGIN_IN_BYTES as u32) * 15u32) as usize;
        const MEMORY_BUDGET_NUM_BYTES_MAX: usize = u32::MAX as usize - MARGIN_IN_BYTES - 1;

        memory_limit.clamp(MEMORY_BUDGET_NUM_BYTES_MIN, MEMORY_BUDGET_NUM_BYTES_MAX)
    }
}

#[async_trait]
impl FulltextIndexCreator for TantivyFulltextIndexCreator {
    async fn push_text(&mut self, text: &str) -> Result<()> {
        let writer = self.writer.as_mut().context(FinishedSnafu)?;
        let doc = doc!(self.text_field => text, self.rowid_field => self.max_rowid);
        self.max_rowid += 1;
        writer.add_document(doc).context(TantivySnafu)?;
        Ok(())
    }

    async fn finish(
        &mut self,
        puffin_writer: &mut (impl PuffinWriter + Send),
        blob_key: &str,
        put_options: PutOptions,
    ) -> Result<u64> {
        self.finish_writer().await?;

        let property_key = KEY_FULLTEXT_CONFIG.to_string();
        let property_value = serde_json::to_string(&self.config).context(SerializeToJsonSnafu)?;

        puffin_writer
            .put_dir(
                blob_key,
                self.directory.path.clone(),
                put_options,
                HashMap::from([(property_key, property_value)]),
            )
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)
    }

    async fn abort(&mut self) -> Result<()> {
        if self.writer.is_some() || self.finishing.is_some() {
            self.finish_writer().await?;
        }
        match tokio::fs::remove_dir_all(&self.directory.path).await {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            removed => removed.context(IoSnafu)?,
        }
        self.directory.removed.store(true, Ordering::Relaxed);
        Ok(())
    }

    fn memory_usage(&self) -> usize {
        // Unable to get the memory usage of `IndexWriter`.
        0
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use common_test_util::temp_dir::create_temp_dir;
    use futures::AsyncRead;
    use tantivy::TantivyDocument;
    use tantivy::collector::DocSetCollector;
    use tantivy::query::QueryParser;
    use tantivy::schema::Value;

    use super::*;
    use crate::fulltext_index::Analyzer;

    #[derive(Default)]
    struct MockPuffinWriter {
        blocked: Option<Arc<tokio::sync::Notify>>,
    }

    #[async_trait]
    impl PuffinWriter for MockPuffinWriter {
        async fn put_blob<R>(
            &mut self,
            _key: &str,
            _raw_data: R,
            _options: PutOptions,
            _properties: HashMap<String, String>,
        ) -> puffin::error::Result<u64>
        where
            R: AsyncRead + Send,
        {
            unreachable!()
        }

        async fn put_dir(
            &mut self,
            _key: &str,
            dir: PathBuf,
            _options: PutOptions,
            _properties: HashMap<String, String>,
        ) -> puffin::error::Result<u64> {
            if let Some(blocked) = &self.blocked {
                assert!(dir.join("meta.json").exists());
                blocked.notify_one();
                futures::future::pending::<()>().await;
            }
            Ok(0)
        }
        fn set_footer_lz4_compressed(&mut self, _lz4_compressed: bool) {
            unreachable!()
        }

        async fn finish(self) -> puffin::error::Result<u64> {
            Ok(0)
        }
    }

    async fn assert_directory_removed(path: &Path) {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while tokio::fs::try_exists(path).await.unwrap() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("temporary Tantivy directory leaked");
    }

    #[tokio::test]
    async fn test_creator_drop_cleans_temporary_files() {
        for stage in ["active", "finished", "aborted"] {
            let root = create_temp_dir("tantivy-drop-");
            let path = root.path().join("index");
            let mut creator = TantivyFulltextIndexCreator::new(&path, Config::default(), 1)
                .await
                .unwrap();
            creator.push_text("indexed text").await.unwrap();
            assert!(path.join("meta.json").exists());
            match stage {
                "finished" => {
                    creator
                        .finish(
                            &mut MockPuffinWriter::default(),
                            "index",
                            PutOptions::default(),
                        )
                        .await
                        .unwrap();
                }
                "aborted" => {
                    creator.abort().await.unwrap();
                }
                _ => {}
            }
            drop(creator);
            assert_directory_removed(&path).await;
            assert!(root.path().exists());
        }
    }

    #[tokio::test]
    async fn test_abort_after_cancelled_finish_cleans_temporary_files() {
        let root = create_temp_dir("tantivy-cancel-finish-");
        let path = root.path().join("index");
        let mut creator = TantivyFulltextIndexCreator::new(&path, Config::default(), 1)
            .await
            .unwrap();
        creator.push_text("indexed text").await.unwrap();
        let blocked = Arc::new(tokio::sync::Notify::new());
        let mut writer = MockPuffinWriter {
            blocked: Some(blocked.clone()),
        };
        {
            let finishing = creator.finish(&mut writer, "index", PutOptions::default());
            tokio::pin!(finishing);
            tokio::select! {
                _ = blocked.notified() => {}
                result = &mut finishing => panic!("finish did not reach the blocked upload: {result:?}"),
                _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => panic!("finish stalled"),
            }
        }
        creator.abort().await.unwrap();
        creator.abort().await.unwrap();
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn test_detached_initialization_cleans_its_result() {
        let root = create_temp_dir("tantivy-detached-init-");
        let path = root.path().join("index");
        let creating_path = path.clone();
        let (ready, initialized) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        // The real constructor runs on the blocking pool. Detaching its handle
        // must clean even if cancellation happens before the result is delivered.
        let creating = common_runtime::spawn_blocking_global(move || {
            let creator =
                TantivyFulltextIndexCreator::create(creating_path, Config::default(), 1).unwrap();
            ready.send(()).unwrap();
            released.recv().unwrap();
            creator
        });
        tokio::time::timeout(std::time::Duration::from_secs(10), initialized)
            .await
            .unwrap()
            .unwrap();
        assert!(path.join("meta.json").exists());
        drop(creating);
        release.send(()).unwrap();
        assert_directory_removed(&path).await;
    }

    #[tokio::test]
    async fn test_initialization_preserves_nonempty_directory() {
        let root = create_temp_dir("tantivy-existing-");
        let sentinel = root.path().join("existing");
        tokio::fs::write(&sentinel, "must survive").await.unwrap();
        assert!(
            TantivyFulltextIndexCreator::new(root.path(), Config::default(), 1)
                .await
                .is_err()
        );
        assert_eq!(
            "must survive",
            tokio::fs::read_to_string(&sentinel).await.unwrap()
        );
    }

    #[tokio::test]
    async fn test_creator_basic() {
        let memory_limits = [1, 64_000_000, usize::MAX];

        for memory_limit in memory_limits {
            let temp_dir = create_temp_dir("test_creator_basic_");

            let texts = vec!["hello", "world", "hello, world", "foo!", "Bar"];
            let config = Config::default();
            let _creator = build_index(&texts, temp_dir.path(), config, memory_limit).await;

            let cases = [
                ("hello", vec![0u32, 2]),
                ("world", vec![1, 2]),
                ("foo", vec![3]),
                ("bar", vec![4]),
            ];
            query_and_check(temp_dir.path(), config, &cases).await;
        }
    }

    #[tokio::test]
    async fn test_creator_case_sensitive() {
        let memory_limits = [1, 64_000_000, usize::MAX];

        for memory_limit in memory_limits {
            let temp_dir = create_temp_dir("test_creator_case_sensitive_");

            let texts = vec!["hello", "world", "hello, world", "foo!", "Bar"];
            let config = Config {
                case_sensitive: true,
                ..Config::default()
            };
            let _creator = build_index(&texts, temp_dir.path(), config, memory_limit).await;

            let cases = [
                ("hello", vec![0u32, 2]),
                ("world", vec![1, 2]),
                ("foo", vec![3]),
                ("Foo", vec![]),
                ("FOO", vec![]),
                ("bar", vec![]),
                ("Bar", vec![4]),
                ("BAR", vec![]),
            ];
            query_and_check(temp_dir.path(), config, &cases).await;
        }
    }

    #[tokio::test]
    async fn test_creator_chinese() {
        let memory_limits = [1, 64_000_000, usize::MAX];

        for memory_limit in memory_limits {
            let temp_dir = create_temp_dir("test_creator_chinese_");

            let texts = vec!["你好", "世界", "你好，世界", "你好世界", "foo!", "Bar"];
            let config = Config {
                analyzer: Analyzer::Chinese,
                ..Config::default()
            };
            let _creator = build_index(&texts, temp_dir.path(), config, memory_limit).await;

            let cases = [
                ("你好", vec![0u32, 2, 3]),
                ("世界", vec![1, 2, 3]),
                ("foo", vec![4]),
                ("bar", vec![5]),
            ];
            query_and_check(temp_dir.path(), config, &cases).await;
        }
    }

    #[tokio::test]
    async fn test_creator_chinese_case_sensitive() {
        let memory_limits = [1, 64_000_000, usize::MAX];

        for memory_limit in memory_limits {
            let temp_dir = create_temp_dir("test_creator_chinese_case_sensitive_");

            let texts = vec!["你好", "世界", "你好，世界", "你好世界", "foo!", "Bar"];
            let config = Config {
                case_sensitive: true,
                analyzer: Analyzer::Chinese,
            };
            let _creator = build_index(&texts, temp_dir.path(), config, memory_limit).await;

            let cases = [
                ("你好", vec![0u32, 2, 3]),
                ("世界", vec![1, 2, 3]),
                ("foo", vec![4]),
                ("bar", vec![]),
                ("Foo", vec![]),
                ("FOO", vec![]),
                ("Bar", vec![5]),
                ("BAR", vec![]),
            ];
            query_and_check(temp_dir.path(), config, &cases).await;
        }
    }

    async fn build_index(
        texts: &[&str],
        path: &Path,
        config: Config,
        memory_limit: usize,
    ) -> TantivyFulltextIndexCreator {
        let mut creator = TantivyFulltextIndexCreator::new(path, config, memory_limit)
            .await
            .unwrap();
        for text in texts {
            creator.push_text(text).await.unwrap();
        }
        creator
            .finish(&mut MockPuffinWriter::default(), "", PutOptions::default())
            .await
            .unwrap();
        creator
    }

    async fn query_and_check(path: &Path, config: Config, cases: &[(&str, Vec<u32>)]) {
        let mut index = Index::open_in_dir(path).unwrap();
        index.set_tokenizers(config.build_tantivy_tokenizer());
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        for (query, expected) in cases {
            let query_parser = QueryParser::for_index(
                &index,
                vec![index.schema().get_field(TEXT_FIELD_NAME).unwrap()],
            );
            let query = query_parser.parse_query(query).unwrap();
            let docs = searcher.search(&query, &DocSetCollector).unwrap();

            let mut res = vec![];
            let rowid_field = searcher.schema().get_field(ROWID_FIELD_NAME).unwrap();
            for doc_addr in &docs {
                let doc: TantivyDocument = searcher.doc(*doc_addr).unwrap();
                let rowid = doc.get_first(rowid_field).unwrap().as_u64().unwrap();
                assert_eq!(rowid as u32, doc_addr.doc_id);
                res.push(rowid as u32);
            }

            res.sort();
            assert_eq!(res, *expected);
        }
    }
}
