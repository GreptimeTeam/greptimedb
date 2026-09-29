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
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use async_trait::async_trait;
use common_error::ext::BoxedError;
use puffin::puffin_manager::{PuffinWriter, PutOptions};
use snafu::{OptionExt, ResultExt};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::bloom_filter::creator::BloomFilterCreator;
use crate::external_provider::ExternalTempFileProvider;
use crate::fulltext_index::create::FulltextIndexCreator;
use crate::fulltext_index::error::{
    AbortedSnafu, BiErrorsSnafu, BloomFilterFinishSnafu, ExternalSnafu, PuffinAddBlobSnafu, Result,
    SerializeToJsonSnafu,
};
use crate::fulltext_index::tokenizer::{Analyzer, ChineseTokenizer, EnglishTokenizer};
use crate::fulltext_index::{Config, KEY_FULLTEXT_CONFIG};

const PIPE_BUFFER_SIZE_FOR_SENDING_BLOB: usize = 8192;

/// `BloomFilterFulltextIndexCreator` is for creating a fulltext index using a bloom filter.
pub struct BloomFilterFulltextIndexCreator {
    inner: Option<BloomFilterCreator>,
    analyzer: Analyzer,
    config: Config,
}

impl BloomFilterFulltextIndexCreator {
    pub fn new(
        config: Config,
        rows_per_segment: usize,
        false_positive_rate: f64,
        intermediate_provider: Arc<dyn ExternalTempFileProvider>,
        global_memory_usage: Arc<AtomicUsize>,
        global_memory_usage_threshold: Option<usize>,
    ) -> Self {
        let tokenizer = match config.analyzer {
            crate::fulltext_index::Analyzer::English => Box::new(EnglishTokenizer) as _,
            crate::fulltext_index::Analyzer::Chinese => Box::new(ChineseTokenizer) as _,
        };
        let analyzer = Analyzer::new(tokenizer, config.case_sensitive);

        let inner = BloomFilterCreator::new(
            rows_per_segment,
            false_positive_rate,
            intermediate_provider,
            global_memory_usage,
            global_memory_usage_threshold,
        );
        Self {
            inner: Some(inner),
            analyzer,
            config,
        }
    }

    /// Pushes a text to the index.
    ///
    /// [`FulltextIndexCreator::push_text`] just delegates to this inherent method so
    /// callers holding the concrete creator skip the boxed future of the trait method.
    pub async fn push_text(&mut self, text: &str) -> Result<()> {
        let mut token_buf = Vec::new();
        let hashes = self.analyzer.analyze_text_hashes(text, &mut token_buf);
        self.inner
            .as_mut()
            .context(AbortedSnafu)?
            .push_row_hashes(hashes)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;
        Ok(())
    }
}

#[async_trait]
impl FulltextIndexCreator for BloomFilterFulltextIndexCreator {
    async fn push_text(&mut self, text: &str) -> Result<()> {
        Self::push_text(self, text).await
    }

    async fn finish(
        &mut self,
        puffin_writer: &mut (impl PuffinWriter + Send),
        blob_key: &str,
        mut put_options: PutOptions,
    ) -> Result<u64> {
        // Compressing the bloom filter doesn't reduce the size but hurts read performance.
        // Always disable compression here.
        put_options.compression = None;

        let creator = self.inner.as_mut().context(AbortedSnafu)?;

        let (tx, rx) = tokio::io::duplex(PIPE_BUFFER_SIZE_FOR_SENDING_BLOB);

        let property_key = KEY_FULLTEXT_CONFIG.to_string();
        let property_value = serde_json::to_string(&self.config).context(SerializeToJsonSnafu)?;

        let (index_finish, puffin_add_blob) = futures::join!(
            creator.finish(tx.compat_write()),
            puffin_writer.put_blob(
                blob_key,
                rx.compat(),
                put_options,
                HashMap::from([(property_key, property_value)]),
            )
        );

        match (
            puffin_add_blob.context(PuffinAddBlobSnafu),
            index_finish.context(BloomFilterFinishSnafu),
        ) {
            (Err(e1), Err(e2)) => BiErrorsSnafu {
                first: Box::new(e1),
                second: Box::new(e2),
            }
            .fail()?,

            (Ok(_), e @ Err(_)) => e?,
            (e @ Err(_), Ok(_)) => e.map(|_| ())?,
            (Ok(written_bytes), Ok(_)) => {
                return Ok(written_bytes);
            }
        }
        Ok(0)
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.take().context(AbortedSnafu)?;
        Ok(())
    }

    fn memory_usage(&self) -> usize {
        self.inner
            .as_ref()
            .map(|i| i.memory_usage())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use futures::AsyncReadExt;
    use greptime_proto::v1::index::BloomFilterMeta;
    use prost::Message;

    use super::*;
    use crate::external_provider::MockExternalTempFileProvider;

    /// A `PuffinWriter` that collects the blob written by `put_blob`.
    #[derive(Default)]
    struct CollectBlobPuffinWriter {
        blob: Vec<u8>,
    }

    #[async_trait]
    impl PuffinWriter for CollectBlobPuffinWriter {
        async fn put_blob<R>(
            &mut self,
            _key: &str,
            raw_data: R,
            _options: PutOptions,
            _properties: HashMap<String, String>,
        ) -> puffin::error::Result<u64>
        where
            R: futures::AsyncRead + Send,
        {
            futures::pin_mut!(raw_data);
            raw_data.read_to_end(&mut self.blob).await.unwrap();
            Ok(self.blob.len() as u64)
        }

        async fn put_dir(
            &mut self,
            _key: &str,
            _dir: PathBuf,
            _options: PutOptions,
            _properties: HashMap<String, String>,
        ) -> puffin::error::Result<u64> {
            unreachable!()
        }

        fn set_footer_lz4_compressed(&mut self, _lz4_compressed: bool) {
            unreachable!()
        }

        async fn finish(self) -> puffin::error::Result<u64> {
            Ok(0)
        }
    }

    /// Pushes a text through the `FulltextIndexCreator` trait.
    async fn push_via_trait(creator: &mut impl FulltextIndexCreator, text: &str) {
        creator.push_text(text).await.unwrap();
    }

    /// Builds the bloom filter blob for `texts`.
    ///
    /// If `via_trait` is true, texts are pushed through the `FulltextIndexCreator` trait
    /// instead of the inherent method.
    async fn build_blob(texts: &[&str], rows_per_segment: usize, via_trait: bool) -> Vec<u8> {
        let mut creator = BloomFilterFulltextIndexCreator::new(
            Config::default(),
            rows_per_segment,
            0.01,
            Arc::new(MockExternalTempFileProvider::new()),
            Arc::new(AtomicUsize::new(0)),
            None,
        );

        for text in texts {
            if via_trait {
                push_via_trait(&mut creator, text).await;
            } else {
                creator.push_text(text).await.unwrap();
            }
        }

        let mut writer = CollectBlobPuffinWriter::default();
        creator
            .finish(&mut writer, "bloom", PutOptions::default())
            .await
            .unwrap();
        writer.blob
    }

    /// Decodes the `BloomFilterMeta` trailer of a bloom filter blob.
    fn blob_meta(blob: &[u8]) -> BloomFilterMeta {
        let meta_size_offset = blob.len() - size_of::<u32>();
        let meta_size = u32::from_le_bytes(blob[meta_size_offset..].try_into().unwrap()) as usize;
        BloomFilterMeta::decode(&blob[meta_size_offset - meta_size..meta_size_offset]).unwrap()
    }

    /// Compile-time check: `push_text` is callable without `FulltextIndexCreator` in scope,
    /// so a call on the concrete creator resolves to the inherent method and not to the
    /// boxed future of the trait method.
    mod push_text_is_inherent {
        use crate::fulltext_index::create::BloomFilterFulltextIndexCreator;

        #[allow(dead_code)]
        async fn push(creator: &mut BloomFilterFulltextIndexCreator, text: &str) {
            creator.push_text(text).await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_push_text_inherent_and_trait_match() {
        // Empty strings stand for NULL values and for columns that are absent from the
        // batch: both still count as one row per text.
        let texts = [
            "hello world",
            "",
            "greptime",
            "foo bar",
            "",
            "hello greptime",
        ];

        // `1` finalizes a segment for every text, `4` crosses a segment boundary.
        for rows_per_segment in [1, 4] {
            let direct = build_blob(&texts, rows_per_segment, false).await;
            let via_trait = build_blob(&texts, rows_per_segment, true).await;

            assert!(!direct.is_empty());
            assert_eq!(direct.len(), via_trait.len());
            assert!(direct == via_trait, "blobs differ");

            let meta = blob_meta(&direct);
            assert_eq!(meta.rows_per_segment, rows_per_segment as u64);
            assert_eq!(meta.row_count, texts.len() as u64);
            assert_eq!(
                meta.segment_count,
                texts.len().div_ceil(rows_per_segment) as u64
            );
        }
    }
}
