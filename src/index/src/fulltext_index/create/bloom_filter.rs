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
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use async_trait::async_trait;
use common_error::ext::BoxedError;
use puffin::blob_metadata::BlobMetadata;
use puffin::puffin_manager::{PuffinWriter, PutOptions};
use snafu::{OptionExt, ResultExt};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::bloom_filter::creator::BloomFilterCreator;
use crate::external_provider::ExternalTempFileProvider;
use crate::fulltext_index::create::FulltextIndexCreator;
use crate::fulltext_index::error::{
    AbortedSnafu, BiErrorsSnafu, BloomFilterFinishSnafu, ExternalSnafu, PropertyNotFoundSnafu,
    PuffinAddBlobSnafu, Result, SerializeToJsonSnafu,
};
use crate::fulltext_index::tokenizer::{Analyzer, ChineseTokenizer, EnglishTokenizer};
use crate::fulltext_index::Config;

const PIPE_BUFFER_SIZE_FOR_SENDING_BLOB: usize = 8192;

pub const KEY_FULLTEXT_CONFIG: &str = "fulltext_config";

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
}

#[async_trait]
impl FulltextIndexCreator for BloomFilterFulltextIndexCreator {
    async fn push_text(&mut self, text: &str) -> Result<()> {
        let tokens = self.analyzer.analyze_text(text)?;
        self.inner
            .as_mut()
            .context(AbortedSnafu)?
            .push_row_elems(tokens)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;
        Ok(())
    }

    async fn finish(
        &mut self,
        puffin_writer: &mut (impl PuffinWriter + Send),
        blob_key: &str,
        put_options: PutOptions,
    ) -> Result<u64> {
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
                HashMap::from([(property_key, property_value)])
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

/// Reads the fulltext config from the blob metadata.
pub fn read_fulltext_config(blob_metadata: &BlobMetadata) -> Result<Config> {
    let property_value =
        blob_metadata
            .properties
            .get(KEY_FULLTEXT_CONFIG)
            .context(PropertyNotFoundSnafu {
                property_name: KEY_FULLTEXT_CONFIG,
            })?;
    serde_json::from_str(property_value).context(SerializeToJsonSnafu)
}
