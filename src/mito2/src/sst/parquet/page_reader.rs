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

//! Parquet page reader.

use std::collections::VecDeque;
use std::sync::Arc;

use parquet::column::page::{Page, PageMetadata, PageReader};
use parquet::errors::Result;

use crate::cache::{CacheManagerRef, PageKey, PageValue};

/// A reader that can cache pages from the underlying reader.
pub(crate) enum CachedPageReader<T> {
    /// Reads from the underlying reader.
    Reader {
        /// Inner page reader to get pages.
        page_reader: T,
    },
    /// Reads from cached pages.
    Pages {
        /// Cached pages.
        pages: VecDeque<Page>,
    },
}

impl<T: PageReader> CachedPageReader<T> {
    /// Returns a new reader that caches all pages for the `page_reader` if the
    /// `cache_manager` is `Some`.
    pub(crate) fn new(
        page_reader: T,
        cache_manager: Option<CacheManagerRef>,
        page_key: PageKey,
    ) -> Result<Self> {
        let Some(cache) = &cache_manager else {
            return Ok(CachedPageReader::Reader { page_reader });
        };

        if let Some(page_value) = cache.get_pages(&page_key) {
            // We already cached all pages.
            return Ok(CachedPageReader::from_pages(&page_value.pages));
        }

        // Cache miss. We load pages from the reader.
        let pages = page_reader.collect::<Result<Vec<_>>>()?;
        let page_value = Arc::new(PageValue::new(pages));
        // Puts into the cache.
        cache.put_pages(page_key, page_value.clone());

        Ok(CachedPageReader::from_pages(&page_value.pages))
    }

    /// Returns a new reader from existing pages.
    fn from_pages(pages: &[Page]) -> Self {
        Self::Pages {
            pages: pages.iter().cloned().collect(),
        }
    }
}

impl<T: PageReader> PageReader for CachedPageReader<T> {
    fn get_next_page(&mut self) -> Result<Option<Page>> {
        match self {
            CachedPageReader::Reader { page_reader } => page_reader.get_next_page(),
            CachedPageReader::Pages { pages } => Ok(pages.pop_front()),
        }
    }

    fn peek_next_page(&mut self) -> Result<Option<PageMetadata>> {
        match self {
            CachedPageReader::Reader { page_reader } => page_reader.peek_next_page(),
            CachedPageReader::Pages { pages } => Ok(pages.front().map(page_to_page_meta)),
        }
    }

    fn skip_next_page(&mut self) -> Result<()> {
        match self {
            CachedPageReader::Reader { page_reader } => page_reader.skip_next_page(),
            CachedPageReader::Pages { pages } => {
                // When the `SerializedPageReader` is in `SerializedPageReaderState::Pages` state, it never pops
                // the dictionary page, which is always the first page. See:
                // https://github.com/apache/arrow-rs/blob/1d6feeacebb8d0d659d493b783ba381940973745/parquet/src/file/serialized_reader.rs#L766-L770
                // But the `GenericColumnReader` will read the dictionary page before skipping records so it ensures the
                // dictionary page is read first.
                // https://github.com/apache/arrow-rs/blob/65f7be856099d389b0d0eafa9be47fad25215ee6/parquet/src/column/reader.rs#L322-L331
                pages.pop_front();
                Ok(())
            }
        }
    }
}

impl<T: PageReader> Iterator for CachedPageReader<T> {
    type Item = Result<Page>;

    fn next(&mut self) -> Option<Self::Item> {
        self.get_next_page().transpose()
    }
}

/// Get [PageMetadata] from `page`.
///
/// The conversion is based on [decode_page()](https://github.com/apache/arrow-rs/blob/1d6feeacebb8d0d659d493b783ba381940973745/parquet/src/file/serialized_reader.rs#L438-L481)
/// and [PageMetadata](https://github.com/apache/arrow-rs/blob/65f7be856099d389b0d0eafa9be47fad25215ee6/parquet/src/column/page.rs#L279-L301).
fn page_to_page_meta(page: &Page) -> PageMetadata {
    match page {
        Page::DataPage { num_values, .. } => PageMetadata {
            num_rows: None,
            num_levels: Some(*num_values as usize),
            is_dict: false,
        },
        Page::DataPageV2 {
            num_values,
            num_rows,
            ..
        } => PageMetadata {
            num_rows: Some(*num_rows as usize),
            num_levels: Some(*num_values as usize),
            is_dict: false,
        },
        Page::DictionaryPage { .. } => PageMetadata {
            num_rows: None,
            num_levels: None,
            is_dict: true,
        },
    }
}
