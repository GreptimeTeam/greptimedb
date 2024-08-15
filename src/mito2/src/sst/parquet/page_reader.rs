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

use std::sync::Arc;

use parquet::column::page::{Page, PageMetadata, PageReader};
use parquet::errors::Result;
use parquet::file::reader::SerializedPageReader;
use store_api::storage::RegionId;

use crate::cache::{CacheManagerRef, PageKey, PageValue};
use crate::sst::file::FileId;
use crate::sst::parquet::row_group::ColumnChunkData;

/// A reader that reads pages from the cache.
pub(crate) struct CachedPageReader {
    /// Page cache.
    cache: CacheManagerRef,
    /// Reader to fall back. `None` indicates the reader is exhausted.
    reader: Option<SerializedPageReader<ColumnChunkData>>,
    region_id: RegionId,
    file_id: FileId,
    row_group_idx: usize,
    column_idx: usize,
    /// Current page index.
    current_page_idx: usize,
}

impl CachedPageReader {
    /// Returns a new reader from a cache and a reader.
    pub(crate) fn new(
        cache: CacheManagerRef,
        reader: SerializedPageReader<ColumnChunkData>,
        region_id: RegionId,
        file_id: FileId,
        row_group_idx: usize,
        column_idx: usize,
    ) -> Self {
        Self {
            cache,
            reader: Some(reader),
            region_id,
            file_id,
            row_group_idx,
            column_idx,
            current_page_idx: 0,
        }
    }
}

impl PageReader for CachedPageReader {
    fn get_next_page(&mut self) -> Result<Option<Page>> {
        let Some(reader) = self.reader.as_mut() else {
            // The reader is exhausted.
            return Ok(None);
        };

        // Tries to get it from the cache first.
        let key = PageKey::Uncompressed {
            region_id: self.region_id,
            file_id: self.file_id,
            row_group_idx: self.row_group_idx,
            column_idx: self.column_idx,
            page_idx: self.current_page_idx,
        };
        if let Some(page) = self.cache.get_pages(&key) {
            // Cache hit.
            // Bumps the page index.
            self.current_page_idx += 1;
            // The reader skips this page.
            reader.skip_next_page()?;
            debug_assert!(page.uncompressed.is_some());
            return Ok(page.uncompressed.clone());
        }

        // Cache miss, load the page from the reader.
        let Some(page) = reader.get_next_page()? else {
            // The reader is exhausted.
            self.reader = None;
            return Ok(None);
        };
        // Puts the page into the cache.
        self.cache
            .put_pages(key, Arc::new(PageValue::new_uncompressed(page.clone())));
        // Bumps the page index.
        self.current_page_idx += 1;

        Ok(Some(page))
    }

    fn peek_next_page(&mut self) -> Result<Option<PageMetadata>> {
        // The reader is exhausted.
        let Some(reader) = self.reader.as_mut() else {
            return Ok(None);
        };
        // It only decodes the page header so we don't query the cache.
        reader.peek_next_page()
    }

    fn skip_next_page(&mut self) -> Result<()> {
        // The reader is exhausted.
        let Some(reader) = self.reader.as_mut() else {
            return Ok(());
        };
        // When the `SerializedPageReader` is in `SerializedPageReaderState::Pages` state, it never pops
        // the dictionary page. So it always return the dictionary page as the first page. See:
        // https://github.com/apache/arrow-rs/blob/1d6feeacebb8d0d659d493b783ba381940973745/parquet/src/file/serialized_reader.rs#L766-L770
        // But the `GenericColumnReader` will read the dictionary page before skipping records so it won't skip dictionary page.
        // So we don't need to handle the dictionary page specifically in this method.
        // https://github.com/apache/arrow-rs/blob/65f7be856099d389b0d0eafa9be47fad25215ee6/parquet/src/column/reader.rs#L322-L331
        self.current_page_idx += 1;
        reader.skip_next_page()
    }
}

impl Iterator for CachedPageReader {
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
