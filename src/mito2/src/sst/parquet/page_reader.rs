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

use parquet::column::page::{Page, PageMetadata, PageReader};
use parquet::errors::Result;

/// A reader that reads all pages from a cache.
pub(crate) struct RowGroupCachedReader {
    /// Cached pages.
    pages: VecDeque<Page>,
}

impl RowGroupCachedReader {
    /// Returns a new reader from pages of a column in a row group.
    pub(crate) fn new(pages: &[Page]) -> Self {
        Self {
            pages: pages.iter().cloned().collect(),
        }
    }
}

impl PageReader for RowGroupCachedReader {
    fn get_next_page(&mut self) -> Result<Option<Page>> {
        Ok(self.pages.pop_front())
    }

    fn peek_next_page(&mut self) -> Result<Option<PageMetadata>> {
        Ok(self.pages.front().map(page_to_page_meta))
    }

    fn skip_next_page(&mut self) -> Result<()> {
        // When the `SerializedPageReader` is in `SerializedPageReaderState::Pages` state, it never pops
        // the dictionary page. So it always return the dictionary page as the first page. See:
        // https://github.com/apache/arrow-rs/blob/1d6feeacebb8d0d659d493b783ba381940973745/parquet/src/file/serialized_reader.rs#L766-L770
        // But the `GenericColumnReader` will read the dictionary page before skipping records so it won't skip dictionary page.
        // So we don't need to handle the dictionary page specifically in this method.
        // https://github.com/apache/arrow-rs/blob/65f7be856099d389b0d0eafa9be47fad25215ee6/parquet/src/column/reader.rs#L322-L331
        self.pages.pop_front();
        Ok(())
    }
}

impl Iterator for RowGroupCachedReader {
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
