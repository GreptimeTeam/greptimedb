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

//! Fixed-size byte page layout for immutable cached content.

use std::io;
use std::num::NonZeroU64;
use std::ops::Range;

/// Fixed-size logical byte pages within one immutable file.
#[derive(Debug, Clone, Copy)]
pub struct PageLayout {
    page_size: NonZeroU64,
    file_size: u64,
}

impl PageLayout {
    /// Creates a layout. The final page is clipped to the file size.
    pub fn new(page_size: NonZeroU64, file_size: u64) -> Self {
        Self {
            page_size,
            file_size,
        }
    }

    /// Returns the half-open page-ID range intersecting a byte request.
    ///
    /// Requests are clipped at EOF. Empty or wholly out-of-file requests return
    /// an empty range; reversed ranges are rejected. This does not allocate one
    /// entry per page, even for large files.
    pub fn page_ids(&self, request: Range<u64>) -> io::Result<Range<u64>> {
        validate_range(&request)?;
        let start = request.start.min(self.file_size);
        let end = request.end.min(self.file_size);
        let first = start / self.page_size.get();
        if start == end {
            return Ok(first..first);
        }
        // end > 0 here. Unlike (end + page_size - 1) / page_size,
        // this expression cannot overflow at the end of a large file.
        let last = (end - 1) / self.page_size.get();
        Ok(first..last + 1)
    }

    /// Returns a page's file byte range, or `None` for a page outside the file.
    pub fn page_range(&self, page_id: u64) -> Option<Range<u64>> {
        let start = page_id.checked_mul(self.page_size.get())?;
        if start >= self.file_size {
            return None;
        }
        let len = self.page_size.get().min(self.file_size - start);
        Some(start..start + len)
    }

    /// Returns the requested slice relative to a page's buffer.
    ///
    /// Returns `None` if the page and request do not intersect. The caller can
    /// borrow this slice for an immediate response; caching a slice of a much
    /// larger I/O buffer may retain that entire allocation and needs a copy.
    pub fn request_slice(
        &self,
        page_id: u64,
        request: Range<u64>,
    ) -> io::Result<Option<Range<usize>>> {
        validate_range(&request)?;
        let Some(page) = self.page_range(page_id) else {
            return Ok(None);
        };
        let start = request.start.max(page.start);
        let end = request.end.min(page.end);
        if start >= end {
            return Ok(None);
        }
        Ok(Some(
            to_usize(start - page.start)?..to_usize(end - page.start)?,
        ))
    }
}

fn validate_range(range: &Range<u64>) -> io::Result<()> {
    if range.start > range.end {
        return Err(invalid_input("Byte range start exceeds end"));
    }
    Ok(())
}

fn to_usize(value: u64) -> io::Result<usize> {
    usize::try_from(value).map_err(|_| invalid_input("Byte offset exceeds addressable memory"))
}

fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nonzero(value: u64) -> NonZeroU64 {
        NonZeroU64::new(value).unwrap()
    }

    #[test]
    fn test_page_alignment_and_eof() {
        let layout = PageLayout::new(nonzero(64), 150);
        for (request, expected) in [
            (0..0, 0..0),
            (0..64, 0..1),
            (32..96, 0..2),
            (64..128, 1..2),
            (128..200, 2..3),
            (150..200, 2..2),
        ] {
            assert_eq!(layout.page_ids(request).unwrap(), expected);
        }
        assert_eq!(layout.page_range(2), Some(128..150));
        assert_eq!(layout.page_range(3), None);
        assert_eq!(layout.request_slice(0, 32..96).unwrap(), Some(32..64));
        assert_eq!(layout.request_slice(1, 32..96).unwrap(), Some(0..32));
        assert_eq!(layout.request_slice(2, 128..200).unwrap(), Some(0..22));
        assert_eq!(layout.request_slice(1, 0..64).unwrap(), None);
        assert_eq!(PageLayout::new(nonzero(64), 0).page_range(0), None);
        assert!(layout.page_ids(Range { start: 2, end: 1 }).is_err());
        assert!(layout.request_slice(0, Range { start: 2, end: 1 }).is_err());
    }

    #[test]
    fn test_large_file_boundaries() {
        let layout = PageLayout::new(nonzero(1), u64::MAX);
        assert_eq!(layout.page_ids(0..u64::MAX).unwrap(), 0..u64::MAX);
        assert_eq!(
            layout.page_range(u64::MAX - 1),
            Some(u64::MAX - 1..u64::MAX)
        );
        assert_eq!(layout.page_range(u64::MAX), None);
        let layout = PageLayout::new(nonzero(64), u64::MAX);
        assert_eq!(layout.page_range(u64::MAX), None);
        let last = u64::MAX / 64;
        assert_eq!(layout.page_range(last), Some(last * 64..u64::MAX));
        assert_eq!(
            layout.page_ids(u64::MAX - 1..u64::MAX).unwrap(),
            last..last + 1
        );
    }
}
