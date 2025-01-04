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

mod r#async;

use pin_project::pin_project;

/// `PartialReader` to perform synchronous or asynchronous reads on a portion of a resource.
#[pin_project]
pub struct PartialReader<R> {
    /// offset of the portion in the resource
    offset: u64,

    /// size of the portion in the resource
    size: u64,

    /// Resource for the portion.
    /// The `offset` and `size` fields are used to determine the slice of `source` to read.
    #[pin]
    source: R,
}

impl<R> PartialReader<R> {
    /// Creates a new `PartialReader` for the given resource.
    pub fn new(source: R, offset: u64, size: u64) -> Self {
        Self {
            offset,
            size,
            source,
        }
    }

    /// Returns the size of the portion in portion.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Returns whether the portion is empty.
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn is_empty_returns_true_for_zero_length_blob() {
        let data: Vec<u8> = (0..100).collect();
        let reader = PartialReader::new(Cursor::new(data), 10, 0);
        assert!(reader.is_empty());
        // assert!(reader.is_eof());
    }

    #[test]
    fn is_empty_returns_false_for_non_zero_length_blob() {
        let data: Vec<u8> = (0..100).collect();
        let reader = PartialReader::new(Cursor::new(data), 10, 30);
        assert!(!reader.is_empty());
    }
}
