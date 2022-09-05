//! Common structs and utilities for read.

mod merge;

use async_trait::async_trait;
use datatypes::vectors::{MutableVector, VectorRef};

use crate::error::Result;

/// Storage internal representation of a batch of rows.
///
/// `Batch` must contain at least one column, but might not hold any row.
// Now the structure of `Batch` is still unstable, all pub fields may be changed.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Batch {
    /// Rows organized in columnar format.
    ///
    /// Columns follow the same order convention of region schema:
    /// key, value, internal columns.
    columns: Vec<VectorRef>,
}

impl Batch {
    /// Create a new `Batch` from `columns`.
    ///
    /// # Panics
    /// Panics if
    /// - `columns` is empty.
    /// - vectors in `columns` have different length.
    pub fn new(columns: Vec<VectorRef>) -> Batch {
        Self::assert_columns(&columns);

        Batch { columns }
    }

    #[inline]
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    #[inline]
    pub fn num_rows(&self) -> usize {
        // The invariant of `Batch::new()` ensure columns isn't empty.
        self.columns[0].len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }

    #[inline]
    pub fn columns(&self) -> &[VectorRef] {
        &self.columns
    }

    #[inline]
    pub fn column(&self, idx: usize) -> &VectorRef {
        &self.columns[idx]
    }

    /// Slice the batch, returning a new batch.
    ///
    /// # Panics
    /// Panics if `offset + length > self.num_rows()`.
    fn slice(&self, offset: usize, length: usize) -> Batch {
        let columns = self
            .columns
            .iter()
            .map(|v| v.slice(offset, length))
            .collect();
        Batch { columns }
    }

    fn assert_columns(columns: &[VectorRef]) {
        assert!(columns.is_empty());
        let length = columns[0].len();
        assert!(columns.iter().all(|col| col.len() == length));
    }
}

struct BatchBuilder {
    builders: Vec<Box<dyn MutableVector>>,
}

impl BatchBuilder {
    // FIXME(yingwen): create BatchBuilder from data types.
    fn new() -> BatchBuilder {
        BatchBuilder {
            builders: Vec::new(),
        }
    }

    /// Returns number of rows already in this builder.
    fn num_rows(&self) -> usize {
        unimplemented!()
    }

    /// Returns true if no rows in this builder.
    fn is_empty(&self) -> bool {
        unimplemented!()
    }

    /// Push slice of batch into the builder.
    ///
    /// # Panics
    /// Panics if `offset + length > batch.num_rows()`.
    fn extend_slice_of(&mut self, batch: &Batch, offset: usize, length: usize) -> Result<()> {
        unimplemented!()
    }

    /// Push `i-th` row of batch into the builder.
    ///
    /// # Panics
    /// Panics if `i` is out of bound.
    fn push_row_of(&mut self, batch: &Batch, i: usize) -> Result<()> {
        unimplemented!()
    }

    /// Create a new [Batch] and reset this builder.
    fn build(&mut self) -> Result<Batch> {
        unimplemented!()
    }
}

/// Async batch reader.
#[async_trait]
pub trait BatchReader: Send {
    // TODO(yingwen): Schema of batch.

    /// Fetch next [Batch].
    ///
    /// Returns `Ok(None)` when the reader has reached its end and calling `next_batch()`
    /// again won't return batch again.
    ///
    /// If `Err` is returned, caller should not call this method again, the implementor
    /// may or may not panic in such case.
    async fn next_batch(&mut self) -> Result<Option<Batch>>;
}

/// Pointer to [BatchReader].
pub type BoxedBatchReader = Box<dyn BatchReader>;

/// Concat reader inputs.
pub struct ConcatReader {
    readers: Vec<BoxedBatchReader>,
    curr_idx: usize,
}

impl ConcatReader {
    pub fn new(readers: Vec<BoxedBatchReader>) -> ConcatReader {
        ConcatReader {
            readers,
            curr_idx: 0,
        }
    }
}

#[async_trait]
impl BatchReader for ConcatReader {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        loop {
            if self.curr_idx >= self.readers.len() {
                return Ok(None);
            }

            let reader = &mut self.readers[self.curr_idx];
            match reader.next_batch().await? {
                Some(batch) => return Ok(Some(batch)),
                None => self.curr_idx += 1,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read::BatchReader;
    use crate::test_util::read_util;

    #[tokio::test]
    async fn test_concat_reader_empty() {
        let mut reader = ConcatReader::new(Vec::new());

        assert!(reader.next_batch().await.unwrap().is_none());
        // Call next_batch() again is allowed.
        assert!(reader.next_batch().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_concat_multiple_readers() {
        let readers = vec![
            read_util::build_boxed_reader(&[&[(1, Some(1)), (2, Some(2))], &[(3, None)]]),
            read_util::build_boxed_reader(&[&[(4, None)]]),
            read_util::build_boxed_reader(&[&[(5, Some(5)), (6, Some(6))]]),
        ];

        let mut reader = ConcatReader::new(readers);

        read_util::check_reader_with_kv_batch(
            &mut reader,
            &[
                &[(1, Some(1)), (2, Some(2))],
                &[(3, None)],
                &[(4, None)],
                &[(5, Some(5)), (6, Some(6))],
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_concat_reader_with_empty_reader() {
        let readers = vec![
            read_util::build_boxed_reader(&[&[(1, Some(1)), (2, Some(2))], &[(3, None)]]),
            // Empty reader.
            read_util::build_boxed_reader(&[&[]]),
            read_util::build_boxed_reader(&[&[(5, Some(5)), (6, Some(6))]]),
        ];

        let mut reader = ConcatReader::new(readers);

        read_util::check_reader_with_kv_batch(
            &mut reader,
            &[
                &[(1, Some(1)), (2, Some(2))],
                &[(3, None)],
                &[(5, Some(5)), (6, Some(6))],
            ],
        )
        .await;
    }
}
