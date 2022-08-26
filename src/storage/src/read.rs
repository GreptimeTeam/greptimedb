//! Common structs and utilities for read.

use async_trait::async_trait;
use datatypes::vectors::VectorRef;

use crate::error::Result;

/// Storage internal representation of a batch of rows.
// Now the structure of `Batch` is still unstable, all pub fields may be changed.
#[derive(Debug, Default, PartialEq)]
pub struct Batch {
    /// Rows organized in columnar format.
    ///
    /// Columns follow the same order convention of region schema:
    /// key, value, internal columns.
    columns: Vec<VectorRef>,
}

impl Batch {
    pub fn new(columns: Vec<VectorRef>) -> Batch {
        Batch { columns }
    }

    #[inline]
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    #[inline]
    pub fn columns(&self) -> &[VectorRef] {
        &self.columns
    }

    #[inline]
    pub fn column(&self, idx: usize) -> &VectorRef {
        &self.columns[idx]
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
            read_util::build_boxed_vec_reader(&[&[(1, Some(1)), (2, Some(2))], &[(3, None)]]),
            read_util::build_boxed_vec_reader(&[&[(4, None)]]),
            read_util::build_boxed_vec_reader(&[&[(5, Some(5)), (6, Some(6))]]),
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
            read_util::build_boxed_vec_reader(&[&[(1, Some(1)), (2, Some(2))], &[(3, None)]]),
            // Empty reader.
            read_util::build_boxed_vec_reader(&[&[]]),
            read_util::build_boxed_vec_reader(&[&[(5, Some(5)), (6, Some(6))]]),
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
