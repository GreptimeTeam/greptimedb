//! Common structs and utilities for read.

mod merge;

use async_trait::async_trait;
use datatypes::data_type::DataType;
use datatypes::prelude::ConcreteDataType;
use datatypes::vectors::{MutableVector, VectorRef};
use snafu::{ensure, ResultExt};

use crate::error::{self, Result};

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
        assert!(!columns.is_empty());
        let length = columns[0].len();
        assert!(columns.iter().all(|col| col.len() == length));
    }
}

pub struct BatchBuilder {
    builders: Vec<Box<dyn MutableVector>>,
}

impl BatchBuilder {
    /// Create a new `BatchBuilder` from data types with given `capacity`.
    ///
    /// # Panics
    /// Panics if `types` is empty.
    pub fn with_capacity<'a, I>(types: I, capacity: usize) -> BatchBuilder
    where
        I: IntoIterator<Item = &'a ConcreteDataType>,
    {
        let builders: Vec<_> = types
            .into_iter()
            .map(|t| t.create_mutable_vector(capacity))
            .collect();
        assert!(!builders.is_empty());

        BatchBuilder { builders }
    }

    /// Returns number of rows already in this builder.
    #[inline]
    pub fn num_rows(&self) -> usize {
        self.builders[0].len()
    }

    /// Returns true if no rows in this builder.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }

    /// Extend the builder by slice of batch.
    ///
    /// # Panics
    /// Panics if `offset + length > batch.num_rows()`.
    pub fn extend_slice_of(&mut self, batch: &Batch, offset: usize, length: usize) -> Result<()> {
        for (builder, column) in self.builders.iter_mut().zip(batch.columns()) {
            builder
                .extend_slice_of(&**column, offset, length)
                .context(error::PushBatchSnafu)?;
        }

        Ok(())
    }

    /// Push `i-th` row of batch into the builder.
    ///
    /// # Panics
    /// Panics if `i` is out of bound.
    pub fn push_row_of(&mut self, batch: &Batch, i: usize) -> Result<()> {
        for (builder, column) in self.builders.iter_mut().zip(batch.columns()) {
            let value = column.get_ref(i);
            builder
                .push_value_ref(value)
                .context(error::PushBatchSnafu)?;
        }

        Ok(())
    }

    /// Create a new [Batch] and reset this builder.
    pub fn build(&mut self) -> Result<Batch> {
        // Checks length of each builder.
        let rows = self.num_rows();
        for (i, builder) in self.builders.iter().enumerate() {
            ensure!(
                rows == builder.len(),
                error::BuildBatchSnafu {
                    msg: format!(
                        "expect row num {} but builder {} has {}",
                        rows,
                        i,
                        builder.len()
                    ),
                }
            );
        }

        let columns = self.builders.iter_mut().map(|b| b.to_vector()).collect();

        Ok(Batch { columns })
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
