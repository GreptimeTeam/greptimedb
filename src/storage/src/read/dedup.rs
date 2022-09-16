use async_trait::async_trait;
use datatypes::arrow::bitmap::MutableBitmap;
use datatypes::vectors::BooleanVector;

use crate::error::Result;
use crate::read::{Batch, BatchOp, BatchReader};
use crate::schema::ProjectedSchemaRef;

/// A reader that dedup rows from inner reader.
struct DedupReader<R> {
    /// Projected schema to read.
    schema: ProjectedSchemaRef,
    /// The inner reader.
    reader: R,
    /// Previous batch from the reader.
    prev_batch: Option<Batch>,
}

impl<R> DedupReader<R> {
    fn new(schema: ProjectedSchemaRef, reader: R) -> DedupReader<R> {
        DedupReader {
            schema,
            reader,
            prev_batch: None,
        }
    }

    /// Take `batch` and then returns a new batch with no duplicated rows.
    fn dedup_batch(&mut self, batch: Batch) -> Result<Batch> {
        if batch.is_empty() {
            // No need to update `prev_batch` if current batch is empty.
            return Ok(batch);
        }

        // The `arrow` filter needs `BooleanArray` as input so there is no convenient
        // and efficient way to reuse the bitmap. Though we could use `MutableBooleanArray`,
        // but we couldn't zero all bits in the mutable array easily.
        let mut selected = MutableBitmap::from_len_zeroed(batch.num_rows());
        self.schema
            .dedup(&batch, &mut selected, self.prev_batch.as_ref());

        // Store current batch to `prev_batch` so we could compare the next batch
        // with this batch. Use `clone_from` to reuse allocated memory if possible.
        self.prev_batch
            .get_or_insert_with(Batch::default)
            .clone_from(&batch);

        let filter = BooleanVector::from(selected);
        // Filter duplicate rows.
        self.schema.filter(&batch, &filter)
    }
}

#[async_trait]
impl<R: BatchReader> BatchReader for DedupReader<R> {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        self.reader
            .next_batch()
            .await?
            .map(|batch| self.dedup_batch(batch))
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use store_api::storage::OpType;

    use super::*;
    use crate::test_util::read_util;

    #[tokio::test]
    async fn test_dedup_reader_empty() {
        let schema = read_util::new_projected_schema();
        let reader = read_util::build_vec_reader(&[]);
        let mut reader = DedupReader::new(schema, reader);

        assert!(reader.next_batch().await.unwrap().is_none());
        // Call next_batch() again is allowed.
        assert!(reader.next_batch().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_dedup_by_sequence() {
        let schema = read_util::new_projected_schema();
        let reader = read_util::build_full_vec_reader(&[
            // key, value, sequence, op_type
            &[
                (100, 1, 1000, OpType::Put),
                (100, 2, 999, OpType::Put),
                (100, 3, 998, OpType::Put),
                (101, 1, 1000, OpType::Put),
            ],
            &[
                (101, 2, 999, OpType::Put),
                (102, 12, 1000, OpType::Put),
                (103, 13, 1000, OpType::Put),
            ],
            &[(103, 2, 999, OpType::Put)],
        ]);
        let mut reader = DedupReader::new(schema, reader);

        let result = read_util::collect_kv_batch(&mut reader).await;
        let expect = [
            (100, Some(1)),
            (101, Some(1)),
            (102, Some(12)),
            (103, Some(13)),
        ];
        assert_eq!(&expect, &result[..]);
    }

    #[tokio::test]
    async fn test_dedup_contains_empty() {
        let schema = read_util::new_projected_schema();
        let reader = read_util::build_full_vec_reader(&[
            // key, value, sequence, op_type
            &[
                (100, 1, 1000, OpType::Put),
                (100, 2, 999, OpType::Put),
                (101, 1, 1000, OpType::Put),
            ],
            &[],
            &[(101, 2, 999, OpType::Put), (102, 12, 1000, OpType::Put)],
        ]);
        let mut reader = DedupReader::new(schema, reader);

        let result = read_util::collect_kv_batch(&mut reader).await;
        let expect = [(100, Some(1)), (101, Some(1)), (102, Some(12))];
        assert_eq!(&expect, &result[..]);
    }
}
