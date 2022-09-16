use crate::schema::ProjectedSchemaRef;
use crate::read::{Batch, BatchReader};
use async_trait::async_trait;
use crate::error::Result;

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
}

#[async_trait]
impl<R: BatchReader> BatchReader for DedupReader<R> {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        unimplemented!()
    }
}
