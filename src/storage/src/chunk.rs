use std::sync::Arc;

use async_trait::async_trait;
use snafu::ResultExt;
use store_api::storage::{Chunk, ChunkReader, SchemaRef};

use crate::error::{self, Error, Result};
use crate::memtable::{BoxedBatchIterator, IterContext, MemtableSet};
use crate::read::{Batch, BatchReader, ConcatReader};
use crate::schema::{ProjectedSchema, ProjectedSchemaRef, RegionSchemaRef};
use crate::sst::{AccessLayerRef, FileHandle, LevelMetas, ReadOptions, Visitor};

type BoxedIterator = Box<dyn Iterator<Item = Result<Batch>> + Send>;

/// Chunk reader implementation.
// Now we use async-trait to implement the chunk reader, which is easier to implement than
// using `Stream`, maybe change to `Stream` if we find out it is more efficient and have
// necessary to do so.
pub struct ChunkReaderImpl {
    schema: ProjectedSchemaRef,
    iter: Option<BoxedIterator>,
    sst_reader: ConcatReader,
}

#[async_trait]
impl ChunkReader for ChunkReaderImpl {
    type Error = Error;

    fn schema(&self) -> &SchemaRef {
        // FIXME(yingwen): Schema after projection.
        self.schema.user_schema()
    }

    async fn next_chunk(&mut self) -> Result<Option<Chunk>> {
        let batch = match self.fetch_batch().await? {
            Some(b) => b,
            None => return Ok(None),
        };

        // TODO(yingwen): [projection] Use schema to convert batch into chunk.
        let chunk = batch_to_chunk(batch);

        Ok(Some(chunk))
    }
}

impl ChunkReaderImpl {
    pub fn new(
        schema: ProjectedSchemaRef,
        iter: BoxedIterator,
        sst_reader: ConcatReader,
    ) -> ChunkReaderImpl {
        ChunkReaderImpl {
            schema,
            iter: Some(iter),
            sst_reader,
        }
    }

    async fn fetch_batch(&mut self) -> Result<Option<Batch>> {
        if let Some(iter) = &mut self.iter {
            match iter.next() {
                Some(b) => return Ok(Some(b?)),
                None => self.iter = None,
            }
        }

        self.sst_reader.next_batch().await
    }
}

// Assumes the schema is the same as key columns combine with value columns.
fn batch_to_chunk(mut batch: Batch) -> Chunk {
    let mut columns = Vec::with_capacity(batch.keys.len() + batch.values.len());
    columns.append(&mut batch.keys);
    columns.append(&mut batch.values);

    Chunk::new(columns)
}

/// Builder to create a new [ChunkReaderImpl] from scan request.
pub struct ChunkReaderBuilder {
    schema: RegionSchemaRef,
    projection: Option<Vec<usize>>,
    sst_layer: AccessLayerRef,
    iter_ctx: IterContext,
    iters: Vec<BoxedBatchIterator>,
    files_to_read: Vec<FileHandle>,
}

impl ChunkReaderBuilder {
    pub fn new(schema: RegionSchemaRef, sst_layer: AccessLayerRef) -> Self {
        ChunkReaderBuilder {
            schema,
            projection: None,
            sst_layer,
            iter_ctx: IterContext::default(),
            iters: Vec::new(),
            files_to_read: Vec::new(),
        }
    }

    /// Reserve space for iterating `num` memtables.
    pub fn reserve_num_memtables(mut self, num: usize) -> Self {
        self.iters.reserve(num);
        self
    }

    pub fn projection(mut self, projection: Option<Vec<usize>>) -> Self {
        self.projection = projection;
        self
    }

    pub fn iter_ctx(mut self, iter_ctx: IterContext) -> Self {
        self.iter_ctx = iter_ctx;
        self
    }

    pub fn pick_memtables(mut self, memtables: &MemtableSet) -> Result<Self> {
        for (_range, mem) in memtables.iter() {
            let iter = mem.iter(&self.iter_ctx)?;

            self.iters.push(iter);
        }

        Ok(self)
    }

    pub fn pick_ssts(mut self, ssts: &LevelMetas) -> Result<Self> {
        ssts.visit_levels(&mut self)?;

        Ok(self)
    }

    pub async fn build(self) -> Result<ChunkReaderImpl> {
        let schema = Arc::new(
            ProjectedSchema::new(self.schema, self.projection)
                .context(error::InvalidProjectionSnafu)?,
        );

        // Now we just simply chain all iterators together, ignore duplications/ordering.
        let iter = Box::new(self.iters.into_iter().flatten());

        let read_opts = ReadOptions {
            batch_size: self.iter_ctx.batch_size,
        };
        let mut sst_readers = Vec::with_capacity(self.files_to_read.len());
        for file in &self.files_to_read {
            let reader = self
                .sst_layer
                .read_sst(file.file_name(), schema.clone(), &read_opts)
                .await?;

            sst_readers.push(reader);
        }
        let reader = ConcatReader::new(sst_readers);

        Ok(ChunkReaderImpl::new(schema, iter, reader))
    }
}

impl Visitor for ChunkReaderBuilder {
    fn visit(&mut self, _level: usize, files: &[FileHandle]) -> Result<()> {
        // TODO(yingwen): Filter files by time range.

        // Now we read all files, so just reserve enough space to hold all files.
        self.files_to_read.reserve(files.len());
        for file in files {
            // We can't invoke async functions here, so we collects all files first, and
            // create the batch reader later in `ChunkReaderBuilder`.
            self.files_to_read.push(file.clone());
        }

        Ok(())
    }
}
