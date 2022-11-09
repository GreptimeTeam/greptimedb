use std::sync::Arc;

use async_trait::async_trait;
use common_query::logical_plan::Expr;
use snafu::ResultExt;
use store_api::storage::{Chunk, ChunkReader, SchemaRef, SequenceNumber};
use table::predicate::Predicate;

use crate::error::{self, Error, Result};
use crate::memtable::{IterContext, MemtableRef, MemtableSet};
use crate::read::{BoxedBatchReader, DedupReader, MergeReaderBuilder};
use crate::schema::{ProjectedSchema, ProjectedSchemaRef, RegionSchemaRef};
use crate::sst::{AccessLayerRef, FileHandle, LevelMetas, ReadOptions, Visitor};

/// Chunk reader implementation.
// Now we use async-trait to implement the chunk reader, which is easier to implement than
// using `Stream`, maybe change to `Stream` if we find out it is more efficient and have
// necessary to do so.
pub struct ChunkReaderImpl {
    schema: ProjectedSchemaRef,
    batch_reader: BoxedBatchReader,
}

#[async_trait]
impl ChunkReader for ChunkReaderImpl {
    type Error = Error;

    fn schema(&self) -> &SchemaRef {
        self.schema.projected_user_schema()
    }

    async fn next_chunk(&mut self) -> Result<Option<Chunk>> {
        let batch = match self.batch_reader.next_batch().await? {
            Some(b) => b,
            None => return Ok(None),
        };

        let chunk = self.schema.batch_to_chunk(&batch);

        Ok(Some(chunk))
    }
}

impl ChunkReaderImpl {
    pub fn new(schema: ProjectedSchemaRef, batch_reader: BoxedBatchReader) -> ChunkReaderImpl {
        ChunkReaderImpl {
            schema,
            batch_reader,
        }
    }
}

/// Builder to create a new [ChunkReaderImpl] from scan request.
pub struct ChunkReaderBuilder {
    schema: RegionSchemaRef,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    sst_layer: AccessLayerRef,
    iter_ctx: IterContext,
    memtables: Vec<MemtableRef>,
    files_to_read: Vec<FileHandle>,
}

impl ChunkReaderBuilder {
    pub fn new(schema: RegionSchemaRef, sst_layer: AccessLayerRef) -> Self {
        ChunkReaderBuilder {
            schema,
            projection: None,
            filters: vec![],
            sst_layer,
            iter_ctx: IterContext::default(),
            memtables: Vec::new(),
            files_to_read: Vec::new(),
        }
    }

    /// Reserve space for iterating `num` memtables.
    pub fn reserve_num_memtables(mut self, num: usize) -> Self {
        self.memtables.reserve(num);
        self
    }

    pub fn projection(mut self, projection: Option<Vec<usize>>) -> Self {
        self.projection = projection;
        self
    }

    pub fn filters(mut self, filters: Vec<Expr>) -> Self {
        self.filters = filters;
        self
    }

    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.iter_ctx.batch_size = batch_size;
        self
    }

    pub fn visible_sequence(mut self, sequence: SequenceNumber) -> Self {
        self.iter_ctx.visible_sequence = sequence;
        self
    }

    pub fn pick_memtables(mut self, memtables: MemtableRef) -> Self {
        self.memtables.push(memtables);
        self
    }

    pub fn pick_ssts(mut self, ssts: &LevelMetas) -> Result<Self> {
        ssts.visit_levels(&mut self)?;

        Ok(self)
    }

    pub async fn build(mut self) -> Result<ChunkReaderImpl> {
        let schema = Arc::new(
            ProjectedSchema::new(self.schema, self.projection)
                .context(error::InvalidProjectionSnafu)?,
        );

        let num_sources = self.memtables.len() + self.files_to_read.len();
        let mut reader_builder = MergeReaderBuilder::with_capacity(schema.clone(), num_sources)
            .batch_size(self.iter_ctx.batch_size);

        self.iter_ctx.projected_schema = Some(schema.clone());
        for mem in self.memtables {
            let iter = mem.iter(&self.iter_ctx)?;
            reader_builder = reader_builder.push_batch_iter(iter);
        }

        let read_opts = ReadOptions {
            batch_size: self.iter_ctx.batch_size,
            projected_schema: schema.clone(),
            predicate: Predicate::new(self.filters),
        };
        for file in &self.files_to_read {
            let reader = self
                .sst_layer
                .read_sst(file.file_name(), &read_opts)
                .await?;

            reader_builder = reader_builder.push_batch_reader(reader);
        }

        let reader = reader_builder.build();
        let reader = DedupReader::new(schema.clone(), reader);

        Ok(ChunkReaderImpl::new(schema, Box::new(reader)))
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
