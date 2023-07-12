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

//! A mock storage engine for table test purpose.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use common_error::mock::MockError;
use common_telemetry::logging;
use datatypes::prelude::{DataType, Value, VectorRef};
use datatypes::schema::{ColumnSchema, Schema};
use storage::metadata::{RegionMetaImpl, RegionMetadata};
use storage::write_batch::WriteBatch;
use store_api::storage::{
    AlterRequest, Chunk, ChunkReader, CloseOptions, CompactContext, CreateOptions, EngineContext,
    FlushContext, GetRequest, GetResponse, OpenOptions, ReadContext, Region, RegionDescriptor,
    RegionId, ScanRequest, ScanResponse, SchemaRef, Snapshot, StorageEngine, WriteContext,
    WriteResponse,
};

pub type Result<T> = std::result::Result<T, MockError>;

pub struct MockChunkReader {
    schema: SchemaRef,
    memtable: MockMemtable,
    read: bool,
}

#[async_trait]
impl ChunkReader for MockChunkReader {
    type Error = MockError;

    fn user_schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn next_chunk(&mut self) -> Result<Option<Chunk>> {
        if self.read {
            return Ok(None);
        }

        let columns = self
            .schema
            .column_schemas()
            .iter()
            .map(|column_schema| {
                let data = self.memtable.get(&column_schema.name).unwrap();
                let mut builder = column_schema.data_type.create_mutable_vector(data.len());
                for v in data {
                    builder.push_value_ref(v.as_value_ref());
                }
                builder.to_vector()
            })
            .collect::<Vec<VectorRef>>();
        self.read = true;

        Ok(Some(Chunk::new(columns)))
    }

    fn project_chunk(&self, chunk: Chunk) -> Chunk {
        chunk
    }
}

pub struct MockSnapshot {
    schema: SchemaRef,
    region: Arc<MockRegionInner>,
}

#[async_trait]
impl Snapshot for MockSnapshot {
    type Error = MockError;
    type Reader = MockChunkReader;

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn scan(
        &self,
        _ctx: &ReadContext,
        request: ScanRequest,
    ) -> Result<ScanResponse<MockChunkReader>> {
        let memtable = {
            let memtable = self.region.memtable.read().unwrap();
            memtable.clone()
        };

        let schema = self.schema();
        let projection_schema = if let Some(projection) = request.projection {
            let mut columns = Vec::with_capacity(projection.len());
            for idx in projection {
                columns.push(
                    schema
                        .column_schema_by_name(schema.column_name_by_index(idx))
                        .unwrap()
                        .clone(),
                );
            }
            Arc::new(Schema::new(columns))
        } else {
            schema.clone()
        };

        let reader = MockChunkReader {
            schema: projection_schema,
            memtable,
            read: false,
        };
        Ok(ScanResponse { reader })
    }

    async fn get(&self, _ctx: &ReadContext, _request: GetRequest) -> Result<GetResponse> {
        Ok(GetResponse {})
    }
}

// Clones a MockRegion is not cheap as we need to clone the string name, but for test
// purpose the cost should be acceptable.
#[derive(Debug, Clone)]
pub struct MockRegion {
    pub inner: Arc<MockRegionInner>,
}

#[derive(Debug)]
pub struct MockRegionInner {
    name: String,
    pub metadata: ArcSwap<RegionMetadata>,
    memtable: Arc<RwLock<MockMemtable>>,
}

/// A columnar memtable, maps column name to data of that column in each row.
type MockMemtable = HashMap<String, Vec<Value>>;

#[async_trait]
impl Region for MockRegion {
    type Error = MockError;
    type Meta = RegionMetaImpl;
    type WriteRequest = WriteBatch;
    type Snapshot = MockSnapshot;

    fn id(&self) -> RegionId {
        self.inner.metadata.load().id()
    }

    fn name(&self) -> &str {
        &self.inner.name
    }

    fn in_memory_metadata(&self) -> RegionMetaImpl {
        RegionMetaImpl::new(self.inner.metadata.load().clone())
    }

    async fn write(&self, _ctx: &WriteContext, request: WriteBatch) -> Result<WriteResponse> {
        self.inner.write(request);
        Ok(WriteResponse {})
    }

    fn snapshot(&self, _ctx: &ReadContext) -> Result<MockSnapshot> {
        Ok(MockSnapshot {
            schema: self.inner.metadata.load().user_schema().clone(),
            region: self.inner.clone(),
        })
    }

    fn write_request(&self) -> WriteBatch {
        let metadata = self.inner.metadata.load();
        let user_schema = metadata.user_schema().clone();
        let row_key_end = metadata.schema().store_schema().row_key_end();

        WriteBatch::new(user_schema, row_key_end)
    }

    async fn alter(&self, request: AlterRequest) -> Result<()> {
        let current = self.inner.metadata.load();
        // Mock engine just panic if failed to create a new metadata.
        let metadata = current.alter(&request).unwrap();
        self.inner.update_metadata(metadata);

        Ok(())
    }

    async fn drop_region(&self) -> Result<()> {
        Ok(())
    }

    fn disk_usage_bytes(&self) -> u64 {
        0
    }

    async fn flush(&self, _ctx: &FlushContext) -> Result<()> {
        unimplemented!()
    }

    async fn compact(&self, _ctx: &CompactContext) -> std::result::Result<(), Self::Error> {
        unimplemented!()
    }
}

impl MockRegionInner {
    fn new(metadata: RegionMetadata) -> Self {
        let memtable = metadata
            .user_schema()
            .column_schemas()
            .iter()
            .map(|x| (x.name.clone(), vec![]))
            .collect();
        Self {
            name: metadata.name().to_string(),
            metadata: ArcSwap::new(Arc::new(metadata)),
            memtable: Arc::new(RwLock::new(memtable)),
        }
    }

    fn update_metadata(&self, metadata: RegionMetadata) {
        {
            let mut memtable = self.memtable.write().unwrap();

            // Now drop columns is not supported.
            let rows = memtable.values().last().unwrap().len();
            for column in metadata.user_schema().column_schemas() {
                let _ = memtable
                    .entry(column.name.clone())
                    .or_insert_with(|| vec![Value::Null; rows]);
            }
        }
        let _ = self.metadata.swap(Arc::new(metadata));
    }

    fn write(&self, request: WriteBatch) {
        let metadata = self.metadata.load();

        let mut memtable = self.memtable.write().unwrap();

        for mutation in &request.payload().mutations {
            for ColumnSchema { name, .. } in metadata.user_schema().column_schemas() {
                let column = memtable.get_mut(name).unwrap();
                if let Some(data) = mutation.record_batch.column_by_name(name) {
                    (0..data.len()).for_each(|i| column.push(data.get(i)));
                }
            }
        }
    }
}

type RegionMap = HashMap<String, MockRegion>;

#[derive(Debug, Default)]
struct RegionManager {
    opened_regions: RegionMap,
    closed_regions: RegionMap,
}

#[derive(Debug, Clone, Default)]
pub struct MockEngine {
    regions: Arc<Mutex<RegionManager>>,
}

#[async_trait]
impl StorageEngine for MockEngine {
    type Error = MockError;
    type Region = MockRegion;

    async fn open_region(
        &self,
        _ctx: &EngineContext,
        name: &str,
        _opts: &OpenOptions,
    ) -> Result<Option<MockRegion>> {
        logging::info!("Mock engine create region, name: {}", name);

        let mut regions = self.regions.lock().unwrap();
        if let Some(region) = regions.opened_regions.get(name) {
            return Ok(Some(region.clone()));
        }

        if let Some(region) = regions.closed_regions.remove(name) {
            let _ = regions
                .opened_regions
                .insert(name.to_string(), region.clone());
            return Ok(Some(region));
        }

        return Ok(None);
    }

    async fn close_region(
        &self,
        _ctx: &EngineContext,
        name: &str,
        _opts: &CloseOptions,
    ) -> Result<()> {
        let mut regions = self.regions.lock().unwrap();

        if let Some(region) = regions.opened_regions.remove(name) {
            let _ = regions.closed_regions.insert(name.to_string(), region);
        }

        Ok(())
    }

    async fn create_region(
        &self,
        _ctx: &EngineContext,
        descriptor: RegionDescriptor,
        _opts: &CreateOptions,
    ) -> Result<MockRegion> {
        logging::info!("Mock engine create region, descriptor: {:?}", descriptor);

        let mut regions = self.regions.lock().unwrap();
        if let Some(region) = regions.opened_regions.get(&descriptor.name) {
            return Ok(region.clone());
        }

        let name = descriptor.name.clone();
        let metadata = descriptor.try_into().unwrap();
        let region = MockRegion {
            inner: Arc::new(MockRegionInner::new(metadata)),
        };
        let _ = regions.opened_regions.insert(name, region.clone());

        Ok(region)
    }

    async fn drop_region(&self, _ctx: &EngineContext, _region: Self::Region) -> Result<()> {
        Ok(())
    }

    fn get_region(&self, _ctx: &EngineContext, name: &str) -> Result<Option<MockRegion>> {
        let regions = self.regions.lock().unwrap();
        Ok(regions.opened_regions.get(name).cloned())
    }

    async fn close(&self, _ctx: &EngineContext) -> Result<()> {
        Ok(())
    }
}
