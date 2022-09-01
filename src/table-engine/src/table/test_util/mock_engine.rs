//! A mock storage engine for table test purpose.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use common_error::mock::MockError;
use common_telemetry::logging;
use datatypes::prelude::{Value, VectorBuilder};
use datatypes::schema::ColumnSchema;
use storage::metadata::{RegionMetaImpl, RegionMetadata};
use storage::write_batch::{Mutation, WriteBatch};
use store_api::storage::{
    Chunk, ChunkReader, CreateOptions, EngineContext, GetRequest, GetResponse, OpenOptions,
    ReadContext, Region, RegionDescriptor, RegionId, RegionMeta, ScanRequest, ScanResponse,
    SchemaRef, Snapshot, StorageEngine, WriteContext, WriteResponse,
};

pub type Result<T> = std::result::Result<T, MockError>;

pub struct MockChunkReader {
    schema: SchemaRef,
    memtable: MockMemtable,
}

#[async_trait]
impl ChunkReader for MockChunkReader {
    type Error = MockError;

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn next_chunk(&mut self) -> Result<Option<Chunk>> {
        let mut columns = vec![];
        for ColumnSchema {
            name, data_type, ..
        } in self.schema.column_schemas()
        {
            let data = self.memtable.get(name).unwrap();
            let mut builder = VectorBuilder::new(data_type.clone());
            for v in data {
                if v.is_null() {
                    builder.push_null();
                } else {
                    builder.push(v);
                }
            }
            columns.push(builder.finish());
        }
        Ok(Some(Chunk::new(columns)))
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
        _request: ScanRequest,
    ) -> Result<ScanResponse<MockChunkReader>> {
        let memtable = {
            let memtable = self.region.memtable.read().unwrap();
            memtable.clone()
        };
        let reader = MockChunkReader {
            schema: self.schema().clone(),
            memtable,
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
    // FIXME(yingwen): Remove this once name is provided by metadata.
    name: String,
    pub inner: Arc<MockRegionInner>,
}

#[derive(Debug)]
pub struct MockRegionInner {
    pub metadata: ArcSwap<RegionMetadata>,
    memtable: Arc<RwLock<MockMemtable>>,
}

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
        &self.name
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
        WriteBatch::new(self.in_memory_metadata().schema().clone())
    }
}

impl MockRegionInner {
    fn new(metadata: RegionMetadata) -> Self {
        let mut memtable = HashMap::new();
        for column in metadata.user_schema().column_schemas() {
            memtable.insert(column.name.clone(), vec![]);
        }
        Self {
            metadata: ArcSwap::new(Arc::new(metadata)),
            memtable: Arc::new(RwLock::new(memtable)),
        }
    }

    fn update_metadata(&self, metadata: RegionMetadata) {
        {
            let mut memtable = self.memtable.write().unwrap();

            let rows = memtable.values().last().unwrap().len();

            // currently dropping columns are not supported, so we only add columns here
            for column in metadata.user_schema().column_schemas() {
                let _ = memtable
                    .entry(column.name.clone())
                    .or_insert_with(|| vec![Value::Null; rows]);
            }
        }
        self.metadata.swap(Arc::new(metadata));
    }

    fn write(&self, request: WriteBatch) {
        let metadata = self.metadata.load();

        let mut memtable = self.memtable.write().unwrap();

        for Mutation::Put(put) in request.iter() {
            for ColumnSchema { name, .. } in metadata.user_schema().column_schemas() {
                let column = memtable.get_mut(name).unwrap();
                if let Some(data) = put.column_by_name(name) {
                    (0..data.len()).for_each(|i| column.push(data.get(i)));
                } else {
                    column.extend_from_slice(&vec![Value::Null; put.num_rows()]);
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
            regions
                .opened_regions
                .insert(name.to_string(), region.clone());
            return Ok(Some(region));
        }

        return Ok(None);
    }

    async fn close_region(&self, _ctx: &EngineContext, _region: MockRegion) -> Result<()> {
        unimplemented!()
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
            name: name.clone(),
            inner: Arc::new(MockRegionInner::new(metadata)),
        };
        regions.opened_regions.insert(name, region.clone());

        Ok(region)
    }

    async fn drop_region(&self, _ctx: &EngineContext, _region: Self::Region) -> Result<()> {
        unimplemented!()
    }

    fn get_region(&self, _ctx: &EngineContext, name: &str) -> Result<Option<MockRegion>> {
        let regions = self.regions.lock().unwrap();
        Ok(regions.opened_regions.get(name).cloned())
    }

    async fn alter_region(&self, _ctx: &EngineContext, descriptor: RegionDescriptor) -> Result<()> {
        let region_name = descriptor.name.clone();
        let metadata = descriptor.try_into().unwrap();

        let mut regions = self.regions.lock().unwrap();
        let region = regions.opened_regions.get_mut(&region_name).unwrap();

        region.inner.update_metadata(metadata);
        Ok(())
    }
}
