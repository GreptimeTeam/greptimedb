//! A mock storage engine for table test purpose.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use common_error::mock::MockError;
use common_error::prelude::StatusCode;
use common_telemetry::logging;
use storage::metadata::{RegionMetaImpl, RegionMetadataRef};
use storage::write_batch::WriteBatch;
use store_api::storage::{
    Chunk, ChunkReader, CreateOptions, EngineContext, GetRequest, GetResponse, OpenOptions,
    ReadContext, Region, RegionDescriptor, ScanRequest, ScanResponse, SchemaRef, Snapshot,
    StorageEngine, WriteContext, WriteResponse,
};

pub type Result<T> = std::result::Result<T, MockError>;

pub struct MockChunkReader {
    schema: SchemaRef,
}

#[async_trait]
impl ChunkReader for MockChunkReader {
    type Error = MockError;

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn next_chunk(&mut self) -> Result<Option<Chunk>> {
        Ok(None)
    }
}

pub struct MockSnapshot {
    metadata: RegionMetadataRef,
}

#[async_trait]
impl Snapshot for MockSnapshot {
    type Error = MockError;
    type Reader = MockChunkReader;

    fn schema(&self) -> &SchemaRef {
        &self.metadata.schema
    }

    async fn scan(
        &self,
        _ctx: &ReadContext,
        _request: ScanRequest,
    ) -> Result<ScanResponse<MockChunkReader>> {
        let reader = MockChunkReader {
            schema: self.metadata.schema.clone(),
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
    // We share the same metadata definition with the storage engine.
    metadata: RegionMetadataRef,
}

#[async_trait]
impl Region for MockRegion {
    type Error = MockError;
    type Meta = RegionMetaImpl;
    type WriteRequest = WriteBatch;
    type Snapshot = MockSnapshot;

    fn name(&self) -> &str {
        &self.name
    }

    fn in_memory_metadata(&self) -> RegionMetaImpl {
        RegionMetaImpl::new(self.metadata.clone())
    }

    async fn write(&self, _ctx: &WriteContext, _request: WriteBatch) -> Result<WriteResponse> {
        Ok(WriteResponse {})
    }

    fn snapshot(&self, _ctx: &ReadContext) -> Result<MockSnapshot> {
        Ok(MockSnapshot {
            metadata: self.metadata.clone(),
        })
    }

    fn write_request(&self, schema: SchemaRef) -> WriteBatch {
        WriteBatch::new(schema)
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

        Err(MockError::with_backtrace(StatusCode::Unexpected))
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
            metadata: Arc::new(metadata),
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
}
