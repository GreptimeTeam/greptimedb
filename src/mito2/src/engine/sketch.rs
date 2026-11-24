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

//! Prepares pinned SST inputs for the engine's sketch-reading capability.

use async_trait::async_trait;
use common_error::ext::BoxedError;
use snafu::OptionExt;
use store_api::metadata::RegionMetadataRef;
use store_api::sketch::{
    SketchReader, SketchRequest, SketchResponse, SketchSplitRequest, SketchSplitResponse,
    SketchTimeRange,
};
use store_api::storage::RegionId;

use crate::engine::MitoEngine;
use crate::error::{RegionNotFoundSnafu, UnsupportedOperationSnafu};
use crate::sst::index::puffin_manager::SstPuffinManager;
use crate::sst::index::sketch_reader::{SketchReadContext, SketchReadFiles, SketchReadHandler};

#[async_trait]
impl SketchReader for MitoEngine {
    async fn stat(
        &self,
        region_id: RegionId,
        request: SketchRequest,
    ) -> Result<SketchResponse, BoxedError> {
        request.validate()?;
        let prepared = self.build_sketch_read(region_id, request.time_range)?;
        prepared.handler.stat(&prepared.context(), &request).await
    }

    async fn query(
        &self,
        region_id: RegionId,
        request: SketchRequest,
    ) -> Result<SketchResponse, BoxedError> {
        request.validate()?;
        let prepared = self.build_sketch_read(region_id, request.time_range)?;
        prepared.handler.query(&prepared.context(), &request).await
    }

    async fn query_with_max_level(
        &self,
        region_id: RegionId,
        request: SketchRequest,
        max_level: u8,
    ) -> Result<SketchResponse, BoxedError> {
        request.validate()?;
        let mut prepared = self.build_sketch_read(region_id, request.time_range)?;
        prepared.files = prepared.files.with_max_level(max_level);
        prepared.handler.query(&prepared.context(), &request).await
    }

    async fn split(
        &self,
        region_id: RegionId,
        request: SketchSplitRequest,
    ) -> Result<SketchSplitResponse, BoxedError> {
        request.validate()?;
        let prepared = self.build_sketch_read(region_id, request.time_range)?;
        prepared.handler.split(&prepared.context(), &request).await
    }
}

impl MitoEngine {
    fn build_sketch_read(
        &self,
        region_id: RegionId,
        time_range: SketchTimeRange,
    ) -> Result<PreparedSketchRead<'_>, BoxedError> {
        let region = self
            .find_region(region_id)
            .context(RegionNotFoundSnafu { region_id })
            .map_err(BoxedError::new)?;
        let handler = self
            .inner
            .sketch_read_handler
            .as_deref()
            .with_context(|| UnsupportedOperationSnafu {
                err_msg: "read sketches".to_string(),
            })
            .map_err(BoxedError::new)?;
        let version = region.version();
        let files = SketchReadFiles::new(version.ssts.clone(), time_range);
        let manager = region
            .access_layer
            .build_puffin_manager()
            .with_puffin_metadata_cache(
                self.inner
                    .workers
                    .cache_manager()
                    .puffin_metadata_cache()
                    .cloned(),
            );
        Ok(PreparedSketchRead {
            handler,
            metadata: version.metadata.clone(),
            files,
            puffin_manager: manager,
        })
    }
}

/// Owns one call's inputs so their file handles remain pinned across asynchronous reads.
struct PreparedSketchRead<'a> {
    handler: &'a dyn SketchReadHandler,
    metadata: RegionMetadataRef,
    files: SketchReadFiles,
    puffin_manager: SstPuffinManager,
}

impl PreparedSketchRead<'_> {
    fn context(&self) -> SketchReadContext<'_> {
        SketchReadContext {
            metadata: &self.metadata,
            files: &self.files,
            puffin_manager: &self.puffin_manager,
        }
    }
}

#[cfg(test)]
mod tests {
    use common_error::ext::ErrorExt;
    use common_error::status_code::StatusCode;
    use common_time::Timestamp;
    use store_api::sketch::{SketchQuery, SketchTimeRange};

    use super::*;
    use crate::test_util::TestEnv;

    #[tokio::test]
    async fn test_invalid_split_request_precedes_region_lookup() {
        let mut env = TestEnv::new().await;
        let engine = env.create_engine(Default::default()).await;
        let request = SketchSplitRequest {
            time_range: SketchTimeRange {
                start: Timestamp::new_millisecond(0),
                end: Timestamp::new_millisecond(300_000),
            },
            column: "value".to_string(),
            parts: 2,
            initial_files: 128,
            min_files: 512,
            max_files: 2048,
            seed: 42,
            max_cdf_delta: f64::NAN,
            min_partition_ratio: 0.7,
            max_partition_ratio: 1.3,
        };
        assert_eq!(
            SketchReader::split(&engine, RegionId::new(1, 1), request)
                .await
                .unwrap_err()
                .status_code(),
            StatusCode::InvalidArguments
        );
    }

    #[tokio::test]
    async fn test_invalid_request_precedes_region_lookup() {
        let mut env = TestEnv::new().await;
        let engine = env.create_engine(Default::default()).await;
        let request = SketchRequest {
            sampling: None,
            time_range: SketchTimeRange {
                start: Timestamp::new_microsecond(1001),
                end: Timestamp::new_microsecond(1000),
            },
            columns: vec!["value".to_string()],
            queries: vec![SketchQuery::DistinctCount],
        };
        let region_id = RegionId::new(1, 1);
        assert_eq!(
            SketchReader::stat(&engine, region_id, request.clone())
                .await
                .unwrap_err()
                .status_code(),
            StatusCode::InvalidArguments
        );
        assert_eq!(
            SketchReader::query(&engine, region_id, request)
                .await
                .unwrap_err()
                .status_code(),
            StatusCode::InvalidArguments
        );
    }
}
