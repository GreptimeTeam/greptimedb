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

//! Region-bound sketch access without retaining an SST snapshot between calls.

use common_error::ext::BoxedError;
use snafu::{OptionExt, ResultExt};
use store_api::region_engine::RegionEngineRef;
use store_api::sketch::{SketchReader, SketchRequest, SketchResponse};
use store_api::storage::RegionId;

use crate::error::SketchReaderUnavailableSnafu;

/// Lightweight Region-bound reader. Each operation obtains a fresh engine snapshot.
pub struct RegionSketchReader {
    engine: RegionEngineRef,
    region_id: RegionId,
}

impl RegionSketchReader {
    pub(crate) fn new(engine: RegionEngineRef, region_id: RegionId) -> Option<Self> {
        engine.sketch_reader()?;
        Some(Self { engine, region_id })
    }

    /// Returns persisted sketch coverage; does not read algorithm payloads or scan Parquet.
    pub async fn stat(
        &self,
        request: SketchRequest,
    ) -> std::result::Result<SketchResponse, BoxedError> {
        self.reader()?.stat(self.region_id, request).await
    }

    /// Returns estimates and coverage from complete buckets. Missing statistics remain explicit gaps.
    pub async fn query(
        &self,
        request: SketchRequest,
    ) -> std::result::Result<SketchResponse, BoxedError> {
        self.reader()?.query(self.region_id, request).await
    }

    fn reader(&self) -> std::result::Result<&dyn SketchReader, BoxedError> {
        self.engine
            .sketch_reader()
            .context(SketchReaderUnavailableSnafu {
                region_id: self.region_id,
            })
            .map_err(BoxedError::new)
    }
}

impl crate::region_server::RegionServer {
    pub(crate) async fn handle_sketch_rpc(
        &self,
        request: api::v1::region::SketchRequest,
        query: bool,
    ) -> servers::error::Result<api::v1::region::RegionResponse> {
        let response = self
            .handle_sketch_request(request, query)
            .await
            .context(servers::error::ExecuteGrpcRequestSnafu)?;
        Ok(api::v1::region::RegionResponse {
            header: Some(api::v1::ResponseHeader {
                status: Some(api::v1::Status {
                    status_code: common_error::status_code::StatusCode::Success as _,
                    ..Default::default()
                }),
            }),
            sketch: response.sketch,
            ..Default::default()
        })
    }

    async fn handle_sketch_request(
        &self,
        request: api::v1::region::SketchRequest,
        query: bool,
    ) -> std::result::Result<api::region::RegionResponse, BoxedError> {
        let region_id = RegionId::from_u64(request.region_id);
        let request = SketchRequest::try_from(request)?;
        let reader = self
            .sketch_reader(region_id)
            .map_err(BoxedError::new)?
            .context(SketchReaderUnavailableSnafu { region_id })
            .map_err(BoxedError::new)?;
        let response = if query {
            reader.query(request).await?
        } else {
            reader.stat(request).await?
        };
        let mut result = api::region::RegionResponse::new(0);
        result.sketch = Some(response.try_into()?);
        Ok(result)
    }
}
