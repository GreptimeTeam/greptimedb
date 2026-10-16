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

use std::sync::Arc;

use api::region::RegionResponse;
use api::v1::flow::{DirtyWindowRequests, FlowRequest, FlowResponse};
use api::v1::region::{InsertRequests, RegionRequest};
pub use common_base::AffectedRows;
use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_query::error::Error as CommonQueryError;
use common_query::request::{QueryRequest, RAW_SERIES_REQUEST_EXTENSION_KEY, RawSeriesRequest};
use common_query::series_frame::{raw_series_frame_ordering, unpack_raw_series};
use common_recordbatch::SendableRecordBatchStream;
use common_recordbatch::adapter::DfRecordBatchStreamAdapter;
use snafu::{ResultExt, location};
use store_api::region_engine::SendableRawSeriesStream;

use crate::error::{ExternalSnafu, Result};
use crate::peer::Peer;

/// The trait for handling requests to datanode.
#[async_trait::async_trait]
pub trait Datanode: Send + Sync {
    /// Handles DML, and DDL requests.
    async fn handle_request(&self, request: Arc<RegionRequest>) -> Result<RegionResponse>;

    /// Handles query requests
    async fn handle_query(&self, request: QueryRequest) -> Result<SendableRecordBatchStream>;

    /// Handles a bound raw-series query and decodes its series frames.
    ///
    /// The binding is an expectation supplied by the already-authorized internal planner; it is
    /// not evidence of provenance or an independent authorization grant.
    async fn handle_raw_series_query(
        &self,
        mut request: QueryRequest,
        binding: RawSeriesRequest,
    ) -> Result<SendableRawSeriesStream> {
        if request.region_id != binding.physical_metadata.region_id {
            return Err(BoxedError::new(PlainError::new(
                "raw-series request region does not match binding".to_string(),
                StatusCode::InvalidArguments,
            )))
            .context(ExternalSnafu);
        }
        let (expected_schema, label_source) = binding
            .bind(Arc::new(binding.physical_metadata.clone()))
            .map_err(|error| {
                BoxedError::new(CommonQueryError::GeneralDataFusion {
                    error,
                    location: location!(),
                })
            })
            .context(ExternalSnafu)?;
        if request
            .header
            .as_ref()
            .and_then(|header| header.query_context.as_ref())
            .is_some_and(|context| {
                context
                    .extensions
                    .contains_key(RAW_SERIES_REQUEST_EXTENSION_KEY)
            })
        {
            return Err(BoxedError::new(PlainError::new(
                "raw-series request marker already exists".to_string(),
                StatusCode::InvalidArguments,
            )))
            .context(ExternalSnafu);
        }
        let extension = binding
            .to_extension_value()
            .map_err(|error| {
                BoxedError::new(CommonQueryError::GeneralDataFusion {
                    error,
                    location: location!(),
                })
            })
            .context(ExternalSnafu)?;
        request
            .header
            .get_or_insert_default()
            .query_context
            .get_or_insert_default()
            .extensions
            .insert(RAW_SERIES_REQUEST_EXTENSION_KEY.to_string(), extension);
        let stream = self.handle_query(request).await?;
        let ordering = raw_series_frame_ordering(stream.schema().arrow_schema().as_ref());
        let stream = Box::pin(DfRecordBatchStreamAdapter::new(stream));
        ordering
            .and_then(|ordering| unpack_raw_series(stream, expected_schema, label_source, ordering))
            .map_err(|error| {
                BoxedError::new(CommonQueryError::GeneralDataFusion {
                    error,
                    location: location!(),
                })
            })
            .context(ExternalSnafu)
    }
}

impl dyn Datanode + '_ {
    /// Handles an owned or shared request without cloning its payload.
    pub async fn handle<R>(&self, request: R) -> Result<RegionResponse>
    where
        R: Into<Arc<RegionRequest>> + Send,
    {
        self.handle_request(request.into()).await
    }
}

pub type DatanodeRef = Arc<dyn Datanode>;

/// The trait for handling requests to flownode
#[async_trait::async_trait]
pub trait Flownode: Send + Sync {
    async fn handle(&self, request: FlowRequest) -> Result<FlowResponse>;

    async fn handle_inserts(&self, request: InsertRequests) -> Result<FlowResponse>;

    /// Handles requests to mark time window as dirty.
    async fn handle_mark_window_dirty(&self, req: DirtyWindowRequests) -> Result<FlowResponse>;
}

pub type FlownodeRef = Arc<dyn Flownode>;

/// Datanode manager
#[async_trait::async_trait]
pub trait DatanodeManager: Send + Sync {
    async fn datanode(&self, node: &Peer) -> DatanodeRef;
}

pub type DatanodeManagerRef = Arc<dyn DatanodeManager>;

/// Flownode manager
#[async_trait::async_trait]
pub trait FlownodeManager: Send + Sync {
    async fn flownode(&self, node: &Peer) -> FlownodeRef;
}

pub type FlownodeManagerRef = Arc<dyn FlownodeManager>;

/// Node manager
pub trait NodeManager: DatanodeManager + FlownodeManager {}

impl<T: DatanodeManager + FlownodeManager> NodeManager for T {}

pub type NodeManagerRef = Arc<dyn NodeManager>;
