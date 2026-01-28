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

use std::pin::Pin;
use std::sync::Arc;

use api::v1::greptime_request::Request;
use async_trait::async_trait;
use common_base::AffectedRows;
use common_grpc::flight::do_put::DoPutResponse;
use common_query::Output;
use futures::Stream;
use session::context::QueryContextRef;
use table::TableRef;

use crate::error::Result;
use crate::grpc::flight::{PutRecordBatchRequest, PutRecordBatchRequestStream};

pub type ServerGrpcQueryHandlerRef = Arc<dyn GrpcQueryHandler + Send + Sync>;

pub type RawRecordBatch = bytes::Bytes;

#[async_trait]
pub trait GrpcQueryHandler {
    async fn do_query(&self, query: Request, ctx: QueryContextRef) -> Result<Output>;

    async fn put_record_batch(
        &self,
        request: PutRecordBatchRequest,
        table_ref: &mut Option<TableRef>,
        ctx: QueryContextRef,
    ) -> Result<AffectedRows>;

    fn handle_put_record_batch_stream(
        &self,
        stream: PutRecordBatchRequestStream,
        ctx: QueryContextRef,
    ) -> Pin<Box<dyn Stream<Item = Result<DoPutResponse>> + Send>>;
}
