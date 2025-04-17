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

use api::v1::greptime_request::Request;
use async_trait::async_trait;
use common_base::AffectedRows;
use common_error::ext::{BoxedError, ErrorExt};
use common_query::Output;
use session::context::QueryContextRef;
use snafu::ResultExt;
use table::table_name::TableName;

use crate::error::{self, Result};

pub type GrpcQueryHandlerRef<E> = Arc<dyn GrpcQueryHandler<Error = E> + Send + Sync>;
pub type ServerGrpcQueryHandlerRef = GrpcQueryHandlerRef<error::Error>;

pub type RawRecordBatch = bytes::Bytes;

#[async_trait]
pub trait GrpcQueryHandler {
    type Error: ErrorExt;

    async fn do_query(
        &self,
        query: Request,
        ctx: QueryContextRef,
    ) -> std::result::Result<Output, Self::Error>;

    async fn put_record_batch(
        &self,
        table: &TableName,
        record_batch: RawRecordBatch,
    ) -> std::result::Result<AffectedRows, Self::Error>;
}

pub struct ServerGrpcQueryHandlerAdapter<E>(GrpcQueryHandlerRef<E>);

impl<E> ServerGrpcQueryHandlerAdapter<E> {
    pub fn arc(handler: GrpcQueryHandlerRef<E>) -> Arc<Self> {
        Arc::new(Self(handler))
    }
}

#[async_trait]
impl<E> GrpcQueryHandler for ServerGrpcQueryHandlerAdapter<E>
where
    E: ErrorExt + Send + Sync + 'static,
{
    type Error = error::Error;

    async fn do_query(&self, query: Request, ctx: QueryContextRef) -> Result<Output> {
        self.0
            .do_query(query, ctx)
            .await
            .map_err(BoxedError::new)
            .context(error::ExecuteGrpcQuerySnafu)
    }

    async fn put_record_batch(
        &self,
        table: &TableName,
        record_batch: RawRecordBatch,
    ) -> Result<AffectedRows> {
        self.0
            .put_record_batch(table, record_batch)
            .await
            .map_err(BoxedError::new)
            .context(error::ExecuteGrpcRequestSnafu)
    }
}
