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

use async_trait::async_trait;
use client::Output;
use common_base::AffectedRows;
use common_error::ext::BoxedError;
use common_function::handlers::TableMutationHandler;
use common_query::error as query_error;
use common_query::error::Result as QueryResult;
use session::context::QueryContextRef;
use snafu::ResultExt;
use store_api::storage::RegionId;
use table::requests::{
    BuildIndexTableRequest, CompactTableRequest, DeleteRequest as TableDeleteRequest,
    FlushTableRequest, InsertRequest as TableInsertRequest,
};

use crate::delete::DeleterRef;
use crate::insert::InserterRef;
use crate::request::RequesterRef;

pub struct TableMutationOperator {
    inserter: InserterRef,
    deleter: DeleterRef,
    requester: RequesterRef,
}

impl TableMutationOperator {
    pub fn new(inserter: InserterRef, deleter: DeleterRef, requester: RequesterRef) -> Self {
        Self {
            inserter,
            deleter,
            requester,
        }
    }
}

#[async_trait]
impl TableMutationHandler for TableMutationOperator {
    async fn insert(
        &self,
        request: TableInsertRequest,
        ctx: QueryContextRef,
    ) -> QueryResult<Output> {
        self.inserter
            .handle_table_insert(request, ctx)
            .await
            .map_err(BoxedError::new)
            .context(query_error::TableMutationSnafu)
    }

    async fn delete(
        &self,
        request: TableDeleteRequest,
        ctx: QueryContextRef,
    ) -> QueryResult<AffectedRows> {
        self.deleter
            .handle_table_delete(request, ctx)
            .await
            .map_err(BoxedError::new)
            .context(query_error::TableMutationSnafu)
    }

    async fn flush(
        &self,
        request: FlushTableRequest,
        ctx: QueryContextRef,
    ) -> QueryResult<AffectedRows> {
        self.requester
            .handle_table_flush(request, ctx)
            .await
            .map_err(BoxedError::new)
            .context(query_error::TableMutationSnafu)
    }

    async fn compact(
        &self,
        request: CompactTableRequest,
        ctx: QueryContextRef,
    ) -> QueryResult<AffectedRows> {
        self.requester
            .handle_table_compaction(request, ctx)
            .await
            .map_err(BoxedError::new)
            .context(query_error::TableMutationSnafu)
    }

    async fn build_index(
        &self,
        request: BuildIndexTableRequest,
        ctx: QueryContextRef,
    ) -> QueryResult<AffectedRows> {
        self.requester
            .handle_table_build_index(request, ctx)
            .await
            .map_err(BoxedError::new)
            .context(query_error::TableMutationSnafu)
    }
    
    async fn flush_region(
        &self,
        region_id: RegionId,
        ctx: QueryContextRef,
    ) -> QueryResult<AffectedRows> {
        self.requester
            .handle_region_flush(region_id, ctx)
            .await
            .map_err(BoxedError::new)
            .context(query_error::TableMutationSnafu)
    }

    async fn compact_region(
        &self,
        region_id: RegionId,
        ctx: QueryContextRef,
    ) -> QueryResult<AffectedRows> {
        self.requester
            .handle_region_compaction(region_id, ctx)
            .await
            .map_err(BoxedError::new)
            .context(query_error::TableMutationSnafu)
    }
}
