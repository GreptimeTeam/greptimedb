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

use async_trait::async_trait;
use common_query::physical_plan::PhysicalPlanRef;
use common_recordbatch::{EmptyRecordBatchStream, SendableRecordBatchStream};
use store_api::storage::ScanRequest;

use crate::metadata::{TableInfo, TableInfoBuilder, TableInfoRef, TableMetaBuilder, TableType};
use crate::requests::{CreateTableRequest, InsertRequest};
use crate::table::scan::SimpleTableScan;
use crate::{Result, Table};

pub struct EmptyTable {
    info: TableInfoRef,
}

impl EmptyTable {
    pub fn new(req: CreateTableRequest) -> Self {
        let schema = Arc::new(req.schema.try_into().unwrap());
        let table_meta = TableMetaBuilder::default()
            .schema(schema)
            .primary_key_indices(req.primary_key_indices)
            .next_column_id(0)
            .options(req.table_options)
            .build();
        let table_info = TableInfoBuilder::default()
            .catalog_name(req.catalog_name)
            .schema_name(req.schema_name)
            .name(req.table_name)
            .meta(table_meta.unwrap())
            .table_type(TableType::Temporary)
            .desc(req.desc)
            .build()
            .unwrap();

        Self {
            info: Arc::new(table_info),
        }
    }

    pub fn from_table_info(info: &TableInfo) -> Self {
        Self {
            info: Arc::new(info.clone()),
        }
    }
}

#[async_trait]
impl Table for EmptyTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self as _
    }

    fn schema(&self) -> datatypes::schema::SchemaRef {
        self.info.meta.schema.clone()
    }

    fn table_info(&self) -> TableInfoRef {
        self.info.clone()
    }

    async fn insert(&self, _request: InsertRequest) -> Result<usize> {
        Ok(0)
    }

    async fn scan(
        &self,
        _projection: Option<&Vec<usize>>,
        _filters: &[common_query::prelude::Expr],
        _limit: Option<usize>,
    ) -> Result<PhysicalPlanRef> {
        let scan = SimpleTableScan::new(Box::pin(EmptyRecordBatchStream::new(self.schema())));
        Ok(Arc::new(scan))
    }

    async fn scan_to_stream(&self, request: ScanRequest) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(EmptyRecordBatchStream::new(self.schema())))
    }
}
