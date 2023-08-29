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

use common_error::ext::BoxedError;
use common_recordbatch::{EmptyRecordBatchStream, SendableRecordBatchStream};
use datatypes::schema::SchemaRef;
use store_api::data_source::DataSource;
use store_api::storage::ScanRequest;

use crate::metadata::{
    FilterPushDownType, TableInfo, TableInfoBuilder, TableMetaBuilder, TableType,
};
use crate::requests::CreateTableRequest;
use crate::thin_table::{ThinTable, ThinTableAdapter};
use crate::TableRef;

pub struct EmptyTable;

impl EmptyTable {
    pub fn table(req: CreateTableRequest) -> TableRef {
        let schema = Arc::new(req.schema.try_into().unwrap());
        let table_meta = TableMetaBuilder::default()
            .schema(schema)
            .primary_key_indices(req.primary_key_indices)
            .next_column_id(0)
            .options(req.table_options)
            .region_numbers(req.region_numbers)
            .engine(req.engine)
            .build();
        let table_info = TableInfoBuilder::default()
            .table_id(req.id)
            .catalog_name(req.catalog_name)
            .schema_name(req.schema_name)
            .name(req.table_name)
            .meta(table_meta.unwrap())
            .table_type(TableType::Temporary)
            .desc(req.desc)
            .build()
            .unwrap();

        Self::from_table_info(&table_info)
    }

    pub fn from_table_info(info: &TableInfo) -> TableRef {
        let thin_table = ThinTable::new(Arc::new(info.clone()), FilterPushDownType::Unsupported);
        let data_source = Arc::new(EmptyDataSource {
            schema: info.meta.schema.clone(),
        });
        Arc::new(ThinTableAdapter::new(thin_table, data_source))
    }
}

struct EmptyDataSource {
    schema: SchemaRef,
}

impl DataSource for EmptyDataSource {
    fn get_stream(
        &self,
        _request: ScanRequest,
    ) -> std::result::Result<SendableRecordBatchStream, BoxedError> {
        Ok(Box::pin(EmptyRecordBatchStream::new(self.schema.clone())))
    }
}
