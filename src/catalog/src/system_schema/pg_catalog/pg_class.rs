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

use std::sync::{Arc, Weak};

use common_catalog::consts::PG_CATALOG_PG_CLASS_TABLE_ID;
use common_recordbatch::SendableRecordBatchStream;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use store_api::storage::{ConcreteDataType, ScanRequest};

use super::table_names::PG_CLASS;
use super::{oid_column, PGCatalogTable};
use crate::error::Result;
use crate::CatalogManager;

const CLASS_RELKIND: &str = "relkind";
const CLASS_RELOWNER: &str = "relowner";
const CLASS_RELNAME: &str = "relname";
const CLASS_RELNAMESPACE: &str = "relnamespace";

pub(super) struct PGCatalogPgClass {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,
}

impl PGCatalogPgClass {
    pub(super) fn new(catalog_name: String, catalog_manager: Weak<dyn CatalogManager>) -> Self {
        Self {
            schema: Self::schema(),
            catalog_name,
            catalog_manager,
        }
    }

    pub(super) fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            oid_column(),
            ColumnSchema::new(CLASS_RELNAME, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(
                CLASS_RELNAMESPACE,
                ConcreteDataType::uint32_datatype(),
                false,
            ),
            ColumnSchema::new(CLASS_RELKIND, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(CLASS_RELOWNER, ConcreteDataType::uint32_datatype(), false),
        ]))
    }
}

impl PGCatalogTable for PGCatalogPgClass {
    fn table_id(&self) -> store_api::storage::TableId {
        PG_CATALOG_PG_CLASS_TABLE_ID
    }

    fn table_name(&self) -> &'static str {
        PG_CLASS
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn to_stream(&self, request: ScanRequest) -> Result<SendableRecordBatchStream> {
        let schema = self.schema.arrow_schema().clone();
        todo!()
    }
}
