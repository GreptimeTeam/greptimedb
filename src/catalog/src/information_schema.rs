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

mod tables;

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::streaming::{PartitionStream, StreamingTable};
use snafu::ResultExt;
use table::table::adapter::TableAdapter;
use table::TableRef;

use crate::error::{DatafusionSnafu, Result, TableSchemaMismatchSnafu};
use crate::information_schema::tables::InformationSchemaTables;
use crate::{CatalogProviderRef, SchemaProvider};

const TABLES: &str = "tables";

pub(crate) struct InformationSchemaProvider {
    catalog_name: String,
    catalog_provider: CatalogProviderRef,
}

impl InformationSchemaProvider {
    pub(crate) fn new(catalog_name: String, catalog_provider: CatalogProviderRef) -> Self {
        Self {
            catalog_name,
            catalog_provider,
        }
    }
}

#[async_trait]
impl SchemaProvider for InformationSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn table_names(&self) -> Result<Vec<String>> {
        Ok(vec![TABLES.to_string()])
    }

    async fn table(&self, name: &str) -> Result<Option<TableRef>> {
        let table = if name.eq_ignore_ascii_case(TABLES) {
            Arc::new(InformationSchemaTables::new(
                self.catalog_name.clone(),
                self.catalog_provider.clone(),
            ))
        } else {
            return Ok(None);
        };

        let table = Arc::new(
            StreamingTable::try_new(table.schema().clone(), vec![table]).with_context(|_| {
                DatafusionSnafu {
                    msg: format!("Failed to get InformationSchema table '{name}'"),
                }
            })?,
        );
        let table = TableAdapter::new(table).context(TableSchemaMismatchSnafu)?;
        Ok(Some(Arc::new(table)))
    }

    async fn table_exist(&self, name: &str) -> Result<bool> {
        Ok(matches!(name.to_ascii_lowercase().as_str(), TABLES))
    }
}
