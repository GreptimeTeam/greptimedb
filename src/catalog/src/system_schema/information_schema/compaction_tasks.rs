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

use common_catalog::consts::INFORMATION_SCHEMA_COMPACTION_TASKS_TABLE_ID;
use common_error::ext::BoxedError;
use common_recordbatch::SendableRecordBatchStream;
use common_recordbatch::adapter::AsyncRecordBatchStreamAdapter;
use datafusion::physical_plan::ExecutionPlan;
use datatypes::schema::SchemaRef;
use snafu::ResultExt;
use store_api::compaction_task::CompactionTaskEntry;
use store_api::storage::{ScanRequest, TableId};

use crate::CatalogManager;
use crate::error::Result;
use crate::information_schema::{
    COMPACTION_TASKS, DatanodeInspectKind, InformationSchemaTableScope, InformationTable,
};
use crate::system_schema::SystemTable;
use crate::system_schema::utils;

/// Information schema table for live compaction tasks.
pub struct InformationSchemaCompactionTasks {
    schema: SchemaRef,
    catalog_manager: Weak<dyn CatalogManager>,
}

impl InformationSchemaCompactionTasks {
    pub(crate) fn new(catalog_manager: Weak<dyn CatalogManager>) -> Self {
        Self {
            schema: CompactionTaskEntry::schema(),
            catalog_manager,
        }
    }
}

impl InformationTable for InformationSchemaCompactionTasks {
    fn scope(&self) -> InformationSchemaTableScope {
        InformationSchemaTableScope::Global
    }
}

impl SystemTable for InformationSchemaCompactionTasks {
    fn table_id(&self) -> TableId {
        INFORMATION_SCHEMA_COMPACTION_TASKS_TABLE_ID
    }

    fn table_name(&self) -> &'static str {
        COMPACTION_TASKS
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn to_stream(&self, request: ScanRequest) -> Result<SendableRecordBatchStream> {
        let schema = utils::projected_schema(&self.schema, &request)?;

        let info_ext = utils::information_extension(&self.catalog_manager)?;
        let req =
            utils::inspect_request(DatanodeInspectKind::CompactionTasks, &self.schema, request)?;

        let future = async move {
            info_ext
                .inspect_datanode(req)
                .await
                .map_err(BoxedError::new)
                .context(common_recordbatch::error::ExternalSnafu)
        };
        Ok(Box::pin(AsyncRecordBatchStreamAdapter::new(
            schema,
            Box::pin(future),
        )))
    }

    fn scan_plan(&self, request: ScanRequest) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        utils::inspect_plan(
            &self.catalog_manager,
            DatanodeInspectKind::CompactionTasks,
            &self.schema,
            request,
        )
    }
}
