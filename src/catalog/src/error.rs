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

use std::any::Any;
use std::fmt::Debug;

use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use common_query::error::datafusion_status_code;
use datafusion::error::DataFusionError;
use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Failed to list catalogs"))]
    ListCatalogs {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to list {}'s schemas", catalog))]
    ListSchemas {
        #[snafu(implicit)]
        location: Location,
        catalog: String,
        source: BoxedError,
    },

    #[snafu(display("Failed to list {}.{}'s tables", catalog, schema))]
    ListTables {
        #[snafu(implicit)]
        location: Location,
        catalog: String,
        schema: String,
        source: BoxedError,
    },

    #[snafu(display("Failed to list nodes in cluster"))]
    ListNodes {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to region stats in cluster"))]
    ListRegionStats {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to list flow stats"))]
    ListFlowStats {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to list flows in catalog {catalog}"))]
    ListFlows {
        #[snafu(implicit)]
        location: Location,
        catalog: String,
        source: BoxedError,
    },

    #[snafu(display("Flow info not found: {flow_name} in catalog {catalog_name}"))]
    FlowInfoNotFound {
        flow_name: String,
        catalog_name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Can't convert value to json, input={input}"))]
    Json {
        input: String,
        #[snafu(source)]
        error: serde_json::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to get information extension client"))]
    GetInformationExtension {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to list procedures"))]
    ListProcedures {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Procedure id not found"))]
    ProcedureIdNotFound {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("convert proto data error"))]
    ConvertProtoData {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to create table, table info: {}", table_info))]
    CreateTable {
        table_info: String,
        #[snafu(implicit)]
        location: Location,
        source: table::error::Error,
    },

    #[snafu(display("Cannot find catalog by name: {}", catalog_name))]
    CatalogNotFound {
        catalog_name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Cannot find schema {} in catalog {}", schema, catalog))]
    SchemaNotFound {
        catalog: String,
        schema: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table `{}` already exists", table))]
    TableExists {
        table: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table not found: {}", table))]
    TableNotExist {
        table: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("View info not found: {}", name))]
    ViewInfoNotFound {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "View plan columns changed from: {} to: {}",
        origin_names,
        actual_names
    ))]
    ViewPlanColumnsChanged {
        origin_names: String,
        actual_names: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Partition manager not found, it's not expected."))]
    PartitionManagerNotFound {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to find table partitions"))]
    FindPartitions { source: partition::error::Error },

    #[snafu(display("Failed to find region routes"))]
    FindRegionRoutes { source: partition::error::Error },

    #[snafu(display("Failed to create recordbatch"))]
    CreateRecordBatch {
        #[snafu(implicit)]
        location: Location,
        source: common_recordbatch::error::Error,
    },

    #[snafu(display("Internal error"))]
    Internal {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to upgrade weak catalog manager reference"))]
    UpgradeWeakCatalogManagerRef {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to decode logical plan for view: {}", name))]
    DecodePlan {
        name: String,
        #[snafu(implicit)]
        location: Location,
        source: common_query::error::Error,
    },

    #[snafu(display("Illegal access to catalog: {} and schema: {}", catalog, schema))]
    QueryAccessDenied { catalog: String, schema: String },

    #[snafu(display("DataFusion error"))]
    Datafusion {
        #[snafu(source)]
        error: DataFusionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to project view columns"))]
    ProjectViewColumns {
        #[snafu(source)]
        error: DataFusionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Table metadata manager error"))]
    TableMetadataManager {
        source: common_meta::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to get table cache"))]
    GetTableCache {
        source: common_meta::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to get view info from cache"))]
    GetViewCache {
        source: common_meta::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Cache not found: {name}"))]
    CacheNotFound {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to cast the catalog manager"))]
    CastManager {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to invoke frontend services"))]
    InvokeFrontend {
        source: common_frontend::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Meta client is not provided"))]
    MetaClientMissing {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to find frontend node: {}", addr))]
    FrontendNotFound {
        addr: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to handle query"))]
    HandleQuery {
        source: common_meta::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to project schema"))]
    ProjectSchema {
        source: datatypes::error::Error,
        #[snafu(implicit)]
        location: Location,
    },
}

impl Error {
    pub fn should_fail(&self) -> bool {
        use Error::*;

        matches!(
            self,
            GetViewCache { .. }
                | ViewInfoNotFound { .. }
                | DecodePlan { .. }
                | ViewPlanColumnsChanged { .. }
                | ProjectViewColumns { .. }
        )
    }
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::SchemaNotFound { .. }
            | Error::CatalogNotFound { .. }
            | Error::FindPartitions { .. }
            | Error::FindRegionRoutes { .. }
            | Error::CacheNotFound { .. }
            | Error::CastManager { .. }
            | Error::Json { .. }
            | Error::GetInformationExtension { .. }
            | Error::PartitionManagerNotFound { .. }
            | Error::ProcedureIdNotFound { .. } => StatusCode::Unexpected,

            Error::ViewPlanColumnsChanged { .. } => StatusCode::InvalidArguments,

            Error::ViewInfoNotFound { .. } => StatusCode::TableNotFound,

            Error::FlowInfoNotFound { .. } => StatusCode::FlowNotFound,

            Error::UpgradeWeakCatalogManagerRef { .. } => StatusCode::Internal,

            Error::CreateRecordBatch { source, .. } => source.status_code(),
            Error::TableExists { .. } => StatusCode::TableAlreadyExists,
            Error::TableNotExist { .. } => StatusCode::TableNotFound,
            Error::ListCatalogs { source, .. }
            | Error::ListNodes { source, .. }
            | Error::ListSchemas { source, .. }
            | Error::ListTables { source, .. }
            | Error::ListFlows { source, .. }
            | Error::ListFlowStats { source, .. }
            | Error::ListProcedures { source, .. }
            | Error::ListRegionStats { source, .. }
            | Error::ConvertProtoData { source, .. } => source.status_code(),

            Error::CreateTable { source, .. } => source.status_code(),

            Error::DecodePlan { source, .. } => source.status_code(),

            Error::Internal { source, .. } => source.status_code(),

            Error::QueryAccessDenied { .. } => StatusCode::AccessDenied,
            Error::Datafusion { error, .. } => datafusion_status_code::<Self>(error, None),
            Error::ProjectViewColumns { .. } => StatusCode::EngineExecuteQuery,
            Error::TableMetadataManager { source, .. } => source.status_code(),
            Error::GetViewCache { source, .. } | Error::GetTableCache { source, .. } => {
                source.status_code()
            }
            Error::InvokeFrontend { source, .. } => source.status_code(),
            Error::FrontendNotFound { .. } | Error::MetaClientMissing { .. } => {
                StatusCode::Unexpected
            }
            Error::HandleQuery { source, .. } => source.status_code(),
            Error::ProjectSchema { source, .. } => source.status_code(),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl From<Error> for DataFusionError {
    fn from(e: Error) -> Self {
        DataFusionError::External(Box::new(e))
    }
}

#[cfg(test)]
mod tests {
    use snafu::GenerateImplicitData;

    use super::*;

    #[test]
    pub fn test_errors_to_datafusion_error() {
        let e: DataFusionError = Error::TableExists {
            table: "test_table".to_string(),
            location: Location::generate(),
        }
        .into();
        match e {
            DataFusionError::External(_) => {}
            _ => {
                panic!("catalog error should be converted to DataFusionError::Internal")
            }
        }
    }
}
