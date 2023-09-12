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

pub mod adapter;
mod metrics;
pub mod numbers;
pub mod scan;

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use common_query::logical_plan::Expr;
use common_recordbatch::SendableRecordBatchStream;
use datatypes::schema::SchemaRef;
use store_api::storage::{RegionNumber, ScanRequest};

use crate::error::{Result, UnsupportedSnafu};
use crate::metadata::{FilterPushDownType, TableId, TableInfoRef, TableType};
use crate::requests::{AlterTableRequest, DeleteRequest, InsertRequest};
use crate::stats::TableStatistics;
use crate::RegionStat;

pub type AlterContext = anymap::Map<dyn Any + Send + Sync>;

/// Table abstraction.
#[async_trait]
pub trait Table: Send + Sync {
    /// Returns the table as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Get a reference to the schema for this table
    fn schema(&self) -> SchemaRef;

    /// Get a reference to the table info.
    fn table_info(&self) -> TableInfoRef;

    /// Get the type of this table for metadata/catalog purposes.
    fn table_type(&self) -> TableType;

    /// Insert values into table.
    ///
    /// Returns number of inserted rows.
    async fn insert(&self, _request: InsertRequest) -> Result<usize> {
        UnsupportedSnafu {
            operation: "INSERT",
        }
        .fail()?
    }

    async fn scan_to_stream(&self, request: ScanRequest) -> Result<SendableRecordBatchStream>;

    /// Tests whether the table provider can make use of any or all filter expressions
    /// to optimise data retrieval.
    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<FilterPushDownType>> {
        Ok(vec![FilterPushDownType::Unsupported; filters.len()])
    }

    /// Alter table.
    async fn alter(&self, _context: AlterContext, _request: &AlterTableRequest) -> Result<()> {
        UnsupportedSnafu {
            operation: "ALTER TABLE",
        }
        .fail()?
    }

    /// Delete rows in the table.
    ///
    /// Returns number of deleted rows.
    async fn delete(&self, _request: DeleteRequest) -> Result<usize> {
        UnsupportedSnafu {
            operation: "DELETE",
        }
        .fail()?
    }

    /// Flush table.
    ///
    /// Options:
    /// - region_number: specify region to flush.
    /// - wait: Whether to wait until flush is done.
    async fn flush(&self, region_number: Option<RegionNumber>, wait: Option<bool>) -> Result<()> {
        let _ = (region_number, wait);
        UnsupportedSnafu { operation: "FLUSH" }.fail()?
    }

    /// Close the table.
    async fn close(&self, _regions: &[RegionNumber]) -> Result<()> {
        Ok(())
    }

    /// Get region stats in this table.
    fn region_stats(&self) -> Result<Vec<RegionStat>> {
        UnsupportedSnafu {
            operation: "REGION_STATS",
        }
        .fail()?
    }

    /// Return true if contains the region
    fn contains_region(&self, _region: RegionNumber) -> Result<bool> {
        UnsupportedSnafu {
            operation: "contain_region",
        }
        .fail()?
    }

    /// Get statistics for this table, if available
    fn statistics(&self) -> Option<TableStatistics> {
        None
    }

    async fn compact(&self, region_number: Option<RegionNumber>, wait: Option<bool>) -> Result<()> {
        let _ = (region_number, wait);
        UnsupportedSnafu {
            operation: "COMPACTION",
        }
        .fail()?
    }

    async fn truncate(&self) -> Result<()> {
        UnsupportedSnafu {
            operation: "TRUNCATE",
        }
        .fail()?
    }
}

pub type TableRef = Arc<dyn Table>;

#[async_trait::async_trait]
pub trait TableIdProvider {
    async fn next_table_id(&self) -> Result<TableId>;
}

pub type TableIdProviderRef = Arc<dyn TableIdProvider + Send + Sync>;
