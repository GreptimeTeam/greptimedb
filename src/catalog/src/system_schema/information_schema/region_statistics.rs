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

use arrow_schema::SchemaRef as ArrowSchemaRef;
use common_catalog::consts::INFORMATION_SCHEMA_REGION_STATISTICS_TABLE_ID;
use common_config::Mode;
use common_error::ext::BoxedError;
use common_meta::cluster::ClusterInfo;
use common_meta::datanode::RegionStat;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{DfSendableRecordBatchStream, RecordBatch, SendableRecordBatchStream};
use common_telemetry::tracing::warn;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter as DfRecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream as DfPartitionStream;
use datatypes::prelude::{ConcreteDataType, ScalarVectorBuilder, VectorRef};
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::value::Value;
use datatypes::vectors::{StringVectorBuilder, UInt64VectorBuilder};
use snafu::ResultExt;
use store_api::storage::{ScanRequest, TableId};

use super::{InformationTable, REGION_STATISTICS};
use crate::error::{CreateRecordBatchSnafu, InternalSnafu, ListRegionStatsSnafu, Result};
use crate::information_schema::Predicates;
use crate::system_schema::utils;
use crate::CatalogManager;

const REGION_ID: &str = "region_id";
const MEMTABLE_SIZE: &str = "memtable_size";
const MANIFEST_SIZE: &str = "manifest_size";
const SST_SIZE: &str = "sst_size";
const ENGINE: &str = "engine";
const REGION_ROLE: &str = "region_role";

const INIT_CAPACITY: usize = 42;

/// The `REGION_STATISTICS` table provides information about the region statistics. Including fields:
///
/// - `region_id`: The region id.
/// - `memtable_size`: The memtable size in bytes.
/// - `manifest_size`: The manifest size in bytes.
/// - `sst_size`: The sst size in bytes.
/// - `engine`: The engine type.
/// - `region_role`: The region role.
///
pub(super) struct InformationSchemaRegionStatistics {
    schema: SchemaRef,
    catalog_manager: Weak<dyn CatalogManager>,
}

impl InformationSchemaRegionStatistics {
    pub(super) fn new(catalog_manager: Weak<dyn CatalogManager>) -> Self {
        Self {
            schema: Self::schema(),
            catalog_manager,
        }
    }

    pub(crate) fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            ColumnSchema::new(REGION_ID, ConcreteDataType::uint64_datatype(), false),
            ColumnSchema::new(MEMTABLE_SIZE, ConcreteDataType::uint64_datatype(), true),
            ColumnSchema::new(MANIFEST_SIZE, ConcreteDataType::uint64_datatype(), true),
            ColumnSchema::new(SST_SIZE, ConcreteDataType::uint64_datatype(), true),
            ColumnSchema::new(ENGINE, ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(REGION_ROLE, ConcreteDataType::string_datatype(), true),
        ]))
    }

    fn builder(&self) -> InformationSchemaRegionStatisticsBuilder {
        InformationSchemaRegionStatisticsBuilder::new(
            self.schema.clone(),
            self.catalog_manager.clone(),
        )
    }
}

impl InformationTable for InformationSchemaRegionStatistics {
    fn table_id(&self) -> TableId {
        INFORMATION_SCHEMA_REGION_STATISTICS_TABLE_ID
    }

    fn table_name(&self) -> &'static str {
        REGION_STATISTICS
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn to_stream(&self, request: ScanRequest) -> Result<SendableRecordBatchStream> {
        let schema = self.schema.arrow_schema().clone();
        let mut builder = self.builder();

        let stream = Box::pin(DfRecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move {
                builder
                    .make_region_statistics(Some(request))
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ));

        Ok(Box::pin(
            RecordBatchStreamAdapter::try_new(stream)
                .map_err(BoxedError::new)
                .context(InternalSnafu)?,
        ))
    }
}

struct InformationSchemaRegionStatisticsBuilder {
    schema: SchemaRef,
    catalog_manager: Weak<dyn CatalogManager>,

    region_id: UInt64VectorBuilder,
    memtable_sizes: UInt64VectorBuilder,
    manifest_sizes: UInt64VectorBuilder,
    sst_sizes: UInt64VectorBuilder,
    engines: StringVectorBuilder,
    region_roles: StringVectorBuilder,
}

impl InformationSchemaRegionStatisticsBuilder {
    fn new(schema: SchemaRef, catalog_manager: Weak<dyn CatalogManager>) -> Self {
        Self {
            schema,
            catalog_manager,
            region_id: UInt64VectorBuilder::with_capacity(INIT_CAPACITY),
            memtable_sizes: UInt64VectorBuilder::with_capacity(INIT_CAPACITY),
            manifest_sizes: UInt64VectorBuilder::with_capacity(INIT_CAPACITY),
            sst_sizes: UInt64VectorBuilder::with_capacity(INIT_CAPACITY),
            engines: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            region_roles: StringVectorBuilder::with_capacity(INIT_CAPACITY),
        }
    }

    /// Construct a new `InformationSchemaRegionStatistics` from the collected data.
    async fn make_region_statistics(
        &mut self,
        request: Option<ScanRequest>,
    ) -> Result<RecordBatch> {
        let predicates = Predicates::from_scan_request(&request);
        let mode = utils::running_mode(&self.catalog_manager)?.unwrap_or(Mode::Standalone);

        match mode {
            Mode::Standalone => {
                // TODO(weny): implement it
            }
            Mode::Distributed => {
                if let Some(meta_client) = utils::meta_client(&self.catalog_manager)? {
                    let region_stats = meta_client
                        .list_region_stats()
                        .await
                        .map_err(BoxedError::new)
                        .context(ListRegionStatsSnafu)?;
                    for region_stat in region_stats {
                        self.add_region_statistic(&predicates, region_stat);
                    }
                } else {
                    warn!("Meta client is not available");
                }
            }
        }

        self.finish()
    }

    fn add_region_statistic(&mut self, predicate: &Predicates, region_stat: RegionStat) {
        let row = [
            (REGION_ID, &Value::from(region_stat.id.as_u64())),
            (MEMTABLE_SIZE, &Value::from(region_stat.memtable_size)),
            (MANIFEST_SIZE, &Value::from(region_stat.manifest_size)),
            (SST_SIZE, &Value::from(region_stat.sst_size)),
            (ENGINE, &Value::from(region_stat.engine.as_str())),
            (REGION_ROLE, &Value::from(region_stat.role.to_string())),
        ];

        if !predicate.eval(&row) {
            return;
        }

        self.region_id.push(Some(region_stat.id.as_u64()));
        self.memtable_sizes.push(Some(region_stat.memtable_size));
        self.manifest_sizes.push(Some(region_stat.manifest_size));
        self.sst_sizes.push(Some(region_stat.sst_size));
        self.engines.push(Some(&region_stat.engine));
        self.region_roles.push(Some(&region_stat.role.to_string()));
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let columns: Vec<VectorRef> = vec![
            Arc::new(self.region_id.finish()),
            Arc::new(self.memtable_sizes.finish()),
            Arc::new(self.manifest_sizes.finish()),
            Arc::new(self.sst_sizes.finish()),
            Arc::new(self.engines.finish()),
            Arc::new(self.region_roles.finish()),
        ];

        RecordBatch::new(self.schema.clone(), columns).context(CreateRecordBatchSnafu)
    }
}

impl DfPartitionStream for InformationSchemaRegionStatistics {
    fn schema(&self) -> &ArrowSchemaRef {
        self.schema.arrow_schema()
    }

    fn execute(&self, _: Arc<TaskContext>) -> DfSendableRecordBatchStream {
        let schema = self.schema.arrow_schema().clone();
        let mut builder = self.builder();
        Box::pin(DfRecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move {
                builder
                    .make_region_statistics(None)
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ))
    }
}
