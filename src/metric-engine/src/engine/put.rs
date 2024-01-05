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

use std::hash::{BuildHasher, Hash, Hasher};

use api::v1::value::ValueData;
use api::v1::{ColumnDataType, ColumnSchema, Row, Rows, SemanticType};
use common_telemetry::{error, info};
use snafu::OptionExt;
use store_api::metric_engine_consts::{
    DATA_SCHEMA_TABLE_ID_COLUMN_NAME, DATA_SCHEMA_TSID_COLUMN_NAME,
};
use store_api::region_request::{AffectedRows, RegionPutRequest};
use store_api::storage::{RegionId, TableId};

use crate::engine::MetricEngineInner;
use crate::error::{
    ColumnNotFoundSnafu, ForbiddenPhysicalAlterSnafu, LogicalRegionNotFoundSnafu, Result,
};
use crate::metrics::FORBIDDEN_OPERATION_COUNT;
use crate::utils::{to_data_region_id, to_metadata_region_id};

// A random number
const TSID_HASH_SEED: u32 = 846793005;

impl MetricEngineInner {
    /// Dispatch region put request
    pub async fn put_region(
        &self,
        region_id: RegionId,
        request: RegionPutRequest,
    ) -> Result<AffectedRows> {
        let is_putting_physical_region = self
            .state
            .read()
            .unwrap()
            .physical_regions()
            .contains_key(&region_id);

        if is_putting_physical_region {
            info!(
                "Metric region received put request {request:?} on physical region {region_id:?}"
            );
            FORBIDDEN_OPERATION_COUNT.inc();

            ForbiddenPhysicalAlterSnafu.fail()
        } else {
            self.put_logical_region(region_id, request).await
        }
    }

    async fn put_logical_region(
        &self,
        logical_region_id: RegionId,
        mut request: RegionPutRequest,
    ) -> Result<AffectedRows> {
        let physical_region_id = *self
            .state
            .read()
            .unwrap()
            .logical_regions()
            .get(&logical_region_id)
            .with_context(|| LogicalRegionNotFoundSnafu {
                region_id: logical_region_id,
            })?;
        let data_region_id = to_data_region_id(physical_region_id);

        self.verify_put_request(logical_region_id, physical_region_id, &request)
            .await?;

        // write to data region
        // TODO: retrieve table name
        self.modify_rows(logical_region_id.table_id(), &mut request.rows)?;
        self.data_region.write_data(data_region_id, request).await
    }

    /// Verifies a put request for a logical region against its corresponding metadata region.
    ///
    /// Includes:
    /// - Check if the logical region exists
    /// - Check if the columns exist
    async fn verify_put_request(
        &self,
        logical_region_id: RegionId,
        physical_region_id: RegionId,
        request: &RegionPutRequest,
    ) -> Result<()> {
        // check if the region exists
        let metadata_region_id = to_metadata_region_id(physical_region_id);
        if !self
            .metadata_region
            .is_logical_region_exists(metadata_region_id, logical_region_id)
            .await?
        {
            error!("Trying to write to an nonexistent region {logical_region_id}");
            return LogicalRegionNotFoundSnafu {
                region_id: logical_region_id,
            }
            .fail();
        }

        // check if the columns exist
        for col in &request.rows.schema {
            if self
                .metadata_region
                .column_semantic_type(metadata_region_id, logical_region_id, &col.column_name)
                .await?
                .is_none()
            {
                return ColumnNotFoundSnafu {
                    name: col.column_name.clone(),
                    region_id: logical_region_id,
                }
                .fail();
            }
        }

        Ok(())
    }

    /// Perform metric engine specific logic to incoming rows.
    /// - Change the semantic type of tag columns to field
    /// - Add table_id column
    /// - Generate tsid
    fn modify_rows(&self, table_id: TableId, rows: &mut Rows) -> Result<()> {
        // gather tag column indices
        let tag_col_indices = rows
            .schema
            .iter()
            .enumerate()
            .filter_map(|(idx, col)| {
                if col.semantic_type == SemanticType::Tag as i32 {
                    Some((idx, col.column_name.clone()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        // generate new schema
        rows.schema = rows
            .schema
            .clone()
            .into_iter()
            .map(|mut col| {
                if col.semantic_type == SemanticType::Tag as i32 {
                    col.semantic_type = SemanticType::Field as i32;
                }
                col
            })
            .collect::<Vec<_>>();
        // add table_name column
        rows.schema.push(ColumnSchema {
            column_name: DATA_SCHEMA_TABLE_ID_COLUMN_NAME.to_string(),
            datatype: ColumnDataType::Uint32 as i32,
            semantic_type: SemanticType::Tag as _,
            datatype_extension: None,
        });
        // add tsid column
        rows.schema.push(ColumnSchema {
            column_name: DATA_SCHEMA_TSID_COLUMN_NAME.to_string(),
            datatype: ColumnDataType::Uint64 as i32,
            semantic_type: SemanticType::Tag as _,
            datatype_extension: None,
        });

        // fill internal columns
        for row in &mut rows.rows {
            Self::fill_internal_columns(table_id, &tag_col_indices, row);
        }

        Ok(())
    }

    /// Fills internal columns of a row with table name and a hash of tag values.
    fn fill_internal_columns(
        table_id: TableId,
        tag_col_indices: &[(usize, String)],
        row: &mut Row,
    ) {
        let mut hasher = mur3::Hasher128::with_seed(TSID_HASH_SEED);
        for (idx, name) in tag_col_indices {
            let tag = row.values[*idx].clone();
            name.hash(&mut hasher);
            // The type is checked before. So only null is ignored.
            if let Some(ValueData::StringValue(string)) = tag.value_data {
                string.hash(&mut hasher);
            }
        }
        // TSID is 64 bits, simply truncate the 128 bits hash
        let (hash, _) = hasher.finish128();

        // fill table id and tsid
        row.values.push(ValueData::U32Value(table_id).into());
        row.values.push(ValueData::U64Value(hash).into());
    }
}

#[cfg(test)]
mod tests {
    use common_recordbatch::RecordBatches;
    use store_api::region_engine::RegionEngine;
    use store_api::region_request::RegionRequest;
    use store_api::storage::ScanRequest;

    use super::*;
    use crate::test_util::{self, TestEnv};

    #[tokio::test]
    async fn test_write_logical_region() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;

        // prepare data
        let schema = test_util::row_schema_with_tags(&["job"]);
        let rows = test_util::build_rows(1, 5);
        let request = RegionRequest::Put(RegionPutRequest {
            rows: Rows { schema, rows },
        });

        // write data
        let logical_region_id = env.default_logical_region_id();
        let count = env
            .metric()
            .handle_request(logical_region_id, request)
            .await
            .unwrap();
        assert_eq!(count, 5);

        // read data from physical region
        let physical_region_id = env.default_physical_region_id();
        let request = ScanRequest::default();
        let stream = env
            .metric()
            .handle_query(physical_region_id, request)
            .await
            .unwrap();
        let batches = RecordBatches::try_collect(stream).await.unwrap();
        let expected = "\
+-------------------------+----------------+------------+----------------------+-------+
| greptime_timestamp      | greptime_value | __table_id | __tsid               | job   |
+-------------------------+----------------+------------+----------------------+-------+
| 1970-01-01T00:00:00     | 0.0            | 3          | 12881218023286672757 | tag_0 |
| 1970-01-01T00:00:00.001 | 1.0            | 3          | 12881218023286672757 | tag_0 |
| 1970-01-01T00:00:00.002 | 2.0            | 3          | 12881218023286672757 | tag_0 |
| 1970-01-01T00:00:00.003 | 3.0            | 3          | 12881218023286672757 | tag_0 |
| 1970-01-01T00:00:00.004 | 4.0            | 3          | 12881218023286672757 | tag_0 |
+-------------------------+----------------+------------+----------------------+-------+";
        assert_eq!(expected, batches.pretty_print().unwrap(), "physical region");

        // read data from logical region
        let request = ScanRequest::default();
        let stream = env
            .metric()
            .handle_query(logical_region_id, request)
            .await
            .unwrap();
        let batches = RecordBatches::try_collect(stream).await.unwrap();
        let expected = "\
+-------------------------+----------------+-------+
| greptime_timestamp      | greptime_value | job   |
+-------------------------+----------------+-------+
| 1970-01-01T00:00:00     | 0.0            | tag_0 |
| 1970-01-01T00:00:00.001 | 1.0            | tag_0 |
| 1970-01-01T00:00:00.002 | 2.0            | tag_0 |
| 1970-01-01T00:00:00.003 | 3.0            | tag_0 |
| 1970-01-01T00:00:00.004 | 4.0            | tag_0 |
+-------------------------+----------------+-------+";
        assert_eq!(expected, batches.pretty_print().unwrap(), "logical region");
    }

    #[tokio::test]
    async fn test_write_logical_region_row_count() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;
        let engine = env.metric();

        // add columns
        let logical_region_id = env.default_logical_region_id();
        let columns = &["odd", "even", "Ev_En"];
        let alter_request = test_util::alter_logical_region_add_tag_columns(123456, columns);
        engine
            .handle_request(logical_region_id, RegionRequest::Alter(alter_request))
            .await
            .unwrap();

        // prepare data
        let schema = test_util::row_schema_with_tags(columns);
        let rows = test_util::build_rows(3, 100);
        let request = RegionRequest::Put(RegionPutRequest {
            rows: Rows { schema, rows },
        });

        // write data
        let count = engine
            .handle_request(logical_region_id, request)
            .await
            .unwrap();
        assert_eq!(100, count);
    }

    #[tokio::test]
    async fn test_write_physical_region() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;
        let engine = env.metric();

        let physical_region_id = env.default_physical_region_id();
        let schema = test_util::row_schema_with_tags(&["abc"]);
        let rows = test_util::build_rows(1, 100);
        let request = RegionRequest::Put(RegionPutRequest {
            rows: Rows { schema, rows },
        });

        engine
            .handle_request(physical_region_id, request)
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn test_write_nonexist_logical_region() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;
        let engine = env.metric();

        let logical_region_id = RegionId::new(175, 8345);
        let schema = test_util::row_schema_with_tags(&["def"]);
        let rows = test_util::build_rows(1, 100);
        let request = RegionRequest::Put(RegionPutRequest {
            rows: Rows { schema, rows },
        });

        engine
            .handle_request(logical_region_id, request)
            .await
            .unwrap_err();
    }
}
