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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use api::v1::value::ValueData;
use api::v1::{ColumnDataType, ColumnSchema, OpType, SemanticType};
use arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, Float64Array, RecordBatch, TimestampMillisecondArray,
    UInt64Array, UInt8Array,
};
use arrow::compute;
use arrow_schema::Field;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_meta::node_manager::NodeManagerRef;
use common_query::prelude::{GREPTIME_PHYSICAL_TABLE, GREPTIME_TIMESTAMP, GREPTIME_VALUE};
use itertools::Itertools;
use metric_engine::row_modifier::{RowModifier, RowsIter};
use mito_codec::row_converter::SparsePrimaryKeyCodec;
use operator::schema_helper::{
    ensure_logical_tables_for_metrics, metadatas_for_region_ids, LogicalSchema, LogicalSchemas,
    SchemaHelper,
};
use partition::manager::PartitionRuleManagerRef;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::consts::{
    ReservedColumnId, OP_TYPE_COLUMN_NAME, PRIMARY_KEY_COLUMN_NAME, SEQUENCE_COLUMN_NAME,
};
use store_api::storage::{ColumnId, RegionId};
use table::metadata::TableId;

use crate::error;
use crate::prom_row_builder::{PromCtx, TableBuilder};

pub struct MetricsBatchBuilder {
    schema_helper: SchemaHelper,
    builders: HashMap<String /*schema*/, HashMap<String /*logical table name*/, BatchEncoder>>,
    partition_manager: PartitionRuleManagerRef,
    node_manager: NodeManagerRef,
}

impl MetricsBatchBuilder {
    pub fn new(
        schema_helper: SchemaHelper,
        partition_manager: PartitionRuleManagerRef,
        node_manager: NodeManagerRef,
    ) -> Self {
        MetricsBatchBuilder {
            schema_helper,
            builders: Default::default(),
            partition_manager,
            node_manager,
        }
    }

    /// Detected the DDL requirements according to the staged table rows.
    pub async fn create_or_alter_physical_tables(
        &self,
        tables: &HashMap<PromCtx, HashMap<String, TableBuilder>>,
        query_ctx: &QueryContextRef,
    ) -> error::Result<()> {
        // Physical table name -> logical tables -> tags in logical table
        let mut tags: HashMap<String, HashMap<String, HashSet<String>>> = HashMap::default();
        let catalog = query_ctx.current_catalog();
        let schema = query_ctx.current_schema();

        for (ctx, tables) in tables {
            for (logical_table_name, table_builder) in tables {
                let physical_table_name = self
                    .determine_physical_table_name(
                        logical_table_name,
                        &ctx.physical_table,
                        catalog,
                        &schema,
                    )
                    .await?;
                tags.entry(physical_table_name)
                    .or_default()
                    .entry(logical_table_name.clone())
                    .or_default()
                    .extend(table_builder.tags().cloned());
            }
        }
        let logical_schemas = tags_to_logical_schemas(tags);
        ensure_logical_tables_for_metrics(&self.schema_helper, &logical_schemas, query_ctx)
            .await
            .context(error::OperatorSnafu)?;

        Ok(())
    }

    /// Finds physical table id for logical table.
    async fn determine_physical_table_name(
        &self,
        logical_table_name: &str,
        physical_table_name: &Option<String>,
        catalog: &str,
        schema: &str,
    ) -> error::Result<String> {
        let logical_table = self
            .schema_helper
            .get_table(catalog, schema, logical_table_name)
            .await
            .context(error::OperatorSnafu)?;
        if let Some(logical_table) = logical_table {
            // logical table already exist, just return the physical table
            let logical_table_id = logical_table.table_info().table_id();
            let physical_table_id = self
                .schema_helper
                .table_route_manager()
                .get_physical_table_id(logical_table_id)
                .await
                .context(error::CommonMetaSnafu)?;
            let physical_table = self
                .schema_helper
                .catalog_manager()
                .tables_by_ids(catalog, schema, &[physical_table_id])
                .await
                .context(error::CatalogSnafu)?
                .swap_remove(0);
            return Ok(physical_table.table_info().name.clone());
        }

        // Logical table not exist, try assign logical table to a physical table.
        let physical_table_name = physical_table_name
            .as_deref()
            .unwrap_or(GREPTIME_PHYSICAL_TABLE);
        Ok(physical_table_name.to_string())
    }

    /// Retrieves physical region metadata of given logical table names.
    ///
    /// The `logical_tables` is a list of table names, each entry contains the schema name and the table name.
    /// Returns the following mapping: `schema => logical table => (logical table id, region 0 metadata of the physical table)`.
    pub(crate) async fn collect_physical_region_metadata(
        &self,
        logical_tables: &[(String, String)],
        query_ctx: &QueryContextRef,
    ) -> error::Result<HashMap<String, HashMap<String, (TableId, RegionMetadataRef)>>> {
        let catalog = query_ctx.current_catalog();
        // Logical and physical table ids.
        let mut table_ids = Vec::with_capacity(logical_tables.len());
        let mut physical_region_ids = HashSet::new();
        for (schema, table_name) in logical_tables {
            let logical_table = self
                .schema_helper
                .get_table(catalog, schema, table_name)
                .await
                .context(error::OperatorSnafu)?
                .context(error::TableNotFoundSnafu {
                    catalog,
                    schema: schema,
                    table: table_name,
                })?;
            let logical_table_id = logical_table.table_info().table_id();
            let physical_table_id = self
                .schema_helper
                .table_route_manager()
                .get_physical_table_id(logical_table_id)
                .await
                .context(error::CommonMetaSnafu)?;
            table_ids.push((logical_table_id, physical_table_id));
            // We only get metadata from region 0.
            physical_region_ids.insert(RegionId::new(physical_table_id, 0));
        }

        // Batch get physical metadata.
        let physical_region_ids = physical_region_ids.into_iter().collect_vec();
        let region_metadatas = metadatas_for_region_ids(
            &self.partition_manager,
            &self.node_manager,
            &physical_region_ids,
            query_ctx,
        )
        .await
        .context(error::OperatorSnafu)?;
        let mut result_map: HashMap<_, HashMap<_, _>> = HashMap::new();
        let region_metadatas: HashMap<_, _> = region_metadatas
            .into_iter()
            .flatten()
            .map(|meta| (meta.region_id, Arc::new(meta)))
            .collect();
        for (i, (schema, table_name)) in logical_tables.iter().enumerate() {
            let physical_table_id = table_ids[i].1;
            let physical_region_id = RegionId::new(physical_table_id, 0);
            let physical_metadata =
                region_metadatas.get(&physical_region_id).with_context(|| {
                    error::UnexpectedResultSnafu {
                        reason: format!(
                            "Physical region metadata {} for table {} not found",
                            physical_region_id, table_name
                        ),
                    }
                })?;

            match result_map.get_mut(schema) {
                Some(table_map) => {
                    table_map.insert(
                        table_name.clone(),
                        (table_ids[i].0, physical_metadata.clone()),
                    );
                }
                None => {
                    let mut table_map = HashMap::new();
                    table_map.insert(
                        table_name.clone(),
                        (table_ids[i].0, physical_metadata.clone()),
                    );
                    result_map.insert(schema.to_string(), table_map);
                }
            }
        }

        Ok(result_map)
    }

    /// Builds [RecordBatch] from rows with primary key encoded.
    /// Potentially we also need to modify the column name of timestamp and value field to
    /// match the schema of physical tables.
    /// Note:
    /// Make sure all logical table and physical table are created when reach here and the mapping
    /// from logical table name to physical table ref is stored in [physical_region_metadata].
    pub(crate) async fn append_rows_to_batch(
        &mut self,
        current_catalog: Option<String>,
        current_schema: Option<String>,
        table_data: &mut HashMap<PromCtx, HashMap<String, TableBuilder>>,
        physical_region_metadata: &HashMap<String, HashMap<String, (TableId, RegionMetadataRef)>>,
    ) -> error::Result<()> {
        for (ctx, tables_in_schema) in table_data {
            for (logical_table_name, table) in tables_in_schema {
                // use session catalog.
                let catalog = current_catalog.as_deref().unwrap_or(DEFAULT_CATALOG_NAME);
                // schema in PromCtx precedes session schema.
                let schema = ctx
                    .schema
                    .as_deref()
                    .or(current_schema.as_deref())
                    .unwrap_or(DEFAULT_SCHEMA_NAME);
                // Look up physical region metadata by schema and table name
                let schema_metadata =
                    physical_region_metadata
                        .get(schema)
                        .context(error::TableNotFoundSnafu {
                            catalog,
                            schema,
                            table: logical_table_name,
                        })?;
                let (logical_table_id, physical_table) = schema_metadata
                    .get(logical_table_name)
                    .context(error::TableNotFoundSnafu {
                        catalog,
                        schema,
                        table: logical_table_name,
                    })?;

                let encoder = self
                    .builders
                    .entry(schema.to_string())
                    .or_default()
                    .entry(logical_table_name.clone())
                    .or_insert_with(|| Self::create_sparse_encoder(&physical_table));
                encoder.append_rows(*logical_table_id, std::mem::take(table))?;
            }
        }
        Ok(())
    }

    /// Finishes current record batch builder and returns record batches grouped by physical table id.
    pub(crate) fn finish(
        self,
    ) -> error::Result<
        HashMap<
            String, /*schema name*/
            HashMap<String /*logical table name*/, (RecordBatch, (i64, i64))>,
        >,
    > {
        let mut table_batches: HashMap<String, HashMap<String, (RecordBatch, (i64, i64))>> =
            HashMap::with_capacity(self.builders.len());

        for (schema_name, schema_tables) in self.builders {
            let schema_batches = table_batches.entry(schema_name).or_default();
            for (logical_table_name, table_data) in schema_tables {
                let rb = table_data.finish()?;
                if let Some(v) = rb {
                    schema_batches.entry(logical_table_name).insert_entry(v);
                }
            }
        }
        Ok(table_batches)
    }

    /// Creates Encoder that converts Rows into RecordBatch with primary key encoded.
    fn create_sparse_encoder(physical_region_meta: &RegionMetadataRef) -> BatchEncoder {
        let name_to_id: HashMap<_, _> = physical_region_meta
            .column_metadatas
            .iter()
            .map(|c| (c.column_schema.name.clone(), c.column_id))
            .collect();
        BatchEncoder::new(name_to_id)
    }
}

struct BatchEncoder {
    name_to_id: HashMap<String, ColumnId>,
    encoded_primary_key_array_builder: BinaryBuilder,
    timestamps: Vec<i64>,
    value: Vec<f64>,
    pk_codec: SparsePrimaryKeyCodec,
    timestamp_range: Option<(i64, i64)>,
}

impl BatchEncoder {
    fn new(name_to_id: HashMap<String, ColumnId>) -> BatchEncoder {
        Self {
            name_to_id,
            encoded_primary_key_array_builder: BinaryBuilder::with_capacity(16, 0),
            timestamps: Vec::with_capacity(16),
            value: Vec::with_capacity(16),
            pk_codec: SparsePrimaryKeyCodec::schemaless(),
            timestamp_range: None,
        }
    }

    /// Creates the schema of output record batch.
    fn schema() -> arrow::datatypes::SchemaRef {
        Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new(GREPTIME_VALUE, arrow::datatypes::DataType::Float64, false),
            Field::new(
                GREPTIME_TIMESTAMP,
                arrow::datatypes::DataType::Timestamp(
                    arrow::datatypes::TimeUnit::Millisecond,
                    None,
                ),
                false,
            ),
            Field::new(
                PRIMARY_KEY_COLUMN_NAME,
                arrow::datatypes::DataType::Binary,
                false,
            ),
            Field::new(
                SEQUENCE_COLUMN_NAME,
                arrow::datatypes::DataType::UInt64,
                false,
            ),
            Field::new(
                OP_TYPE_COLUMN_NAME,
                arrow::datatypes::DataType::UInt8,
                false,
            ),
        ]))
    }

    fn append_rows(
        &mut self,
        logical_table_id: TableId,
        mut table_builder: TableBuilder,
    ) -> error::Result<()> {
        // todo(hl): we can simplified the row iter because schema in TableBuilder is known (ts, val, tags...)
        let row_insert_request = table_builder.as_row_insert_request("don't care".to_string());

        let mut iter = RowsIter::new(row_insert_request.rows.unwrap(), &self.name_to_id);

        let mut encode_buf = vec![];
        for row in iter.iter_mut() {
            let (table_id, ts_id) = RowModifier::fill_internal_columns(logical_table_id, &row);
            let internal_columns = [
                (
                    ReservedColumnId::table_id(),
                    api::helper::pb_value_to_value_ref(&table_id, &None),
                ),
                (
                    ReservedColumnId::tsid(),
                    api::helper::pb_value_to_value_ref(&ts_id, &None),
                ),
            ];
            self.pk_codec
                .encode_to_vec(internal_columns.into_iter(), &mut encode_buf)
                .context(error::EncodePrimaryKeySnafu)?;
            self.pk_codec
                .encode_to_vec(row.primary_keys(), &mut encode_buf)
                .context(error::EncodePrimaryKeySnafu)?;
            self.encoded_primary_key_array_builder
                .append_value(&encode_buf);

            // process timestamp and field. We already know the position of timestamps and values in [TableBuilder].
            let ValueData::TimestampMillisecondValue(ts) =
                // safety: timestamp values cannot be null
                row.value_at(0).value_data.as_ref().unwrap()
            else {
                return error::InvalidTimestampValueTypeSnafu.fail();
            };
            self.timestamps.push(*ts);
            if let Some((min, max)) = &mut self.timestamp_range {
                *min = (*min).min(*ts);
                *max = (*max).max(*ts);
            } else {
                self.timestamp_range = Some((*ts, *ts));
            }

            // safety: field values cannot be null in prom remote write
            let ValueData::F64Value(val) = row.value_at(1).value_data.as_ref().unwrap() else {
                return error::InvalidFieldValueTypeSnafu.fail();
            };
            self.value.push(*val);
        }

        debug_assert_eq!(self.value.len(), self.timestamps.len());
        debug_assert_eq!(
            self.value.len(),
            self.encoded_primary_key_array_builder.len()
        );
        Ok(())
    }

    fn finish(mut self) -> error::Result<Option<(RecordBatch, (i64, i64))>> {
        if self.timestamps.is_empty() {
            return Ok(None);
        }
        let num_rows = self.timestamps.len();
        let value = Float64Array::from(self.value);
        let timestamp = TimestampMillisecondArray::from(self.timestamps);

        let op_type = Arc::new(UInt8Array::from_value(OpType::Put as u8, num_rows)) as ArrayRef;
        // todo: now we set sequence all to 0.
        let sequence = Arc::new(UInt64Array::from_value(0, num_rows)) as ArrayRef;

        let pk = self.encoded_primary_key_array_builder.finish();
        let indices = compute::sort_to_indices(&pk, None, None).context(error::ArrowSnafu)?;

        // Sort arrays
        let value = compute::take(&value, &indices, None).context(error::ArrowSnafu)?;
        let ts = compute::take(&timestamp, &indices, None).context(error::ArrowSnafu)?;
        let pk = compute::take(&pk, &indices, None).context(error::ArrowSnafu)?;
        let rb = RecordBatch::try_new(Self::schema(), vec![value, ts, pk, sequence, op_type])
            .context(error::ArrowSnafu)?;
        Ok(Some((rb, self.timestamp_range.unwrap())))
    }
}

fn tags_to_logical_schemas(
    tags: HashMap<String, HashMap<String, HashSet<String>>>,
) -> LogicalSchemas {
    let schemas: HashMap<String, Vec<LogicalSchema>> = tags
        .into_iter()
        .map(|(physical, logical_tables)| {
            let schemas: Vec<_> = logical_tables
                .into_iter()
                .map(|(logical, tags)| {
                    let mut columns: Vec<_> = tags
                        .into_iter()
                        .map(|tag_name| ColumnSchema {
                            column_name: tag_name,
                            datatype: ColumnDataType::String as i32,
                            semantic_type: SemanticType::Tag as i32,
                            ..Default::default()
                        })
                        .collect();
                    columns.push(ColumnSchema {
                        column_name: GREPTIME_TIMESTAMP.to_string(),
                        datatype: ColumnDataType::TimestampNanosecond as i32,
                        semantic_type: SemanticType::Timestamp as i32,
                        ..Default::default()
                    });
                    columns.push(ColumnSchema {
                        column_name: GREPTIME_VALUE.to_string(),
                        datatype: ColumnDataType::Float64 as i32,
                        semantic_type: SemanticType::Field as i32,
                        ..Default::default()
                    });
                    LogicalSchema {
                        name: logical,
                        columns,
                    }
                })
                .collect();
            (physical, schemas)
        })
        .collect();

    LogicalSchemas { schemas }
}
