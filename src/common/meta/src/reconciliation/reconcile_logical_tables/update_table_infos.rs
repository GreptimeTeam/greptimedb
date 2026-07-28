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
use std::collections::HashMap;

use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::info;
use serde::{Deserialize, Serialize};
use snafu::OptionExt;
use store_api::metadata::RegionMetadata;
use store_api::storage::TableId;
use table::metadata::TableInfo;
use table::table_name::TableName;
use table::table_reference::TableReference;

use crate::cache_invalidator::Context as CacheContext;
use crate::ddl::utils::raw_table_info;
use crate::ddl::utils::table_info::{
    batch_update_table_info_values, get_all_table_info_values_by_table_ids,
};
use crate::error::{Result, TableInfoNotFoundSnafu, UnexpectedSnafu};
use crate::instruction::CacheIdent;
use crate::reconciliation::reconcile_logical_tables::reconciliation_end::ReconciliationEnd;
use crate::reconciliation::reconcile_logical_tables::resolve_table_metadatas::ResolveTableMetadatas;
use crate::reconciliation::reconcile_logical_tables::{ReconcileLogicalTablesContext, State};
use crate::reconciliation::utils::{
    build_table_meta_from_column_metadatas, logical_table_info_matches_region_metadata,
    validate_logical_region_metadata,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateTableInfos;

#[async_trait::async_trait]
#[typetag::serde]
impl State for UpdateTableInfos {
    async fn next(
        &mut self,
        ctx: &mut ReconcileLogicalTablesContext,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        if ctx.persistent_ctx.update_region_metadatas.is_empty() {
            if ctx.persistent_ctx.update_table_infos.is_empty() {
                return Ok((Box::new(ReconciliationEnd), Status::executing(false)));
            }

            // A procedure persisted by an older build retained only column metadata. Resolve it
            // again rather than applying an update without authoritative ordered primary keys.
            ctx.persistent_ctx.update_table_infos.clear();
            return Ok((Box::new(ResolveTableMetadatas), Status::executing(true)));
        }

        let all_table_names = ctx
            .persistent_ctx
            .logical_table_ids
            .iter()
            .cloned()
            .zip(
                ctx.persistent_ctx
                    .logical_tables
                    .iter()
                    .map(|t| t.table_ref()),
            )
            .collect::<HashMap<_, _>>();
        let table_ids = ctx
            .persistent_ctx
            .update_region_metadatas
            .iter()
            .map(|(table_id, _)| *table_id)
            .collect::<Vec<_>>();
        let table_names = table_ids
            .iter()
            .map(|table_id| *all_table_names.get(table_id).unwrap())
            .collect::<Vec<_>>();
        let table_info_values = get_all_table_info_values_by_table_ids(
            ctx.table_metadata_manager.table_info_manager(),
            &table_ids,
            &table_names,
        )
        .await?;
        let physical_table_info = ctx
            .table_metadata_manager
            .table_info_manager()
            .get(ctx.table_id())
            .await?
            .with_context(|| TableInfoNotFoundSnafu {
                table: format!("table id - {}", ctx.table_id()),
            })?;
        let physical_table_info = &physical_table_info.table_info;

        let mut table_info_values_to_update =
            Vec::with_capacity(ctx.persistent_ctx.update_region_metadatas.len());
        let mut updated_table_ids =
            Vec::with_capacity(ctx.persistent_ctx.update_region_metadatas.len());
        for ((table_id, region_metadata), table_info_value) in ctx
            .persistent_ctx
            .update_region_metadatas
            .iter()
            .zip(table_info_values)
        {
            if let Some(new_table_info) = Self::build_new_table_info(
                *table_id,
                region_metadata,
                &table_info_value.table_info,
                physical_table_info,
            )? {
                updated_table_ids.push(*table_id);
                table_info_values_to_update.push((table_info_value, new_table_info));
            }
        }
        let table_id = ctx.table_id();
        let table_name = ctx.table_name();

        let updated_table_info_num = table_info_values_to_update.len();
        if updated_table_info_num == 0 {
            ctx.persistent_ctx.update_table_infos.clear();
            ctx.persistent_ctx.update_region_metadatas.clear();
            return Ok((Box::new(ReconciliationEnd), Status::executing(false)));
        }
        batch_update_table_info_values(&ctx.table_metadata_manager, table_info_values_to_update)
            .await?;

        info!(
            "Updated table infos for logical tables: {:?}, physical table: {}, table_id: {}",
            ctx.persistent_ctx
                .update_region_metadatas
                .iter()
                .map(|(table_id, _)| table_id)
                .collect::<Vec<_>>(),
            table_id,
            table_name,
        );

        let cache_ctx = CacheContext {
            subject: Some(format!(
                "Invalidate table by reconcile logical tables, physical_table_id: {}",
                table_id
            )),
        };
        let updated_table_names = updated_table_ids
            .iter()
            .map(|table_id| *all_table_names.get(table_id).unwrap())
            .collect::<Vec<_>>();
        let idents = Self::build_cache_ident_keys(
            table_id,
            table_name,
            &updated_table_ids,
            &updated_table_names,
        );
        ctx.cache_invalidator
            .invalidate(&cache_ctx, &idents)
            .await?;

        ctx.persistent_ctx.update_table_infos.clear();
        ctx.persistent_ctx.update_region_metadatas.clear();
        // Update metrics.
        let metrics = ctx.mut_metrics();
        metrics.update_table_info_count = updated_table_info_num;
        Ok((Box::new(ReconciliationEnd), Status::executing(false)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl UpdateTableInfos {
    fn build_new_table_info(
        table_id: TableId,
        region_metadata: &RegionMetadata,
        table_info: &TableInfo,
        physical_table_info: &TableInfo,
    ) -> Result<Option<TableInfo>> {
        validate_logical_region_metadata(physical_table_info, region_metadata)?;

        // Validate or restore the persisted mapping before deciding whether this is a genuine
        // schema rebuild. This makes complete conflicts fail closed even if metadata changed
        // between resolution and the CAS update.
        let mut new_table_info = table_info.clone();
        raw_table_info::populate_logical_table_column_ids(
            physical_table_info,
            &mut new_table_info,
        )?;

        if logical_table_info_matches_region_metadata(&new_table_info, region_metadata) {
            if new_table_info.meta.column_ids == table_info.meta.column_ids {
                return Ok(None);
            }
            new_table_info.ident.version = table_info.ident.version + 1;
            return Ok(Some(new_table_info));
        }

        let partition_key_names = Self::partition_key_names(table_info)?;
        let table_ref = table_info.table_ref();
        let table_meta = build_table_meta_from_column_metadatas(
            table_id,
            table_ref,
            &table_info.meta,
            None,
            &region_metadata.column_metadatas,
        )?;

        new_table_info.meta = table_meta;
        new_table_info.sort_columns();
        Self::remap_sorted_roles(&mut new_table_info, region_metadata, &partition_key_names)?;
        raw_table_info::populate_logical_table_column_ids(
            physical_table_info,
            &mut new_table_info,
        )?;
        new_table_info.ident.version = table_info.ident.version + 1;

        Ok(Some(new_table_info))
    }

    fn partition_key_names(table_info: &TableInfo) -> Result<Vec<String>> {
        let columns = table_info.meta.schema.column_schemas();
        table_info
            .meta
            .partition_key_indices
            .iter()
            .map(|index| {
                columns
                    .get(*index)
                    .map(|column| column.name.clone())
                    .with_context(|| UnexpectedSnafu {
                        err_msg: format!(
                            "Logical table {} ({}) has invalid partition key index {}",
                            table_info.name, table_info.ident.table_id, index
                        ),
                    })
            })
            .collect()
    }

    fn remap_sorted_roles(
        table_info: &mut TableInfo,
        region_metadata: &RegionMetadata,
        partition_key_names: &[String],
    ) -> Result<()> {
        let index_by_name = table_info
            .meta
            .schema
            .column_schemas()
            .iter()
            .enumerate()
            .map(|(index, column)| (column.name.as_str(), index))
            .collect::<HashMap<_, _>>();
        let name_by_id = region_metadata
            .column_metadatas
            .iter()
            .map(|column| (column.column_id, column.column_schema.name.as_str()))
            .collect::<HashMap<_, _>>();

        table_info.meta.primary_key_indices = region_metadata
            .primary_key
            .iter()
            .map(|column_id| {
                let column_name = name_by_id.get(column_id).with_context(|| UnexpectedSnafu {
                    err_msg: format!(
                        "Logical region primary-key ID {} has no column metadata",
                        column_id
                    ),
                })?;
                index_by_name
                    .get(*column_name)
                    .copied()
                    .with_context(|| UnexpectedSnafu {
                        err_msg: format!(
                            "Logical region primary-key column {} is missing after rebuild",
                            column_name
                        ),
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        table_info.meta.partition_key_indices = partition_key_names
            .iter()
            .map(|column_name| {
                index_by_name
                    .get(column_name.as_str())
                    .copied()
                    .with_context(|| UnexpectedSnafu {
                        err_msg: format!(
                            "Logical partition-key column {} is missing after rebuild",
                            column_name
                        ),
                    })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(())
    }

    fn build_cache_ident_keys(
        physical_table_id: TableId,
        physical_table_name: &TableName,
        table_ids: &[TableId],
        table_names: &[TableReference],
    ) -> Vec<CacheIdent> {
        let mut cache_keys = Vec::with_capacity(table_ids.len() * 2 + 2);
        cache_keys.push(CacheIdent::TableId(physical_table_id));
        cache_keys.push(CacheIdent::TableName(physical_table_name.clone()));
        cache_keys.extend(
            table_ids
                .iter()
                .map(|table_id| CacheIdent::TableId(*table_id)),
        );
        cache_keys.extend(
            table_names
                .iter()
                .map(|table_ref| CacheIdent::TableName((*table_ref).into())),
        );

        cache_keys
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::SemanticType;
    use datatypes::schema::{ColumnSchema, Schema};
    use store_api::metadata::{ColumnMetadata, RegionMetadata};
    use store_api::storage::RegionId;

    use super::*;
    use crate::ddl::test_util::region_metadata::build_region_metadata;
    use crate::ddl::test_util::{
        test_create_logical_table_task, test_create_physical_table_task, test_physical_table_info,
    };

    fn table_infos() -> (TableInfo, TableInfo) {
        let physical =
            test_physical_table_info(test_create_physical_table_task("physical").table_info);
        let logical = test_create_logical_table_task("logical").table_info;
        (physical, logical)
    }

    fn logical_column_metadatas(
        physical_table_info: &TableInfo,
        logical_table_info: &TableInfo,
    ) -> Vec<ColumnMetadata> {
        let column_ids = raw_table_info::logical_column_ids_from_physical_table_info(
            physical_table_info,
            logical_table_info.meta.schema.column_schemas(),
        )
        .unwrap();
        logical_table_info
            .meta
            .schema
            .column_schemas()
            .iter()
            .zip(column_ids)
            .enumerate()
            .map(|(index, (column_schema, column_id))| ColumnMetadata {
                column_schema: column_schema.clone(),
                semantic_type: if logical_table_info.meta.primary_key_indices.contains(&index) {
                    SemanticType::Tag
                } else if column_schema.is_time_index() {
                    SemanticType::Timestamp
                } else {
                    SemanticType::Field
                },
                column_id,
            })
            .collect()
    }

    fn logical_region_metadata(
        physical_table_info: &TableInfo,
        logical_table_info: &TableInfo,
    ) -> RegionMetadata {
        let column_metadatas = logical_column_metadatas(physical_table_info, logical_table_info);
        build_region_metadata(
            RegionId::new(logical_table_info.ident.table_id, 0),
            &column_metadatas,
        )
    }

    #[test]
    fn test_id_only_repair_replaces_empty_and_partial_ids() {
        let (physical, logical) = table_infos();
        let region_metadata = logical_region_metadata(&physical, &logical);
        let expected_column_ids = raw_table_info::logical_column_ids_from_physical_table_info(
            &physical,
            logical.meta.schema.column_schemas(),
        )
        .unwrap();

        for column_ids in [
            vec![],
            expected_column_ids[..1].to_vec(),
            expected_column_ids.iter().copied().chain([999]).collect(),
        ] {
            let mut legacy = logical.clone();
            legacy.meta.column_ids = column_ids;
            let new_table_info = UpdateTableInfos::build_new_table_info(
                legacy.ident.table_id,
                &region_metadata,
                &legacy,
                &physical,
            )
            .unwrap()
            .unwrap();

            assert_eq!(new_table_info.meta.schema, legacy.meta.schema);
            assert_eq!(new_table_info.meta.column_ids, expected_column_ids);
            assert_eq!(new_table_info.ident.version, legacy.ident.version + 1);
            let mut expected_meta = legacy.meta.clone();
            expected_meta.column_ids = expected_column_ids.clone();
            assert_eq!(new_table_info.meta, expected_meta);
        }
    }

    #[test]
    fn test_id_only_repair_skips_complete_matching_ids() {
        let (physical, mut logical) = table_infos();
        logical.meta.column_ids = raw_table_info::logical_column_ids_from_physical_table_info(
            &physical,
            logical.meta.schema.column_schemas(),
        )
        .unwrap();
        let region_metadata = logical_region_metadata(&physical, &logical);

        assert!(
            UpdateTableInfos::build_new_table_info(
                logical.ident.table_id,
                &region_metadata,
                &logical,
                &physical,
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn test_reordered_datanode_columns_do_not_rebuild_matching_table() {
        let (physical, mut logical) = table_infos();
        raw_table_info::populate_logical_table_column_ids(&physical, &mut logical).unwrap();
        let mut region_metadata = logical_region_metadata(&physical, &logical);
        region_metadata.column_metadatas.reverse();

        assert!(
            UpdateTableInfos::build_new_table_info(
                logical.ident.table_id,
                &region_metadata,
                &logical,
                &physical,
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn test_primary_key_membership_and_order_follow_region_authority() {
        let (physical, mut logical) = table_infos();
        let cpu_index = logical
            .meta
            .schema
            .column_schemas()
            .iter()
            .position(|column| column.name == "cpu")
            .unwrap();
        logical.meta.primary_key_indices.push(cpu_index);
        raw_table_info::populate_logical_table_column_ids(&physical, &mut logical).unwrap();
        let mut region_metadata = logical_region_metadata(&physical, &logical);
        region_metadata.primary_key.reverse();

        let new_table_info = UpdateTableInfos::build_new_table_info(
            logical.ident.table_id,
            &region_metadata,
            &logical,
            &physical,
        )
        .unwrap()
        .unwrap();
        let primary_key_names = new_table_info
            .meta
            .primary_key_indices
            .iter()
            .map(|index| {
                new_table_info.meta.schema.column_schemas()[*index]
                    .name
                    .as_str()
            })
            .collect::<Vec<_>>();
        assert_eq!(primary_key_names, vec!["cpu", "host"]);
    }

    #[test]
    fn test_id_only_repair_rejects_complete_conflicting_ids() {
        let (physical, mut logical) = table_infos();
        logical.meta.column_ids = raw_table_info::logical_column_ids_from_physical_table_info(
            &physical,
            logical.meta.schema.column_schemas(),
        )
        .unwrap();
        logical.meta.column_ids[0] += 100;
        let region_metadata = logical_region_metadata(&physical, &logical);

        assert!(
            UpdateTableInfos::build_new_table_info(
                logical.ident.table_id,
                &region_metadata,
                &logical,
                &physical,
            )
            .is_err()
        );
    }

    #[test]
    fn test_id_only_repair_rejects_datanode_and_physical_id_mismatch() {
        let (physical, logical) = table_infos();
        let mut region_metadata = logical_region_metadata(&physical, &logical);
        region_metadata.column_metadatas[0].column_id += 100;

        assert!(
            UpdateTableInfos::build_new_table_info(
                logical.ident.table_id,
                &region_metadata,
                &logical,
                &physical,
            )
            .is_err()
        );
    }

    #[test]
    fn test_schema_mismatch_rebuilds_then_enforces_physical_ids() {
        let (physical, mut stale_logical) = table_infos();
        raw_table_info::populate_logical_table_column_ids(&physical, &mut stale_logical).unwrap();
        let region_metadata = logical_region_metadata(&physical, &stale_logical);
        let primary_key_names = stale_logical
            .meta
            .primary_key_indices
            .iter()
            .map(|index| {
                stale_logical.meta.schema.column_schemas()[*index]
                    .name
                    .clone()
            })
            .collect::<Vec<_>>();
        let mut columns = stale_logical.meta.schema.column_schemas().to_vec();
        let cpu_index = columns
            .iter()
            .position(|column| column.name == "cpu")
            .unwrap();
        columns.remove(cpu_index);
        stale_logical.meta.schema = Arc::new(Schema::new(columns));
        stale_logical.meta.primary_key_indices = stale_logical
            .meta
            .schema
            .column_schemas()
            .iter()
            .enumerate()
            .filter_map(|(index, column)| primary_key_names.contains(&column.name).then_some(index))
            .collect();
        stale_logical.meta.column_ids =
            raw_table_info::logical_column_ids_from_physical_table_info(
                &physical,
                stale_logical.meta.schema.column_schemas(),
            )
            .unwrap();

        let new_table_info = UpdateTableInfos::build_new_table_info(
            stale_logical.ident.table_id,
            &region_metadata,
            &stale_logical,
            &physical,
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            new_table_info.meta.schema.column_schemas().len(),
            region_metadata.column_metadatas.len()
        );
        assert!(
            new_table_info
                .meta
                .schema
                .column_schemas()
                .iter()
                .any(|column| column.name == "cpu")
        );
        assert_eq!(
            new_table_info.meta.column_ids,
            raw_table_info::logical_column_ids_from_physical_table_info(
                &physical,
                new_table_info.meta.schema.column_schemas(),
            )
            .unwrap()
        );
    }

    #[test]
    fn test_schema_mismatch_rebuilds_type_and_nullability_from_region_authority() {
        let (physical, mut stale_logical) = table_infos();
        raw_table_info::populate_logical_table_column_ids(&physical, &mut stale_logical).unwrap();
        let region_metadata = logical_region_metadata(&physical, &stale_logical);
        let mut columns = stale_logical.meta.schema.column_schemas().to_vec();
        let cpu_index = columns
            .iter()
            .position(|column| column.name == "cpu")
            .unwrap();
        let cpu = columns[cpu_index].clone();
        columns[cpu_index] =
            ColumnSchema::new(cpu.name.clone(), cpu.data_type.clone(), !cpu.is_nullable());
        stale_logical.meta.schema = Arc::new(Schema::new(columns));

        let new_table_info = UpdateTableInfos::build_new_table_info(
            stale_logical.ident.table_id,
            &region_metadata,
            &stale_logical,
            &physical,
        )
        .unwrap()
        .unwrap();

        let expected_cpu = region_metadata
            .column_metadatas
            .iter()
            .find(|column| column.column_schema.name == "cpu")
            .unwrap();
        let rebuilt_cpu = new_table_info
            .meta
            .schema
            .column_schemas()
            .iter()
            .find(|column| column.name == "cpu")
            .unwrap();
        assert_eq!(rebuilt_cpu, &expected_cpu.column_schema);
    }

    #[test]
    fn test_schema_rebuild_preserves_ordered_partition_keys() {
        let (physical, mut stale_logical) = table_infos();
        raw_table_info::populate_logical_table_column_ids(&physical, &mut stale_logical).unwrap();
        let region_metadata = logical_region_metadata(&physical, &stale_logical);
        let columns = stale_logical.meta.schema.column_schemas();
        let host_index = columns
            .iter()
            .position(|column| column.name == "host")
            .unwrap();
        let cpu_index = columns
            .iter()
            .position(|column| column.name == "cpu")
            .unwrap();
        stale_logical.meta.partition_key_indices = vec![host_index, cpu_index];
        // Preserve the schema while making the persisted primary-key role stale.
        stale_logical.meta.primary_key_indices.clear();

        let new_table_info = UpdateTableInfos::build_new_table_info(
            stale_logical.ident.table_id,
            &region_metadata,
            &stale_logical,
            &physical,
        )
        .unwrap()
        .unwrap();

        let partition_names = new_table_info
            .meta
            .partition_key_indices
            .iter()
            .map(|index| {
                new_table_info.meta.schema.column_schemas()[*index]
                    .name
                    .as_str()
            })
            .collect::<Vec<_>>();
        assert_eq!(partition_names, vec!["host", "cpu"]);
    }
}
