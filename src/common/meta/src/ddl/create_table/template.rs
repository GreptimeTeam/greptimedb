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

use api::v1::column_def::try_as_column_def;
use api::v1::meta::Partition;
use api::v1::region::{CreateRequest, RegionColumnDef};
use api::v1::{ColumnDef, CreateTableExpr, SemanticType};
use common_telemetry::warn;
use snafu::{OptionExt, ResultExt};
use store_api::metric_engine_consts::LOGICAL_TABLE_METADATA_KEY;
use store_api::storage::{RegionId, RegionNumber};
use table::metadata::{RawTableInfo, TableId};

use crate::error::{self, Result};
use crate::reconciliation::utils::build_column_metadata_from_table_info;
use crate::wal_provider::prepare_wal_options;

/// Constructs a [CreateRequest] based on the provided [RawTableInfo].
///
/// Note: This function is primarily intended for creating logical tables.
///
/// Logical table templates keep the original column order and primary key indices from
/// `RawTableInfo` (including internal columns when present), because these are used to
/// reconstruct the logical schema on the engine side.
pub fn build_template_from_raw_table_info(raw_table_info: &RawTableInfo) -> Result<CreateRequest> {
    let primary_key_indices = &raw_table_info.meta.primary_key_indices;
    let column_defs = raw_table_info
        .meta
        .schema
        .column_schemas
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let is_primary_key = primary_key_indices.contains(&i);
            let column_def = try_as_column_def(c, is_primary_key)
                .context(error::ConvertColumnDefSnafu { column: &c.name })?;
            Ok(RegionColumnDef {
                column_def: Some(column_def),
                // The column id will be overridden by the metric engine.
                // So we just use the index as the column id.
                column_id: i as u32,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let options = HashMap::from(&raw_table_info.meta.options);
    let template = CreateRequest {
        region_id: 0,
        engine: raw_table_info.meta.engine.clone(),
        column_defs,
        primary_key: raw_table_info
            .meta
            .primary_key_indices
            .iter()
            .map(|i| *i as u32)
            .collect(),
        path: String::new(),
        options,
        partition: None,
    };

    Ok(template)
}

/// Constructs a [CreateRequest] based on the provided [RawTableInfo] for physical table.
///
/// Note: This function is primarily intended for creating physical table.
///
/// Physical table templates mark primary
/// keys by tag semantic type to match the physical storage layout.
pub fn build_template_from_raw_table_info_for_physical_table(
    raw_table_info: &RawTableInfo,
) -> Result<CreateRequest> {
    let name_to_ids = raw_table_info
        .name_to_ids()
        .context(error::MissingColumnIdsSnafu)?;
    let column_metadatas = build_column_metadata_from_table_info(
        &raw_table_info.meta.schema.column_schemas,
        &raw_table_info.meta.primary_key_indices,
        &name_to_ids,
    )?;
    let primary_keys = column_metadatas
        .iter()
        .filter(|c| c.semantic_type == SemanticType::Tag)
        .map(|c| c.column_schema.name.clone())
        .collect::<HashSet<_>>();
    let (primary_key, column_defs): (Vec<_>, Vec<_>) = column_metadatas
        .iter()
        .map(|c| {
            let column_def = try_as_column_def(
                &c.column_schema,
                primary_keys.contains(&c.column_schema.name),
            )
            .context(error::ConvertColumnDefSnafu {
                column: &c.column_schema.name,
            })?;
            let region_column_def = RegionColumnDef {
                column_def: Some(column_def),
                column_id: c.column_id,
            };

            Ok((c.column_id, region_column_def))
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .unzip();

    let options = HashMap::from(&raw_table_info.meta.options);
    let template = CreateRequest {
        region_id: 0,
        engine: raw_table_info.meta.engine.clone(),
        column_defs,
        primary_key,
        path: String::new(),
        options,
        partition: None,
    };

    Ok(template)
}

pub(crate) fn build_template(create_table_expr: &CreateTableExpr) -> Result<CreateRequest> {
    let column_defs = create_table_expr
        .column_defs
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let semantic_type = if create_table_expr.time_index == c.name {
                SemanticType::Timestamp
            } else if create_table_expr.primary_keys.contains(&c.name) {
                SemanticType::Tag
            } else {
                SemanticType::Field
            };

            RegionColumnDef {
                column_def: Some(ColumnDef {
                    name: c.name.clone(),
                    data_type: c.data_type,
                    is_nullable: c.is_nullable,
                    default_constraint: c.default_constraint.clone(),
                    semantic_type: semantic_type as i32,
                    comment: String::new(),
                    datatype_extension: c.datatype_extension.clone(),
                    options: c.options.clone(),
                }),
                column_id: i as u32,
            }
        })
        .collect::<Vec<_>>();

    let primary_key = create_table_expr
        .primary_keys
        .iter()
        .map(|key| {
            column_defs
                .iter()
                .find_map(|c| {
                    c.column_def.as_ref().and_then(|x| {
                        if &x.name == key {
                            Some(c.column_id)
                        } else {
                            None
                        }
                    })
                })
                .context(error::PrimaryKeyNotFoundSnafu { key })
        })
        .collect::<Result<_>>()?;

    let template = CreateRequest {
        region_id: 0,
        engine: create_table_expr.engine.clone(),
        column_defs,
        primary_key,
        path: String::new(),
        options: create_table_expr.table_options.clone(),
        partition: None,
    };

    Ok(template)
}

/// Builder for [PbCreateRegionRequest].
pub struct CreateRequestBuilder {
    template: CreateRequest,
    /// Optional. Only for metric engine.
    physical_table_id: Option<TableId>,
}

impl CreateRequestBuilder {
    pub fn new(template: CreateRequest, physical_table_id: Option<TableId>) -> Self {
        Self {
            template,
            physical_table_id,
        }
    }

    pub fn template(&self) -> &CreateRequest {
        &self.template
    }

    pub fn build_one(
        &self,
        region_id: RegionId,
        storage_path: String,
        region_wal_options: &HashMap<RegionNumber, String>,
        partition_exprs: &HashMap<RegionNumber, String>,
    ) -> CreateRequest {
        let mut request = self.template.clone();

        request.region_id = region_id.as_u64();
        request.path = storage_path;
        // Stores the encoded wal options into the request options.
        prepare_wal_options(&mut request.options, region_id, region_wal_options);
        request.partition = Some(prepare_partition_expr(region_id, partition_exprs));

        if let Some(physical_table_id) = self.physical_table_id {
            // Logical table has the same region numbers with physical table, and they have a one-to-one mapping.
            // For example, region 0 of logical table must resides with region 0 of physical table. So here we can
            // simply concat the physical table id and the logical region number to get the physical region id.
            let physical_region_id = RegionId::new(physical_table_id, region_id.region_number());

            request.options.insert(
                LOGICAL_TABLE_METADATA_KEY.to_string(),
                physical_region_id.as_u64().to_string(),
            );
        }

        request
    }
}

fn prepare_partition_expr(
    region_id: RegionId,
    partition_exprs: &HashMap<RegionNumber, String>,
) -> Partition {
    let expr = partition_exprs.get(&region_id.region_number()).cloned();
    if expr.is_none() {
        warn!("region {} has no partition expr", region_id);
    }

    Partition {
        expression: expr.unwrap_or_default(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use store_api::storage::{RegionId, RegionNumber};

    use super::*;

    #[test]
    fn test_build_one_sets_partition_expr_per_region() {
        // minimal template
        let template = CreateRequest {
            region_id: 0,
            engine: "mito".to_string(),
            column_defs: vec![],
            primary_key: vec![],
            path: String::new(),
            options: Default::default(),
            partition: None,
        };
        let builder = CreateRequestBuilder::new(template, None);

        let mut partition_exprs: HashMap<RegionNumber, String> = HashMap::new();
        let expr_a =
            r#"{"Expr":{"lhs":{"Column":"a"},"op":"Eq","rhs":{"Value":{"UInt32":1}}}}"#.to_string();
        partition_exprs.insert(0, expr_a.clone());

        let r0 = builder.build_one(
            RegionId::new(42, 0),
            "/p".to_string(),
            &Default::default(),
            &partition_exprs,
        );
        assert_eq!(r0.partition.as_ref().unwrap().expression, expr_a);
    }
}
