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

use std::collections::HashMap;

use api::v1::region::{CreateRequest, RegionColumnDef};
use api::v1::{ColumnDef, CreateTableExpr, SemanticType};
use snafu::OptionExt;
use store_api::metric_engine_consts::LOGICAL_TABLE_METADATA_KEY;
use store_api::storage::{RegionId, RegionNumber};
use table::metadata::TableId;

use crate::error;
use crate::error::Result;
use crate::wal_options_allocator::prepare_wal_options;

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
                    datatype_extension: c.datatype_extension,
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
        engine: create_table_expr.engine.to_string(),
        column_defs,
        primary_key,
        path: String::new(),
        options: create_table_expr.table_options.clone(),
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
    pub(crate) fn new(template: CreateRequest, physical_table_id: Option<TableId>) -> Self {
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
    ) -> CreateRequest {
        let mut request = self.template.clone();

        request.region_id = region_id.as_u64();
        request.path = storage_path;
        // Stores the encoded wal options into the request options.
        prepare_wal_options(&mut request.options, region_id, region_wal_options);

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
