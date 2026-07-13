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

use std::collections::HashSet;
use std::sync::Arc;

use api::v1::SemanticType;
use datatypes::prelude::ConcreteDataType;
use snafu::ResultExt;
use store_api::metadata::{RegionMetadata, RegionMetadataBuilder, RegionMetadataRef};
use store_api::metric_engine_consts::{
    DATA_SCHEMA_TABLE_ID_COLUMN_NAME, DATA_SCHEMA_TSID_COLUMN_NAME,
    metric_engine_value_int_column_name,
};
use store_api::region_request::AlterKind;
use store_api::storage::consts::ReservedColumnId;

use crate::error::{InvalidMetadataSnafu, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MetricValueColumn {
    pub(crate) value_index: usize,
    pub(crate) int_index: usize,
}

pub(crate) fn metric_value_columns(metadata: &RegionMetadata) -> Vec<MetricValueColumn> {
    if !is_metric_engine_data_region(metadata) {
        return Vec::new();
    }

    metadata
        .field_columns()
        .filter(|column| column.column_schema.data_type == ConcreteDataType::float64_datatype())
        .filter_map(|value_column| {
            let int_name = metric_engine_value_int_column_name(&value_column.column_schema.name);
            let int_column = metadata.column_by_name(&int_name)?;
            if int_column.semantic_type != SemanticType::Field
                || int_column.column_schema.data_type != ConcreteDataType::int64_datatype()
            {
                return None;
            }

            Some(MetricValueColumn {
                value_index: metadata.column_index_by_name(&value_column.column_schema.name)?,
                int_index: metadata.column_index_by_name(&int_name)?,
            })
        })
        .collect()
}

pub(crate) fn visible_region_metadata(metadata: &RegionMetadataRef) -> Result<RegionMetadataRef> {
    let split_columns = metric_value_columns(metadata);
    if split_columns.is_empty() {
        return Ok(metadata.clone());
    }

    let int_indices = split_columns
        .into_iter()
        .map(|column| column.int_index)
        .collect::<HashSet<_>>();
    let visible_columns = metadata
        .column_metadatas
        .iter()
        .enumerate()
        .filter(|(index, _)| !int_indices.contains(index))
        .map(|(_, column)| column.clone())
        .collect::<Vec<_>>();

    let primary_key = metadata.primary_key.clone();
    let mut builder = RegionMetadataBuilder::from_existing((**metadata).clone());
    builder
        .alter(AlterKind::SyncColumns {
            column_metadatas: visible_columns,
        })
        .context(InvalidMetadataSnafu)?;
    builder.primary_key(primary_key);
    builder.build().map(Arc::new).context(InvalidMetadataSnafu)
}

pub(crate) fn is_metric_engine_data_region(metadata: &RegionMetadata) -> bool {
    let has_internal_tag = |name, column_id| {
        metadata.column_by_name(name).is_some_and(|column| {
            column.semantic_type == SemanticType::Tag
                && column.column_id == column_id
                && metadata.primary_key.contains(&column_id)
        })
    };

    has_internal_tag(
        DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
        ReservedColumnId::table_id(),
    ) && has_internal_tag(DATA_SCHEMA_TSID_COLUMN_NAME, ReservedColumnId::tsid())
}
