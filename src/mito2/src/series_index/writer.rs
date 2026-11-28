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

//! Schema constraints shared by the native samples and series codecs.

use crate::error::{InvalidMetaSnafu, Result};
use crate::series_index::{MAX_TS_COLUMN, MIN_TS_COLUMN, ROW_COUNT_COLUMN};
use datatypes::prelude::ConcreteDataType;
use snafu::{OptionExt, ensure};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::storage::{ColumnId, consts::ReservedColumnId};

pub(crate) fn validate_metadata(metadata: &RegionMetadata) -> Result<()> {
    for column in &metadata.column_metadatas {
        ensure!(
            !matches!(
                column.column_schema.name.as_str(),
                MIN_TS_COLUMN | MAX_TS_COLUMN | ROW_COUNT_COLUMN
            ),
            InvalidMetaSnafu {
                reason: format!(
                    "series index internal column name {} is already in use",
                    column.column_schema.name
                ),
            }
        );
    }
    ensure!(
        metadata.primary_key_encoding == PrimaryKeyEncoding::Sparse,
        InvalidMetaSnafu {
            reason: "series index only supports sparse primary-key encoding",
        }
    );
    ensure!(
        metadata
            .primary_key
            .starts_with(&[ReservedColumnId::table_id(), ReservedColumnId::tsid()]),
        InvalidMetaSnafu {
            reason: "series index requires (__table_id, __tsid) as the primary-key prefix",
        }
    );
    let table_id = metadata
        .column_by_id(ReservedColumnId::table_id())
        .context(InvalidMetaSnafu {
            reason: "series index metadata is missing __table_id",
        })?;
    let tsid = metadata
        .column_by_id(ReservedColumnId::tsid())
        .context(InvalidMetaSnafu {
            reason: "series index metadata is missing __tsid",
        })?;
    ensure!(
        table_id.column_schema.data_type == ConcreteDataType::uint32_datatype()
            && tsid.column_schema.data_type == ConcreteDataType::uint64_datatype(),
        InvalidMetaSnafu {
            reason: "series index requires UInt32 __table_id and UInt64 __tsid",
        }
    );
    for column in metadata.primary_key_columns() {
        if is_reserved_column(column.column_id) {
            continue;
        }
        ensure!(
            column.column_schema.data_type == ConcreteDataType::string_datatype(),
            InvalidMetaSnafu {
                reason: format!(
                    "series index requires string tag column {}, got {}",
                    column.column_schema.name, column.column_schema.data_type
                ),
            }
        );
    }
    Ok(())
}

pub(crate) fn tag_columns(metadata: &RegionMetadataRef) -> Vec<(ColumnId, String)> {
    metadata
        .primary_key_columns()
        .filter(|column| !is_reserved_column(column.column_id))
        .map(|column| (column.column_id, column.column_schema.name.clone()))
        .collect()
}

fn is_reserved_column(column_id: ColumnId) -> bool {
    column_id == ReservedColumnId::table_id() || column_id == ReservedColumnId::tsid()
}
