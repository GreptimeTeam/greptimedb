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

mod column_to_row;
mod row_to_region;
mod stmt_to_region;
mod table_to_region;

use api::v1::SemanticType;
pub use column_to_row::ColumnToRow;
pub use row_to_region::RowToRegion;
use snafu::{OptionExt, ResultExt};
pub use stmt_to_region::StatementToRegion;
use table::metadata::TableInfo;
pub use table_to_region::TableToRegion;

use crate::error::{ColumnNotFoundSnafu, MissingTimeIndexColumnSnafu, Result};

fn semantic_type(table_info: &TableInfo, column: &str) -> Result<SemanticType> {
    let table_meta = &table_info.meta;
    let table_schema = &table_meta.schema;

    let time_index_column = &table_schema
        .timestamp_column()
        .with_context(|| table::error::MissingTimeIndexColumnSnafu {
            table_name: table_info.name.to_string(),
        })
        .context(MissingTimeIndexColumnSnafu)?
        .name;

    let semantic_type = if column == time_index_column {
        SemanticType::Timestamp
    } else {
        let column_index = table_schema.column_index_by_name(column);
        let column_index = column_index.context(ColumnNotFoundSnafu {
            msg: format!("unable to find column {column} in table schema"),
        })?;

        if table_meta.primary_key_indices.contains(&column_index) {
            SemanticType::Tag
        } else {
            SemanticType::Field
        }
    };

    Ok(semantic_type)
}
