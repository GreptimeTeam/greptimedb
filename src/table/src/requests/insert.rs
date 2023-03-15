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

use common_recordbatch::RecordBatch;
use datafusion_common::ResolvedTableReference;
use datatypes::prelude::VectorRef;
use datatypes::schema::SchemaRef;
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionNumber;

use crate::error::{CastVectorSnafu, ColumnNotExistsSnafu, Result};

#[derive(Debug)]
pub struct InsertRequest {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub columns_values: HashMap<String, VectorRef>,
    pub region_number: RegionNumber,
}

impl InsertRequest {
    pub fn try_from_recordbatch(
        table_name: &ResolvedTableReference,
        table_schema: SchemaRef,
        recordbatch: RecordBatch,
    ) -> Result<Self> {
        let mut columns_values = HashMap::with_capacity(recordbatch.num_columns());

        // column schemas in recordbatch must match its vectors, otherwise it's corrupted
        for (vector_schema, vector) in recordbatch
            .schema
            .column_schemas()
            .iter()
            .zip(recordbatch.columns().iter())
        {
            let column_name = &vector_schema.name;
            let column_schema = table_schema
                .column_schema_by_name(column_name)
                .with_context(|| ColumnNotExistsSnafu {
                    table_name: table_name.table.to_string(),
                    column_name,
                })?;
            let vector = if vector_schema.data_type != column_schema.data_type {
                vector
                    .cast(&column_schema.data_type)
                    .with_context(|_| CastVectorSnafu {
                        from_type: vector.data_type(),
                        to_type: column_schema.data_type.clone(),
                    })?
            } else {
                vector.clone()
            };

            columns_values.insert(column_name.clone(), vector);
        }

        Ok(InsertRequest {
            catalog_name: table_name.catalog.to_string(),
            schema_name: table_name.schema.to_string(),
            table_name: table_name.table.to_string(),
            columns_values,
            region_number: 0,
        })
    }
}
