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

//! Utilities to create a [RegionMetadata](store_api::metadata::RegionMetadata).

use api::v1::SemanticType;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataBuilder};
use store_api::storage::RegionId;

/// Builder to builds a region with schema `ts, k0, k1, ..., v0, v1, ...`.
///
/// All tags and fields have int64 type.
#[derive(Debug)]
pub struct TestRegionMetadataBuilder {
    region_id: RegionId,
    ts_name: String,
    num_tags: usize,
    num_fields: usize,
}

impl Default for TestRegionMetadataBuilder {
    fn default() -> Self {
        Self {
            region_id: RegionId::new(1, 1),
            ts_name: "ts".to_string(),
            num_tags: 1,
            num_fields: 1,
        }
    }
}

impl TestRegionMetadataBuilder {
    /// Sets ts name.
    pub fn ts_name(&mut self, value: &str) -> &mut Self {
        self.ts_name = value.to_string();
        self
    }

    /// Sets tags num.
    pub fn num_tags(&mut self, value: usize) -> &mut Self {
        self.num_tags = value;
        self
    }

    /// Sets fields num.
    pub fn num_fields(&mut self, value: usize) -> &mut Self {
        self.num_fields = value;
        self
    }

    /// Builds a metadata.
    pub fn build(&self) -> RegionMetadata {
        let mut builder = RegionMetadataBuilder::new(self.region_id);
        let mut column_id = 0;
        builder.push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                &self.ts_name,
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id,
        });
        // For simplicity, we use the same data type for tag/field columns.
        let mut primary_key = Vec::with_capacity(self.num_tags);
        for i in 0..self.num_tags {
            column_id += 1;
            builder.push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    format!("k{i}"),
                    ConcreteDataType::int64_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Tag,
                column_id,
            });
            primary_key.push(i as u32 + 1);
        }
        for i in 0..self.num_fields {
            column_id += 1;
            builder.push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    format!("v{i}"),
                    ConcreteDataType::int64_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Field,
                column_id,
            });
        }
        builder.primary_key(primary_key);
        builder.build().unwrap()
    }
}
