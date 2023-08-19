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

use api::v1::SemanticType;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataBuilder};
use store_api::storage::RegionId;

/// Build a basic region metadata for testing.
/// It contains three columns:
/// - ts: timestamp millisecond, semantic type: `Timestamp`, column id: 45
/// - pk: string, semantic type: `Tag`, column id: 36
/// - val: float64, semantic type: `Field`, column id: 251
pub fn basic_region_metadata() -> RegionMetadata {
    let mut builder = RegionMetadataBuilder::new(RegionId::new(23, 33));
    builder
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: 45,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("pk", ConcreteDataType::string_datatype(), false),
            semantic_type: SemanticType::Tag,
            column_id: 36,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("val", ConcreteDataType::float64_datatype(), false),
            semantic_type: SemanticType::Field,
            column_id: 251,
        })
        .primary_key(vec![36]);
    builder.build().unwrap()
}
