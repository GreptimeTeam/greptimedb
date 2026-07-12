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

//! Cache size of different cache value.

use parquet::file::metadata::ParquetMetaData;

/// Returns estimated size of [ParquetMetaData].
pub fn parquet_meta_size(meta: &ParquetMetaData) -> usize {
    meta.memory_size()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parquet::basic::{Repetition, Type as PhysicalType};
    use parquet::file::metadata::{ColumnIndexBuilder, FileMetaData, ParquetMetaDataBuilder};
    use parquet::schema::types::{SchemaDescriptor, Type as SchemaType};

    use super::*;

    #[test]
    fn parquet_meta_size_counts_byte_array_column_index_buffers() {
        let field = Arc::new(
            SchemaType::primitive_type_builder("tag", PhysicalType::BYTE_ARRAY)
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .unwrap(),
        );
        let schema = Arc::new(
            SchemaType::group_type_builder("schema")
                .with_fields(vec![field])
                .build()
                .unwrap(),
        );
        let schema_descr = Arc::new(SchemaDescriptor::new(schema));
        let file_metadata = FileMetaData::new(2, 3, None, None, schema_descr, None);

        let mut column_index = ColumnIndexBuilder::new(PhysicalType::BYTE_ARRAY);
        for page in 0..3u8 {
            column_index.append(false, vec![page; 4096], vec![page + 1; 4096], 0);
        }
        let metadata = ParquetMetaDataBuilder::new(file_metadata)
            .set_column_index(Some(vec![vec![column_index.build().unwrap()]]))
            .build();

        let min_max_bytes = 3 * 4096 * 2;
        assert!(
            parquet_meta_size(&metadata) >= min_max_bytes,
            "metadata size should include the byte-array page-index min/max buffers"
        );
    }
}
