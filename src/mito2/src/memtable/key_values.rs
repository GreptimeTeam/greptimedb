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

use greptime_proto::v1::mito::{Mutation, OpType};
use greptime_proto::v1::{Row, Value};
use store_api::storage::SequenceNumber;

use crate::metadata::{RegionMetadata, SemanticType};
use crate::request::WriteRequest;

/// Key-value view of mutations.
#[derive(Debug)]
pub struct KeyValues {
    /// List of valid mutations.
    mutations: Vec<Mutation>,
    /// Key value read helper for each mutation.
    ///
    /// Mutations might have different schema so each mutation has a helper.
    helpers: Vec<ReadRowHelper>,
}

impl KeyValues {
    /// Iterate by key value.
    fn iter(&self) -> impl Iterator<Item = KeyValue> {
        self.mutations
            .iter()
            .zip(self.helpers.iter())
            .filter(|(mutation, _)| mutation.rows.is_some())
            .map(|(mutation, helper)| {
                let rows = mutation.rows.as_ref().unwrap();
                rows.rows.iter().enumerate().map(|(idx, row)| {
                    KeyValue {
                        row,
                        helper,
                        sequence: mutation.sequence + idx as u64,
                        // Safety: This is a valid mutation.
                        op_type: OpType::from_i32(mutation.op_type).unwrap(),
                    }
                })
            })
            .flatten()
    }
}

/// Helper to read rows in key, value order.
#[derive(Debug)]
pub(crate) struct ReadRowHelper {
    /// Key and value column indices.
    ///
    /// `indices[..num_primary_key_column]` are primary key columns, `indices[num_primary_key_column]`
    /// is the timestamp column and remainings are field columns.
    indices: Vec<usize>,
    /// Number of primary key columns.
    num_primary_key_column: usize,
}

impl ReadRowHelper {
    /// Creates a [ReadRowHelper] for specific `request`.
    ///
    /// # Panics
    /// The `request` must fill their missing columns and have same columns with `metadata`.
    /// Otherwise this method will panic.
    fn new(metadata: &RegionMetadata, request: &WriteRequest) -> ReadRowHelper {
        let rows = &request.rows;
        assert_eq!(
            metadata.column_metadatas.len(),
            rows.schema.len(),
            "Length mismatch, column_metas: {:?}, rows_schema: {:?}",
            metadata.column_metadatas,
            rows.schema
        );

        let mut indices = Vec::with_capacity(metadata.column_metadatas.len());

        // Get primary key indices.
        for pk_id in &metadata.primary_key {
            // Safety: Id comes from primary key.
            let column = metadata.column_by_id(*pk_id).unwrap();
            let index = request
                .column_index_by_name(&column.column_schema.name)
                .unwrap();
            indices.push(index);
        }
        // Get timestamp index.
        let ts_index = request
            .column_index_by_name(&metadata.time_index_column().column_schema.name)
            .unwrap();
        indices.push(ts_index);
        // Iterate columns and find field columns.
        for column in metadata.column_metadatas.iter() {
            if column.semantic_type == SemanticType::Field {
                // Get index in request for each field column.
                let index = request
                    .column_index_by_name(&column.column_schema.name)
                    .unwrap();
                indices.push(index);
            }
        }

        ReadRowHelper {
            indices,
            num_primary_key_column: metadata.primary_key.len(),
        }
    }
}

/// Key value view of a row.
pub struct KeyValue<'a> {
    row: &'a Row,
    helper: &'a ReadRowHelper,
    sequence: SequenceNumber,
    op_type: OpType,
}

impl<'a> KeyValue<'a> {
    /// Get key columns.
    pub fn keys(&self) -> impl Iterator<Item = &Value> {
        self.helper.indices[..self.helper.num_primary_key_column]
            .iter()
            .map(|idx| &self.row.values[*idx])
    }

    /// Get field columns.
    pub fn fields(&self) -> impl Iterator<Item = &Value> {
        self.helper.indices[self.helper.num_primary_key_column + 1..]
            .iter()
            .map(|idx| &self.row.values[*idx])
    }

    /// Get sequence.
    pub fn sequence(&self) -> SequenceNumber {
        self.sequence
    }

    /// Get op type.
    pub fn op_type(&self) -> OpType {
        self.op_type
    }
}
