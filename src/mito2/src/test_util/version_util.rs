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

//! Utilities to mock version.

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::Arc;

use api::v1::helper::{tag_column_schema, time_index_column_schema};
use api::v1::value::ValueData;
use api::v1::{self, ColumnDataType, Mutation, OpType, Row, Rows, SemanticType};
use common_time::Timestamp;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataBuilder};
use store_api::storage::{FileId, RegionId};

use crate::manifest::action::RegionEdit;
use crate::memtable::time_partition::TimePartitions;
use crate::memtable::{KeyValues, MemtableBuilderRef};
use crate::region::version::{Version, VersionBuilder, VersionControl};
use crate::sst::file::FileMeta;
use crate::sst::file_purger::FilePurgerRef;
use crate::test_util::memtable_util::EmptyMemtableBuilder;
use crate::test_util::{new_noop_file_purger, ts_ms_value};

fn new_region_metadata(region_id: RegionId) -> RegionMetadata {
    let mut builder = RegionMetadataBuilder::new(region_id);
    builder
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: 1,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("tag_0", ConcreteDataType::string_datatype(), true),
            semantic_type: SemanticType::Tag,
            column_id: 2,
        })
        .primary_key(vec![2]);
    builder.build().unwrap()
}

// Builder to mock a version control.
pub(crate) struct VersionControlBuilder {
    metadata: RegionMetadata,
    file_purger: FilePurgerRef,
    memtable_builder: MemtableBuilderRef,
    files: HashMap<FileId, FileMeta>,
}

impl VersionControlBuilder {
    pub(crate) fn new() -> VersionControlBuilder {
        VersionControlBuilder {
            metadata: new_region_metadata(RegionId::new(1, 1)),
            file_purger: new_noop_file_purger(),
            memtable_builder: Arc::new(EmptyMemtableBuilder::default()),
            files: HashMap::new(),
        }
    }

    pub(crate) fn region_id(&self) -> RegionId {
        self.metadata.region_id
    }

    pub(crate) fn file_purger(&self) -> FilePurgerRef {
        self.file_purger.clone()
    }

    pub(crate) fn set_memtable_builder(&mut self, builder: MemtableBuilderRef) -> &mut Self {
        self.memtable_builder = builder;
        self
    }

    pub(crate) fn push_l0_file(&mut self, start_ms: i64, end_ms: i64) -> &mut Self {
        let file_id = FileId::random();
        self.files.insert(
            file_id,
            FileMeta {
                region_id: self.metadata.region_id,
                file_id,
                time_range: (
                    Timestamp::new_millisecond(start_ms),
                    Timestamp::new_millisecond(end_ms),
                ),
                level: 0,
                file_size: 0, // We don't care file size.
                available_indexes: Default::default(),
                indexes: Default::default(),
                index_file_size: 0,
                index_file_id: None,
                num_rows: 0,
                num_row_groups: 0,
                num_series: 0,
                sequence: NonZeroU64::new(start_ms as u64),
                partition_expr: match &self.metadata.partition_expr {
                    Some(json_str) => partition::expr::PartitionExpr::from_json_str(json_str)
                        .expect("partition expression should be valid JSON"),
                    None => None,
                },
            },
        );
        self
    }

    pub(crate) fn build_version(&self) -> Version {
        let metadata = Arc::new(self.metadata.clone());
        let mutable = Arc::new(TimePartitions::new(
            metadata.clone(),
            self.memtable_builder.clone(),
            0,
            None,
        ));
        VersionBuilder::new(metadata, mutable)
            .add_files(self.file_purger.clone(), self.files.values().cloned())
            .build()
    }

    pub(crate) fn build(&self) -> VersionControl {
        let version = self.build_version();
        VersionControl::new(version)
    }
}

/// Put rows to the mutable memtable in the version.
pub(crate) fn write_rows_to_version(
    version: &Version,
    tag: &str,
    start_ts: usize,
    num_rows: usize,
) {
    let mut rows = Vec::with_capacity(num_rows);
    for idx in 0..num_rows {
        let ts = (start_ts + idx) as i64;
        let values = vec![
            ts_ms_value(ts),
            v1::Value {
                value_data: Some(ValueData::StringValue(tag.to_string())),
            },
        ];
        rows.push(Row { values });
    }
    let schema = vec![
        time_index_column_schema("ts", ColumnDataType::TimestampMillisecond),
        tag_column_schema("tag_0", ColumnDataType::String),
    ];
    let rows = Rows { rows, schema };
    let mutation = Mutation {
        op_type: OpType::Put as i32,
        sequence: start_ts as u64, // The sequence may be incorrect, but it's fine in test.
        rows: Some(rows),
        write_hint: None,
    };
    let key_values = KeyValues::new(&version.metadata, mutation).unwrap();
    version.memtables.mutable.write(&key_values).unwrap();
}

/// Add mocked l0 files to the version control.
/// `files_to_add` are slice of `(start_ms, end_ms)`.
pub(crate) fn apply_edit(
    version_control: &VersionControl,
    files_to_add: &[(i64, i64)],
    files_to_remove: &[FileMeta],
    purger: FilePurgerRef,
) {
    let region_id = version_control.current().version.metadata.region_id;
    let files_to_add = files_to_add
        .iter()
        .map(|(start_ms, end_ms)| {
            FileMeta {
                region_id,
                file_id: FileId::random(),
                time_range: (
                    Timestamp::new_millisecond(*start_ms),
                    Timestamp::new_millisecond(*end_ms),
                ),
                level: 0,
                file_size: 0, // We don't care file size.
                available_indexes: Default::default(),
                indexes: Default::default(),
                index_file_size: 0,
                index_file_id: None,
                num_rows: 0,
                num_row_groups: 0,
                num_series: 0,
                sequence: NonZeroU64::new(*start_ms as u64),
                partition_expr: match &version_control.current().version.metadata.partition_expr {
                    Some(json_str) => partition::expr::PartitionExpr::from_json_str(json_str)
                        .expect("partition expression should be valid JSON"),
                    None => None,
                },
            }
        })
        .collect();

    version_control.apply_edit(
        Some(RegionEdit {
            files_to_add,
            files_to_remove: files_to_remove.to_vec(),
            timestamp_ms: None,
            compaction_time_window: None,
            flushed_entry_id: None,
            flushed_sequence: None,
            committed_sequence: None,
        }),
        &[],
        purger,
    );
}
