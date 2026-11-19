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

//! Logical column snapshots and physical schema lookups for the metric engine.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;

use datatypes::schema::ColumnDefaultConstraint;
use intern_arc::{HashInterner, InternedHash};
use snafu::{OptionExt, ResultExt};
use store_api::metadata::{ColumnMetadata, RegionMetadata};
use store_api::storage::{RegionId, TableId};

use crate::engine::MetricEngineInner;
use crate::error::{MissingPhysicalColumnSnafu, Result, SerializeColumnMetadataSnafu};

/// A snapshot of a logical region's columns in name order.
pub(crate) type LogicalColumns = Arc<[InternedHash<ScopedColumnMetadata>]>;

/// Shares identical column definitions belonging to the same physical table.
/// The final reader or cache entry to release a column retires its interner entry.
#[derive(Default)]
pub(crate) struct ColumnMetadataInterner(HashInterner<ScopedColumnMetadata>);

impl ColumnMetadataInterner {
    fn intern(
        &self,
        physical_table_id: TableId,
        metadata: ColumnMetadata,
    ) -> Result<InternedHash<ScopedColumnMetadata>> {
        let literal_default = match metadata.column_schema.default_constraint() {
            Some(ColumnDefaultConstraint::Value(value)) if !value.is_null() => {
                Some(serde_json::to_vec(value).context(SerializeColumnMetadataSnafu)?)
            }
            _ => None,
        };
        Ok(self.0.intern_sized(ScopedColumnMetadata {
            physical_table_id,
            metadata,
            literal_default,
        }))
    }
}

/// A column definition shared within one physical table.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ScopedColumnMetadata {
    physical_table_id: TableId,
    metadata: ColumnMetadata,
    /// Preserves observable differences between equal literal values, such as signed zero.
    literal_default: Option<Vec<u8>>,
}

impl Hash for ScopedColumnMetadata {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Old and new definitions of the same column can coexist while readers
        // hold snapshots. Equality compares the full definition.
        self.physical_table_id.hash(state);
        self.metadata.column_id.hash(state);
        self.metadata.column_schema.name.hash(state);
    }
}

impl Deref for ScopedColumnMetadata {
    type Target = ColumnMetadata;

    fn deref(&self) -> &ColumnMetadata {
        &self.metadata
    }
}

/// Looks up a requested column in the schema installed by physical CREATE or ALTER.
pub(crate) fn physical_column<'a>(
    metadata: &'a RegionMetadata,
    name: &str,
) -> Result<&'a ColumnMetadata> {
    metadata
        .column_by_name(name)
        .with_context(|| MissingPhysicalColumnSnafu {
            name,
            region_id: metadata.region_id,
        })
}

impl MetricEngineInner {
    /// Updates state from the installed physical schema before writing logical metadata.
    pub(crate) fn cache_new_physical_columns<'a>(
        &self,
        metadata: &RegionMetadata,
        names: impl Iterator<Item = &'a str>,
    ) -> Result<()> {
        let columns = names
            .map(|name| Ok((name.to_string(), physical_column(metadata, name)?.clone())))
            .collect::<Result<Vec<_>>>()?;
        self.state
            .write()
            .unwrap()
            .add_physical_columns(metadata.region_id, columns);
        Ok(())
    }

    /// Loads a logical region's columns ordered by name.
    pub async fn load_logical_columns(
        &self,
        physical_region_id: RegionId,
        logical_region_id: RegionId,
    ) -> Result<LogicalColumns> {
        if let Some(columns) = self
            .state
            .read()
            .unwrap()
            .logical_columns()
            .get(&logical_region_id)
        {
            return Ok(columns.clone());
        }

        let _read_guard = self
            .metadata_region
            .read_lock_logical_region(logical_region_id)
            .await?;
        let columns = self
            .metadata_region
            .logical_columns(physical_region_id, logical_region_id)
            .await?;
        let mut by_id = HashMap::with_capacity(columns.len());
        for (_, column) in columns {
            let column_id = column.column_id;
            let column = self
                .column_metadata_interner
                .intern(physical_region_id.table_id(), column)?;
            by_id.insert(column_id, column);
        }

        let mut state = self.state.write().unwrap();
        // Preserve columns already published by a concurrent load.
        if let Some(existing) = state.logical_columns().get(&logical_region_id) {
            by_id.extend(existing.iter().map(|c| (c.column_id, c.clone())));
        }
        let mut columns: Vec<_> = by_id.into_values().collect();
        columns.sort_unstable_by(|a, b| a.column_schema.name.cmp(&b.column_schema.name));
        let columns: LogicalColumns = columns.into();
        state.set_logical_columns(logical_region_id, columns.clone());
        Ok(columns)
    }
}

#[cfg(test)]
mod tests {
    use std::ptr;

    use api::v1::SemanticType;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use datatypes::value::Value;

    use super::*;
    use crate::test_util::TestEnv;

    #[tokio::test]
    async fn test_logical_columns_shared_by_physical_table() {
        let env = TestEnv::new().await;
        let metric = env.metric();
        let mut snapshots = Vec::new();
        for (physical, logical) in [
            (RegionId::new(1, 0), RegionId::new(3, 0)),
            (RegionId::new(1, 1), RegionId::new(4, 1)),
            (RegionId::new(2, 0), RegionId::new(5, 0)),
        ] {
            env.create_physical_region(physical, &TestEnv::default_table_dir(), vec![])
                .await;
            env.create_logical_region(physical, logical).await;
            let columns = metric
                .inner
                .load_logical_columns(physical, logical)
                .await
                .unwrap();
            let cached = metric
                .inner
                .load_logical_columns(physical, logical)
                .await
                .unwrap();
            assert!(Arc::ptr_eq(&columns, &cached));
            snapshots.push(columns);
        }
        let [first, same_table, other_table] = snapshots.as_slice() else {
            unreachable!()
        };
        assert_eq!(first.len(), 3);
        assert_eq!(first.len(), same_table.len());
        assert_eq!(first.len(), other_table.len());
        for ((a, b), c) in first.iter().zip(same_table.iter()).zip(other_table.iter()) {
            assert!(ptr::eq(a.as_ref(), b.as_ref()));
            assert_eq!(a.metadata, c.metadata);
            assert!(!ptr::eq(a.as_ref(), c.as_ref()));
        }
    }

    #[test]
    fn test_distinct_column_versions_retire_after_last_reader() {
        let interner = ColumnMetadataInterner::default();
        let column = |default: f64| ColumnMetadata {
            column_id: 7,
            semantic_type: SemanticType::Field,
            column_schema: ColumnSchema::new("value", ConcreteDataType::float64_datatype(), true)
                .with_default_constraint(Some(ColumnDefaultConstraint::Value(Value::from(default))))
                .unwrap(),
        };
        let old = interner.intern(1, column(-0.0)).unwrap();
        let reader = old.clone();
        let new = interner.intern(1, column(0.0)).unwrap();
        assert!(!ptr::eq(old.as_ref(), new.as_ref()));
        assert!(ptr::eq(
            new.as_ref(),
            interner.intern(1, column(0.0)).unwrap().as_ref()
        ));
        drop(old);
        drop(new);
        assert_eq!(interner.0.len(), 1);
        drop(reader);
        assert!(interner.0.is_empty());
    }
}
