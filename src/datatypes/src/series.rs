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

pub mod frame;
pub mod histogram;
pub mod raw;
pub mod samples;
pub mod time;

#[cfg(test)]
mod tests;

use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow::array::{Array, StringArray};
use arrow::record_batch::RecordBatch;

use crate::error::{InvalidSeriesSnafu, Result};

/// Ordering guarantee within one stream and each of its output partitions.
///
/// Every series has strictly increasing sample positions, including across chunks; positions
/// are raw timestamps or, for evaluated series, current-stage evaluation times or grid positions.
/// They need not all be explicit times on the grid. No mode permits
/// duplicate positions. `Interleaved` permits A1, B1, A2; only the latter two modes keep each
/// series contiguous. `KeyThenTime` also orders by this stream's series identity, then time:
/// raw streams use the complete bound-compatible identity (not just a TSID or hash), while
/// evaluated streams use current name/label identity, not storage primary keys or object address/hash.
/// This ordering
/// is not a query-global ordering, partition-ownership rule, or finality guarantee.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, Hash)]
pub enum SeriesOrdering {
    /// Series chunks may interleave; positions within each series remain strictly increasing.
    Interleaved,
    /// Each series is contiguous; distinct series need not be ordered.
    Contiguous,
    /// Series are ordered by stream identity and then by strictly increasing position.
    KeyThenTime,
}

/// A label with shared UTF-8 name and value strings.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, Hash)]
pub struct Label {
    /// Label name.
    pub name: Arc<str>,
    /// Label value.
    pub value: Arc<str>,
}

/// The PromQL-visible labels of one series: an optional metric name (`__name__`)
/// plus its name-sorted ordinary labels.
///
/// The metric name is only the `__name__` value (see [`Self::name`]); this type is
/// the whole label set a PromQL series shows. It is neither a canonical
/// matching/grouping identity nor a storage identity (read key / primary key).
#[derive(Clone, Debug)]
pub struct SeriesLabels {
    name: Option<Arc<str>>,
    tags: LabelStorage,
}

impl serde::Serialize for SeriesLabels {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        (&self.name, self.tags()).serialize(serializer)
    }
}
impl<'de> serde::Deserialize<'de> for SeriesLabels {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let (name, tags): (Option<Arc<str>>, Vec<Label>) =
            serde::Deserialize::deserialize(deserializer)?;
        Self::try_new(name, tags).map_err(serde::de::Error::custom)
    }
}

/// Shared, bounded series metadata and the owner of its memory reservation.
/// Columns map logical label names to UTF-8 arrays; null labels are absent.
pub struct SeriesLabelBatch {
    batch: RecordBatch,
    columns: Vec<(Arc<str>, usize)>,
    _owner: Arc<dyn Any + Send + Sync>,
}

impl std::fmt::Debug for SeriesLabelBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeriesLabelBatch")
            .field("rows", &self.batch.num_rows())
            .field("columns", &self.columns)
            .finish_non_exhaustive()
    }
}

impl SeriesLabelBatch {
    /// Validates mappings once. The owner must account for the retained backing buffers.
    pub fn try_new(
        batch: RecordBatch,
        mut columns: Vec<(Arc<str>, usize)>,
        owner: Arc<dyn Any + Send + Sync>,
    ) -> Result<Self> {
        columns.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        if columns.iter().any(|(name, index)| {
            name.as_ref() == "__name__"
                || batch
                    .columns()
                    .get(*index)
                    .is_none_or(|a| !a.as_any().is::<StringArray>())
        }) || columns.windows(2).any(|p| p[0].0 == p[1].0)
        {
            return invalid_series("invalid batch label mapping");
        }
        Ok(Self {
            batch,
            columns,
            _owner: owner,
        })
    }

    /// Estimates retained buffers and one row view per batch row, including lazy
    /// owned-label materialization. Shared buffers are conservatively counted per
    /// array. Additional independent views of a row need their own reservation.
    /// This is a reservation estimate, not an allocator-exact accounting contract.
    pub fn estimated_size(&self) -> usize {
        let arc_header = 2 * std::mem::size_of::<usize>();
        let row_views = self
            .batch
            .num_rows()
            .saturating_mul(std::mem::size_of::<BatchLabels>() + arc_header);
        let mut bytes = self
            .batch
            .get_array_memory_size()
            .saturating_add(std::mem::size_of::<Self>() + arc_header)
            .saturating_add(
                self.columns
                    .capacity()
                    .saturating_mul(std::mem::size_of::<(Arc<str>, usize)>()),
            )
            .saturating_add(row_views);
        for (name, column) in &self.columns {
            bytes = bytes.saturating_add(name.len() + arc_header);
            for row in 0..self.batch.num_rows() {
                if let Some(value) = self.value(*column, row) {
                    // Vec-to-Arc conversion may temporarily retain both label arrays.
                    bytes = bytes
                        .saturating_add(2 * std::mem::size_of::<Label>())
                        .saturating_add(value.len())
                        .saturating_add(arc_header + std::mem::align_of::<usize>());
                }
            }
        }
        bytes.saturating_add(self.batch.num_rows().saturating_mul(arc_header))
    }

    fn value(&self, column: usize, row: usize) -> Option<&str> {
        let strings = self
            .batch
            .column(column)
            .as_any()
            .downcast_ref::<StringArray>()?;
        (!strings.is_null(row)).then(|| strings.value(row))
    }
}

#[derive(Clone, Debug)]
enum LabelStorage {
    Owned(Arc<[Label]>),
    Batch(Arc<BatchLabels>),
}

#[derive(Debug)]
struct BatchLabels {
    batch: Arc<SeriesLabelBatch>,
    row: usize,
    // Existing consumers requesting owned labels share one lazy materialization.
    materialized: OnceLock<Arc<[Label]>>,
}

impl PartialEq for SeriesLabels {
    fn eq(&self, other: &Self) -> bool {
        if self.name != other.name {
            return false;
        }
        if let (LabelStorage::Batch(a), LabelStorage::Batch(b)) = (&self.tags, &other.tags)
            && Arc::ptr_eq(a, b)
        {
            return true;
        }
        self.tags() == other.tags()
    }
}
impl Eq for SeriesLabels {}

impl std::hash::Hash for SeriesLabels {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&self.name, state);
        std::hash::Hash::hash(self.tags(), state);
    }
}

impl SeriesLabels {
    /// Creates a metric name, sorting labels and rejecting duplicates or `__name__` tags.
    pub fn try_new(name: Option<Arc<str>>, mut tags: Vec<Label>) -> Result<Self> {
        tags.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        if tags.iter().any(|tag| tag.name.as_ref() == "__name__") {
            return invalid_series("__name__ must be stored as the metric name");
        }
        if tags.windows(2).any(|pair| pair[0].name == pair[1].name) {
            return invalid_series("duplicate label name");
        }
        Ok(Self {
            name,
            tags: LabelStorage::Owned(tags.into()),
        })
    }

    /// Returns the metric name, if present.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Returns ordinary labels sorted by name.
    pub fn tags(&self) -> &[Label] {
        match &self.tags {
            LabelStorage::Owned(tags) => tags,
            LabelStorage::Batch(labels) => labels.materialized.get_or_init(|| {
                labels
                    .batch
                    .columns
                    .iter()
                    .filter_map(|(name, column)| {
                        labels.batch.value(*column, labels.row).map(|value| Label {
                            name: name.clone(),
                            value: Arc::from(value),
                        })
                    })
                    .collect::<Vec<_>>()
                    .into()
            }),
        }
    }

    /// Creates a shared row view without encoding keys or allocating owned label strings.
    pub fn from_batch(
        name: Option<Arc<str>>,
        batch: Arc<SeriesLabelBatch>,
        row: usize,
    ) -> Result<Self> {
        if row >= batch.batch.num_rows() {
            return invalid_series("series label row out of bounds");
        }
        Ok(Self {
            name,
            tags: LabelStorage::Batch(Arc::new(BatchLabels {
                batch,
                row,
                materialized: OnceLock::new(),
            })),
        })
    }

    /// Returns a label value, distinguishing absence from an empty value.
    pub fn get(&self, label: &str) -> Option<&str> {
        if label == "__name__" {
            self.name()
        } else if let LabelStorage::Batch(labels) = &self.tags {
            let i = labels
                .batch
                .columns
                .binary_search_by(|(name, _)| name.as_ref().cmp(label))
                .ok()?;
            labels.batch.value(labels.batch.columns[i].1, labels.row)
        } else {
            self.tags()
                .binary_search_by(|tag| tag.name.as_ref().cmp(label))
                .ok()
                .map(|index| self.tags()[index].value.as_ref())
        }
    }

    /// Sets the metric name or an ordinary label, preserving other shared labels.
    pub fn set(&mut self, label: &str, value: Arc<str>) -> Result<()> {
        if label == "__name__" {
            self.name = Some(value);
            return Ok(());
        }
        match self
            .tags()
            .binary_search_by(|tag| tag.name.as_ref().cmp(label))
        {
            Ok(index) => {
                let mut tags = self.tags().to_vec();
                tags[index].value = value;
                self.tags = LabelStorage::Owned(tags.into());
            }
            Err(index) => {
                let mut tags = self.tags().to_vec();
                tags.insert(
                    index,
                    Label {
                        name: Arc::from(label),
                        value,
                    },
                );
                self.tags = LabelStorage::Owned(tags.into());
            }
        }
        Ok(())
    }

    /// Removes a label or clears the metric name.
    pub fn remove(&mut self, label: &str) {
        if label == "__name__" {
            self.name = None;
        } else if let Ok(index) = self
            .tags()
            .binary_search_by(|tag| tag.name.as_ref().cmp(label))
        {
            let mut tags = self.tags().to_vec();
            tags.remove(index);
            self.tags = LabelStorage::Owned(tags.into());
        }
    }

    /// Keeps only the named labels and metric name.
    pub fn keep_only(&mut self, labels: &[&str]) {
        if !labels.contains(&"__name__") {
            self.name = None;
        }
        let kept: Vec<_> = self
            .tags()
            .iter()
            .filter(|tag| labels.contains(&tag.name.as_ref()))
            .cloned()
            .collect();
        if kept.len() != self.tags().len() {
            self.tags = LabelStorage::Owned(kept.into());
        }
    }

    /// Removes the selected labels and metric name.
    pub fn remove_all(&mut self, labels: &[&str]) {
        if labels.contains(&"__name__") {
            self.name = None;
        }
        let kept: Vec<_> = self
            .tags()
            .iter()
            .filter(|tag| !labels.contains(&tag.name.as_ref()))
            .cloned()
            .collect();
        if kept.len() != self.tags().len() {
            self.tags = LabelStorage::Owned(kept.into());
        }
    }
}

fn invalid_series<T>(reason: &str) -> Result<T> {
    InvalidSeriesSnafu {
        reason: reason.to_string(),
    }
    .fail()
}
