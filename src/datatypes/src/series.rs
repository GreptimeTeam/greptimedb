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

use std::sync::Arc;

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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SeriesOrdering {
    /// Series chunks may interleave; positions within each series remain strictly increasing.
    Interleaved,
    /// Each series is contiguous; distinct series need not be ordered.
    Contiguous,
    /// Series are ordered by stream identity and then by strictly increasing position.
    KeyThenTime,
}

/// A label with shared UTF-8 name and value strings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Label {
    /// Label name.
    pub name: Arc<str>,
    /// Label value.
    pub value: Arc<str>,
}

/// A metric name and its name-sorted ordinary labels.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetricName {
    name: Option<Arc<str>>,
    tags: Arc<[Label]>,
}

impl MetricName {
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
            tags: tags.into(),
        })
    }

    /// Returns the metric name, if present.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Returns ordinary labels sorted by name.
    pub fn tags(&self) -> &[Label] {
        &self.tags
    }

    /// Returns a label value, distinguishing absence from an empty value.
    pub fn get(&self, label: &str) -> Option<&str> {
        if label == "__name__" {
            self.name()
        } else {
            self.tags
                .binary_search_by(|tag| tag.name.as_ref().cmp(label))
                .ok()
                .map(|index| self.tags[index].value.as_ref())
        }
    }

    /// Sets the metric name or an ordinary label, preserving other shared labels.
    pub fn set(&mut self, label: &str, value: Arc<str>) -> Result<()> {
        if label == "__name__" {
            self.name = Some(value);
            return Ok(());
        }
        match self
            .tags
            .binary_search_by(|tag| tag.name.as_ref().cmp(label))
        {
            Ok(index) => {
                let mut tags = self.tags.to_vec();
                tags[index].value = value;
                self.tags = tags.into();
            }
            Err(index) => {
                let mut tags = self.tags.to_vec();
                tags.insert(
                    index,
                    Label {
                        name: Arc::from(label),
                        value,
                    },
                );
                self.tags = tags.into();
            }
        }
        Ok(())
    }

    /// Removes a label or clears the metric name.
    pub fn remove(&mut self, label: &str) {
        if label == "__name__" {
            self.name = None;
        } else if let Ok(index) = self
            .tags
            .binary_search_by(|tag| tag.name.as_ref().cmp(label))
        {
            let mut tags = self.tags.to_vec();
            tags.remove(index);
            self.tags = tags.into();
        }
    }

    /// Keeps only the named labels and metric name.
    pub fn keep_only(&mut self, labels: &[&str]) {
        if !labels.contains(&"__name__") {
            self.name = None;
        }
        let kept: Vec<_> = self
            .tags
            .iter()
            .filter(|tag| labels.contains(&tag.name.as_ref()))
            .cloned()
            .collect();
        if kept.len() != self.tags.len() {
            self.tags = kept.into();
        }
    }

    /// Removes the selected labels and metric name.
    pub fn remove_all(&mut self, labels: &[&str]) {
        if labels.contains(&"__name__") {
            self.name = None;
        }
        let kept: Vec<_> = self
            .tags
            .iter()
            .filter(|tag| !labels.contains(&tag.name.as_ref()))
            .cloned()
            .collect();
        if kept.len() != self.tags.len() {
            self.tags = kept.into();
        }
    }
}

fn invalid_series<T>(reason: &str) -> Result<T> {
    InvalidSeriesSnafu {
        reason: reason.to_string(),
    }
    .fail()
}
