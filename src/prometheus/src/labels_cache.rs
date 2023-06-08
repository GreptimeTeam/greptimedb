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

use std::collections::HashSet;
use std::iter::IntoIterator;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use lru::LruCache;
use string_interner::{StringInterner, Symbol};

/// Metric -> [Label] LRU cache
#[derive(Clone)]
pub struct LabelsCache {
    inner: Arc<Mutex<LabelsCacheInner>>,
}

impl LabelsCache {
    /// Create a labels cache with capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(LabelsCacheInner::new(capacity))),
        }
    }

    /// Put metric and labels into cache
    pub fn put<T>(&self, metric: &str, labels: T)
    where
        T: IntoIterator,
        T::Item: AsRef<str>,
    {
        let mut inner = self.inner.lock().unwrap();
        inner.put(metric, labels);
    }

    /// Put metric and labels into cache
    pub fn add<T>(&self, metric: &str, labels: T)
    where
        T: IntoIterator,
        T::Item: AsRef<str>,
    {
        let mut inner = self.inner.lock().unwrap();
        inner.add(metric, labels);
    }

    /// Returns true when the metric and labels are all in cache
    pub fn contains_labels<T>(&self, metric: &str, labels: T) -> bool
    where
        T: IntoIterator,
        T::Item: AsRef<str>,
    {
        let mut inner = self.inner.lock().unwrap();
        inner.contains_labels(metric, labels)
    }

    /// Returns the difference labels, i.e., the labels that are in argument but not in cache.
    pub fn diff_labels<T>(&self, metric: &str, labels: T) -> HashSet<String>
    where
        T: IntoIterator,
        T::Item: AsRef<str>,
    {
        let mut inner = self.inner.lock().unwrap();
        inner.diff_labels(metric, labels)
    }

    /// Return the metric's labels, return none if it's not cached.
    pub fn labels(&self, metric: &str) -> Option<Vec<Option<String>>> {
        let mut inner = self.inner.lock().unwrap();
        inner.labels(metric)
    }
}

struct LabelsCacheInner {
    interner: StringInterner,
    cache: LruCache<usize, HashSet<usize>>,
}

impl LabelsCacheInner {
    fn new(capacity: usize) -> Self {
        Self {
            interner: StringInterner::default(),
            cache: LruCache::new(NonZeroUsize::new(capacity).unwrap()),
        }
    }

    fn put<T>(&mut self, metric: &str, labels: T)
    where
        T: IntoIterator,
        T::Item: AsRef<str>,
    {
        let sym = self.interner.get_or_intern(metric).to_usize();
        let labels = labels
            .into_iter()
            .map(|label| self.interner.get_or_intern(label).to_usize())
            .collect();

        self.cache.put(sym, labels);
    }

    fn add<T>(&mut self, metric: &str, labels: T)
    where
        T: IntoIterator,
        T::Item: AsRef<str>,
    {
        let sym = self.interner.get_or_intern(metric).to_usize();
        let labels: HashSet<_> = labels
            .into_iter()
            .map(|label| self.interner.get_or_intern(label).to_usize())
            .collect();

        let exists_labels = self.cache.get_or_insert_mut(sym, HashSet::new);

        exists_labels.extend(labels);
    }

    fn contains_labels<T>(&mut self, metric: &str, labels: T) -> bool
    where
        T: IntoIterator,
        T::Item: AsRef<str>,
    {
        let sym = self.interner.get_or_intern(metric).to_usize();
        let labels: HashSet<_> = labels
            .into_iter()
            .map(|label| self.interner.get_or_intern(label).to_usize())
            .collect();

        match self.cache.get(&sym) {
            None => false,
            Some(cached_labels) => labels.is_subset(cached_labels),
        }
    }

    fn diff_labels<T>(&mut self, metric: &str, labels: T) -> HashSet<String>
    where
        T: IntoIterator,
        T::Item: AsRef<str>,
    {
        let sym = self.interner.get_or_intern(metric).to_usize();

        match self.cache.get(&sym) {
            None => labels.into_iter().map(|s| s.as_ref().to_string()).collect(),
            Some(cached_labels) => {
                let labels_syms: HashSet<_> = labels
                    .into_iter()
                    .map(|label| self.interner.get_or_intern(label).to_usize())
                    .collect();

                let diff = labels_syms.difference(cached_labels);
                diff.into_iter()
                    .filter_map(|sym| Self::resolve_string(&self.interner, *sym))
                    .collect()
            }
        }
    }

    fn labels(&mut self, metric: &str) -> Option<Vec<Option<String>>> {
        let sym = self.interner.get_or_intern(metric).to_usize();

        self.cache.get(&sym).map(|syms| {
            syms.iter()
                .map(|sym| Self::resolve_string(&self.interner, *sym))
                .collect()
        })
    }

    fn resolve_string(interner: &StringInterner, sym: usize) -> Option<String> {
        match Symbol::try_from_usize(sym) {
            // MUST NOT be none, but we have to take care of it.
            None => None,
            Some(sym) => interner.resolve(sym).map(|s| s.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_labels_cache() {
        let cache = LabelsCache::new(10);

        cache.put("metric1", &["label1", "label3"]);
        cache.put("metric2", &["label2", "label3", "label4"]);

        assert!(cache.contains_labels::<&[&str; 0]>("metric1", &[]));
        assert!(cache.contains_labels("metric1", &["label1"]));
        assert!(cache.contains_labels("metric1", &["label3"]));
        assert!(cache.contains_labels("metric1", &["label3", "label1"]));
        assert!(cache.contains_labels("metric1", &["label1", "label3"]));
        assert!(!cache.contains_labels("metric1", &["label1", "label2"]));
        assert!(!cache.contains_labels("metric1", &["label2"]));

        assert!(cache.contains_labels("metric2", &["label2"]));
        assert!(cache.contains_labels("metric2", &["label2", "label3"]));
        assert!(cache.contains_labels("metric2", &["label2", "label3", "label4"]));
        assert!(!cache.contains_labels("metric2", &["label1"]));
        assert!(!cache.contains_labels("metric2", &["label1", "label2"]));

        assert_eq!(
            HashSet::from(["label2".to_string()]),
            cache.diff_labels("metric1", &["label1", "label2"])
        );
        assert_eq!(
            HashSet::from(["label2".to_string(), "label4".to_string()]),
            cache.diff_labels("metric1", &["label2", "label4"])
        );
        assert_eq!(
            HashSet::from(["label1".to_string()]),
            cache.diff_labels("metric2", &["label1", "label2"])
        );
        assert_eq!(
            HashSet::from(["label1".to_string(), "label5".to_string()]),
            cache.diff_labels("metric2", &["label1", "label2", "label4", "label5"])
        );

        let mut labels = cache.labels("metric1").unwrap();
        labels.sort();
        assert_eq!(
            vec![Some("label1".to_string()), Some("label3".to_string())],
            labels
        );
        let mut labels = cache.labels("metric2").unwrap();
        labels.sort();
        assert_eq!(
            vec![
                Some("label2".to_string()),
                Some("label3".to_string()),
                Some("label4".to_string())
            ],
            labels
        );

        assert!(cache.labels("metric_not_exists").is_none());
    }
}
