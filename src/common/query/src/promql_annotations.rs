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

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, Weak};

use datafusion::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use datafusion_common::{DataFusionError, Result as DfResult};
use once_cell::sync::Lazy;

type PromqlAnnotationStateRef = Mutex<PromqlAnnotationState>;

static PROMQL_ANNOTATION_REGISTRY: Lazy<Mutex<HashMap<String, Weak<PromqlAnnotationStateRef>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

#[derive(Debug, Default)]
struct PromqlAnnotationState {
    warnings: BTreeSet<String>,
    infos: BTreeSet<String>,
}

#[derive(Clone, Debug)]
pub struct PromqlAnnotationCollector {
    inner: Arc<PromqlAnnotationStateRef>,
}

impl Default for PromqlAnnotationCollector {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(PromqlAnnotationState::default())),
        }
    }
}

impl PromqlAnnotationCollector {
    pub fn record_warning<S: Into<String>>(&self, warning: S) {
        self.inner
            .lock()
            .expect("promql annotation collector poisoned")
            .warnings
            .insert(warning.into());
    }

    pub fn record_info<S: Into<String>>(&self, info: S) {
        self.inner
            .lock()
            .expect("promql annotation collector poisoned")
            .infos
            .insert(info.into());
    }

    pub fn append_to(&self, warnings: &mut Vec<String>, infos: &mut Vec<String>) {
        let inner = self
            .inner
            .lock()
            .expect("promql annotation collector poisoned");
        append_unique(warnings, inner.warnings.iter().cloned());
        append_unique(infos, inner.infos.iter().cloned());
    }
}

impl ConfigExtension for PromqlAnnotationCollector {
    const PREFIX: &'static str = "greptime_promql_annotations";
}

impl ExtensionOptions for PromqlAnnotationCollector {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> DfResult<()> {
        Err(DataFusionError::NotImplemented(format!(
            "PromqlAnnotationCollector does not support set key: {key} with value: {value}"
        )))
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        vec![]
    }
}

pub fn promql_annotation_collector(query_id: &str) -> PromqlAnnotationCollector {
    let mut registry = PROMQL_ANNOTATION_REGISTRY
        .lock()
        .expect("promql annotation registry poisoned");
    registry.retain(|_, collector| collector.strong_count() > 0);

    if let Some(inner) = registry.get(query_id).and_then(Weak::upgrade) {
        return PromqlAnnotationCollector { inner };
    }

    let collector = PromqlAnnotationCollector::default();
    registry.insert(query_id.to_string(), Arc::downgrade(&collector.inner));
    collector
}

pub fn get_promql_annotation_collector(query_id: &str) -> Option<PromqlAnnotationCollector> {
    let mut registry = PROMQL_ANNOTATION_REGISTRY
        .lock()
        .expect("promql annotation registry poisoned");
    registry.retain(|_, collector| collector.strong_count() > 0);
    registry
        .get(query_id)
        .and_then(Weak::upgrade)
        .map(|inner| PromqlAnnotationCollector { inner })
}

fn append_unique(target: &mut Vec<String>, values: impl Iterator<Item = String>) {
    let mut seen = target.iter().cloned().collect::<BTreeSet<_>>();
    for value in values {
        if seen.insert(value.clone()) {
            target.push(value);
        }
    }
    target.sort();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collector_deduplicates_and_orders_annotations() {
        let collector = PromqlAnnotationCollector::default();
        collector.record_info("z-info");
        collector.record_info("a-info");
        collector.record_info("z-info");
        collector.record_warning("b-warning");
        collector.record_warning("a-warning");

        let mut warnings = vec!["c-warning".to_string()];
        let mut infos = vec![];
        collector.append_to(&mut warnings, &mut infos);

        assert_eq!(warnings, vec!["a-warning", "b-warning", "c-warning"]);
        assert_eq!(infos, vec!["a-info", "z-info"]);
    }

    #[test]
    fn registry_prunes_dead_collectors() {
        let key = "registry_prunes_dead_collectors";
        {
            let collector = promql_annotation_collector(key);
            assert!(get_promql_annotation_collector(key).is_some());
            drop(collector);
        }

        assert!(get_promql_annotation_collector(key).is_none());
        assert!(!PROMQL_ANNOTATION_REGISTRY.lock().unwrap().contains_key(key));
    }
}
