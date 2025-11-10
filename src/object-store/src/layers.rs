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

mod lru_cache;

pub use lru_cache::*;
pub use opendal::layers::*;
pub use prometheus::build_prometheus_metrics_layer;

mod prometheus {
    use std::sync::{Mutex, OnceLock};

    use opendal::layers::PrometheusLayer;

    static PROMETHEUS_LAYER: OnceLock<Mutex<PrometheusLayer>> = OnceLock::new();

    /// This logical tries to extract parent path from the object storage operation
    /// the function also relies on assumption that the region path is built from
    /// pattern `<data|index>/catalog/schema/table_id/...` OR `greptimedb/object_cache/<read|write>/...`
    ///
    /// We'll get the data/catalog/schema from path.
    pub fn build_prometheus_metrics_layer(_with_path_label: bool) -> PrometheusLayer {
        PROMETHEUS_LAYER
            .get_or_init(|| {
                // let path_level = if with_path_label { 3 } else { 0 };

                // opendal doesn't support dynamic path label trim
                // we have uuid in index path, which causes the label size to explode
                // remove path label first, waiting for later fix
                // TODO(shuiyisong): add dynamic path label trim for opendal

                let layer = PrometheusLayer::builder().register_default().unwrap();
                Mutex::new(layer)
            })
            .lock()
            .unwrap()
            .clone()
    }
}
