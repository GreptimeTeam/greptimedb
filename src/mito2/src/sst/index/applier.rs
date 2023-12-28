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

pub mod builder;

use std::sync::Arc;

use index::inverted_index::search::index_apply::IndexApplier;
use object_store::ObjectStore;

#[allow(dead_code)]
#[derive(Clone)]
pub struct SstIndexApplier {
    region_dir: String,
    object_store: ObjectStore,
    index_applier: Arc<dyn IndexApplier>,
}

impl SstIndexApplier {
    pub fn new(
        region_dir: String,
        object_store: ObjectStore,
        index_applier: Arc<dyn IndexApplier>,
    ) -> Self {
        Self {
            region_dir,
            object_store,
            index_applier,
        }
    }
}
