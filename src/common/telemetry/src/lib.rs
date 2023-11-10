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

pub mod logging;
mod macros;
pub mod metric;
mod panic_hook;
pub mod tracing_context;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub use logging::{init_default_ut_logging, init_global_logging};
pub use metric::dump_metrics;
use once_cell::sync::OnceCell;
pub use panic_hook::set_panic_hook;
use rand::random;
pub use {common_error, tracing};

static NODE_ID: OnceCell<u64> = OnceCell::new();

pub fn init_node_id(node_id: Option<String>) {
    let node_id = node_id.map(|id| calculate_hash(&id)).unwrap_or(random());
    match NODE_ID.set(node_id) {
        Ok(_) => {}
        Err(_) => warn!("node_id is already initialized"),
    }
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}
