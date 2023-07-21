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

pub use logging::{init_default_ut_logging, init_global_logging};
pub use metric::init_default_metrics_recorder;
use once_cell::sync::OnceCell;
pub use panic_hook::set_panic_hook;
use parking_lot::Mutex;
use snowflake::SnowflakeIdBucket;
pub use {common_error, tracing, tracing_appender, tracing_futures, tracing_subscriber};

static NODE_ID: OnceCell<u64> = OnceCell::new();
static TRACE_BUCKET: OnceCell<Mutex<SnowflakeIdBucket>> = OnceCell::new();

pub fn gen_trace_id() -> u64 {
    let mut bucket = TRACE_BUCKET
        .get_or_init(|| {
            // if node_id is not initialized, how about random one?
            let node_id = NODE_ID.get_or_init(|| 0);
            info!("initializing bucket with node_id: {}", node_id);
            let bucket = SnowflakeIdBucket::new(1, (*node_id) as i32);
            Mutex::new(bucket)
        })
        .lock();
    (*bucket).get_id() as u64
}

pub fn init_node_id(node_id: u64) {
    match NODE_ID.set(node_id) {
        Ok(_) => {}
        Err(_) => warn!("node_id is already initialized"),
    }
}
