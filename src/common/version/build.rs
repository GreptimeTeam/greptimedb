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

use build_data::{format_timestamp, get_source_time};

fn main() -> shadow_rs::SdResult<()> {
    println!("cargo:rerun-if-changed=.git/refs/heads");
    println!(
        "cargo:rustc-env=SOURCE_TIMESTAMP={}",
        if let Ok(t) = get_source_time() {
            format_timestamp(t)
        } else {
            "".to_string()
        }
    );
    build_data::set_BUILD_TIMESTAMP();
    shadow_rs::new()
}
