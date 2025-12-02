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

fn main() {
    // Set DEFAULT_CATALOG_NAME from environment variable or use default value
    let default_catalog_name =
        std::env::var("DEFAULT_CATALOG_NAME").unwrap_or_else(|_| "greptime".to_string());

    println!(
        "cargo:rustc-env=DEFAULT_CATALOG_NAME={}",
        default_catalog_name
    );

    // Rerun build script if the environment variable changes
    println!("cargo:rerun-if-env-changed=DEFAULT_CATALOG_NAME");
}
