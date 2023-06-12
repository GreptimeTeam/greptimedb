// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod table_route;

pub use crate::key::table_route::{TableRouteKey, TABLE_ROUTE_PREFIX};

pub const REMOVED_PREFIX: &str = "__removed";

pub fn to_removed_key(key: &str) -> String {
    format!("{REMOVED_PREFIX}-{key}")
}

#[cfg(test)]
mod tests {
    use crate::key::to_removed_key;

    #[test]
    fn test_to_removed_key() {
        let key = "test_key";
        let removed = "__removed-test_key";
        assert_eq!(removed, to_removed_key(key));
    }
}
