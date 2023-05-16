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

use std::hash::{Hash, Hasher};

use ahash::AHasher;
use datafusion_expr::LogicalPlan;

/// Calculate u64 hash for a [LogicalPlan].
pub fn hash_plan(plan: &LogicalPlan) -> u64 {
    let mut hasher = AHasher::default();
    plan.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod test {
    use datafusion_expr::LogicalPlanBuilder;

    use super::*;

    #[test]
    fn hash_two_plan() {
        let plan1 = LogicalPlanBuilder::empty(false).build().unwrap();
        let plan2 = LogicalPlanBuilder::empty(false)
            .explain(false, false)
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(hash_plan(&plan1), hash_plan(&plan1));
        assert_ne!(hash_plan(&plan1), hash_plan(&plan2));
    }
}
