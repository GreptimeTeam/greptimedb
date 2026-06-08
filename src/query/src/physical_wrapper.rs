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

use std::sync::{Arc, RwLock};

use datafusion::config::ConfigOptions;
use datafusion::physical_plan::ExecutionPlan;
use session::context::QueryContextRef;

/// wrap physical plan with additional layer
/// e.g: metrics retrieving layer upon physical plan
pub trait PhysicalPlanWrapper: Send + Sync + 'static {
    fn wrap(
        &self,
        origin: Arc<dyn ExecutionPlan>,
        ctx: QueryContextRef,
        config: &ConfigOptions,
    ) -> Arc<dyn ExecutionPlan>;
}

pub type PhysicalPlanWrapperRef = Arc<dyn PhysicalPlanWrapper>;

#[derive(Default)]
pub struct PhysicalPlanWrappers {
    wrappers: RwLock<Vec<PhysicalPlanWrapperRef>>,
}

impl PhysicalPlanWrappers {
    pub fn push(&self, wrapper: PhysicalPlanWrapperRef) {
        self.wrappers.write().unwrap().push(wrapper);
    }

    pub fn wrap(
        &self,
        origin: Arc<dyn ExecutionPlan>,
        ctx: QueryContextRef,
        config: &ConfigOptions,
    ) -> Arc<dyn ExecutionPlan> {
        let wrappers = self.wrappers.read().unwrap().clone();
        wrappers.into_iter().fold(origin, |plan, wrapper| {
            wrapper.wrap(plan, ctx.clone(), config)
        })
    }
}

pub type PhysicalPlanWrappersRef = Arc<PhysicalPlanWrappers>;

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use datafusion::arrow::datatypes::Schema;
    use datafusion::config::ConfigOptions;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::empty::EmptyExec;
    use session::context::{QueryContextBuilder, QueryContextRef};

    use super::{PhysicalPlanWrapper, PhysicalPlanWrappers};

    struct RecordingWrapper {
        marker: &'static str,
        calls: Arc<Mutex<Vec<&'static str>>>,
    }

    impl PhysicalPlanWrapper for RecordingWrapper {
        fn wrap(
            &self,
            origin: Arc<dyn ExecutionPlan>,
            _ctx: QueryContextRef,
            _config: &ConfigOptions,
        ) -> Arc<dyn ExecutionPlan> {
            self.calls.lock().unwrap().push(self.marker);
            origin
        }
    }

    #[test]
    fn physical_plan_wrappers_apply_all_in_order() {
        let wrappers = PhysicalPlanWrappers::default();
        let calls = Arc::new(Mutex::new(Vec::new()));
        wrappers.push(Arc::new(RecordingWrapper {
            marker: "first",
            calls: calls.clone(),
        }));
        wrappers.push(Arc::new(RecordingWrapper {
            marker: "second",
            calls: calls.clone(),
        }));

        let plan = Arc::new(EmptyExec::new(Arc::new(Schema::empty()))) as Arc<dyn ExecutionPlan>;
        let ctx = Arc::new(QueryContextBuilder::default().build());
        let wrapped = wrappers.wrap(Arc::clone(&plan), ctx, &ConfigOptions::default());

        assert!(Arc::ptr_eq(&plan, &wrapped));
        assert_eq!(*calls.lock().unwrap(), vec!["first", "second"]);
    }
}
