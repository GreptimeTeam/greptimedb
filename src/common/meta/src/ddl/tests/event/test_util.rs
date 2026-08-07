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

use std::collections::HashSet;
use std::sync::Arc;

use api::v1::Value;
use common_event_recorder::EventTypeFilter;
use common_event_recorder::event_table::jsonb_value;
use common_procedure::{EventRuntimeContext, EventTrigger, Procedure, ProcedureId, ProcedureState};

use crate::rpc::ddl::EventContext;

pub(crate) fn default_event_context_value() -> Value {
    jsonb_value(&serde_json::to_value(EventContext::default()).unwrap())
}

pub(crate) fn procedure_trigger_value(trigger: &str) -> Value {
    jsonb_value(&serde_json::json!({"type": trigger}))
}

pub(crate) fn assert_event_filter(procedure: &dyn Procedure, event_type: &str) {
    let state = ProcedureState::Running;
    let runtime_context = |event_type_filter| EventRuntimeContext {
        procedure_id: ProcedureId::random(),
        lifecycle_state: &state,
        trigger: EventTrigger::Submitted,
        event_type_filter: Arc::new(event_type_filter),
    };

    let allowed = procedure
        .event(&runtime_context(EventTypeFilter::Only(HashSet::from([
            event_type.to_string(),
        ]))))
        .unwrap();
    assert_eq!(allowed.event_type(), event_type);

    for denied in [HashSet::from(["other_event".to_string()]), HashSet::new()] {
        assert!(
            procedure
                .event(&runtime_context(EventTypeFilter::Only(denied)))
                .is_none()
        );
    }
}
