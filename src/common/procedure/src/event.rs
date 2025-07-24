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

use std::any::Any;

use common_event_recorder::Event;
use common_time::timestamp::{TimeUnit, Timestamp};

use crate::{ProcedureId, ProcedureState};

/// `ProcedureEvent` represents an event emitted by a procedure during its execution lifecycle.
#[derive(Debug, Clone)]
pub struct ProcedureEvent {
    /// Unique identifier associated with the originating procedure instance.
    pub procedure_id: ProcedureId,
    /// Human-readable, serialized representation of the procedure's metadata that is essential for the caller to interpret the procedure's state.
    /// This field is intended to be deserialized by consumers into a concrete data structure.
    pub procedure_dump_data: String,
    /// The state of the procedure.
    pub state: ProcedureState,
    /// The timestamp of the event.
    pub timestamp: Timestamp,
}

impl ProcedureEvent {
    pub fn new(
        procedure_id: ProcedureId,
        procedure_dump_data: String,
        state: ProcedureState,
    ) -> Self {
        Self {
            procedure_id,
            procedure_dump_data,
            state,
            timestamp: Timestamp::current_time(TimeUnit::Nanosecond),
        }
    }
}

impl Event for ProcedureEvent {
    fn event_type(&self) -> &str {
        "procedure_event"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
