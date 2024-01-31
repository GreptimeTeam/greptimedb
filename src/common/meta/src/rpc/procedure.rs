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

use api::v1::meta::{
    ProcedureId as PbProcedureId, ProcedureStateResponse as PbProcedureStateResponse,
    ProcedureStatus as PbProcedureStatus,
};
use common_procedure::{ProcedureId, ProcedureState};
use snafu::ResultExt;

use crate::error::{ParseProcedureIdSnafu, Result};

/// Cast the protobuf [`ProcedureId`] to common [`ProcedureId`].
pub fn pb_pid_to_pid(pid: &PbProcedureId) -> Result<ProcedureId> {
    ProcedureId::parse_str(&String::from_utf8_lossy(&pid.key)).context(ParseProcedureIdSnafu)
}

/// Cast the common [`ProcedureId`] to protobuf [`ProcedureId`].
pub fn pid_to_pb_pid(pid: ProcedureId) -> PbProcedureId {
    PbProcedureId {
        key: pid.to_string().into(),
    }
}

/// Cast the common [`ProcedureState`] to pb [`ProcedureStateResponse`].
pub fn procedure_state_to_pb_response(state: &ProcedureState) -> PbProcedureStateResponse {
    let (status, error) = match state {
        ProcedureState::Running => (PbProcedureStatus::Running, String::default()),
        ProcedureState::Done { .. } => (PbProcedureStatus::Done, String::default()),
        ProcedureState::Retrying { error } => (PbProcedureStatus::Retrying, error.to_string()),
        ProcedureState::Failed { error } => (PbProcedureStatus::Failed, error.to_string()),
    };

    PbProcedureStateResponse {
        status: status.into(),
        error,
        ..Default::default()
    }
}
