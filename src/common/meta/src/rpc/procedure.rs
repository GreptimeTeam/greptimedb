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

use std::time::Duration;

pub use api::v1::meta::{MigrateRegionResponse, ProcedureStateResponse};
use api::v1::meta::{
    ProcedureId as PbProcedureId, ProcedureStateResponse as PbProcedureStateResponse,
    ProcedureStatus as PbProcedureStatus,
};
use common_procedure::{ProcedureId, ProcedureState};
use snafu::ResultExt;

use crate::error::{ParseProcedureIdSnafu, Result};

/// A request to migrate region.
#[derive(Clone)]
pub struct MigrateRegionRequest {
    pub region_id: u64,
    pub from_peer: u64,
    pub to_peer: u64,
    pub replay_timeout: Duration,
}

/// Cast the protobuf [`ProcedureId`] to common [`ProcedureId`].
pub fn pb_pid_to_pid(pid: &PbProcedureId) -> Result<ProcedureId> {
    ProcedureId::parse_str(&String::from_utf8_lossy(&pid.key)).with_context(|_| {
        ParseProcedureIdSnafu {
            key: hex::encode(&pid.key),
        }
    })
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
        ProcedureState::PrepareRollback { error } => {
            (PbProcedureStatus::PrepareRollback, error.to_string())
        }
        ProcedureState::RollingBack { error } => {
            (PbProcedureStatus::RollingBack, error.to_string())
        }
    };

    PbProcedureStateResponse {
        status: status.into(),
        error,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_procedure::Error;
    use snafu::Location;

    use super::*;

    #[test]
    fn test_pid_pb_pid_conversion() {
        let pid = ProcedureId::random();

        let pb_pid = pid_to_pb_pid(pid);

        assert_eq!(pid, pb_pid_to_pid(&pb_pid).unwrap());
    }

    #[test]
    fn test_procedure_state_to_pb_response() {
        let state = ProcedureState::Running;
        let resp = procedure_state_to_pb_response(&state);
        assert_eq!(PbProcedureStatus::Running as i32, resp.status);
        assert!(resp.error.is_empty());

        let state = ProcedureState::Done { output: None };
        let resp = procedure_state_to_pb_response(&state);
        assert_eq!(PbProcedureStatus::Done as i32, resp.status);
        assert!(resp.error.is_empty());

        let state = ProcedureState::Retrying {
            error: Arc::new(Error::ManagerNotStart {
                location: Location::default(),
            }),
        };
        let resp = procedure_state_to_pb_response(&state);
        assert_eq!(PbProcedureStatus::Retrying as i32, resp.status);
        assert_eq!("Procedure Manager is stopped", resp.error);

        let state = ProcedureState::Failed {
            error: Arc::new(Error::ManagerNotStart {
                location: Location::default(),
            }),
        };
        let resp = procedure_state_to_pb_response(&state);
        assert_eq!(PbProcedureStatus::Failed as i32, resp.status);
        assert_eq!("Procedure Manager is stopped", resp.error);
    }
}
