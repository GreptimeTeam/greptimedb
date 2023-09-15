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

use common_error::ext::BoxedError;
use common_procedure::error::Error as ProcedureError;
use snafu::{location, Location};

use crate::error::{self, Error};
use crate::peer::Peer;

pub fn handle_operate_region_error(datanode: Peer) -> impl FnOnce(crate::error::Error) -> Error {
    move |err| {
        if matches!(err, crate::error::Error::RetryLater { .. }) {
            error::Error::RetryLater {
                source: BoxedError::new(err),
            }
        } else {
            error::Error::OperateDatanode {
                location: location!(),
                peer: datanode,
                source: BoxedError::new(err),
            }
        }
    }
}

pub fn handle_retry_error(e: Error) -> ProcedureError {
    if matches!(e, error::Error::RetryLater { .. }) {
        ProcedureError::retry_later(e)
    } else {
        ProcedureError::external(e)
    }
}

#[inline]
pub fn region_storage_path(catalog: &str, schema: &str) -> String {
    format!("{}/{}", catalog, schema)
}
