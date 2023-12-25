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

use crate::error::{CastSnafu, Result};
use crate::kafka::EntryId;

/// A wrapper of kafka offset.
pub(crate) struct Offset(pub i64);

impl TryFrom<Offset> for EntryId {
    type Error = crate::error::Error;

    fn try_from(offset: Offset) -> Result<Self> {
        EntryId::try_from(offset.0).map_err(|_| CastSnafu.build())
    }
}

impl TryFrom<EntryId> for Offset {
    type Error = crate::error::Error;

    fn try_from(entry_id: EntryId) -> Result<Self> {
        i64::try_from(entry_id)
            .map(Offset)
            .map_err(|_| CastSnafu.build())
    }
}
