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

use common_time::Timestamp;
use itertools::Itertools;
use snafu::OptionExt;

use crate::election::LeaderKey;
use crate::error::{Result, UnexpectedSnafu};

#[cfg(feature = "mysql_kvbackend")]
pub mod mysql;
#[cfg(feature = "pg_kvbackend")]
pub mod postgres;

// Separator between value and expire time in the lease string.
// A lease is put into rds election in the format:
// <node_info> || __metadata_lease_sep || <expire_time>
const LEASE_SEP: &str = r#"||__metadata_lease_sep||"#;

/// Parse the value and expire time from the given string retrieved from rds.
fn parse_value_and_expire_time(value: &str) -> Result<(String, Timestamp)> {
    let (value, expire_time) =
        value
            .split(LEASE_SEP)
            .collect_tuple()
            .with_context(|| UnexpectedSnafu {
                violated: format!(
                    "Invalid value {}, expect node info || {} || expire time",
                    value, LEASE_SEP
                ),
            })?;
    // Given expire_time is in the format 'YYYY-MM-DD HH24:MI:SS.MS'
    let expire_time = match Timestamp::from_str(expire_time, None) {
        Ok(ts) => ts,
        Err(_) => UnexpectedSnafu {
            violated: format!("Invalid timestamp: {}", expire_time),
        }
        .fail()?,
    };
    Ok((value.to_string(), expire_time))
}

/// LeaderKey used for [LeaderChangeMessage] in rds election components.
#[derive(Debug, Clone, Default)]
struct RdsLeaderKey {
    name: Vec<u8>,
    key: Vec<u8>,
    rev: i64,
    lease: i64,
}

impl LeaderKey for RdsLeaderKey {
    fn name(&self) -> &[u8] {
        &self.name
    }

    fn key(&self) -> &[u8] {
        &self.key
    }

    fn revision(&self) -> i64 {
        self.rev
    }

    fn lease_id(&self) -> i64 {
        self.lease
    }
}

/// Lease information for rds election.
#[derive(Default, Clone, Debug)]
struct Lease {
    leader_value: String,
    expire_time: Timestamp,
    current: Timestamp,
    // `origin` is the original value of the lease, used for CAS.
    origin: String,
}
