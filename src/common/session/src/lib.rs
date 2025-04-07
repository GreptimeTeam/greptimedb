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

use strum::{AsRefStr, Display, EnumString};

/// Defines the read preference for frontend route operations,
/// determining whether to read from the region leader or follower.
#[derive(Debug, Clone, Copy, Default, EnumString, Display, AsRefStr, PartialEq, Eq)]
pub enum ReadPreference {
    #[default]
    // Reads all operations from the region leader. This is the default mode.
    #[strum(serialize = "leader", to_string = "LEADER")]
    Leader,
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::ReadPreference;

    #[test]
    fn test_read_preference() {
        assert_eq!(ReadPreference::Leader.to_string(), "LEADER");

        let read_preference = ReadPreference::from_str("LEADER").unwrap();
        assert_eq!(read_preference, ReadPreference::Leader);

        let read_preference = ReadPreference::from_str("leader").unwrap();
        assert_eq!(read_preference, ReadPreference::Leader);

        ReadPreference::from_str("follower").unwrap_err();
    }
}
