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

use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display, EnumIter, EnumString};

/// Defines the read preference for frontend route operations,
/// determining whether to read from the region leader or follower.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    EnumIter,
    EnumString,
    Display,
    AsRefStr,
    PartialEq,
    Serialize,
    Deserialize,
)]
pub enum ReadPreference {
    #[default]
    // Reads all operations from the region leader. This is the default mode.
    #[strum(serialize = "leader", to_string = "LEADER")]
    Leader,
}

/// Wire values accepted by `ReadPreference::from_str`, in lower case.
pub const READ_PREFERENCE_VALUES: &[&str] = &["leader"];

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use strum::IntoEnumIterator;

    use crate::{READ_PREFERENCE_VALUES, ReadPreference};

    #[test]
    fn test_read_preference_values_match_enum() {
        let values: Vec<_> = ReadPreference::iter()
            .map(|variant| {
                let wire = variant.to_string().to_ascii_lowercase();
                assert_eq!(ReadPreference::from_str(&wire).unwrap(), variant);
                wire
            })
            .collect();
        assert_eq!(values, READ_PREFERENCE_VALUES);
    }

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
