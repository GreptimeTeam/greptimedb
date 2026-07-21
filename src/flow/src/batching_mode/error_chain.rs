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

use std::error::Error;

pub(crate) fn error_chain_contains_any(err: &(dyn Error + 'static), markers: &[&str]) -> bool {
    fn contains_marker(text: String, markers: &[&str]) -> bool {
        let text = text.to_ascii_lowercase();
        markers.iter().any(|marker| text.contains(marker))
    }

    if contains_marker(err.to_string(), markers) || contains_marker(format!("{err:?}"), markers) {
        return true;
    }

    let mut source = err.source();
    while let Some(err) = source {
        if contains_marker(err.to_string(), markers) || contains_marker(format!("{err:?}"), markers)
        {
            return true;
        }
        source = err.source();
    }

    false
}
