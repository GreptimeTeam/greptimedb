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

// For the given format: `x-greptime-hints: auto_create_table=true, ttl=7d`
pub const HINTS_KEY: &str = "x-greptime-hints";
/// Deprecated, use `HINTS_KEY` instead. Notes if "x-greptime-hints" is set, keys with this prefix will be ignored.
pub const HINTS_KEY_PREFIX: &str = "x-greptime-hint-";

pub const READ_PREFERENCE_HINT: &str = "read_preference";

/// Deprecated, use `HINTS_KEY` instead.
pub const HINT_KEYS: [&str; 7] = [
    "x-greptime-hint-auto_create_table",
    "x-greptime-hint-ttl",
    "x-greptime-hint-append_mode",
    "x-greptime-hint-merge_mode",
    "x-greptime-hint-physical_table",
    "x-greptime-hint-skip_wal",
    "x-greptime-hint-read_preference",
];
