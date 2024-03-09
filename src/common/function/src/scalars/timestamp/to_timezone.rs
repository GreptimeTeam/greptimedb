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

use common_time::{Timestamp, Timezone};
use datatypes::value::Value;

#[derive(Clone, Debug, Default)]
pub struct ToTimezoneFunction;

const NAME: &str = "to_timezone";

fn convert_to_timezone(arg: &str) -> Option<Timezone> {
    match Timezone::from_tz_string(arg) {
        Ok(ts) => Some(ts),
        Err(_) => None,
    }
}

fn convert_to_timestamp(arg: &Value) -> Option<Timestamp> {
    match arg {
        Value::Int64(ts) => Some(Timestamp::from(*ts)),
        Value::String(ts) => match Timestamp::from_str(ts.as_utf8(), None) {
            Ok(ts) => Some(ts),
            Err(_) => None,
        },
        Value::Timestamp(ts) => Some(*ts),
        _ => None,
    }
}

fn process_to_timezone(
    times: Vec<Option<Timestamp>>,
    tzs: Vec<Option<Timezone>>,
) -> Vec<Option<String>> {
    times
        .iter()
        .zip(tzs.iter())
        .map(|(time, tz)| match (time, tz) {
            (Some(time), _) => Some(time.to_timezone_aware_string(tz.as_ref())),
            _ => None,
        })
        .collect::<Vec<Option<String>>>()
}
