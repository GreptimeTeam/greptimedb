// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use arrow_flight::FlightData;
use prost::Message;

use crate::v1::{admin_result, AdminResult, MutateResult, ObjectResult, ResultHeader};

pub const PROTOCOL_VERSION: u32 = 1;

pub type Success = u32;
pub type Failure = u32;

#[derive(Default)]
pub struct ObjectResultBuilder {
    version: u32,
    code: u32,
    err_msg: Option<String>,
    flight_data: Option<Vec<FlightData>>,
}

impl ObjectResultBuilder {
    pub fn new() -> Self {
        Self {
            version: PROTOCOL_VERSION,
            ..Default::default()
        }
    }

    #[allow(dead_code)]
    pub fn version(mut self, version: u32) -> Self {
        self.version = version;
        self
    }

    pub fn status_code(mut self, code: u32) -> Self {
        self.code = code;
        self
    }

    pub fn err_msg(mut self, err_msg: String) -> Self {
        self.err_msg = Some(err_msg);
        self
    }

    pub fn flight_data(mut self, flight_data: Vec<FlightData>) -> Self {
        self.flight_data = Some(flight_data);
        self
    }

    pub fn build(self) -> ObjectResult {
        let header = Some(ResultHeader {
            version: self.version,
            code: self.code,
            err_msg: self.err_msg.unwrap_or_default(),
        });

        let flight_data = if let Some(flight_data) = self.flight_data {
            flight_data
                .into_iter()
                .map(|x| x.encode_to_vec())
                .collect::<Vec<Vec<u8>>>()
        } else {
            vec![]
        };
        ObjectResult {
            header,
            flight_data,
        }
    }
}

#[derive(Debug)]
pub struct AdminResultBuilder {
    version: u32,
    code: u32,
    err_msg: Option<String>,
    mutate: Option<(Success, Failure)>,
}

impl AdminResultBuilder {
    pub fn status_code(mut self, code: u32) -> Self {
        self.code = code;
        self
    }

    pub fn err_msg(mut self, err_msg: String) -> Self {
        self.err_msg = Some(err_msg);
        self
    }

    pub fn mutate_result(mut self, success: u32, failure: u32) -> Self {
        self.mutate = Some((success, failure));
        self
    }

    pub fn build(self) -> AdminResult {
        let header = Some(ResultHeader {
            version: self.version,
            code: self.code,
            err_msg: self.err_msg.unwrap_or_default(),
        });

        let result = if let Some((success, failure)) = self.mutate {
            Some(admin_result::Result::Mutate(MutateResult {
                success,
                failure,
            }))
        } else {
            None
        };

        AdminResult { header, result }
    }
}

impl Default for AdminResultBuilder {
    fn default() -> Self {
        Self {
            version: PROTOCOL_VERSION,
            code: 0,
            err_msg: None,
            mutate: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_result_builder() {
        let obj_result = ObjectResultBuilder::new()
            .version(101)
            .status_code(500)
            .err_msg("Failed to read this file!".to_string())
            .build();
        let header = obj_result.header.unwrap();
        assert_eq!(101, header.version);
        assert_eq!(500, header.code);
        assert_eq!("Failed to read this file!", header.err_msg);
    }
}
