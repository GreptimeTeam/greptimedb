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

use std::convert::TryFrom;
use std::fs::File;

use snafu::ResultExt;

use crate::error::Error;

// TODO(hl): Implement pread/pwrite for non-Unix platforms
pub fn pread_exact(file: &File, _buf: &mut [u8], _offset: u64) -> Result<(), Error> {
    unimplemented!()
}

pub fn pwrite_all(file: &File, _buf: &[u8], _offset: u64) -> Result<(), Error> {
    unimplemented!()
}
