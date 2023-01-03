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

use std::fs::File;
use std::os::unix::fs::FileExt;

use snafu::ResultExt;

use crate::error::{Error, IoSnafu};

pub fn pread_exact(file: &File, buf: &mut [u8], offset: u64) -> Result<(), Error> {
    file.read_exact_at(buf, offset).context(IoSnafu)
}

pub fn pwrite_all(file: &File, buf: &[u8], offset: u64) -> Result<(), Error> {
    file.write_all_at(buf, offset).context(IoSnafu)
}
