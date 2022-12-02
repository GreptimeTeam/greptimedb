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

pub mod ext;
pub mod format;
pub mod mock;
pub mod status_code;

pub mod prelude {
    pub use snafu::prelude::*;
    pub use snafu::{Backtrace, ErrorCompat};

    pub use crate::ext::{BoxedError, ErrorExt};
    pub use crate::format::DebugFormat;
    pub use crate::status_code::StatusCode;
}

pub use snafu;
