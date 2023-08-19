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

mod error;

use std::ffi::{c_char, CString};
use std::path::PathBuf;

use error::{
    BuildTempPathSnafu, DumpProfileDataSnafu, OpenTempFileSnafu, ProfilingNotEnabledSnafu,
    ReadOptProfSnafu,
};
use snafu::{ensure, ResultExt};
use tokio::io::AsyncReadExt;

use crate::error::Result;

const PROF_DUMP: &[u8] = b"prof.dump\0";
const OPT_PROF: &[u8] = b"opt.prof\0";

pub async fn dump_profile() -> Result<Vec<u8>> {
    ensure!(is_prof_enabled()?, ProfilingNotEnabledSnafu);
    let tmp_path = tempfile::tempdir().map_err(|_| {
        BuildTempPathSnafu {
            path: std::env::temp_dir(),
        }
        .build()
    })?;

    let mut path_buf = PathBuf::from(tmp_path.path());
    path_buf.push("greptimedb.hprof");

    let path = path_buf
        .to_str()
        .ok_or_else(|| BuildTempPathSnafu { path: &path_buf }.build())?
        .to_string();

    let mut bytes = CString::new(path.as_str())
        .map_err(|_| BuildTempPathSnafu { path: &path_buf }.build())?
        .into_bytes_with_nul();

    {
        // #safety: we always expect a valid temp file path to write profiling data to.
        let ptr = bytes.as_mut_ptr() as *mut c_char;
        unsafe {
            tikv_jemalloc_ctl::raw::write(PROF_DUMP, ptr)
                .context(DumpProfileDataSnafu { path: path_buf })?
        }
    }

    let mut f = tokio::fs::File::open(path.as_str())
        .await
        .context(OpenTempFileSnafu { path: &path })?;
    let mut buf = vec![];
    let _ = f
        .read_to_end(&mut buf)
        .await
        .context(OpenTempFileSnafu { path })?;
    Ok(buf)
}

fn is_prof_enabled() -> Result<bool> {
    // safety: OPT_PROF variable, if present, is always a boolean value.
    Ok(unsafe { tikv_jemalloc_ctl::raw::read::<bool>(OPT_PROF).context(ReadOptProfSnafu)? })
}
