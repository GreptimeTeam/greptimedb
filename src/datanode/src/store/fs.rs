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

use std::{fs, path};

use common_telemetry::info;
use object_store::services::Fs;
use object_store::util::join_dir;
use object_store::ObjectStore;
use snafu::prelude::*;

use crate::config::FileConfig;
use crate::error::{self, Result};
use crate::store;

pub(crate) async fn new_fs_object_store(
    data_home: &str,
    _file_config: &FileConfig,
) -> Result<ObjectStore> {
    fs::create_dir_all(path::Path::new(&data_home))
        .context(error::CreateDirSnafu { dir: data_home })?;
    info!("The file storage home is: {}", data_home);

    let atomic_write_dir = join_dir(data_home, ".tmp/");
    store::clean_temp_dir(&atomic_write_dir)?;

    let builder = Fs::default()
        .root(data_home)
        .atomic_write_dir(&atomic_write_dir);

    let object_store = ObjectStore::new(builder)
        .context(error::InitBackendSnafu)?
        .finish();

    Ok(object_store)
}
