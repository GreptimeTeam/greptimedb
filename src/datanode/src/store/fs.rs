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

use common_telemetry::logging::info;
use object_store::services::Fs as FsBuilder;
use object_store::{util, ObjectStore};
use snafu::prelude::*;

use crate::datanode::FileConfig;
use crate::error::{self, Result};
use crate::store;

pub(crate) async fn new_fs_object_store(file_config: &FileConfig) -> Result<ObjectStore> {
    let data_home = util::normalize_dir(&file_config.data_home);
    fs::create_dir_all(path::Path::new(&data_home))
        .context(error::CreateDirSnafu { dir: &data_home })?;
    info!("The file storage home is: {}", &data_home);

    let atomic_write_dir = format!("{data_home}.tmp/");
    store::clean_temp_dir(&atomic_write_dir)?;

    let mut builder = FsBuilder::default();
    let _ = builder.root(&data_home).atomic_write_dir(&atomic_write_dir);

    let object_store = ObjectStore::new(builder)
        .context(error::InitBackendSnafu)?
        .finish();

    Ok(object_store)
}
