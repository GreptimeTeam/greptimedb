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

macro_rules! from_file {
    ($path: expr) => {
        toml::from_str(
            &std::fs::read_to_string($path)
                .context(crate::error::ReadConfigSnafu { path: $path })?,
        )
        .context(crate::error::ParseConfigSnafu)
    };
}

pub(crate) use from_file;

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;

    use serde::{Deserialize, Serialize};
    use snafu::ResultExt;

    use super::*;
    use crate::error::Result;

    #[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
    #[serde(default)]
    struct MockConfig {
        path: String,
        port: u32,
        host: String,
    }

    impl Default for MockConfig {
        fn default() -> Self {
            Self {
                path: "test".to_string(),
                port: 0,
                host: "localhost".to_string(),
            }
        }
    }

    #[test]
    fn test_from_file() -> Result<()> {
        let config = MockConfig {
            path: "/tmp".to_string(),
            port: 999,
            host: "greptime.test".to_string(),
        };

        let dir = tempfile::Builder::new()
            .prefix("test_from_file")
            .tempdir()
            .unwrap();
        let test_file = format!("{}/test.toml", dir.path().to_str().unwrap());

        let s = toml::to_string(&config).unwrap();
        assert!(s.contains("host") && s.contains("path") && s.contains("port"));

        let mut file = File::create(&test_file).unwrap();
        file.write_all(s.as_bytes()).unwrap();

        let loaded_config: MockConfig = from_file!(&test_file)?;
        assert_eq!(loaded_config, config);

        // Only host in file
        let mut file = File::create(&test_file).unwrap();
        file.write_all("host='greptime.test'\n".as_bytes()).unwrap();

        let loaded_config: MockConfig = from_file!(&test_file)?;
        assert_eq!(loaded_config.host, "greptime.test");
        assert_eq!(loaded_config.port, 0);
        assert_eq!(loaded_config.path, "test");

        // Truncate the file.
        let file = File::create(&test_file).unwrap();
        file.set_len(0).unwrap();
        let loaded_config: MockConfig = from_file!(&test_file)?;
        assert_eq!(loaded_config, MockConfig::default());

        Ok(())
    }
}
