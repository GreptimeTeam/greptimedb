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
    use tempdir::TempDir;

    use super::*;
    use crate::error::Result;

    #[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
    struct MockConfig {
        path: String,
        port: u32,
        host: String,
    }

    #[test]
    fn test_from_file() -> Result<()> {
        let config = MockConfig {
            path: "/tmp".to_string(),
            port: 999,
            host: "greptime.test".to_string(),
        };

        let dir = TempDir::new("test_from_file").unwrap();
        let test_file = format!("{}/test.toml", dir.path().to_str().unwrap());

        let s = toml::to_string(&config).unwrap();
        assert!(s.contains("host") && s.contains("path") && s.contains("port"));

        let mut file = File::create(&test_file).unwrap();
        file.write_all(s.as_bytes()).unwrap();

        let loaded_config: MockConfig = from_file!(&test_file)?;
        assert_eq!(loaded_config, config);

        Ok(())
    }
}
