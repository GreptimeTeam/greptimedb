use std::path::Path;

#[derive(Debug, Clone)]
pub struct LogConfig {
    pub append_buffer_size: usize,
    pub max_log_file_size: usize,
    pub log_file_dir: String,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            append_buffer_size: 128,
            max_log_file_size: 1024 * 1024 * 1024,
            log_file_dir: Path::new(env!("HOME"))
                .join("logfiles")
                .to_str()
                .unwrap_or("~/logfiles")
                .to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use common_telemetry::info;

    use super::*;

    #[test]
    pub fn test_default_config() {
        common_telemetry::logging::init_default_ut_logging();
        info!("LogConfig::default(): {:?}", LogConfig::default());
    }
}
