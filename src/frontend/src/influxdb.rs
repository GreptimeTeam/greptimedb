use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InfluxdbOptions {
    pub enable: bool,
}

impl Default for InfluxdbOptions {
    fn default() -> Self {
        Self { enable: true }
    }
}

#[cfg(test)]
mod tests {
    use super::InfluxdbOptions;

    #[test]
    fn test_influxdb_options() {
        let default = InfluxdbOptions::default();
        assert!(default.enable);
    }
}
