use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrometheusOptions {
    pub enable: bool,
}

impl Default for PrometheusOptions {
    fn default() -> Self {
        Self { enable: true }
    }
}

#[cfg(test)]
mod tests {
    use super::PrometheusOptions;

    #[test]
    fn test_influxdb_options() {
        let default = PrometheusOptions::default();
        assert!(default.enable);
    }
}
