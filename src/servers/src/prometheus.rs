//! promethues protcol supportings
use openmetrics_parser::{MetricsExposition, PrometheusType, PrometheusValue};

pub struct Metrics {
    pub exposition: MetricsExposition<PrometheusType, PrometheusValue>,
}
