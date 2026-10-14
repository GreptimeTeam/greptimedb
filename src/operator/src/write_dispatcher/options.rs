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

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Exponential backoff for retryable distributed writes.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct WriteRetryOptions {
    /// Maximum additional attempts after the original write. Zero disables retries.
    pub max_retries: usize,
    /// Initial backoff before jitter is applied.
    #[serde(with = "humantime_serde")]
    pub initial_backoff: Duration,
    /// Maximum backoff before jitter is applied.
    #[serde(with = "humantime_serde")]
    pub max_backoff: Duration,
    /// Multiplier applied to the backoff after each retry.
    pub backoff_multiplier: f32,
}

impl Default for WriteRetryOptions {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff: Duration::from_millis(1500),
            max_backoff: Duration::from_secs(10),
            backoff_multiplier: 2.0,
        }
    }
}

impl WriteRetryOptions {
    /// Checks that the configured backoff is positive, bounded, and nondecreasing.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.initial_backoff.is_zero() {
            return Err("write_retry.initial_backoff must be positive");
        }
        if self.max_backoff < self.initial_backoff {
            return Err("write_retry.max_backoff must be at least initial_backoff");
        }
        if !self.backoff_multiplier.is_finite() || self.backoff_multiplier < 1.0 {
            return Err("write_retry.backoff_multiplier must be finite and at least 1");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_write_retry_options() {
        for options in [
            WriteRetryOptions::default(),
            WriteRetryOptions {
                max_retries: 0,
                ..Default::default()
            },
            WriteRetryOptions {
                backoff_multiplier: 1.0,
                ..Default::default()
            },
        ] {
            options.validate().unwrap();
        }
        for options in [
            WriteRetryOptions {
                initial_backoff: Duration::ZERO,
                ..Default::default()
            },
            WriteRetryOptions {
                max_backoff: Duration::from_secs(1),
                ..Default::default()
            },
            WriteRetryOptions {
                backoff_multiplier: 0.5,
                ..Default::default()
            },
            WriteRetryOptions {
                backoff_multiplier: f32::NAN,
                ..Default::default()
            },
            WriteRetryOptions {
                backoff_multiplier: f32::INFINITY,
                ..Default::default()
            },
        ] {
            assert!(options.validate().is_err(), "{options:?}");
        }
    }
}
