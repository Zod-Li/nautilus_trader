// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Strongly typed configuration for the Alpaca Markets adapter.

use url::Url;

use crate::{
    common::{
        consts::ALPACA_VENUE,
        credential::AlpacaCredential,
        enums::AlpacaEnvironment,
    },
    error::{AlpacaError, AlpacaResult},
};

/// Minimum HTTP request timeout in seconds.
pub const MIN_TIMEOUT_SECS: u64 = 1;
/// Maximum HTTP request timeout in seconds.
pub const MAX_TIMEOUT_SECS: u64 = 120;
/// Maximum retry count for transient HTTP failures.
pub const MAX_RETRIES: u32 = 10;

/// Configuration for the Alpaca Markets REST and WebSocket execution adapter.
#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        name = "AlpacaExecClientConfig",
        module = "nautilus_trader.adapters.alpaca",
        from_py_object
    )
)]
pub struct AlpacaConfig {
    /// Target environment (`Paper` or `Live`).
    pub environment: AlpacaEnvironment,
    /// Base HTTP(S) URL for Alpaca Trading REST API v2.
    pub http_base_url: String,
    /// WebSocket URL for Alpaca `trade_updates` stream.
    pub ws_url: String,
    /// Zeroized API credential pair (`APCA-API-KEY-ID` / `APCA-API-SECRET-KEY`).
    pub credential: AlpacaCredential,
    /// Request timeout in seconds (`1..=120`).
    pub timeout_secs: u64,
    /// Maximum retry attempts on transient `429` / `5xx` errors (`0..=10`).
    pub max_retries: u32,
    /// Initial retry backoff in milliseconds.
    pub retry_initial_ms: u64,
    /// Maximum retry backoff in milliseconds.
    pub retry_max_ms: u64,
    /// Rate limit quota in requests per second (`1..=200`).
    pub rate_limit_per_sec: u32,
    /// Venue identifier (defaults to `"ALPACA"`).
    pub venue: String,
}

impl AlpacaConfig {
    /// Creates a validated [`AlpacaConfig`] for the given environment with default Alpaca URLs.
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaError::InvalidConfig`] if any parameter is invalid.
    pub fn new_for_environment(
        environment: AlpacaEnvironment,
        credential: AlpacaCredential,
        timeout_secs: u64,
        max_retries: u32,
    ) -> AlpacaResult<Self> {
        Self::new_checked(
            environment,
            environment.default_http_url(),
            environment.default_ws_url(),
            credential,
            timeout_secs,
            max_retries,
            250,
            5_000,
            10,
            ALPACA_VENUE,
        )
    }

    /// Creates and validates an [`AlpacaConfig`] with explicit URLs and retry/rate-limit bounds.
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaError::InvalidConfig`] if any URL or numeric bound is invalid.
    #[expect(clippy::too_many_arguments)]
    pub fn new_checked(
        environment: AlpacaEnvironment,
        http_base_url: impl Into<String>,
        ws_url: impl Into<String>,
        credential: AlpacaCredential,
        timeout_secs: u64,
        max_retries: u32,
        retry_initial_ms: u64,
        retry_max_ms: u64,
        rate_limit_per_sec: u32,
        venue: impl Into<String>,
    ) -> AlpacaResult<Self> {
        let cfg = Self {
            environment,
            http_base_url: http_base_url.into().trim_end_matches('/').to_string(),
            ws_url: ws_url.into(),
            credential,
            timeout_secs,
            max_retries,
            retry_initial_ms,
            retry_max_ms,
            rate_limit_per_sec,
            venue: venue.into(),
        };
        cfg.validate()?;
        Ok(cfg)
    }

    /// Validates all fields of [`AlpacaConfig`] against fail-closed invariants.
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaError::InvalidConfig`] if validation fails.
    pub fn validate(&self) -> AlpacaResult<()> {
        validate_url(&self.http_base_url, &["http", "https"], "http_base_url")?;
        validate_url(&self.ws_url, &["ws", "wss"], "ws_url")?;

        if !(MIN_TIMEOUT_SECS..=MAX_TIMEOUT_SECS).contains(&self.timeout_secs) {
            return Err(AlpacaError::InvalidConfig(format!(
                "timeout_secs={} outside valid range [{}..={}]",
                self.timeout_secs, MIN_TIMEOUT_SECS, MAX_TIMEOUT_SECS
            )));
        }

        if self.max_retries > MAX_RETRIES {
            return Err(AlpacaError::InvalidConfig(format!(
                "max_retries={} exceeds maximum allowed ({})",
                self.max_retries, MAX_RETRIES
            )));
        }

        if self.retry_initial_ms == 0 || self.retry_initial_ms > self.retry_max_ms {
            return Err(AlpacaError::InvalidConfig(format!(
                "invalid retry backoff bounds: retry_initial_ms={}, retry_max_ms={}",
                self.retry_initial_ms, self.retry_max_ms
            )));
        }

        if self.rate_limit_per_sec == 0 || self.rate_limit_per_sec > 200 {
            return Err(AlpacaError::InvalidConfig(format!(
                "rate_limit_per_sec={} must be in 1..=200",
                self.rate_limit_per_sec
            )));
        }

        let venue_trimmed = self.venue.trim();
        if venue_trimmed.is_empty()
            || !venue_trimmed
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            return Err(AlpacaError::InvalidConfig(format!(
                "venue '{}' must be non-empty ASCII alphanumeric",
                self.venue
            )));
        }

        Ok(())
    }
}

/// Canonical execution client configuration type alias for `AlpacaExecutionClientFactory`.
pub type AlpacaExecClientConfig = AlpacaConfig;

fn validate_url(raw: &str, allowed_schemes: &[&str], field_name: &str) -> AlpacaResult<()> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(AlpacaError::InvalidConfig(format!(
            "{field_name} must not be empty"
        )));
    }
    let parsed = Url::parse(trimmed).map_err(|e| {
        AlpacaError::InvalidConfig(format!("{field_name} '{trimmed}' is not a valid URL: {e}"))
    })?;
    if !allowed_schemes.contains(&parsed.scheme()) {
        return Err(AlpacaError::InvalidConfig(format!(
            "{field_name} '{trimmed}' must use one of schemes {allowed_schemes:?}"
        )));
    }
    if parsed.host_str().is_none_or(|h| h.trim().is_empty()) {
        return Err(AlpacaError::InvalidConfig(format!(
            "{field_name} '{trimmed}' is missing a valid host"
        )));
    }
    Ok(())
}
