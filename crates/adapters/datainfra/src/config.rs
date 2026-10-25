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

//! Strongly typed, fail-closed configuration for `DataInfra` (ClickHouse, Kafka, Calendar Service).

use serde::{Deserialize, Serialize};
use url::Url;

use crate::error::{DataInfraError, DataInfraResult};

/// Minimum allowed HTTP request timeout in milliseconds.
pub const MIN_REQUEST_TIMEOUT_MS: u64 = 100;
/// Maximum allowed HTTP request timeout in milliseconds.
pub const MAX_REQUEST_TIMEOUT_MS: u64 = 120_000;
/// Maximum allowed retry attempts for transient transport errors.
pub const MAX_ALLOWED_RETRIES: u32 = 10;

/// Configuration for connecting to the `DataInfra` market data plane:
/// - ClickHouse (`ArrowStream` / Parquet north-bound query plane)
/// - Kafka (`sip-feeder` canonical real-time bars, quotes, and trading statuses)
/// - `calendar-service` (`:8012` session schedule and DST boundaries)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        module = "nautilus_trader.adapters.datainfra",
        from_py_object,
        get_all,
        set_all
    )
)]
pub struct DataInfraConfig {
    /// Base HTTP(S) endpoint for ClickHouse or `market-data-service` Arrow transport.
    pub clickhouse_url: String,
    /// Authoritative ClickHouse database name (e.g. `market_data`).
    pub clickhouse_database: String,
    /// HTTP request timeout in milliseconds (`100..=120_000`).
    pub request_timeout_ms: u64,
    /// Maximum retry attempts on transient network failures (`0..=10`).
    pub max_retries: u32,
    /// Seed Kafka broker `host:port` endpoints for real-time streaming.
    pub kafka_brokers: Vec<String>,
    /// Kafka consumer group identifier.
    pub kafka_group_id: String,
    /// Base HTTP(S) URL for `calendar-service` (`:8012`).
    pub calendar_service_url: String,
    /// Default venue code assigned when mapping unqualified US equity symbols.
    pub default_venue: String,
}

impl Default for DataInfraConfig {
    fn default() -> Self {
        Self {
            clickhouse_url: "http://127.0.0.1:8123".to_string(),
            clickhouse_database: "market_data".to_string(),
            request_timeout_ms: 5_000,
            max_retries: 3,
            kafka_brokers: vec!["127.0.0.1:9092".to_string()],
            kafka_group_id: "nautilus-datainfra-live".to_string(),
            calendar_service_url: "http://127.0.0.1:8012".to_string(),
            default_venue: "XNAS".to_string(),
        }
    }
}

impl DataInfraConfig {
    /// Constructs and validates a [`DataInfraConfig`] instance (fail-closed on invalid parameters).
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError::InvalidConfig`] if any endpoint URL, port, database identifier,
    /// timeout, or Kafka broker address is malformed or out of bounds.
    #[expect(clippy::too_many_arguments)]
    pub fn new_checked(
        clickhouse_url: impl Into<String>,
        clickhouse_database: impl Into<String>,
        request_timeout_ms: u64,
        max_retries: u32,
        kafka_brokers: Vec<String>,
        kafka_group_id: impl Into<String>,
        calendar_service_url: impl Into<String>,
        default_venue: impl Into<String>,
    ) -> DataInfraResult<Self> {
        let cfg = Self {
            clickhouse_url: clickhouse_url.into(),
            clickhouse_database: clickhouse_database.into(),
            request_timeout_ms,
            max_retries,
            kafka_brokers,
            kafka_group_id: kafka_group_id.into(),
            calendar_service_url: calendar_service_url.into(),
            default_venue: default_venue.into(),
        };
        cfg.validate()?;
        Ok(cfg)
    }

    /// Validates all fields of [`DataInfraConfig`] against fail-closed invariants.
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError::InvalidConfig`] if validation fails.
    pub fn validate(&self) -> DataInfraResult<()> {
        validate_http_endpoint(&self.clickhouse_url, "clickhouse_url")?;
        validate_http_endpoint(&self.calendar_service_url, "calendar_service_url")?;
        validate_database_name(&self.clickhouse_database)?;

        if !(MIN_REQUEST_TIMEOUT_MS..=MAX_REQUEST_TIMEOUT_MS).contains(&self.request_timeout_ms) {
            return Err(DataInfraError::InvalidConfig(format!(
                "request_timeout_ms={} outside valid range [{}..={}]",
                self.request_timeout_ms, MIN_REQUEST_TIMEOUT_MS, MAX_REQUEST_TIMEOUT_MS
            )));
        }

        if self.max_retries > MAX_ALLOWED_RETRIES {
            return Err(DataInfraError::InvalidConfig(format!(
                "max_retries={} exceeds maximum allowed ({})",
                self.max_retries, MAX_ALLOWED_RETRIES
            )));
        }

        if self.kafka_brokers.is_empty() {
            return Err(DataInfraError::InvalidConfig(
                "kafka_brokers must contain at least one host:port endpoint".to_string(),
            ));
        }

        for broker in &self.kafka_brokers {
            validate_host_port(broker, "kafka_brokers")?;
        }

        let group_trimmed = self.kafka_group_id.trim();
        if group_trimmed.is_empty() {
            return Err(DataInfraError::InvalidConfig(
                "kafka_group_id must not be empty".to_string(),
            ));
        }

        let venue_trimmed = self.default_venue.trim();
        if venue_trimmed.is_empty()
            || !venue_trimmed
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            return Err(DataInfraError::InvalidConfig(format!(
                "default_venue '{}' must be non-empty ASCII alphanumeric",
                self.default_venue
            )));
        }

        Ok(())
    }
}

fn validate_http_endpoint(raw: &str, field_name: &str) -> DataInfraResult<()> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(DataInfraError::InvalidConfig(format!(
            "{field_name} must not be empty"
        )));
    }
    if let Some(after_colon) = trimmed.rsplit(':').next()
        && let Some(port_part) = after_colon.split('/').next()
        && !port_part.is_empty()
        && port_part.chars().all(|c| c.is_ascii_digit())
    {
        let port_num: u32 = port_part.parse().map_err(|_| {
            DataInfraError::InvalidConfig(format!("{field_name} has invalid port in '{trimmed}'"))
        })?;
        if port_num == 0 || port_num > 65_535 {
            return Err(DataInfraError::InvalidConfig(format!(
                "{field_name} port {port_num} out of range 1..=65535"
            )));
        }
    }

    let parsed = Url::parse(trimmed).map_err(|e| {
        DataInfraError::InvalidConfig(format!("{field_name} '{trimmed}' is not a valid URL: {e}"))
    })?;

    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(DataInfraError::InvalidConfig(format!(
            "{field_name} '{trimmed}' must use http or https scheme"
        )));
    }

    let Some(host) = parsed.host_str() else {
        return Err(DataInfraError::InvalidConfig(format!(
            "{field_name} '{trimmed}' is missing a host"
        )));
    };
    if host.trim().is_empty() {
        return Err(DataInfraError::InvalidConfig(format!(
            "{field_name} '{trimmed}' has an empty host"
        )));
    }

    if let Some(port) = parsed.port()
        && port == 0
    {
        return Err(DataInfraError::InvalidConfig(format!(
            "{field_name} '{trimmed}' port must be non-zero"
        )));
    }

    Ok(())
}

fn validate_database_name(name: &str) -> DataInfraResult<()> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(DataInfraError::InvalidConfig(
            "clickhouse_database must not be empty".to_string(),
        ));
    }
    let mut chars = trimmed.chars();
    let Some(first) = chars.next() else {
        return Err(DataInfraError::InvalidConfig(
            "clickhouse_database must not be empty".to_string(),
        ));
    };
    if !(first.is_ascii_alphabetic() || first == '_')
        || !chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(DataInfraError::InvalidConfig(format!(
            "clickhouse_database '{name}' must be a valid SQL identifier (ASCII alphanumeric/underscore)"
        )));
    }
    Ok(())
}

fn validate_host_port(endpoint: &str, field_name: &str) -> DataInfraResult<()> {
    let trimmed = endpoint.trim();
    if trimmed.is_empty() {
        return Err(DataInfraError::InvalidConfig(format!(
            "{field_name} entry must not be empty"
        )));
    }
    let Some((host, port_str)) = trimmed.rsplit_once(':') else {
        return Err(DataInfraError::InvalidConfig(format!(
            "{field_name} entry '{trimmed}' must be in host:port format"
        )));
    };
    if host.trim().is_empty() || host.contains(char::is_whitespace) || host.contains('/') {
        return Err(DataInfraError::InvalidConfig(format!(
            "{field_name} entry '{trimmed}' has invalid host '{host}'"
        )));
    }
    let port: u32 = port_str.parse().map_err(|_| {
        DataInfraError::InvalidConfig(format!(
            "{field_name} entry '{trimmed}' has non-numeric port '{port_str}'"
        ))
    })?;
    if port == 0 || port > 65_535 {
        return Err(DataInfraError::InvalidConfig(format!(
            "{field_name} entry '{trimmed}' port {port} out of valid range 1..=65535"
        )));
    }
    Ok(())
}
