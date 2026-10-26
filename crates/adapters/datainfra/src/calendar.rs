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

//! `CalendarSessionGate` client seam for `calendar-service` (`:8012`).
//!
//! Validates Regular Trading Hours (`RTH`, `09:30–16:00 ET`), half-day early closes (`13:00 ET`),
//! and market holidays with strict fail-closed error handling when `calendar-service` is
//! unreachable or returns `UNSYNCED` status.

use std::sync::Arc;

use async_trait::async_trait;
use http::Method;
use nautilus_core::UnixNanos;
use nautilus_network::http::{HttpClient, HttpRedirectPolicy, create_standard_nautilus_headers};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::{
    config::DataInfraConfig,
    error::{DataInfraError, DataInfraResult},
};

/// Canonical market code queried by [`CalendarSessionGate`].
pub const DEFAULT_CALENDAR_MARKET: &str = "US_EQUITY";

/// Rich domain model representing the response from `calendar-service` (`/api/v1/calendar/session-status`
/// or `/v1/calendar/session-status`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CalendarSessionStatus {
    /// Market code (e.g. `"US_EQUITY"`).
    pub market: String,
    /// UTC epoch seconds at which the status was resolved.
    pub queried_at_utc: i64,
    /// NY trading date (`YYYY-MM-DD`).
    pub trading_date: String,
    /// Whether `trading_date` is an open US equity trading day.
    pub is_trading_day: bool,
    /// Whether `queried_at_utc` falls inside the RTH session window.
    pub is_rth_open: bool,
    /// RTH session open timestamp in UTC seconds (`0` on closed days).
    pub rth_open_utc: i64,
    /// RTH session close timestamp in UTC seconds (`0` on closed days; `13:00 ET` on early-close days).
    pub rth_close_utc: i64,
    /// Whether this trading day is a half-day early close (`13:00 ET`).
    pub is_early_close: bool,
    /// Next RTH open timestamp in UTC seconds, if resolved.
    #[serde(default)]
    pub next_rth_open_utc: Option<i64>,
    /// Authoritative calendar source (e.g. `"ALPACA"`, `"EXCHANGE_CALENDAR"`; `"UNSYNCED"` is rejected fail-closed).
    pub source: String,
}

impl CalendarSessionStatus {
    /// Validates physical and fail-closed invariants of the resolved session status.
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError::CalendarGate`] if `source` is `"UNSYNCED"`, `trading_date` is empty,
    /// or `rth_open_utc` / `rth_close_utc` bounds are inconsistent with `is_trading_day`.
    pub fn validate_sanity(&self) -> DataInfraResult<()> {
        if self.trading_date.trim().is_empty() {
            return Err(DataInfraError::CalendarGate(
                "calendar-service returned empty trading_date (fail-closed)".to_string(),
            ));
        }
        if self.source.trim().is_empty() || self.source.trim().eq_ignore_ascii_case("UNSYNCED") {
            return Err(DataInfraError::CalendarGate(format!(
                "calendar-service session status source is '{}' for {} (fail-closed)",
                self.source, self.trading_date
            )));
        }
        if self.is_trading_day {
            if self.rth_open_utc <= 0 || self.rth_close_utc <= self.rth_open_utc {
                return Err(DataInfraError::CalendarGate(format!(
                    "calendar-service returned invalid RTH bounds for open trading day {}: open={}, close={}",
                    self.trading_date, self.rth_open_utc, self.rth_close_utc
                )));
            }
        } else if self.is_rth_open {
            return Err(DataInfraError::CalendarGate(format!(
                "calendar-service reported is_rth_open=true on non-trading day {}",
                self.trading_date
            )));
        }
        Ok(())
    }

    /// Returns the RTH open timestamp as [`UnixNanos`] if `is_trading_day` is `true`.
    #[must_use]
    pub fn rth_open_ns(&self) -> Option<UnixNanos> {
        if self.is_trading_day && self.rth_open_utc > 0 {
            Some(UnixNanos::from(
                (self.rth_open_utc as u64).saturating_mul(1_000_000_000),
            ))
        } else {
            None
        }
    }

    /// Returns the RTH close timestamp as [`UnixNanos`] if `is_trading_day` is `true`.
    #[must_use]
    pub fn rth_close_ns(&self) -> Option<UnixNanos> {
        if self.is_trading_day && self.rth_close_utc > 0 {
            Some(UnixNanos::from(
                (self.rth_close_utc as u64).saturating_mul(1_000_000_000),
            ))
        } else {
            None
        }
    }

    /// Returns `true` if `ts_ns` falls within the inclusive RTH window `[rth_open_ns, rth_close_ns]`.
    #[must_use]
    pub fn contains_rth_timestamp_ns(&self, ts_ns: UnixNanos) -> bool {
        let Some(open_ns) = self.rth_open_ns() else {
            return false;
        };
        let Some(close_ns) = self.rth_close_ns() else {
            return false;
        };
        ts_ns >= open_ns && ts_ns <= close_ns
    }
}

/// Transport port for querying `calendar-service`.
#[async_trait]
pub trait CalendarTransport: Send + Sync + std::fmt::Debug {
    /// Queries `/api/v1/calendar/session-status` (with fallback to `/v1/calendar/session-status`)
    /// for `market` and optional `at_ts` (UTC seconds).
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError::CalendarGate`] on network error, non-200 HTTP response,
    /// malformed JSON, or failed [`CalendarSessionStatus::validate_sanity`].
    async fn fetch_session_status(
        &self,
        market: &str,
        at_ts_utc: Option<i64>,
    ) -> DataInfraResult<CalendarSessionStatus>;
}

/// Production HTTP transport for `calendar-service` (`:8012`).
#[derive(Debug, Clone)]
pub struct HttpCalendarTransport {
    base_url: String,
    http_client: HttpClient,
}

impl HttpCalendarTransport {
    /// Creates a new [`HttpCalendarTransport`] from [`DataInfraConfig`].
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError`] if config validation or `HttpClient` creation fails.
    pub fn new(config: &DataInfraConfig) -> DataInfraResult<Self> {
        config.validate()?;
        let timeout_secs = (config.request_timeout_ms / 1_000).max(1);
        let headers: std::collections::HashMap<String, String> =
            create_standard_nautilus_headers().into_iter().collect();
        let http_client = HttpClient::builder()
            .headers(headers)
            .timeout_secs(timeout_secs)
            .redirect_policy(HttpRedirectPolicy::Reject)
            .build()
            .map_err(|e| {
                DataInfraError::CalendarGate(format!("failed to create calendar HttpClient: {e}"))
            })?;

        Ok(Self {
            base_url: config.calendar_service_url.trim_end_matches('/').to_string(),
            http_client,
        })
    }

    fn build_endpoint_url(
        &self,
        path: &str,
        market: &str,
        at_ts_utc: Option<i64>,
    ) -> DataInfraResult<String> {
        let raw = format!("{}{}", self.base_url, path);
        let mut parsed = Url::parse(&raw).map_err(|e| {
            DataInfraError::InvalidConfig(format!("invalid calendar_service_url '{raw}': {e}"))
        })?;
        {
            let mut qp = parsed.query_pairs_mut();
            qp.append_pair("market", market);
            if let Some(ts) = at_ts_utc {
                qp.append_pair("at_ts", &ts.to_string());
            }
        }
        Ok(parsed.to_string())
    }
}

#[async_trait]
impl CalendarTransport for HttpCalendarTransport {
    async fn fetch_session_status(
        &self,
        market: &str,
        at_ts_utc: Option<i64>,
    ) -> DataInfraResult<CalendarSessionStatus> {
        let candidate_paths = ["/v1/calendar/session-status"];
        let mut last_err: Option<DataInfraError> = None;

        for path in candidate_paths {
            let url = self.build_endpoint_url(path, market, at_ts_utc)?;
            match self
                .http_client
                .request(Method::GET, url, None, None, None, None, None)
                .await
            {
                Ok(resp) => {
                    let code = resp.status.as_u16();
                    if code == 200 {
                        let status: CalendarSessionStatus = serde_json::from_slice(&resp.body)
                            .map_err(|e| {
                                DataInfraError::CalendarGate(format!(
                                    "failed to decode CalendarSessionStatus JSON: {e}"
                                ))
                            })?;
                        status.validate_sanity()?;
                        return Ok(status);
                    }
                    if code == 404 {
                        last_err = Some(DataInfraError::CalendarGate(format!(
                            "calendar-service endpoint {path} returned HTTP 404"
                        )));
                        continue;
                    }
                    let body_preview = String::from_utf8_lossy(&resp.body);
                    return Err(DataInfraError::CalendarGate(format!(
                        "calendar-service returned HTTP {code}: {body_preview}"
                    )));
                }
                Err(e) => {
                    return Err(DataInfraError::CalendarGate(format!(
                        "calendar-service unreachable ({}): {e}",
                        self.base_url
                    )));
                }
            }
        }

        Err(last_err.unwrap_or_else(|| {
            DataInfraError::CalendarGate("calendar-service session-status query failed".to_string())
        }))
    }
}

/// Fail-closed session boundary gate backed by `calendar-service` (`:8012`).
#[derive(Debug, Clone)]
pub struct CalendarSessionGate {
    market: String,
    transport: Arc<dyn CalendarTransport>,
    cached_status: Arc<RwLock<Option<CalendarSessionStatus>>>,
}

impl CalendarSessionGate {
    /// Creates a new [`CalendarSessionGate`] with an explicit [`CalendarTransport`].
    #[must_use]
    pub fn new(market: impl Into<String>, transport: Arc<dyn CalendarTransport>) -> Self {
        Self {
            market: market.into(),
            transport,
            cached_status: Arc::new(RwLock::new(None)),
        }
    }

    /// Creates a production [`CalendarSessionGate`] from [`DataInfraConfig`].
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError`] if config validation or HTTP transport construction fails.
    pub fn from_config(config: &DataInfraConfig) -> DataInfraResult<Self> {
        let transport = Arc::new(HttpCalendarTransport::new(config)?);
        Ok(Self::new(DEFAULT_CALENDAR_MARKET, transport))
    }

    /// Queries `calendar-service` for the session status at `at_ts_utc` (or current server time if `None`),
    /// validates it fail-closed, caches the latest resolved snapshot, and returns it.
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError::CalendarGate`] if `calendar-service` is unreachable, returns an HTTP error,
    /// or returns an invalid/`UNSYNCED` session status.
    pub async fn refresh_session_status(
        &self,
        at_ts_utc: Option<i64>,
    ) -> DataInfraResult<CalendarSessionStatus> {
        let status = self
            .transport
            .fetch_session_status(&self.market, at_ts_utc)
            .await?;
        status.validate_sanity()?;
        *self.cached_status.write() = Some(status.clone());
        Ok(status)
    }

    /// Returns the most recently cached [`CalendarSessionStatus`], if any.
    #[must_use]
    pub fn cached_status(&self) -> Option<CalendarSessionStatus> {
        self.cached_status.read().clone()
    }

    /// Verifies whether `ts_ns` falls within the currently cached RTH session window
    /// (including `13:00 ET` early-close boundaries).
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError::CalendarGate`] if no session status has been resolved yet
    /// (fail-closed).
    pub fn is_within_cached_rth(&self, ts_ns: UnixNanos) -> DataInfraResult<bool> {
        let guard = self.cached_status.read();
        let Some(ref status) = *guard else {
            return Err(DataInfraError::CalendarGate(
                "CalendarSessionGate has no resolved session status in cache (fail-closed)"
                    .to_string(),
            ));
        };
        Ok(status.contains_rth_timestamp_ns(ts_ns))
    }
}
