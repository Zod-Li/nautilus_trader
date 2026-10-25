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

//! Error hierarchy for the `nautilus-alpaca` adapter.

use thiserror::Error;

/// Represents errors produced by the Alpaca Markets REST and WebSocket adapters.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AlpacaError {
    /// Configuration or credential validation error (fail-closed, non-retryable).
    #[error("Alpaca configuration error: {0}")]
    InvalidConfig(String),
    /// Authentication or authorization error (`401`/`403`, non-retryable).
    #[error("Alpaca authentication/authorization error (HTTP {status}): {message}")]
    AuthError {
        /// HTTP status code (`401` or `403`).
        status: u16,
        /// Error message returned by Alpaca.
        message: String,
    },
    /// Deterministic broker rejection (`400`/`404`/`422`, non-retryable).
    #[error("Alpaca deterministic reject (HTTP {status}, code={code:?}): {message}")]
    DeterministicReject {
        /// HTTP status code.
        status: u16,
        /// Optional Alpaca numeric error code (e.g. `40310000`, `42210000`).
        code: Option<i64>,
        /// Error message returned by Alpaca.
        message: String,
    },
    /// Rate limit exceeded (`429 Too Many Requests`, retryable with backoff).
    #[error("Alpaca rate limit exceeded (HTTP 429): {message}")]
    RateLimited {
        /// Error message or retry hint.
        message: String,
    },
    /// Transient server error (`5xx`, retryable with backoff).
    #[error("Alpaca transient server error (HTTP {status}): {message}")]
    RetryableServer {
        /// HTTP status code (`500..=599`).
        status: u16,
        /// Error body or status reason.
        message: String,
    },
    /// Network or HTTP transport failure (retryable for idempotent queries).
    #[error("Alpaca transport error: {0}")]
    Transport(String),
    /// WebSocket session or transport failure.
    #[error("Alpaca WebSocket error: {0}")]
    WebSocket(String),
    /// Payload deserialization or domain conversion error (non-retryable).
    #[error("Alpaca payload parse error: {0}")]
    Parse(String),
}

impl AlpacaError {
    /// Returns `true` if this error represents a transient failure (`429`, `5xx`, or transport)
    /// that is eligible for exponential backoff retry. Deterministic broker rejections (`400`/`403`/`404`/`422`)
    /// always return `false`.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::RateLimited { .. }
                | Self::RetryableServer { .. }
                | Self::Transport(_)
                | Self::WebSocket(_)
        )
    }
}

/// Result alias for Alpaca adapter operations.
pub type AlpacaResult<T> = Result<T, AlpacaError>;
