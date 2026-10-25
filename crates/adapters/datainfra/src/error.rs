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

//! Domain error types for the `nautilus-datainfra` adapter.

use thiserror::Error;

/// Represents errors produced by the `DataInfra` adapter.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DataInfraError {
    /// Configuration validation failed (fail-closed).
    #[error("DataInfra configuration error: {0}")]
    InvalidConfig(String),
    /// Symbol or venue identifier mapping failed.
    #[error("DataInfra identifier mapping error: {0}")]
    InvalidIdentifier(String),
    /// Timeframe or bar specification mapping failed.
    #[error("DataInfra timeframe mapping error: {0}")]
    UnsupportedTimeframe(String),
    /// Instrument metadata validation or construction failed.
    #[error("DataInfra instrument construction error: {0}")]
    InvalidInstrument(String),
    /// Transport or Arrow stream decoding error.
    #[error("DataInfra transport error: {0}")]
    Transport(String),
    /// Data provenance or sanity check failed (fail-closed).
    #[error("DataInfra provenance guard error: {0}")]
    ProvenanceViolation(String),
    /// Market calendar session gate check failed (fail-closed).
    #[error("DataInfra calendar gate error: {0}")]
    CalendarGate(String),
}

/// Result alias for `DataInfra` operations.
pub type DataInfraResult<T> = Result<T, DataInfraError>;
