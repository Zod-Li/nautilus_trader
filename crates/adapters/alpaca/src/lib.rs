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

//! Alpaca Markets execution adapter for [NautilusTrader](https://nautechsystems.io).

#![warn(rustc::all)]
#![deny(unsafe_code)]
#![deny(unsafe_op_in_unsafe_fn)]
#![deny(nonstandard_style)]
#![deny(missing_debug_implementations)]
#![deny(clippy::missing_errors_doc)]
#![deny(clippy::missing_panics_doc)]
#![deny(rustdoc::broken_intra_doc_links)]

pub mod common;
pub mod config;
pub mod error;
pub mod execution;
pub mod factories;
pub mod http;
pub mod websocket;

#[cfg(feature = "python")]
pub mod python;

pub use common::{
    ALPACA_LIVE_HTTP_URL, ALPACA_LIVE_WS_URL, ALPACA_PAPER_HTTP_URL, ALPACA_PAPER_WS_URL,
    ALPACA_VENUE, AlpacaCredential, AlpacaEnvironment, AlpacaOrderClass, AlpacaOrderSide,
    AlpacaOrderType, AlpacaTimeInForce,
};
pub use config::{AlpacaConfig, AlpacaExecClientConfig};
pub use error::{AlpacaError, AlpacaResult};
pub use execution::AlpacaExecutionClient;
pub use factories::AlpacaExecutionClientFactory;
pub use http::AlpacaHttpClient;
pub use websocket::{
    AlpacaExecutionUpdate, AlpacaTradeEventKind, AlpacaTradeUpdatePayload, AlpacaWsClient,
    AlpacaWsLifecycleObserver, AlpacaWsSessionState,
};
