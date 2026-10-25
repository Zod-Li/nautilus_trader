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

//! REST HTTP transport, wire DTOs, and domain parsers for Alpaca Markets v2.

pub mod client;
pub mod models;
pub mod parse;

pub use client::AlpacaHttpClient;
pub use models::{
    AlpacaAccountResponse, AlpacaApiErrorPayload, AlpacaAssetResponse, AlpacaOrderRequest,
    AlpacaOrderResponse, AlpacaPositionResponse, AlpacaReplaceOrderRequest, AlpacaStopLossSpec,
    AlpacaTakeProfitSpec,
};
pub use parse::{
    decimal_to_money, decimal_to_price, decimal_to_quantity, parse_account_state,
    parse_equity_instrument, parse_instrument_id, parse_order_status_report,
    parse_position_status_report, parse_rfc3339_to_unix_nanos,
};
