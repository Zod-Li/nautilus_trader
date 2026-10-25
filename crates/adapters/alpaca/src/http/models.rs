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

//! Strongly typed wire DTOs for Alpaca Markets REST API v2 (`/v2/account`, `/v2/positions`, `/v2/orders`, `/v2/assets`).

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::common::enums::{
    AlpacaOrderClass, AlpacaOrderSide, AlpacaOrderType, AlpacaTimeInForce,
};

/// Error payload returned by Alpaca REST v2 on 4xx/5xx responses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlpacaApiErrorPayload {
    /// Optional Alpaca numeric error code (e.g. `40310000`, `42210000`).
    #[serde(default)]
    pub code: Option<i64>,
    /// Human-readable error message.
    #[serde(default)]
    pub message: String,
}

/// Alpaca `/v2/account` response DTO.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlpacaAccountResponse {
    /// Account UUID.
    pub id: String,
    /// Account number (e.g. `"PA12345678"`).
    pub account_number: String,
    /// Account status (e.g. `"ACTIVE"`).
    pub status: String,
    /// Base currency (e.g. `"USD"`).
    pub currency: String,
    /// Cash balance as exact decimal.
    pub cash: Decimal,
    /// Total portfolio equity value as exact decimal.
    pub equity: Decimal,
    /// Available buying power as exact decimal.
    pub buying_power: Decimal,
    /// Initial margin requirement currently used.
    #[serde(default)]
    pub initial_margin: Decimal,
    /// Maintenance margin requirement currently used.
    #[serde(default)]
    pub maintenance_margin: Decimal,
    /// Whether pattern day trader flag is active.
    #[serde(default)]
    pub pattern_day_trader: bool,
    /// Daytrade count in the rolling 5-day window.
    #[serde(default)]
    pub daytrade_count: u32,
    /// Whether trading is blocked on the account.
    #[serde(default)]
    pub trading_blocked: bool,
    /// Whether account is blocked.
    #[serde(default)]
    pub account_blocked: bool,
    /// RFC3339 creation timestamp.
    #[serde(default)]
    pub created_at: Option<String>,
}

/// Alpaca `/v2/positions` item DTO.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlpacaPositionResponse {
    /// Asset UUID.
    pub asset_id: String,
    /// Ticker symbol (e.g. `"NVDA"`).
    pub symbol: String,
    /// Exchange code (e.g. `"NASDAQ"`, `"NYSE"`, `"ARCA"`).
    pub exchange: String,
    /// Asset class (e.g. `"us_equity"`).
    pub asset_class: String,
    /// Average entry price as exact decimal.
    pub avg_entry_price: Decimal,
    /// Signed or unsigned share quantity as exact decimal.
    pub qty: Decimal,
    /// Available quantity not locked by open exit orders.
    #[serde(default)]
    pub qty_available: Option<Decimal>,
    /// Position side (`"long"` or `"short"`).
    pub side: String,
    /// Current market value in USD.
    pub market_value: Decimal,
    /// Cost basis in USD.
    pub cost_basis: Decimal,
    /// Unrealized PnL in USD.
    #[serde(default)]
    pub unrealized_pl: Option<Decimal>,
    /// Current price of the asset.
    #[serde(default)]
    pub current_price: Option<Decimal>,
}

/// Take-profit specification for Bracket / OCO orders.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlpacaTakeProfitSpec {
    /// Limit price for the take-profit leg.
    pub limit_price: Decimal,
}

/// Stop-loss specification for Bracket / OCO orders.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlpacaStopLossSpec {
    /// Stop trigger price for the stop-loss leg.
    pub stop_price: Decimal,
    /// Optional limit price for a stop-limit leg.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit_price: Option<Decimal>,
}

/// Request payload for `POST /v2/orders`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlpacaOrderRequest {
    /// Ticker symbol (e.g. `"NVDA"`).
    pub symbol: String,
    /// Order quantity in shares.
    pub qty: Decimal,
    /// Order side (`buy` or `sell`).
    pub side: AlpacaOrderSide,
    /// Order type (`market`, `limit`, `stop`, `stop_limit`, `trailing_stop`).
    #[serde(rename = "type")]
    pub order_type: AlpacaOrderType,
    /// Time in force (`day`, `gtc`, `opg`, `cls`, `ioc`, `fok`).
    pub time_in_force: AlpacaTimeInForce,
    /// Optional limit price (required for `limit` and `stop_limit`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit_price: Option<Decimal>,
    /// Optional stop trigger price (required for `stop` and `stop_limit`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_price: Option<Decimal>,
    /// Optional trail price in dollars (for `trailing_stop`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trail_price: Option<Decimal>,
    /// Optional trail percent (for `trailing_stop`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trail_percent: Option<Decimal>,
    /// Whether order is eligible for extended hours (`pre-market` / `after-hours`).
    #[serde(default)]
    pub extended_hours: bool,
    /// Client-assigned idempotency order ID (`<= 128` chars).
    pub client_order_id: String,
    /// Optional order class (`simple`, `bracket`, `oco`, `oto`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_class: Option<AlpacaOrderClass>,
    /// Optional take-profit leg specification (for `bracket` / `oco`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub take_profit: Option<AlpacaTakeProfitSpec>,
    /// Optional stop-loss leg specification (for `bracket` / `oco`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_loss: Option<AlpacaStopLossSpec>,
}

/// Request payload for `PATCH /v2/orders/{order_id}` (replace order).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlpacaReplaceOrderRequest {
    /// Updated total order quantity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qty: Option<Decimal>,
    /// Updated time in force.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_in_force: Option<AlpacaTimeInForce>,
    /// Updated limit price.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit_price: Option<Decimal>,
    /// Updated stop trigger price.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_price: Option<Decimal>,
    /// Updated trail offset.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trail: Option<Decimal>,
    /// New client order ID for the replacement order.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_order_id: Option<String>,
}

/// Alpaca `/v2/orders` order entity DTO (including nested `legs` for Bracket/OCO orders).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlpacaOrderResponse {
    /// Venue-assigned order UUID.
    pub id: String,
    /// Client-assigned order ID.
    pub client_order_id: String,
    /// Creation timestamp (RFC3339).
    pub created_at: String,
    /// Last update timestamp (RFC3339).
    #[serde(default)]
    pub updated_at: Option<String>,
    /// Submission timestamp (RFC3339).
    #[serde(default)]
    pub submitted_at: Option<String>,
    /// Fill timestamp (RFC3339).
    #[serde(default)]
    pub filled_at: Option<String>,
    /// Expiration timestamp (RFC3339).
    #[serde(default)]
    pub expired_at: Option<String>,
    /// Cancellation timestamp (RFC3339).
    #[serde(default)]
    pub canceled_at: Option<String>,
    /// Failure/rejection timestamp (RFC3339).
    #[serde(default)]
    pub failed_at: Option<String>,
    /// Replaced timestamp (RFC3339).
    #[serde(default)]
    pub replaced_at: Option<String>,
    /// Order ID that replaced this order.
    #[serde(default)]
    pub replaced_by: Option<String>,
    /// Previous order ID that this order replaced.
    #[serde(default)]
    pub replaces: Option<String>,
    /// Asset UUID.
    #[serde(default)]
    pub asset_id: Option<String>,
    /// Ticker symbol (e.g. `"NVDA"`).
    pub symbol: String,
    /// Asset class (e.g. `"us_equity"`).
    #[serde(default)]
    pub asset_class: Option<String>,
    /// Ordered share quantity.
    #[serde(default)]
    pub qty: Option<Decimal>,
    /// Cumulative filled share quantity.
    #[serde(default)]
    pub filled_qty: Decimal,
    /// Volume-weighted average fill price.
    #[serde(default)]
    pub filled_avg_price: Option<Decimal>,
    /// Order class (`simple`, `bracket`, `oco`, `oto`).
    #[serde(default)]
    pub order_class: Option<AlpacaOrderClass>,
    /// Order type (`market`, `limit`, `stop`, `stop_limit`, `trailing_stop`).
    #[serde(rename = "type")]
    pub order_type: AlpacaOrderType,
    /// Order side (`buy` or `sell`).
    pub side: AlpacaOrderSide,
    /// Time in force (`day`, `gtc`, `opg`, `cls`, `ioc`, `fok`).
    pub time_in_force: AlpacaTimeInForce,
    /// Limit price if applicable.
    #[serde(default)]
    pub limit_price: Option<Decimal>,
    /// Stop trigger price if applicable.
    #[serde(default)]
    pub stop_price: Option<Decimal>,
    /// Order status string (e.g. `"new"`, `"accepted"`, `"partially_filled"`, `"filled"`, `"canceled"`, `"rejected"`).
    pub status: String,
    /// Whether extended hours is enabled.
    #[serde(default)]
    pub extended_hours: bool,
    /// Nested child legs for Bracket / OCO / OTO orders.
    #[serde(default)]
    pub legs: Option<Vec<AlpacaOrderResponse>>,
    /// Trailing stop dollar offset.
    #[serde(default)]
    pub trail_price: Option<Decimal>,
    /// Trailing stop percentage offset.
    #[serde(default)]
    pub trail_percent: Option<Decimal>,
    /// High-water mark for trailing stop orders.
    #[serde(default)]
    pub hwm: Option<Decimal>,
}

/// Alpaca `/v2/assets` item DTO.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlpacaAssetResponse {
    /// Asset UUID.
    pub id: String,
    /// Asset class (`"us_equity"`).
    #[serde(rename = "class")]
    pub asset_class: String,
    /// Listing exchange (`"NASDAQ"`, `"NYSE"`, `"ARCA"`, `"BATS"`, `"AMEX"`).
    pub exchange: String,
    /// Ticker symbol (e.g. `"NVDA"`).
    pub symbol: String,
    /// Company / asset name.
    #[serde(default)]
    pub name: String,
    /// Asset status (`"active"` or `"inactive"`).
    pub status: String,
    /// Whether the asset is tradable on Alpaca.
    pub tradable: bool,
    /// Whether the asset is marginable.
    pub marginable: bool,
    /// Whether the asset is shortable.
    pub shortable: bool,
    /// Whether the asset is easy to borrow for short sales.
    pub easy_to_borrow: bool,
    /// Whether the asset supports fractional shares.
    pub fractionable: bool,
    /// Minimum order size if specified.
    #[serde(default)]
    pub min_order_size: Option<Decimal>,
    /// Minimum price increment if specified.
    #[serde(default)]
    pub price_increment: Option<Decimal>,
}
