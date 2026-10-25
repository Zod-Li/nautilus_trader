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

//! Venue-specific enums for Alpaca Markets v2 REST and WebSocket APIs.

use nautilus_model::enums::{ContingencyType, OrderSide, OrderStatus, OrderType, TimeInForce};
use serde::{Deserialize, Serialize};

use crate::{
    common::consts::{
        ALPACA_LIVE_HTTP_URL, ALPACA_LIVE_WS_URL, ALPACA_PAPER_HTTP_URL, ALPACA_PAPER_WS_URL,
    },
    error::{AlpacaError, AlpacaResult},
};

/// Selects between Alpaca Paper Trading and Live Trading endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AlpacaEnvironment {
    /// Paper trading environment (`paper-api.alpaca.markets`).
    Paper,
    /// Live production trading environment (`api.alpaca.markets`).
    Live,
}

impl AlpacaEnvironment {
    /// Returns the default REST base URL for this environment.
    #[must_use]
    pub const fn default_http_url(&self) -> &'static str {
        match self {
            Self::Paper => ALPACA_PAPER_HTTP_URL,
            Self::Live => ALPACA_LIVE_HTTP_URL,
        }
    }

    /// Returns the default WebSocket `trade_updates` URL for this environment.
    #[must_use]
    pub const fn default_ws_url(&self) -> &'static str {
        match self {
            Self::Paper => ALPACA_PAPER_WS_URL,
            Self::Live => ALPACA_LIVE_WS_URL,
        }
    }
}

/// Alpaca order class (`simple`, `bracket`, `oco`, `oto`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AlpacaOrderClass {
    /// Single standalone order (`""` or `"simple"`).
    #[serde(alias = "")]
    Simple,
    /// Entry order with attached OCO take-profit and stop-loss legs (`"bracket"`).
    Bracket,
    /// One-Cancels-Other exit pair (`"oco"`).
    Oco,
    /// One-Triggers-Other order (`"oto"`).
    Oto,
}

impl AlpacaOrderClass {
    /// Maps an [`AlpacaOrderClass`] to a Nautilus [`ContingencyType`].
    #[must_use]
    pub const fn to_contingency_type(self) -> Option<ContingencyType> {
        match self {
            Self::Simple => None,
            Self::Bracket | Self::Oto => Some(ContingencyType::Oto),
            Self::Oco => Some(ContingencyType::Oco),
        }
    }
}

/// Alpaca order side (`buy` or `sell`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AlpacaOrderSide {
    /// Buy side.
    Buy,
    /// Sell side.
    Sell,
}

impl From<AlpacaOrderSide> for OrderSide {
    fn from(side: AlpacaOrderSide) -> Self {
        match side {
            AlpacaOrderSide::Buy => Self::Buy,
            AlpacaOrderSide::Sell => Self::Sell,
        }
    }
}

impl TryFrom<OrderSide> for AlpacaOrderSide {
    type Error = AlpacaError;

    fn try_from(side: OrderSide) -> AlpacaResult<Self> {
        match side {
            OrderSide::Buy => Ok(Self::Buy),
            OrderSide::Sell => Ok(Self::Sell),
        }
    }
}

/// Alpaca order type (`market`, `limit`, `stop`, `stop_limit`, `trailing_stop`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlpacaOrderType {
    /// Market order.
    Market,
    /// Limit order.
    Limit,
    /// Stop (market) order.
    Stop,
    /// Stop-limit order.
    StopLimit,
    /// Trailing stop order.
    TrailingStop,
}

impl From<AlpacaOrderType> for OrderType {
    fn from(order_type: AlpacaOrderType) -> Self {
        match order_type {
            AlpacaOrderType::Market => Self::Market,
            AlpacaOrderType::Limit => Self::Limit,
            AlpacaOrderType::Stop => Self::StopMarket,
            AlpacaOrderType::StopLimit => Self::StopLimit,
            AlpacaOrderType::TrailingStop => Self::TrailingStopMarket,
        }
    }
}

impl TryFrom<OrderType> for AlpacaOrderType {
    type Error = AlpacaError;

    fn try_from(order_type: OrderType) -> AlpacaResult<Self> {
        match order_type {
            OrderType::Market | OrderType::MarketToLimit => Ok(Self::Market),
            OrderType::Limit => Ok(Self::Limit),
            OrderType::StopMarket => Ok(Self::Stop),
            OrderType::StopLimit => Ok(Self::StopLimit),
            OrderType::TrailingStopMarket => Ok(Self::TrailingStop),
            other => Err(AlpacaError::Parse(format!(
                "unsupported OrderType for Alpaca: {other}"
            ))),
        }
    }
}

/// Alpaca time-in-force (`day`, `gtc`, `opg`, `cls`, `ioc`, `fok`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AlpacaTimeInForce {
    /// Day order.
    Day,
    /// Good-til-canceled.
    Gtc,
    /// Market/limit on open.
    Opg,
    /// Market/limit on close.
    Cls,
    /// Immediate-or-cancel.
    Ioc,
    /// Fill-or-kill.
    Fok,
}

impl From<AlpacaTimeInForce> for TimeInForce {
    fn from(tif: AlpacaTimeInForce) -> Self {
        match tif {
            AlpacaTimeInForce::Day => Self::Day,
            AlpacaTimeInForce::Gtc => Self::Gtc,
            AlpacaTimeInForce::Opg => Self::AtTheOpen,
            AlpacaTimeInForce::Cls => Self::AtTheClose,
            AlpacaTimeInForce::Ioc => Self::Ioc,
            AlpacaTimeInForce::Fok => Self::Fok,
        }
    }
}

impl TryFrom<TimeInForce> for AlpacaTimeInForce {
    type Error = AlpacaError;

    fn try_from(tif: TimeInForce) -> AlpacaResult<Self> {
        match tif {
            TimeInForce::Day => Ok(Self::Day),
            TimeInForce::Gtc => Ok(Self::Gtc),
            TimeInForce::AtTheOpen => Ok(Self::Opg),
            TimeInForce::AtTheClose => Ok(Self::Cls),
            TimeInForce::Ioc => Ok(Self::Ioc),
            TimeInForce::Fok => Ok(Self::Fok),
            other => Err(AlpacaError::Parse(format!(
                "unsupported TimeInForce for Alpaca: {other}"
            ))),
        }
    }
}

/// Maps an Alpaca order status string into a Nautilus [`OrderStatus`].
///
/// # Errors
///
/// Returns [`AlpacaError::Parse`] if the status string is unrecognized.
pub fn parse_alpaca_order_status(status: &str) -> AlpacaResult<OrderStatus> {
    match status.trim().to_ascii_lowercase().as_str() {
        "new" | "accepted" | "accepted_for_bidding" | "calculated" | "held" => {
            Ok(OrderStatus::Accepted)
        }
        "pending_new" => Ok(OrderStatus::Submitted),
        "partially_filled" => Ok(OrderStatus::PartiallyFilled),
        "filled" => Ok(OrderStatus::Filled),
        "done_for_day" | "canceled" | "stopped" | "replaced" => Ok(OrderStatus::Canceled),
        "expired" => Ok(OrderStatus::Expired),
        "pending_cancel" => Ok(OrderStatus::PendingCancel),
        "pending_replace" => Ok(OrderStatus::PendingUpdate),
        "rejected" | "suspended" => Ok(OrderStatus::Rejected),
        other => Err(AlpacaError::Parse(format!(
            "unrecognized Alpaca order status '{other}'"
        ))),
    }
}
