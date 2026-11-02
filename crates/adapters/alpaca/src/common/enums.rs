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

/// Alpaca order status values returned by `/v2/orders` and `trade_updates`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlpacaOrderStatus {
    /// Routed and waiting for execution (`"new"`).
    New,
    /// Received by Alpaca and routing to exchanges (`"pending_new"`).
    PendingNew,
    /// Accepted by Alpaca (`"accepted"`).
    Accepted,
    /// Accepted for bidding (`"accepted_for_bidding"`).
    AcceptedForBidding,
    /// Fill price guaranteed by specialist/market maker; execution is imminent/active (`"stopped"`).
    Stopped,
    /// Order calculated (`"calculated"`).
    Calculated,
    /// Contingent leg held awaiting trigger (`"held"`).
    Held,
    /// Partially filled (`"partially_filled"`).
    PartiallyFilled,
    /// Completely filled (`"filled"`).
    Filled,
    /// Done for the trading day (`"done_for_day"`).
    DoneForDay,
    /// Canceled (`"canceled"`).
    Canceled,
    /// Expired (`"expired"`).
    Expired,
    /// Replaced by a modified order (`"replaced"`).
    Replaced,
    /// Waiting for cancellation confirmation (`"pending_cancel"`).
    PendingCancel,
    /// Waiting for replacement confirmation (`"pending_replace"`).
    PendingReplace,
    /// Rejected by venue (`"rejected"`).
    Rejected,
    /// Suspended by venue (`"suspended"`).
    Suspended,
}

impl AlpacaOrderStatus {
    /// Maps this [`AlpacaOrderStatus`] to the canonical Nautilus [`OrderStatus`].
    #[must_use]
    pub const fn to_order_status(self) -> OrderStatus {
        match self {
            Self::New
            | Self::Accepted
            | Self::AcceptedForBidding
            | Self::Calculated
            | Self::Held
            | Self::Stopped => OrderStatus::Accepted,
            Self::PendingNew => OrderStatus::Submitted,
            Self::PartiallyFilled => OrderStatus::PartiallyFilled,
            Self::Filled => OrderStatus::Filled,
            Self::DoneForDay | Self::Canceled | Self::Replaced => OrderStatus::Canceled,
            Self::Expired => OrderStatus::Expired,
            Self::PendingCancel => OrderStatus::PendingCancel,
            Self::PendingReplace => OrderStatus::PendingUpdate,
            Self::Rejected | Self::Suspended => OrderStatus::Rejected,
        }
    }

    /// Returns `true` when the order is open/in-flight on the venue (including [`Self::Stopped`]).
    #[must_use]
    pub const fn is_open(self) -> bool {
        matches!(
            self,
            Self::New
                | Self::PendingNew
                | Self::Accepted
                | Self::AcceptedForBidding
                | Self::Stopped
                | Self::Calculated
                | Self::Held
                | Self::PartiallyFilled
                | Self::PendingCancel
                | Self::PendingReplace
        )
    }
}

impl From<AlpacaOrderStatus> for OrderStatus {
    fn from(status: AlpacaOrderStatus) -> Self {
        status.to_order_status()
    }
}

impl std::str::FromStr for AlpacaOrderStatus {
    type Err = AlpacaError;

    fn from_str(status: &str) -> AlpacaResult<Self> {
        match status.trim().to_ascii_lowercase().as_str() {
            "new" => Ok(Self::New),
            "pending_new" => Ok(Self::PendingNew),
            "accepted" => Ok(Self::Accepted),
            "accepted_for_bidding" => Ok(Self::AcceptedForBidding),
            "stopped" => Ok(Self::Stopped),
            "calculated" => Ok(Self::Calculated),
            "held" => Ok(Self::Held),
            "partially_filled" => Ok(Self::PartiallyFilled),
            "filled" => Ok(Self::Filled),
            "done_for_day" => Ok(Self::DoneForDay),
            "canceled" | "cancelled" => Ok(Self::Canceled),
            "expired" => Ok(Self::Expired),
            "replaced" => Ok(Self::Replaced),
            "pending_cancel" => Ok(Self::PendingCancel),
            "pending_replace" => Ok(Self::PendingReplace),
            "rejected" => Ok(Self::Rejected),
            "suspended" => Ok(Self::Suspended),
            other => Err(AlpacaError::Parse(format!(
                "unrecognized Alpaca order status '{other}'"
            ))),
        }
    }
}

impl std::fmt::Display for AlpacaOrderStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::New => "new",
            Self::PendingNew => "pending_new",
            Self::Accepted => "accepted",
            Self::AcceptedForBidding => "accepted_for_bidding",
            Self::Stopped => "stopped",
            Self::Calculated => "calculated",
            Self::Held => "held",
            Self::PartiallyFilled => "partially_filled",
            Self::Filled => "filled",
            Self::DoneForDay => "done_for_day",
            Self::Canceled => "canceled",
            Self::Expired => "expired",
            Self::Replaced => "replaced",
            Self::PendingCancel => "pending_cancel",
            Self::PendingReplace => "pending_replace",
            Self::Rejected => "rejected",
            Self::Suspended => "suspended",
        };
        f.write_str(s)
    }
}

/// Maps an Alpaca order status string into a Nautilus [`OrderStatus`].
///
/// # Errors
///
/// Returns [`AlpacaError::Parse`] if the status string is unrecognized.
pub fn parse_alpaca_order_status(status: &str) -> AlpacaResult<OrderStatus> {
    status.parse::<AlpacaOrderStatus>().map(OrderStatus::from)
}
