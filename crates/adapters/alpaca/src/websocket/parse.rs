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

//! Domain parser converting Alpaca `trade_updates` payloads into strongly typed
//! [`AlpacaExecutionUpdate`] domain events (`OrderStatusReport` + optional `FillReport`).

use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::{
    enums::{LiquiditySide, OrderSide, OrderStatus, OrderType},
    identifiers::{AccountId, ClientOrderId, InstrumentId, TradeId, VenueOrderId},
    reports::{FillReport, OrderStatusReport},
    types::{Currency, Money, Price, Quantity},
};
use rust_decimal::Decimal;

use super::messages::{AlpacaTradeEventKind, AlpacaTradeUpdatePayload};
use crate::{
    error::{AlpacaError, AlpacaResult},
    http::parse::{
        decimal_to_price, decimal_to_quantity, parse_order_status_report,
        parse_rfc3339_to_unix_nanos,
    },
};

/// Strongly typed domain execution update emitted by the Alpaca `trade_updates` WebSocket handler.
#[derive(Debug, Clone, PartialEq)]
pub struct AlpacaExecutionUpdate {
    /// Trade update lifecycle event kind (`New`, `Fill`, `PartialFill`, `Canceled`, `Expired`, `Rejected`, `Replaced`, `DoneForDay`).
    pub event_kind: AlpacaTradeEventKind,
    /// Client order ID.
    pub client_order_id: ClientOrderId,
    /// Venue order ID.
    pub venue_order_id: VenueOrderId,
    /// Instrument identifier.
    pub instrument_id: InstrumentId,
    /// Order side (`Buy` or `Sell`).
    pub order_side: OrderSide,
    /// Order type (`Market`, `Limit`, `StopMarket`, `StopLimit`, `TrailingStopMarket`).
    pub order_type: OrderType,
    /// Normalized Nautilus order status.
    pub order_status: OrderStatus,
    /// Total order quantity.
    pub order_qty: Quantity,
    /// Cumulative filled quantity (`cum_qty`).
    pub cum_qty: Quantity,
    /// Remaining open quantity (`leaves_qty`).
    pub leaves_qty: Quantity,
    /// Incremental fill quantity (`last_qty`) on `Fill` / `PartialFill` events.
    pub last_qty: Option<Quantity>,
    /// Incremental fill price (`last_px`) on `Fill` / `PartialFill` events.
    pub last_px: Option<Price>,
    /// Cumulative average fill price (`avg_px`) when available.
    pub avg_px: Option<Decimal>,
    /// Resulting position quantity after this execution event, if reported by Alpaca.
    pub position_qty: Option<Decimal>,
    /// Trade execution ID (`TradeId`) on `Fill` / `PartialFill` events.
    pub trade_id: Option<TradeId>,
    /// Event timestamp in UTC nanoseconds (`ts_event`).
    pub ts_event: UnixNanos,
    /// Local initialization timestamp in UTC nanoseconds (`ts_init`).
    pub ts_init: UnixNanos,
    /// Normalized Nautilus [`OrderStatusReport`].
    pub order_report: OrderStatusReport,
    /// Normalized Nautilus [`FillReport`] (populated on `Fill` and `PartialFill`).
    pub fill_report: Option<FillReport>,
}

/// Parses an [`AlpacaTradeUpdatePayload`] into a validated [`AlpacaExecutionUpdate`].
///
/// # Errors
///
/// Returns [`AlpacaError`] if order identifiers, timestamps, or decimal prices/quantities are invalid,
/// or if a `Fill` / `PartialFill` frame is missing incremental fill price (`price`) or quantity (`qty`).
pub fn parse_trade_update_payload(
    payload: &AlpacaTradeUpdatePayload,
    account_id: AccountId,
    venue: &str,
    price_precision: u8,
    size_precision: u8,
    ts_init: UnixNanos,
) -> AlpacaResult<AlpacaExecutionUpdate> {
    let mut order_report = parse_order_status_report(
        &payload.order,
        account_id,
        venue,
        ts_init,
    )?;

    let ts_event = match payload.timestamp.as_deref() {
        Some(ts_str) if !ts_str.trim().is_empty() => parse_rfc3339_to_unix_nanos(ts_str)?,
        _ => order_report.ts_last,
    };
    order_report.ts_last = ts_event;

    order_report.order_status = match payload.event {
        AlpacaTradeEventKind::New => OrderStatus::Accepted,
        AlpacaTradeEventKind::Fill => OrderStatus::Filled,
        AlpacaTradeEventKind::PartialFill => OrderStatus::PartiallyFilled,
        AlpacaTradeEventKind::Canceled | AlpacaTradeEventKind::DoneForDay => OrderStatus::Canceled,
        AlpacaTradeEventKind::Expired => OrderStatus::Expired,
        AlpacaTradeEventKind::Rejected => OrderStatus::Rejected,
        AlpacaTradeEventKind::Replaced => OrderStatus::Accepted,
        AlpacaTradeEventKind::PendingNew => OrderStatus::Submitted,
        AlpacaTradeEventKind::PendingCancel => OrderStatus::PendingCancel,
        AlpacaTradeEventKind::PendingReplace => OrderStatus::PendingUpdate,
        _ => order_report.order_status,
    };

    let client_order_id = order_report.client_order_id.ok_or_else(|| {
        AlpacaError::Parse("trade_update order is missing client_order_id".to_string())
    })?;
    let venue_order_id = order_report.venue_order_id;
    let instrument_id = order_report.instrument_id;
    let order_side = OrderSide::from(payload.order.side);
    let order_type = order_report.order_type;
    let order_status = order_report.order_status;
    let order_qty = order_report.quantity;
    let cum_qty = order_report.filled_qty;

    let order_qty_dec = payload.order.qty.unwrap_or(payload.order.filled_qty);
    let leaves_dec = (order_qty_dec - payload.order.filled_qty).max(Decimal::ZERO);
    let leaves_qty = decimal_to_quantity(leaves_dec, size_precision)?;
    let avg_px = payload.order.filled_avg_price;

    let is_fill_event = matches!(
        payload.event,
        AlpacaTradeEventKind::Fill | AlpacaTradeEventKind::PartialFill
    );

    let (last_qty, last_px, trade_id, fill_report) = if is_fill_event {
        let raw_qty = payload
            .qty
            .filter(|q| *q > Decimal::ZERO)
            .or_else(|| {
                if payload.order.filled_qty > Decimal::ZERO {
                    Some(payload.order.filled_qty)
                } else {
                    None
                }
            })
            .ok_or_else(|| {
                AlpacaError::Parse(format!(
                    "trade_update {:?} for order {} missing positive fill qty",
                    payload.event, venue_order_id
                ))
            })?;
        let raw_px = payload
            .price
            .filter(|p| *p > Decimal::ZERO)
            .or(payload.order.filled_avg_price)
            .ok_or_else(|| {
                AlpacaError::Parse(format!(
                    "trade_update {:?} for order {} missing positive fill price",
                    payload.event, venue_order_id
                ))
            })?;

        let fill_qty = decimal_to_quantity(raw_qty, size_precision)?;
        let fill_px = decimal_to_price(raw_px, price_precision)?;

        let exec_id_str = payload
            .execution_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string)
            .unwrap_or_else(|| format!("{venue_order_id}-{}", ts_event.as_u64()));
        let t_id = TradeId::new_checked(&exec_id_str)
            .map_err(|e| AlpacaError::Parse(format!("invalid execution_id '{exec_id_str}': {e}")))?;

        let liquidity_side = match order_type {
            OrderType::Limit => LiquiditySide::Maker,
            _ => LiquiditySide::Taker,
        };

        let mut report = FillReport::new(
            account_id,
            instrument_id,
            venue_order_id,
            t_id,
            order_side,
            fill_qty,
            fill_px,
            Money::new(0.0, Currency::USD()),
            liquidity_side,
            Some(client_order_id),
            None,
            ts_event,
            ts_init,
            Some(UUID4::new()),
        );
        report.avg_px = avg_px;

        (Some(fill_qty), Some(fill_px), Some(t_id), Some(report))
    } else {
        let opt_qty = match payload.qty {
            Some(q) if q > Decimal::ZERO => Some(decimal_to_quantity(q, size_precision)?),
            _ => None,
        };
        let opt_px = match payload.price {
            Some(p) if p > Decimal::ZERO => Some(decimal_to_price(p, price_precision)?),
            _ => None,
        };
        (opt_qty, opt_px, None, None)
    };

    Ok(AlpacaExecutionUpdate {
        event_kind: payload.event,
        client_order_id,
        venue_order_id,
        instrument_id,
        order_side,
        order_type,
        order_status,
        order_qty,
        cum_qty,
        leaves_qty,
        last_qty,
        last_px,
        avg_px,
        position_qty: payload.position_qty,
        trade_id,
        ts_event,
        ts_init,
        order_report,
        fill_report,
    })
}
