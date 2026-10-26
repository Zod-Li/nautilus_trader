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

//! Pure decimal-to-domain conversion functions mapping Alpaca REST DTOs to
//! Nautilus [`AccountState`], [`OrderStatusReport`], [`PositionStatusReport`], and [`Equity`].
//!
//! No `f64` intermediate conversion is used for prices, quantities, or balances.

use std::str::FromStr;

use jiff::Timestamp;
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::{
    enums::{AccountType, OrderSide, OrderType, PositionSide, TimeInForce, TriggerType},
    events::AccountState,
    identifiers::{
        AccountId, ClientOrderId, InstrumentId, OrderListId, PositionId, Symbol, TradeId, Venue,
        VenueOrderId,
    },
    instruments::{Equity, InstrumentAny},
    reports::{OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, Money, Price, Quantity},
};
use rust_decimal::Decimal;

use crate::{
    common::enums::parse_alpaca_order_status,
    error::{AlpacaError, AlpacaResult},
    http::models::{
        AlpacaAccountResponse, AlpacaAssetResponse, AlpacaOrderResponse, AlpacaPositionResponse,
    },
};

/// Default price precision for US Equities (`2` decimal places, or `4` for sub-dollar/extended).
pub const DEFAULT_EQUITY_PRICE_PRECISION: u8 = 2;
/// Default share quantity precision (`0` for whole shares).
pub const DEFAULT_EQUITY_SIZE_PRECISION: u8 = 0;

/// Parses an RFC3339 timestamp string into [`UnixNanos`].
///
/// # Errors
///
/// Returns [`AlpacaError::Parse`] if the timestamp cannot be parsed or is negative.
pub fn parse_rfc3339_to_unix_nanos(raw: &str) -> AlpacaResult<UnixNanos> {
    let ts: Timestamp = raw
        .trim()
        .parse()
        .map_err(|e| AlpacaError::Parse(format!("invalid RFC3339 timestamp '{raw}': {e}")))?;
    let nanos = ts.as_nanosecond();
    let nanos_u64 = u64::try_from(nanos).map_err(|_| {
        AlpacaError::Parse(format!(
            "timestamp '{raw}' precedes UNIX epoch ({nanos} ns)"
        ))
    })?;
    Ok(UnixNanos::from(nanos_u64))
}

/// Converts a [`Decimal`] into a Nautilus [`Price`] using exact mantissa/scale arithmetic.
///
/// # Errors
///
/// Returns [`AlpacaError::Parse`] if precision or range is invalid.
pub fn decimal_to_price(value: Decimal, min_precision: u8) -> AlpacaResult<Price> {
    let scale_u8 = u8::try_from(value.scale()).unwrap_or(min_precision);
    let precision = scale_u8.max(min_precision).min(9);
    Price::from_decimal_dp(value, precision)
        .map_err(|e| AlpacaError::Parse(format!("invalid Decimal price '{value}': {e}")))
}

/// Converts a [`Decimal`] into a Nautilus [`Quantity`] using exact mantissa/scale arithmetic.
///
/// # Errors
///
/// Returns [`AlpacaError::Parse`] if the quantity is negative or out of range.
pub fn decimal_to_quantity(value: Decimal, min_precision: u8) -> AlpacaResult<Quantity> {
    let scale_u8 = u8::try_from(value.scale()).unwrap_or(min_precision);
    let precision = scale_u8.max(min_precision).min(9);
    Quantity::from_decimal_dp(value, precision)
        .map_err(|e| AlpacaError::Parse(format!("invalid Decimal quantity '{value}': {e}")))
}

/// Converts a [`Decimal`] into a Nautilus [`Money`] using exact decimal arithmetic.
///
/// # Errors
///
/// Returns [`AlpacaError::Parse`] if the amount is outside the representable range.
pub fn decimal_to_money(value: Decimal, currency: Currency) -> AlpacaResult<Money> {
    Money::from_decimal(value, currency).map_err(|e| {
        AlpacaError::Parse(format!(
            "invalid Decimal money '{value}' {}: {e}",
            currency.code
        ))
    })
}

/// Builds a Nautilus [`InstrumentId`] from an Alpaca symbol and venue name.
///
/// # Errors
///
/// Returns [`AlpacaError::Parse`] if the symbol or venue is invalid.
pub fn parse_instrument_id(symbol: &str, venue_str: &str) -> AlpacaResult<InstrumentId> {
    let sym = Symbol::new_checked(symbol.trim())
        .map_err(|e| AlpacaError::Parse(format!("invalid symbol '{symbol}': {e}")))?;
    let ven = Venue::new_checked(venue_str.trim())
        .map_err(|e| AlpacaError::Parse(format!("invalid venue '{venue_str}': {e}")))?;
    Ok(InstrumentId::new(sym, ven))
}

/// Formats a venue trade or order identifier into a valid [`TradeId`] (`<= 36` ASCII characters).
///
/// Preserves `{prefix}{raw_id}` verbatim when `<= 36` characters (e.g. `"FILL-VEN-REC-BRK"`),
/// and compacts hyphenated 36-char UUIDs (`"F-{32_hex_chars}"`, 34 chars) when the prefixed
/// form would exceed [`TradeId`]'s 36-byte capacity.
///
/// # Errors
///
/// Returns [`AlpacaError::Parse`] if the resulting identifier is empty or contains non-ASCII characters.
pub fn parse_alpaca_trade_id(prefix: &str, raw_id: &str) -> AlpacaResult<TradeId> {
    let trimmed = raw_id.trim();
    let candidate = format!("{prefix}{trimmed}");
    let bounded = if candidate.len() <= 36 {
        candidate
    } else {
        let compact_id: String = trimmed.chars().filter(|c| *c != '-').collect();
        let short_prefix = if prefix.is_empty() { "" } else { "F-" };
        let compact_candidate = format!("{short_prefix}{compact_id}");
        if compact_candidate.len() <= 36 {
            compact_candidate
        } else {
            compact_candidate[..36].to_string()
        }
    };
    TradeId::new_checked(&bounded)
        .map_err(|e| AlpacaError::Parse(format!("invalid TradeId '{bounded}': {e}")))
}

/// Converts an [`AlpacaAccountResponse`] into a Nautilus [`AccountState`].
///
/// # Errors
///
/// Returns [`AlpacaError::Parse`] if the account ID, currency, or balances are invalid.
pub fn parse_account_state(
    resp: &AlpacaAccountResponse,
    venue_str: &str,
    ts_init: UnixNanos,
) -> AlpacaResult<AccountState> {
    let account_label = if resp.account_number.trim().is_empty() {
        resp.id.trim()
    } else {
        resp.account_number.trim()
    };
    let account_id = AccountId::new_checked(format!("{}-{account_label}", venue_str.trim()))
        .map_err(|e| AlpacaError::Parse(format!("invalid AccountId: {e}")))?;

    let currency = Currency::from_str(resp.currency.trim()).map_err(|e| {
        AlpacaError::Parse(format!("invalid account currency '{}': {e}", resp.currency))
    })?;

    let total = resp.equity;
    let locked = resp.initial_margin.max(Decimal::ZERO);
    let balance = AccountBalance::from_total_and_locked(total, locked, currency)
        .map_err(|e| AlpacaError::Parse(format!("invalid AccountBalance: {e}")))?;

    let ts_event = match &resp.created_at {
        Some(ts_str) => parse_rfc3339_to_unix_nanos(ts_str).unwrap_or(ts_init),
        None => ts_init,
    };

    Ok(AccountState::new(
        account_id,
        AccountType::Margin,
        vec![balance],
        vec![],
        true,
        UUID4::new(),
        ts_event,
        ts_init,
        Some(currency),
    ))
}

/// Converts an [`AlpacaPositionResponse`] into a Nautilus [`PositionStatusReport`].
///
/// # Errors
///
/// Returns [`AlpacaError::Parse`] if symbol, quantity, or side is invalid.
pub fn parse_position_status_report(
    pos: &AlpacaPositionResponse,
    account_id: AccountId,
    venue_str: &str,
    ts_init: UnixNanos,
) -> AlpacaResult<PositionStatusReport> {
    let instrument_id = parse_instrument_id(&pos.symbol, venue_str)?;
    let abs_qty_dec = pos.qty.abs();
    let quantity = decimal_to_quantity(abs_qty_dec, DEFAULT_EQUITY_SIZE_PRECISION)?;

    let position_side = if abs_qty_dec.is_zero() {
        PositionSide::Flat
    } else {
        match pos.side.trim().to_ascii_lowercase().as_str() {
            "long" => PositionSide::Long,
            "short" => PositionSide::Short,
            other => {
                return Err(AlpacaError::Parse(format!(
                    "unrecognized Alpaca position side '{other}'"
                )));
            }
        }
    };

    let venue_position_id = if pos.asset_id.trim().is_empty() {
        None
    } else {
        Some(
            PositionId::new_checked(pos.asset_id.trim())
                .map_err(|e| AlpacaError::Parse(format!("invalid PositionId: {e}")))?,
        )
    };

    Ok(PositionStatusReport::new(
        account_id,
        instrument_id,
        position_side,
        quantity,
        ts_init,
        ts_init,
        Some(UUID4::new()),
        venue_position_id,
        Some(pos.avg_entry_price),
    ))
}

/// Converts an [`AlpacaOrderResponse`] into a Nautilus [`OrderStatusReport`].
///
/// # Errors
///
/// Returns [`AlpacaError::Parse`] if any identifier, status, timestamp, or decimal value is invalid.
pub fn parse_order_status_report(
    order: &AlpacaOrderResponse,
    account_id: AccountId,
    venue_str: &str,
    ts_init: UnixNanos,
) -> AlpacaResult<OrderStatusReport> {
    let instrument_id = parse_instrument_id(&order.symbol, venue_str)?;
    let venue_order_id = VenueOrderId::new_checked(order.id.trim())
        .map_err(|e| AlpacaError::Parse(format!("invalid VenueOrderId '{}': {e}", order.id)))?;

    let client_order_id = if order.client_order_id.trim().is_empty() {
        None
    } else {
        Some(
            ClientOrderId::new_checked(order.client_order_id.trim()).map_err(|e| {
                AlpacaError::Parse(format!(
                    "invalid ClientOrderId '{}': {e}",
                    order.client_order_id
                ))
            })?,
        )
    };

    let order_side = OrderSide::from(order.side);
    let order_type = OrderType::from(order.order_type);
    let time_in_force = TimeInForce::from(order.time_in_force);
    let order_status = parse_alpaca_order_status(&order.status)?;

    let qty_dec = order.qty.unwrap_or(order.filled_qty);
    let quantity = decimal_to_quantity(qty_dec, DEFAULT_EQUITY_SIZE_PRECISION)?;
    let filled_qty = decimal_to_quantity(order.filled_qty, DEFAULT_EQUITY_SIZE_PRECISION)?;

    let ts_accepted = order
        .submitted_at
        .as_deref()
        .unwrap_or(&order.created_at);
    let ts_accepted_ns = parse_rfc3339_to_unix_nanos(ts_accepted)?;

    let ts_last_str = order
        .filled_at
        .as_deref()
        .or(order.canceled_at.as_deref())
        .or(order.expired_at.as_deref())
        .or(order.failed_at.as_deref())
        .or(order.replaced_at.as_deref())
        .or(order.updated_at.as_deref())
        .unwrap_or(&order.created_at);
    let ts_last_ns = parse_rfc3339_to_unix_nanos(ts_last_str)?;

    let mut report = OrderStatusReport::new(
        account_id,
        instrument_id,
        client_order_id,
        venue_order_id,
        Some(order_side),
        order_type,
        time_in_force,
        order_status,
        quantity,
        filled_qty,
        ts_accepted_ns,
        ts_last_ns,
        ts_init,
        Some(UUID4::new()),
    );

    if let Some(limit_px) = order.limit_price {
        report.price = Some(decimal_to_price(limit_px, DEFAULT_EQUITY_PRICE_PRECISION)?);
    }

    if let Some(stop_px) = order.stop_price {
        report.trigger_price = Some(decimal_to_price(stop_px, DEFAULT_EQUITY_PRICE_PRECISION)?);
        report.trigger_type = Some(TriggerType::Default);
    }

    if let Some(avg_px) = order.filled_avg_price {
        report.avg_px = Some(avg_px);
    }

    if let Some(order_class) = order.order_class {
        report.contingency_type = order_class.to_contingency_type();
    }

    if let Some(legs) = &order.legs
        && !legs.is_empty()
    {
        let mut linked_ids = Vec::with_capacity(legs.len());
        for leg in legs {
            if !leg.client_order_id.trim().is_empty()
                && let Ok(cid) = ClientOrderId::new_checked(leg.client_order_id.trim())
            {
                linked_ids.push(cid);
            }
        }
        if !linked_ids.is_empty() {
            report.linked_order_ids = Some(linked_ids);
        }
        if let Ok(list_id) = OrderListId::new_checked(format!("OL-{}", order.id.trim())) {
            report.order_list_id = Some(list_id);
        }
    }

    Ok(report)
}

/// Converts an [`AlpacaAssetResponse`] into a Nautilus [`InstrumentAny::Equity`].
///
/// # Errors
///
/// Returns [`AlpacaError::Parse`] if symbol or increment fields are invalid.
pub fn parse_equity_instrument(
    asset: &AlpacaAssetResponse,
    venue_str: &str,
    ts_init: UnixNanos,
) -> AlpacaResult<InstrumentAny> {
    let instrument_id = parse_instrument_id(&asset.symbol, venue_str)?;
    let currency = Currency::USD();

    let (price_precision, price_increment) = match asset.price_increment {
        Some(inc) if inc > Decimal::ZERO => {
            let prec = u8::try_from(inc.scale())
                .unwrap_or(DEFAULT_EQUITY_PRICE_PRECISION)
                .clamp(1, 9);
            let px = Price::from_decimal_dp(inc, prec)
                .map_err(|e| AlpacaError::Parse(format!("invalid price_increment: {e}")))?;
            (prec, px)
        }
        _ => {
            let inc = Decimal::new(1, u32::from(DEFAULT_EQUITY_PRICE_PRECISION));
            let px = Price::from_decimal_dp(inc, DEFAULT_EQUITY_PRICE_PRECISION)
                .map_err(|e| AlpacaError::Parse(format!("invalid default price_increment: {e}")))?;
            (DEFAULT_EQUITY_PRICE_PRECISION, px)
        }
    };

    let min_qty_dec = asset
        .min_order_size
        .filter(|d| *d > Decimal::ZERO)
        .unwrap_or(Decimal::ONE);
    let size_precision = if asset.fractionable {
        u8::try_from(min_qty_dec.scale()).unwrap_or(0).min(6)
    } else {
        0
    };
    let min_quantity = Quantity::from_decimal_dp(min_qty_dec, size_precision)
        .map_err(|e| AlpacaError::Parse(format!("invalid min_quantity: {e}")))?;
    let lot_size = Quantity::from_decimal_dp(Decimal::ONE, size_precision)
        .map_err(|e| AlpacaError::Parse(format!("invalid lot_size: {e}")))?;

    let equity = Equity::builder()
        .instrument_id(instrument_id)
        .raw_symbol(instrument_id.symbol)
        .currency(currency)
        .price_precision(price_precision)
        .price_increment(price_increment)
        .lot_size(lot_size)
        .min_quantity(min_quantity)
        .ts_event(ts_init)
        .ts_init(ts_init)
        .build()
        .map_err(|e| AlpacaError::Parse(format!("Equity build failed: {e}")))?;

    Ok(InstrumentAny::Equity(equity))
}
