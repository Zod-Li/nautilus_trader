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

//! Domain builder converting `DataInfra` US equity metadata into [`InstrumentAny::Equity`].

use std::str::FromStr;

use nautilus_core::UnixNanos;
use nautilus_model::{
    instruments::{Equity, InstrumentAny},
    types::{Currency, Price, Quantity},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use ustr::Ustr;

use crate::{
    error::{DataInfraError, DataInfraResult},
    mapping::to_instrument_id,
};

/// Metadata record describing a tradable equity instrument in `DataInfra`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataInfraEquityMetadata {
    /// Ticker symbol (e.g. `"NVDA"`, `"SPY"`).
    pub symbol: String,
    /// Execution or listing venue (e.g. `"ALPACA"`, `"NASDAQ"`, `"ARCA"`).
    pub venue: String,
    /// Optional 12-character ISIN identifier.
    pub isin: Option<String>,
    /// Settlement currency code (defaults to `"USD"`).
    pub currency_code: String,
    /// Decimal price precision (e.g. `2` for `$0.01` tick size, `4` for sub-penny).
    pub price_precision: u8,
    /// Size precision for quantities (`0` for whole shares).
    pub size_precision: u8,
    /// Standard board lot size in shares (e.g. `1` or `100`).
    pub lot_size: u64,
    /// Optional minimum order quantity in shares.
    pub min_quantity: Option<u64>,
    /// Optional maximum order quantity in shares.
    pub max_quantity: Option<u64>,
    /// Initial margin requirement ratio (`0..=1`, e.g. `"0.50"`).
    pub margin_init: Option<Decimal>,
    /// Maintenance margin requirement ratio (`0..=1`, e.g. `"0.25"`).
    pub margin_maint: Option<Decimal>,
    /// Event timestamp in UNIX nanoseconds.
    pub ts_event: UnixNanos,
    /// Initialization timestamp in UNIX nanoseconds.
    pub ts_init: UnixNanos,
}

impl DataInfraEquityMetadata {
    /// Creates standard US equity metadata (`currency_code = "USD"`, `price_precision = 2`, `size_precision = 0`, `lot_size = 1`).
    #[must_use]
    pub fn us_equity(
        symbol: impl Into<String>,
        venue: impl Into<String>,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
    ) -> Self {
        Self {
            symbol: symbol.into(),
            venue: venue.into(),
            isin: None,
            currency_code: "USD".to_string(),
            price_precision: 2,
            size_precision: 0,
            lot_size: 1,
            min_quantity: Some(1),
            max_quantity: None,
            margin_init: None,
            margin_maint: None,
            ts_event,
            ts_init,
        }
    }

    /// Creates default US equity metadata with zeroed `ts_event`/`ts_init`.
    #[must_use]
    pub fn new(symbol: impl Into<String>, venue: impl Into<String>) -> Self {
        Self::us_equity(symbol, venue, UnixNanos::default(), UnixNanos::default())
    }

    /// Overrides the board lot size on this metadata.
    #[must_use]
    pub const fn with_lot_size(mut self, lot_size: u64) -> Self {
        self.lot_size = lot_size;
        self
    }

    /// Validates this metadata and constructs a Nautilus [`Equity`] instrument with an explicit `ts_init`.
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError::InvalidInstrument`] if `price_precision`, `lot_size`,
    /// `currency_code`, or quantity bounds violate Nautilus domain invariants.
    pub fn to_equity_any(&self, ts_init: UnixNanos) -> DataInfraResult<Equity> {
        let mut copy = self.clone();
        copy.ts_init = ts_init;
        if copy.ts_event.as_u64() == 0 {
            copy.ts_event = ts_init;
        }
        copy.to_equity()
    }

    /// Validates this metadata and constructs a Nautilus [`Equity`] instrument.
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError::InvalidInstrument`] if `price_precision`, `lot_size`,
    /// `currency_code`, or quantity bounds violate Nautilus domain invariants.
    pub fn to_equity(&self) -> DataInfraResult<Equity> {
        let instrument_id = to_instrument_id(&self.symbol, &self.venue)
            .map_err(|e| DataInfraError::InvalidInstrument(e.to_string()))?;

        if self.price_precision == 0 || self.price_precision > 9 {
            return Err(DataInfraError::InvalidInstrument(format!(
                "price_precision={} must be in 1..=9",
                self.price_precision
            )));
        }

        if self.size_precision > 9 {
            return Err(DataInfraError::InvalidInstrument(format!(
                "size_precision={} must be <= 9",
                self.size_precision
            )));
        }

        if self.lot_size == 0 {
            return Err(DataInfraError::InvalidInstrument(
                "lot_size must be > 0".to_string(),
            ));
        }

        if let (Some(min_q), Some(max_q)) = (self.min_quantity, self.max_quantity)
            && (min_q == 0 || min_q > max_q)
        {
            return Err(DataInfraError::InvalidInstrument(format!(
                "invalid quantity bounds: min_quantity={min_q}, max_quantity={max_q}"
            )));
        }

        if let Some(min_q) = self.min_quantity
            && min_q == 0
        {
            return Err(DataInfraError::InvalidInstrument(
                "min_quantity must be > 0 when specified".to_string(),
            ));
        }

        let currency = Currency::from_str(self.currency_code.trim()).map_err(|e| {
            DataInfraError::InvalidInstrument(format!(
                "invalid currency_code '{}': {e}",
                self.currency_code
            ))
        })?;

        let tick_dec = Decimal::new(1, u32::from(self.price_precision));
        let price_increment = Price::from_decimal_dp(tick_dec, self.price_precision)
            .map_err(|e| DataInfraError::InvalidInstrument(format!("invalid price_increment: {e}")))?;

        let lot_qty = Quantity::from_decimal_dp(Decimal::from(self.lot_size), self.size_precision)
            .map_err(|e| DataInfraError::InvalidInstrument(format!("invalid lot_size: {e}")))?;

        let min_qty = match self.min_quantity {
            Some(v) => Some(
                Quantity::from_decimal_dp(Decimal::from(v), self.size_precision).map_err(|e| {
                    DataInfraError::InvalidInstrument(format!("invalid min_quantity: {e}"))
                })?,
            ),
            None => None,
        };

        let max_qty = match self.max_quantity {
            Some(v) => Some(
                Quantity::from_decimal_dp(Decimal::from(v), self.size_precision).map_err(|e| {
                    DataInfraError::InvalidInstrument(format!("invalid max_quantity: {e}"))
                })?,
            ),
            None => None,
        };

        let isin = match &self.isin {
            Some(raw_isin) => {
                let trimmed = raw_isin.trim();
                if trimmed.is_empty() {
                    return Err(DataInfraError::InvalidInstrument(
                        "isin must not be empty when Some".to_string(),
                    ));
                }
                Some(Ustr::from(trimmed))
            }
            None => None,
        };

        Equity::builder()
            .instrument_id(instrument_id)
            .raw_symbol(instrument_id.symbol)
            .maybe_isin(isin)
            .currency(currency)
            .price_precision(self.price_precision)
            .price_increment(price_increment)
            .lot_size(lot_qty)
            .maybe_min_quantity(min_qty)
            .maybe_max_quantity(max_qty)
            .maybe_margin_init(self.margin_init)
            .maybe_margin_maint(self.margin_maint)
            .ts_event(self.ts_event)
            .ts_init(self.ts_init)
            .build()
            .map_err(|e| DataInfraError::InvalidInstrument(format!("Equity build failed: {e}")))
    }

    /// Constructs a Nautilus [`InstrumentAny::Equity`] from this metadata.
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError::InvalidInstrument`] if validation fails.
    pub fn to_instrument_any(&self) -> DataInfraResult<InstrumentAny> {
        Ok(InstrumentAny::Equity(self.to_equity()?))
    }
}
