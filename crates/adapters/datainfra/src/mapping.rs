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

//! Bidirectional domain mapping between `DataInfra` symbols/timeframes and
//! Nautilus [`InstrumentId`], [`BarSpecification`], and [`BarType`].

use std::{fmt::Display, str::FromStr};

use nautilus_model::{
    data::bar::{
        BAR_SPEC_1_DAY_LAST, BAR_SPEC_1_HOUR_LAST, BAR_SPEC_1_MINUTE_LAST, BAR_SPEC_1_WEEK_LAST,
        BAR_SPEC_4_HOUR_LAST, BAR_SPEC_15_MINUTE_LAST, BarSpecification, BarType,
    },
    enums::{AggregationSource, BarAggregation, PriceType},
    identifiers::{InstrumentId, Symbol, Venue},
};
use serde::{Deserialize, Serialize};

use crate::error::{DataInfraError, DataInfraResult};

/// Supported canonical multi-timeframe intervals in `DataInfra`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum DataInfraTimeframe {
    /// 1-minute bar (`"1m"`).
    M1,
    /// 15-minute tactical execution bar (`"15m"`).
    M15,
    /// 1-hour bar (`"1h"`).
    H1,
    /// 4-hour cross-sectional bar (`"4h"`).
    H4,
    /// 1-day daily bar (`"1d"`).
    D1,
    /// 1-week weekly bar (`"1w"`).
    W1,
}

impl DataInfraTimeframe {
    /// Returns the canonical `DataInfra` string representation (`"1m"`, `"15m"`, `"1h"`, `"4h"`, `"1d"`, `"1w"`).
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::M1 => "1m",
            Self::M15 => "15m",
            Self::H1 => "1h",
            Self::H4 => "4h",
            Self::D1 => "1d",
            Self::W1 => "1w",
        }
    }

    /// Converts this [`DataInfraTimeframe`] into the corresponding Nautilus [`BarSpecification`] (`PriceType::Last`).
    #[must_use]
    pub const fn to_bar_specification(&self) -> BarSpecification {
        match self {
            Self::M1 => BAR_SPEC_1_MINUTE_LAST,
            Self::M15 => BAR_SPEC_15_MINUTE_LAST,
            Self::H1 => BAR_SPEC_1_HOUR_LAST,
            Self::H4 => BAR_SPEC_4_HOUR_LAST,
            Self::D1 => BAR_SPEC_1_DAY_LAST,
            Self::W1 => BAR_SPEC_1_WEEK_LAST,
        }
    }

    /// Converts a Nautilus [`BarSpecification`] into a [`DataInfraTimeframe`].
    ///
    /// # Errors
    ///
    /// Returns [`DataInfraError::UnsupportedTimeframe`] if the price type is not `Last`
    /// or the `(step, aggregation)` pair is not one of the 6 canonical `DataInfra` timeframes.
    pub fn from_bar_specification(spec: BarSpecification) -> DataInfraResult<Self> {
        if spec.price_type != PriceType::Last {
            return Err(DataInfraError::UnsupportedTimeframe(format!(
                "DataInfra requires PriceType::Last, got {}",
                spec.price_type
            )));
        }
        match (spec.step.get(), spec.aggregation) {
            (1, BarAggregation::Minute) => Ok(Self::M1),
            (15, BarAggregation::Minute) => Ok(Self::M15),
            (1, BarAggregation::Hour) => Ok(Self::H1),
            (4, BarAggregation::Hour) => Ok(Self::H4),
            (1, BarAggregation::Day) => Ok(Self::D1),
            (1, BarAggregation::Week) => Ok(Self::W1),
            (step, agg) => Err(DataInfraError::UnsupportedTimeframe(format!(
                "unsupported bar specification step={step} aggregation={agg}"
            ))),
        }
    }
}

impl Display for DataInfraTimeframe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for DataInfraTimeframe {
    type Err = DataInfraError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim() {
            "1m" | "1Min" | "1MIN" => Ok(Self::M1),
            "15m" | "15Min" | "15MIN" => Ok(Self::M15),
            "1h" | "1Hour" | "1HOUR" => Ok(Self::H1),
            "4h" | "4Hour" | "4HOUR" => Ok(Self::H4),
            "1d" | "1Day" | "1DAY" => Ok(Self::D1),
            "1w" | "1Week" | "1WEEK" => Ok(Self::W1),
            other => Err(DataInfraError::UnsupportedTimeframe(format!(
                "unsupported DataInfra timeframe '{other}' (expected one of: 1m, 15m, 1h, 4h, 1d, 1w)"
            ))),
        }
    }
}

/// Converts a `DataInfra` `(symbol, venue)` pair into a validated Nautilus [`InstrumentId`].
///
/// # Errors
///
/// Returns [`DataInfraError::InvalidIdentifier`] if `symbol` or `venue` is empty, contains
/// whitespace or `.` separators that would corrupt `SYMBOL.VENUE` round-tripping, or fails
/// [`Symbol::new_checked`] / [`Venue::new_checked`].
pub fn to_instrument_id(symbol: &str, venue: &str) -> DataInfraResult<InstrumentId> {
    let sym_trimmed = symbol.trim();
    let venue_trimmed = venue.trim();

    if sym_trimmed.is_empty() {
        return Err(DataInfraError::InvalidIdentifier(
            "symbol must not be empty".to_string(),
        ));
    }
    if venue_trimmed.is_empty() {
        return Err(DataInfraError::InvalidIdentifier(
            "venue must not be empty".to_string(),
        ));
    }
    if sym_trimmed.contains(char::is_whitespace) || venue_trimmed.contains(char::is_whitespace) {
        return Err(DataInfraError::InvalidIdentifier(format!(
            "symbol '{sym_trimmed}' and venue '{venue_trimmed}' must not contain whitespace"
        )));
    }

    let sym = Symbol::new_checked(sym_trimmed).map_err(|e| {
        DataInfraError::InvalidIdentifier(format!("invalid symbol '{sym_trimmed}': {e}"))
    })?;
    let ven = Venue::new_checked(venue_trimmed).map_err(|e| {
        DataInfraError::InvalidIdentifier(format!("invalid venue '{venue_trimmed}': {e}"))
    })?;

    Ok(InstrumentId::new(sym, ven))
}

/// Extracts `(symbol, venue)` strings losslessly from a Nautilus [`InstrumentId`].
#[must_use]
pub fn from_instrument_id(instrument_id: InstrumentId) -> (String, String) {
    (
        instrument_id.symbol.as_str().to_string(),
        instrument_id.venue.as_str().to_string(),
    )
}

/// Constructs an external `LAST`-price Nautilus [`BarType`] from `DataInfra` `(symbol, venue, timeframe)`.
///
/// # Errors
///
/// Returns [`DataInfraError`] if `(symbol, venue)` or `timeframe` is invalid.
pub fn to_bar_type(
    symbol: &str,
    venue: &str,
    timeframe: DataInfraTimeframe,
) -> DataInfraResult<BarType> {
    let instrument_id = to_instrument_id(symbol, venue)?;
    let spec = timeframe.to_bar_specification();
    Ok(BarType::new(
        instrument_id,
        spec,
        AggregationSource::External,
    ))
}

/// Parses a `DataInfra` `(symbol, venue, timeframe_str)` tuple into a Nautilus [`BarType`].
///
/// # Errors
///
/// Returns [`DataInfraError`] if `symbol`, `venue`, or `timeframe_str` is invalid.
pub fn parse_datainfra_bar_type(
    symbol: &str,
    venue: &str,
    timeframe_str: &str,
) -> DataInfraResult<BarType> {
    let tf = DataInfraTimeframe::from_str(timeframe_str)?;
    to_bar_type(symbol, venue, tf)
}

/// Extracts `(symbol, venue, DataInfraTimeframe)` from a Nautilus [`BarType`].
///
/// # Errors
///
/// Returns [`DataInfraError::UnsupportedTimeframe`] if the `BarType` is composite,
/// uses `AggregationSource::Internal`, or has a [`BarSpecification`] outside the
/// canonical `DataInfra` set.
pub fn from_bar_type(bar_type: &BarType) -> DataInfraResult<(String, String, DataInfraTimeframe)> {
    match bar_type {
        BarType::Standard {
            instrument_id,
            spec,
            aggregation_source,
        } => {
            if *aggregation_source != AggregationSource::External {
                return Err(DataInfraError::UnsupportedTimeframe(format!(
                    "DataInfra BarType must use AggregationSource::External, got {aggregation_source}"
                )));
            }
            let tf = DataInfraTimeframe::from_bar_specification(*spec)?;
            let (symbol, venue) = from_instrument_id(*instrument_id);
            Ok((symbol, venue, tf))
        }
        BarType::Composite { .. } => Err(DataInfraError::UnsupportedTimeframe(
            "Composite BarType is not supported by DataInfra".to_string(),
        )),
    }
}
