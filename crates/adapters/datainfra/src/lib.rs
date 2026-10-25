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

//! `DataInfra` market data adapter for [NautilusTrader](https://nautechsystems.io).
//!
//! Bridges the ClickHouse north-bound `ArrowStream` catalog, Kafka `sip-feeder` real-time
//! event bus, and `calendar-service` session boundaries into the native Nautilus engine
//! with zero modifications to Nautilus core crates.

#![warn(rustc::all)]
#![deny(unsafe_code)]
#![deny(unsafe_op_in_unsafe_fn)]
#![deny(nonstandard_style)]
#![deny(missing_debug_implementations)]
#![deny(clippy::missing_errors_doc)]
#![deny(clippy::missing_panics_doc)]
#![deny(rustdoc::broken_intra_doc_links)]

pub mod arrow_decoder;
pub mod calendar;
pub mod catalog;
pub mod clickhouse;
pub mod config;
pub mod data;
pub mod error;
pub mod factories;
pub mod instrument;
pub mod mapping;
pub mod stream;

#[cfg(feature = "python")]
pub mod python;

pub use arrow_decoder::{
    ArrowDecodeOptions, PriceAdjustmentMode, decode_arrow_ipc_stream, decode_bars_arrow_stream,
    decode_bars_batches, decode_bars_parquet, decode_parquet_bytes, decode_quotes_arrow_stream,
    decode_quotes_batches, decode_statuses_arrow_stream, decode_statuses_batches,
};
pub use calendar::{
    CalendarSessionGate, CalendarSessionStatus, CalendarTransport, DEFAULT_CALENDAR_MARKET,
    HttpCalendarTransport,
};
pub use catalog::{DATAINFRA_CATALOG_FACTORY_NAME, DataInfraCatalog};
pub use clickhouse::{
    BarsQueryRequest, ClickHouseArrowTransport, HttpClickHouseArrowTransport, QuotesQueryRequest,
    StatusesQueryRequest, build_sip_quotes_query, build_sip_statuses_query,
    build_v_quant_bars_query,
};
pub use config::DataInfraConfig;
pub use data::{DATAINFRA, DataInfraDataClient};
pub use error::{DataInfraError, DataInfraResult};
pub use factories::{DataInfraDataClientConfig, DataInfraDataClientFactory};
pub use instrument::DataInfraEquityMetadata;
pub use mapping::{
    DataInfraTimeframe, from_bar_type, from_instrument_id, parse_datainfra_bar_type, to_bar_type,
    to_instrument_id,
};
pub use stream::{
    BarStreamVersionTracker, CanonicalBarWire, CanonicalQuoteWire, CanonicalStreamDecoder,
    CanonicalTradingStatusWire, DataInfraStreamEvent, KafkaStreamSessionCoordinator,
    StreamDecodeOutcome, StreamLifecycleListener, TOPIC_SIP_BARS_1M, TOPIC_SIP_BARS_PREFIX,
    TOPIC_SIP_QUOTES, TOPIC_SIP_STATUSES, VersionEvaluation,
};
