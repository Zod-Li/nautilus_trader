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

//! Common domain types, credentials, and constants for the `nautilus-alpaca` adapter.

pub mod consts;
pub mod credential;
pub mod enums;

pub use consts::{
    ALPACA_LIVE_HTTP_URL, ALPACA_LIVE_WS_URL, ALPACA_PAPER_HTTP_URL, ALPACA_PAPER_WS_URL,
    ALPACA_VENUE, HEADER_APCA_API_KEY_ID, HEADER_APCA_API_SECRET_KEY,
};
pub use credential::AlpacaCredential;
pub use enums::{
    AlpacaEnvironment, AlpacaOrderClass, AlpacaOrderSide, AlpacaOrderType, AlpacaTimeInForce,
    parse_alpaca_order_status,
};
