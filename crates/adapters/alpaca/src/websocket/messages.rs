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

//! Strongly typed Alpaca `trade_updates` WebSocket wire frames (`messages.rs`).
//!
//! Strictly separates session control frames (`connected`, `authorization`, `listening`)
//! from business `trade_updates` frames so control signals never leak into downstream
//! execution event consumers.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    error::{AlpacaError, AlpacaResult},
    http::models::AlpacaOrderResponse,
};

/// Canonical stream topic name for Alpaca execution updates.
pub const TRADE_UPDATES_STREAM: &str = "trade_updates";

/// Outbound authentication frame sent on connection/reconnection to Alpaca `trade_updates` WebSocket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlpacaWsAuthenticateCommand {
    /// Action type (`"authenticate"`).
    pub action: String,
    /// Credentials payload.
    pub data: AlpacaWsAuthCredentialsPayload,
}

/// Credentials payload inside [`AlpacaWsAuthenticateCommand`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlpacaWsAuthCredentialsPayload {
    /// Alpaca API Key ID.
    pub key_id: String,
    /// Alpaca API Secret Key.
    pub secret_key: String,
}

impl AlpacaWsAuthenticateCommand {
    /// Constructs a new `"authenticate"` command frame.
    #[must_use]
    pub fn new(key_id: impl Into<String>, secret_key: impl Into<String>) -> Self {
        Self {
            action: "authenticate".to_string(),
            data: AlpacaWsAuthCredentialsPayload {
                key_id: key_id.into(),
                secret_key: secret_key.into(),
            },
        }
    }
}

/// Outbound subscription command frame for Alpaca `trade_updates` WebSocket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlpacaWsListenCommand {
    /// Action type (`"listen"`).
    pub action: String,
    /// Streams payload.
    pub data: AlpacaWsStreamsPayload,
}

/// Streams list inside [`AlpacaWsListenCommand`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlpacaWsStreamsPayload {
    /// Stream names (e.g. `["trade_updates"]`).
    pub streams: Vec<String>,
}

impl AlpacaWsListenCommand {
    /// Constructs a `"listen"` command subscribing to `["trade_updates"]`.
    #[must_use]
    pub fn trade_updates() -> Self {
        Self {
            action: "listen".to_string(),
            data: AlpacaWsStreamsPayload {
                streams: vec![TRADE_UPDATES_STREAM.to_string()],
            },
        }
    }
}

/// Strongly typed trade event kind on Alpaca's `trade_updates` WebSocket stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlpacaTradeEventKind {
    /// Order accepted by venue (`"new"`).
    New,
    /// Order completely filled (`"fill"`).
    Fill,
    /// Order partially filled (`"partial_fill"`).
    PartialFill,
    /// Order canceled (`"canceled"`).
    Canceled,
    /// Order expired (`"expired"`).
    Expired,
    /// Order rejected (`"rejected"`).
    Rejected,
    /// Order replaced/modified (`"replaced"`).
    Replaced,
    /// Order done for day (`"done_for_day"`).
    DoneForDay,
    /// Order pending new (`"pending_new"`).
    PendingNew,
    /// Order pending cancel (`"pending_cancel"`).
    PendingCancel,
    /// Order pending replace (`"pending_replace"`).
    PendingReplace,
    /// Order stopped (`"stopped"`).
    Stopped,
    /// Order suspended (`"suspended"`).
    Suspended,
    /// Order calculated (`"calculated"`).
    Calculated,
    /// Order cancel rejected (`"order_cancel_rejected"`).
    OrderCancelRejected,
    /// Order replace rejected (`"order_replace_rejected"`).
    OrderReplaceRejected,
}

/// Strongly typed `data` payload of a `stream = "trade_updates"` frame.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlpacaTradeUpdatePayload {
    /// Event kind (`new`, `fill`, `partial_fill`, `canceled`, `expired`, `rejected`, `replaced`, `done_for_day`).
    pub event: AlpacaTradeEventKind,
    /// Execution ID for fill events, if provided.
    #[serde(default)]
    pub execution_id: Option<String>,
    /// Fill price of the latest execution (`price`), if applicable.
    #[serde(default)]
    pub price: Option<Decimal>,
    /// Fill quantity of the latest execution (`qty`), if applicable.
    #[serde(default)]
    pub qty: Option<Decimal>,
    /// Resulting position quantity after this event (`position_qty`), if applicable.
    #[serde(default)]
    pub position_qty: Option<Decimal>,
    /// RFC3339 event timestamp (`timestamp`), if provided at the top level.
    #[serde(default)]
    pub timestamp: Option<String>,
    /// Nested Alpaca order snapshot (`order`).
    pub order: AlpacaOrderResponse,
}

/// Session-internal control signal parsed from non-business WebSocket frames.
///
/// Consumed exclusively inside the WebSocket session state machine and never emitted
/// to downstream execution consumers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlpacaWsControlSignal {
    /// Initial transport greeting (`connected`).
    Connected,
    /// Authentication response (`authorized` vs `unauthorized`).
    Authorization {
        /// True when `status == "authorized"`.
        authorized: bool,
        /// Optional error message when unauthorized.
        message: Option<String>,
    },
    /// Stream subscription acknowledgment (`listening`).
    Listening {
        /// Confirmed active streams.
        streams: Vec<String>,
    },
}

/// Internal classification of an inbound WebSocket text/binary frame.
#[derive(Debug, Clone, PartialEq)]
pub enum AlpacaWsFrameClassification {
    /// Session-level control signal (handled internally by [`super::client::AlpacaWsClient`]).
    Control(Vec<AlpacaWsControlSignal>),
    /// Pure business trade update payload.
    TradeUpdate(Box<AlpacaTradeUpdatePayload>),
}

#[derive(Debug, Deserialize)]
struct RawStreamEnvelope {
    stream: String,
    data: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct RawAuthorizationData {
    status: String,
    #[serde(default)]
    action: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawListeningData {
    #[serde(default)]
    streams: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RawV2ControlItem {
    #[serde(rename = "T")]
    msg_type: String,
    #[serde(default)]
    msg: Option<String>,
    #[serde(default)]
    code: Option<i64>,
}

/// Parses a raw WebSocket payload into either session-internal [`AlpacaWsControlSignal`]s
/// or a strongly typed [`AlpacaTradeUpdatePayload`].
///
/// # Errors
///
/// Returns [`AlpacaError::Parse`] if the JSON frame is malformed or unrecognized.
pub fn classify_alpaca_ws_frame(raw_bytes: &[u8]) -> AlpacaResult<AlpacaWsFrameClassification> {
    let trimmed = raw_bytes.trim_ascii();
    if trimmed.is_empty() {
        return Err(AlpacaError::Parse(
            "empty Alpaca WebSocket frame".to_string(),
        ));
    }

    if trimmed.first() == Some(&b'[') {
        let items: Vec<RawV2ControlItem> = serde_json::from_slice(trimmed).map_err(|e| {
            AlpacaError::Parse(format!("failed to parse Alpaca v2 control array frame: {e}"))
        })?;
        let mut signals = Vec::with_capacity(items.len());
        for item in items {
            match (item.msg_type.as_str(), item.msg.as_deref()) {
                ("success", Some("connected")) => signals.push(AlpacaWsControlSignal::Connected),
                ("success", Some("authenticated")) => {
                    signals.push(AlpacaWsControlSignal::Authorization {
                        authorized: true,
                        message: None,
                    });
                }
                ("error", msg) => {
                    let detail = match (item.code, msg) {
                        (Some(code), Some(m)) => format!("code={code}: {m}"),
                        (None, Some(m)) => m.to_string(),
                        (Some(code), None) => format!("code={code}"),
                        (None, None) => "unknown websocket error".to_string(),
                    };
                    signals.push(AlpacaWsControlSignal::Authorization {
                        authorized: false,
                        message: Some(detail),
                    });
                }
                ("subscription", _) => {
                    signals.push(AlpacaWsControlSignal::Listening {
                        streams: vec![TRADE_UPDATES_STREAM.to_string()],
                    });
                }
                (other, _) => {
                    return Err(AlpacaError::Parse(format!(
                        "unrecognized Alpaca v2 array frame type '{other}'"
                    )));
                }
            }
        }
        return Ok(AlpacaWsFrameClassification::Control(signals));
    }

    let envelope: RawStreamEnvelope = serde_json::from_slice(trimmed).map_err(|e| {
        AlpacaError::Parse(format!("failed to parse Alpaca WebSocket envelope: {e}"))
    })?;

    match envelope.stream.as_str() {
        "authorization" => {
            let auth_data: RawAuthorizationData =
                serde_json::from_value(envelope.data).map_err(|e| {
                    AlpacaError::Parse(format!("failed to parse authorization data: {e}"))
                })?;
            let authorized = auth_data.status.eq_ignore_ascii_case("authorized");
            let message = auth_data.message.or(auth_data.action);
            Ok(AlpacaWsFrameClassification::Control(vec![
                AlpacaWsControlSignal::Authorization {
                    authorized,
                    message,
                },
            ]))
        }
        "listening" => {
            let listen_data: RawListeningData =
                serde_json::from_value(envelope.data).map_err(|e| {
                    AlpacaError::Parse(format!("failed to parse listening data: {e}"))
                })?;
            Ok(AlpacaWsFrameClassification::Control(vec![
                AlpacaWsControlSignal::Listening {
                    streams: listen_data.streams,
                },
            ]))
        }
        TRADE_UPDATES_STREAM => {
            let payload: AlpacaTradeUpdatePayload =
                serde_json::from_value(envelope.data).map_err(|e| {
                    AlpacaError::Parse(format!("failed to parse trade_updates data: {e}"))
                })?;
            Ok(AlpacaWsFrameClassification::TradeUpdate(Box::new(payload)))
        }
        other => Err(AlpacaError::Parse(format!(
            "unrecognized Alpaca WebSocket stream '{other}'"
        ))),
    }
}
