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

//! Alpaca `trade_updates` WebSocket client (`AlpacaWsClient`) built on
//! `nautilus_network::websocket::WebSocketClient`, `AuthTracker`, and `SubscriptionState`.
//!
//! Manages authentication, `trade_updates` subscription, and automatic reconnection +
//! resubscription inside the session state machine, emitting only strongly typed
//! [`AlpacaExecutionUpdate`] domain events to downstream consumers.

use std::{
    fmt::Debug,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use nautilus_core::UnixNanos;
use nautilus_model::identifiers::AccountId;
use nautilus_network::{
    RECONNECTED,
    websocket::{
        AuthTracker, SubscriptionState, TransportBackend, WebSocketClient, WebSocketConfig,
        channel_message_handler,
    },
};
use parking_lot::RwLock;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_tungstenite::tungstenite::Message as TungsteniteMessage;

use super::{
    messages::{
        AlpacaWsAuthenticateCommand, AlpacaWsControlSignal, AlpacaWsFrameClassification,
        AlpacaWsListenCommand, TRADE_UPDATES_STREAM, classify_alpaca_ws_frame,
    },
    parse::{AlpacaExecutionUpdate, parse_trade_update_payload},
};
use crate::{
    config::AlpacaConfig,
    error::{AlpacaError, AlpacaResult},
};

/// Internal session state of [`AlpacaWsClient`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlpacaWsSessionState {
    /// Not connected.
    Disconnected,
    /// Transport connected; authentication command sent or pending.
    Authenticating,
    /// Authenticated; `listen` command sent for `trade_updates`.
    Subscribing,
    /// Actively listening on `trade_updates`.
    Listening,
    /// Transport lost; reconnecting and awaiting re-authentication.
    Reconnecting,
    /// Authentication failed or client permanently closed.
    Failed,
}

/// Physically isolated observer for Alpaca WebSocket session lifecycle transitions.
///
/// Keeps control/session transitions completely separate from the [`AlpacaExecutionUpdate`]
/// business stream.
pub trait AlpacaWsLifecycleObserver: Send + Sync + Debug {
    /// Invoked when the session completes authentication (`authorized`).
    fn on_ws_authenticated(&self);

    /// Invoked when `listening` is confirmed for `streams`.
    fn on_ws_listening(&self, streams: &[String]);

    /// Invoked when the transport reconnects (`RECONNECTED`) and initiates re-authentication.
    fn on_ws_reconnecting(&self, reconnect_count: u32);

    /// Invoked when authentication is rejected (`unauthorized`).
    fn on_ws_auth_failed(&self, reason: &str);
}

/// Alpaca `trade_updates` WebSocket client with automatic authentication and reconnect recovery.
pub struct AlpacaWsClient {
    config: AlpacaConfig,
    account_id: AccountId,
    price_precision: u8,
    size_precision: u8,
    ws_client: Option<Arc<WebSocketClient>>,
    auth_tracker: AuthTracker,
    subscriptions: SubscriptionState,
    session_state: Arc<RwLock<AlpacaWsSessionState>>,
    reconnect_count: Arc<AtomicU32>,
    observer: Option<Arc<dyn AlpacaWsLifecycleObserver>>,
    reader_task: Option<JoinHandle<()>>,
}

impl Debug for AlpacaWsClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(AlpacaWsClient))
            .field("environment", &self.config.environment)
            .field("ws_url", &self.config.ws_url)
            .field("account_id", &self.account_id)
            .field("session_state", &*self.session_state.read())
            .field(
                "reconnect_count",
                &self.reconnect_count.load(Ordering::Relaxed),
            )
            .finish()
    }
}

impl AlpacaWsClient {
    /// Creates a new disconnected [`AlpacaWsClient`] from a validated [`AlpacaConfig`].
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaError::InvalidConfig`] if `config` fails validation.
    pub fn new(
        config: AlpacaConfig,
        account_id: AccountId,
        price_precision: u8,
        size_precision: u8,
        observer: Option<Arc<dyn AlpacaWsLifecycleObserver>>,
    ) -> AlpacaResult<Self> {
        config.validate()?;
        Ok(Self {
            config,
            account_id,
            price_precision,
            size_precision,
            ws_client: None,
            auth_tracker: AuthTracker::new(),
            subscriptions: SubscriptionState::new(':'),
            session_state: Arc::new(RwLock::new(AlpacaWsSessionState::Disconnected)),
            reconnect_count: Arc::new(AtomicU32::new(0)),
            observer,
            reader_task: None,
        })
    }

    /// Returns the current [`AlpacaWsSessionState`].
    #[must_use]
    pub fn session_state(&self) -> AlpacaWsSessionState {
        *self.session_state.read()
    }

    /// Returns the number of automatic reconnections processed so far.
    #[must_use]
    pub fn reconnect_count(&self) -> u32 {
        self.reconnect_count.load(Ordering::Acquire)
    }

    /// Returns a reference to the [`AuthTracker`].
    #[must_use]
    pub const fn auth_tracker(&self) -> &AuthTracker {
        &self.auth_tracker
    }

    /// Returns a reference to the [`SubscriptionState`].
    #[must_use]
    pub const fn subscriptions(&self) -> &SubscriptionState {
        &self.subscriptions
    }

    /// Connects to the Alpaca WebSocket endpoint, performs the `authenticate` + `listen`
    /// (`trade_updates`) handshake, and returns an unbounded receiver of pure
    /// [`AlpacaExecutionUpdate`] domain events.
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaError`] if WebSocket dial, authentication, or subscription fails.
    pub async fn connect(
        &mut self,
    ) -> AlpacaResult<mpsc::UnboundedReceiver<AlpacaExecutionUpdate>> {
        let (msg_handler, mut raw_rx) = channel_message_handler();

        let ws_config = WebSocketConfig {
            url: self.config.ws_url.clone(),
            headers: Vec::new(),
            heartbeat_interval_secs: Some(30),
            heartbeat_payload: None,
            connect_timeout_ms: Some(self.config.timeout_secs.saturating_mul(1_000)),
            reconnect_delay_initial_ms: Some(self.config.retry_initial_ms.max(50)),
            reconnect_delay_max_ms: Some(self.config.retry_max_ms.max(200)),
            reconnect_backoff_factor: Some(1.5),
            reconnect_jitter_ms: Some(25),
            reconnect_max_attempts: None,
            idle_timeout_ms: None,
            heartbeat_timeout_secs: None,
            max_message_size_bytes: None,
            max_frame_size_bytes: None,
            writer_capacity: None,
            backend: TransportBackend::Tungstenite,
            proxy_url: None,
        };

        let client = WebSocketClient::builder()
            .config(ws_config)
            .message_handler(msg_handler)
            .connect()
            .await
            .map_err(|e| AlpacaError::WebSocket(format!("failed to connect Alpaca WS: {e}")))?;

        client.set_auth_tracker(self.auth_tracker.clone(), false);
        let ws_client = Arc::new(client);
        self.ws_client = Some(Arc::clone(&ws_client));

        let (update_tx, update_rx) = mpsc::unbounded_channel();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<AlpacaResult<()>>();

        self.subscriptions.mark_subscribe(TRADE_UPDATES_STREAM);

        let auth_cmd_json = serde_json::to_string(&AlpacaWsAuthenticateCommand::new(
            self.config.credential.api_key_id(),
            self.config.credential.api_secret_key(),
        ))
        .map_err(|e| AlpacaError::Parse(format!("failed to serialize auth command: {e}")))?;

        let listen_cmd_json = serde_json::to_string(&AlpacaWsListenCommand::trade_updates())
            .map_err(|e| AlpacaError::Parse(format!("failed to serialize listen command: {e}")))?;

        *self.session_state.write() = AlpacaWsSessionState::Authenticating;
        let _auth_rx = self.auth_tracker.begin();

        ws_client
            .send_text(auth_cmd_json.clone(), None)
            .await
            .map_err(|e| {
                AlpacaError::WebSocket(format!("failed to send initial authenticate frame: {e}"))
            })?;

        let ws_for_task = Arc::clone(&ws_client);
        let auth_tracker = self.auth_tracker.clone();
        let subscriptions = self.subscriptions.clone();
        let session_state = Arc::clone(&self.session_state);
        let reconnect_count = Arc::clone(&self.reconnect_count);
        let observer = self.observer.clone();
        let account_id = self.account_id;
        let venue = self.config.venue.clone();
        let price_precision = self.price_precision;
        let size_precision = self.size_precision;

        let handle = tokio::spawn(async move {
            let mut ready_tx_opt = Some(ready_tx);

            while let Some(msg) = raw_rx.recv().await {
                let raw_bytes: Vec<u8> = match msg {
                    TungsteniteMessage::Text(text) => {
                        if text.as_str() == RECONNECTED {
                            let attempt = reconnect_count.fetch_add(1, Ordering::AcqRel) + 1;
                            *session_state.write() = AlpacaWsSessionState::Reconnecting;
                            auth_tracker.invalidate();
                            let _ = auth_tracker.begin();
                            subscriptions.mark_failure(TRADE_UPDATES_STREAM);
                            subscriptions.mark_subscribe(TRADE_UPDATES_STREAM);
                            if let Some(ref obs) = observer {
                                obs.on_ws_reconnecting(attempt);
                            }
                            *session_state.write() = AlpacaWsSessionState::Authenticating;
                            let _ = ws_for_task.send_text(auth_cmd_json.clone(), None).await;
                            continue;
                        }
                        text.as_bytes().to_vec()
                    }
                    TungsteniteMessage::Binary(bin) => bin.to_vec(),
                    TungsteniteMessage::Ping(_)
                    | TungsteniteMessage::Pong(_)
                    | TungsteniteMessage::Close(_)
                    | TungsteniteMessage::Frame(_) => continue,
                };

                match classify_alpaca_ws_frame(&raw_bytes) {
                    Ok(AlpacaWsFrameClassification::Control(signals)) => {
                        for sig in signals {
                            match sig {
                                AlpacaWsControlSignal::Connected => {}
                                AlpacaWsControlSignal::Authorization {
                                    authorized,
                                    message,
                                } => {
                                    if authorized {
                                        auth_tracker.succeed();
                                        *session_state.write() = AlpacaWsSessionState::Subscribing;
                                        if let Some(ref obs) = observer {
                                            obs.on_ws_authenticated();
                                        }
                                        let _ = ws_for_task
                                            .send_text(listen_cmd_json.clone(), None)
                                            .await;
                                    } else {
                                        let reason = message
                                            .unwrap_or_else(|| "unauthorized".to_string());
                                        auth_tracker.fail(&reason);
                                        *session_state.write() = AlpacaWsSessionState::Failed;
                                        if let Some(ref obs) = observer {
                                            obs.on_ws_auth_failed(&reason);
                                        }
                                        if let Some(tx) = ready_tx_opt.take() {
                                            let _ = tx.send(Err(AlpacaError::AuthError {
                                                status: 401,
                                                message: reason,
                                            }));
                                        }
                                    }
                                }
                                AlpacaWsControlSignal::Listening { streams } => {
                                    if streams.iter().any(|s| s == TRADE_UPDATES_STREAM) {
                                        subscriptions.confirm_subscribe(TRADE_UPDATES_STREAM);
                                        *session_state.write() = AlpacaWsSessionState::Listening;
                                    }
                                    if let Some(ref obs) = observer {
                                        obs.on_ws_listening(&streams);
                                    }
                                    if let Some(tx) = ready_tx_opt.take() {
                                        let _ = tx.send(Ok(()));
                                    }
                                }
                            }
                        }
                    }
                    Ok(AlpacaWsFrameClassification::TradeUpdate(payload)) => {
                        let ts_init = current_unix_nanos();
                        match parse_trade_update_payload(
                            &payload,
                            account_id,
                            &venue,
                            price_precision,
                            size_precision,
                            ts_init,
                        ) {
                            Ok(update) => {
                                if update_tx.send(update).is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                log::error!("Failed to parse Alpaca trade_update frame: {e}");
                            }
                        }
                    }
                    Err(e) => {
                        log::warn!("Ignoring malformed Alpaca WebSocket frame: {e}");
                    }
                }
            }
        });

        self.reader_task = Some(handle);

        let handshake_timeout = Duration::from_secs(self.config.timeout_secs.max(1));
        match tokio::time::timeout(handshake_timeout, ready_rx).await {
            Ok(Ok(Ok(()))) => Ok(update_rx),
            Ok(Ok(Err(auth_err))) => {
                self.disconnect().await;
                Err(auth_err)
            }
            Ok(Err(_closed)) => {
                self.disconnect().await;
                Err(AlpacaError::WebSocket(
                    "Alpaca WebSocket closed before handshake completed".to_string(),
                ))
            }
            Err(_) => {
                self.disconnect().await;
                Err(AlpacaError::WebSocket(format!(
                    "Alpaca WebSocket handshake timed out after {}s",
                    handshake_timeout.as_secs()
                )))
            }
        }
    }

    /// Gracefully disconnects the WebSocket client and aborts the background session task.
    pub async fn disconnect(&mut self) {
        if let Some(ref client) = self.ws_client {
            client.disconnect().await;
        }
        if let Some(task) = self.reader_task.take() {
            task.abort();
        }
        *self.session_state.write() = AlpacaWsSessionState::Disconnected;
    }
}

fn current_unix_nanos() -> UnixNanos {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    UnixNanos::from(u64::try_from(now.as_nanos()).unwrap_or(u64::MAX))
}
