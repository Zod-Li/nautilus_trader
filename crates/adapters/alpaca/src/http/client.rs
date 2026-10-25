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

//! Asynchronous REST v2 client for Alpaca Markets built on [`nautilus_network::http::HttpClient`]
//! and [`nautilus_network::retry::RetryManager`].

use std::{fmt::Debug, num::NonZeroU32};

use http::Method;
use nautilus_network::{
    http::{HttpClient, HttpRedirectPolicy, HttpResponse, create_standard_nautilus_headers},
    ratelimiter::quota::Quota,
    retry::{RetryConfig, RetryManager},
};
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    config::AlpacaConfig,
    error::{AlpacaError, AlpacaResult},
    http::models::{
        AlpacaAccountResponse, AlpacaApiErrorPayload, AlpacaAssetResponse, AlpacaOrderRequest,
        AlpacaOrderResponse, AlpacaPositionResponse, AlpacaReplaceOrderRequest,
    },
};

const ALPACA_RATE_KEY: &str = "alpaca:rest";

/// Alpaca Markets v2 REST API client with rate limiting and retry classification.
pub struct AlpacaHttpClient {
    config: AlpacaConfig,
    client: HttpClient,
    retry_manager: RetryManager<AlpacaError>,
}

impl Debug for AlpacaHttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(AlpacaHttpClient))
            .field("environment", &self.config.environment)
            .field("http_base_url", &self.config.http_base_url)
            .field("venue", &self.config.venue)
            .field("api_key_id", &self.config.credential.masked_key_id())
            .finish()
    }
}

impl AlpacaHttpClient {
    /// Creates a new [`AlpacaHttpClient`] from a validated [`AlpacaConfig`].
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaError::InvalidConfig`] if `config` validation or underlying [`HttpClient`] construction fails.
    pub fn new(config: AlpacaConfig) -> AlpacaResult<Self> {
        config.validate()?;

        let mut headers: std::collections::HashMap<String, String> =
            create_standard_nautilus_headers().into_iter().collect();
        for (k, v) in config.credential.to_http_headers() {
            headers.insert(k, v);
        }
        headers.insert("Content-Type".to_string(), "application/json".to_string());
        headers.insert("Accept".to_string(), "application/json".to_string());

        let rps = NonZeroU32::new(config.rate_limit_per_sec).ok_or_else(|| {
            AlpacaError::InvalidConfig("rate_limit_per_sec must be non-zero".to_string())
        })?;
        let quota = Quota::per_second(rps).ok_or_else(|| {
            AlpacaError::InvalidConfig("failed to construct rate limit quota".to_string())
        })?;

        let client = HttpClient::builder()
            .headers(headers)
            .default_quota(quota)
            .timeout_secs(config.timeout_secs)
            .redirect_policy(HttpRedirectPolicy::Reject)
            .build()
            .map_err(|e| AlpacaError::InvalidConfig(format!("failed to build HttpClient: {e}")))?;

        let retry_config = RetryConfig {
            max_retries: config.max_retries,
            initial_delay_ms: config.retry_initial_ms,
            max_delay_ms: config.retry_max_ms,
            backoff_factor: 2.0,
            jitter_ms: 50,
            operation_timeout_ms: Some(
                config.timeout_secs * 1000 * u64::from(config.max_retries + 1),
            ),
            immediate_first: false,
            max_elapsed_ms: None,
        };
        let retry_manager = RetryManager::new(retry_config);

        Ok(Self {
            config,
            client,
            retry_manager,
        })
    }

    /// Returns a reference to the client's [`AlpacaConfig`].
    #[must_use]
    pub const fn config(&self) -> &AlpacaConfig {
        &self.config
    }

    /// Queries `GET /v2/account`.
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaError`] if transport, authentication, or deserialization fails.
    pub async fn get_account(&self) -> AlpacaResult<AlpacaAccountResponse> {
        self.request_json::<(), AlpacaAccountResponse>(Method::GET, "/v2/account", None, None)
            .await
    }

    /// Queries `GET /v2/positions` for all open positions.
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaError`] if transport, authentication, or deserialization fails.
    pub async fn list_positions(&self) -> AlpacaResult<Vec<AlpacaPositionResponse>> {
        self.request_json::<(), Vec<AlpacaPositionResponse>>(
            Method::GET,
            "/v2/positions",
            None,
            None,
        )
        .await
    }

    /// Queries `GET /v2/orders?status={status}` (e.g. `"open"`, `"closed"`, `"all"`).
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaError`] if transport, authentication, or deserialization fails.
    pub async fn list_orders(&self, status: Option<&str>) -> AlpacaResult<Vec<AlpacaOrderResponse>> {
        let params = status.map(|s| vec![("status".to_string(), s.to_string())]);
        self.request_json::<(), Vec<AlpacaOrderResponse>>(Method::GET, "/v2/orders", params, None)
            .await
    }

    /// Queries `GET /v2/orders:by_client_order_id?client_order_id={client_order_id}`.
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaError`] if the order is not found (`404`) or request fails.
    pub async fn get_order_by_client_order_id(
        &self,
        client_order_id: &str,
    ) -> AlpacaResult<AlpacaOrderResponse> {
        let params = Some(vec![(
            "client_order_id".to_string(),
            client_order_id.to_string(),
        )]);
        self.request_json::<(), AlpacaOrderResponse>(
            Method::GET,
            "/v2/orders:by_client_order_id",
            params,
            None,
        )
        .await
    }

    /// Submits a new order via `POST /v2/orders` (supports Simple, Limit, Bracket, and OCO orders).
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaError::DeterministicReject`] on 403/422 counter rejections (non-retryable)
    /// or [`AlpacaError`] on transport/rate-limit failures.
    pub async fn submit_order(
        &self,
        request: &AlpacaOrderRequest,
    ) -> AlpacaResult<AlpacaOrderResponse> {
        self.request_json::<AlpacaOrderRequest, AlpacaOrderResponse>(
            Method::POST,
            "/v2/orders",
            None,
            Some(request),
        )
        .await
    }

    /// Modifies/replaces an existing order via `PATCH /v2/orders/{venue_order_id}`.
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaError`] if the broker rejects the replacement (`422`) or transport fails.
    pub async fn replace_order(
        &self,
        venue_order_id: &str,
        request: &AlpacaReplaceOrderRequest,
    ) -> AlpacaResult<AlpacaOrderResponse> {
        let path = format!("/v2/orders/{}", venue_order_id.trim());
        self.request_json::<AlpacaReplaceOrderRequest, AlpacaOrderResponse>(
            Method::PATCH,
            &path,
            None,
            Some(request),
        )
        .await
    }

    /// Cancels an open order via `DELETE /v2/orders/{venue_order_id}`.
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaError`] if the order cannot be canceled (`422`/`404`) or transport fails.
    pub async fn cancel_order(&self, venue_order_id: &str) -> AlpacaResult<()> {
        let path = format!("/v2/orders/{}", venue_order_id.trim());
        self.request_empty(Method::DELETE, &path).await
    }

    /// Queries `GET /v2/assets?status=active&asset_class=us_equity`.
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaError`] if transport or deserialization fails.
    pub async fn list_assets(&self) -> AlpacaResult<Vec<AlpacaAssetResponse>> {
        let params = Some(vec![
            ("status".to_string(), "active".to_string()),
            ("asset_class".to_string(), "us_equity".to_string()),
        ]);
        self.request_json::<(), Vec<AlpacaAssetResponse>>(Method::GET, "/v2/assets", params, None)
            .await
    }

    async fn request_json<Req, Resp>(
        &self,
        method: Method,
        path: &str,
        params: Option<Vec<(String, String)>>,
        body_payload: Option<&Req>,
    ) -> AlpacaResult<Resp>
    where
        Req: Serialize + Sync,
        Resp: DeserializeOwned,
    {
        let url = format!("{}{path}", self.config.http_base_url);
        let serialized_body = match body_payload {
            Some(payload) => Some(
                serde_json::to_vec(payload)
                    .map_err(|e| AlpacaError::Parse(format!("failed to serialize request: {e}")))?,
            ),
            None => None,
        };

        let response = self
            .retry_manager
            .invocation(
                path,
                || {
                    let url_clone = url.clone();
                    let method_clone = method.clone();
                    let params_clone = params.clone();
                    let body_clone = serialized_body.clone();
                    async move {
                        let resp = self
                            .client
                            .request_with_params(
                                method_clone,
                                url_clone,
                                params_clone.as_ref(),
                                None,
                                body_clone,
                                Some(self.config.timeout_secs),
                                Some(vec![ALPACA_RATE_KEY.to_string()]),
                            )
                            .await
                            .map_err(|e| AlpacaError::Transport(e.to_string()))?;
                        classify_response(resp)
                    }
                },
                AlpacaError::is_retryable,
                |retry_err| AlpacaError::Transport(retry_err.to_string()),
            )
            .execute()
            .await?;

        serde_json::from_slice::<Resp>(&response.body).map_err(|e| {
            let preview = String::from_utf8_lossy(&response.body);
            AlpacaError::Parse(format!(
                "failed to deserialize response from {path}: {e} (body='{preview}')"
            ))
        })
    }

    async fn request_empty(&self, method: Method, path: &str) -> AlpacaResult<()> {
        let url = format!("{}{path}", self.config.http_base_url);
        let _response = self
            .retry_manager
            .invocation(
                path,
                || {
                    let url_clone = url.clone();
                    let method_clone = method.clone();
                    async move {
                        let resp = self
                            .client
                            .request(
                                method_clone,
                                url_clone,
                                None,
                                None,
                                None,
                                Some(self.config.timeout_secs),
                                Some(vec![ALPACA_RATE_KEY.to_string()]),
                            )
                            .await
                            .map_err(|e| AlpacaError::Transport(e.to_string()))?;
                        classify_response(resp)
                    }
                },
                AlpacaError::is_retryable,
                |retry_err| AlpacaError::Transport(retry_err.to_string()),
            )
            .execute()
            .await?;
        Ok(())
    }
}

fn classify_response(resp: HttpResponse) -> AlpacaResult<HttpResponse> {
    let code = resp.status.as_u16();
    if (200..300).contains(&code) {
        return Ok(resp);
    }

    let parsed_err = serde_json::from_slice::<AlpacaApiErrorPayload>(&resp.body).ok();
    let err_code = parsed_err.as_ref().and_then(|p| p.code);
    let message = parsed_err
        .map(|p| p.message)
        .filter(|m| !m.trim().is_empty())
        .unwrap_or_else(|| String::from_utf8_lossy(&resp.body).into_owned());

    match code {
        401 => Err(AlpacaError::AuthError {
            status: code,
            message,
        }),
        403 => {
            if err_code.is_some() {
                Err(AlpacaError::DeterministicReject {
                    status: code,
                    code: err_code,
                    message,
                })
            } else {
                Err(AlpacaError::AuthError {
                    status: code,
                    message,
                })
            }
        }
        400 | 404 | 409 | 422 => Err(AlpacaError::DeterministicReject {
            status: code,
            code: err_code,
            message,
        }),
        429 => Err(AlpacaError::RateLimited { message }),
        500..=599 => Err(AlpacaError::RetryableServer {
            status: code,
            message,
        }),
        _ => Err(AlpacaError::DeterministicReject {
            status: code,
            code: err_code,
            message,
        }),
    }
}
