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

//! Credential wrapper for Alpaca API keys using zeroized [`SecretString`].

use std::{collections::HashMap, fmt::Debug};

use nautilus_core::string::secret::{REDACTED, SecretString, mask_api_key};

use crate::{
    common::consts::{HEADER_APCA_API_KEY_ID, HEADER_APCA_API_SECRET_KEY},
    error::{AlpacaError, AlpacaResult},
};

/// Holds the Alpaca `APCA-API-KEY-ID` and `APCA-API-SECRET-KEY` in zeroized memory.
#[derive(Clone)]
pub struct AlpacaCredential {
    api_key_id: SecretString,
    api_secret_key: SecretString,
}

impl Debug for AlpacaCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(AlpacaCredential))
            .field("api_key_id", &self.masked_key_id())
            .field("api_secret_key", &REDACTED)
            .finish()
    }
}

impl AlpacaCredential {
    /// Creates a new validated [`AlpacaCredential`] (fail-closed on empty keys).
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaError::InvalidConfig`] if either key is empty or contains whitespace.
    pub fn new_checked(
        api_key_id: impl Into<String>,
        api_secret_key: impl Into<String>,
    ) -> AlpacaResult<Self> {
        let key_id = api_key_id.into();
        let secret = api_secret_key.into();

        if key_id.trim().is_empty() {
            return Err(AlpacaError::InvalidConfig(
                "APCA-API-KEY-ID must not be empty".to_string(),
            ));
        }
        if secret.trim().is_empty() {
            return Err(AlpacaError::InvalidConfig(
                "APCA-API-SECRET-KEY must not be empty".to_string(),
            ));
        }
        if key_id.contains(char::is_whitespace) || secret.contains(char::is_whitespace) {
            return Err(AlpacaError::InvalidConfig(
                "Alpaca API credentials must not contain whitespace".to_string(),
            ));
        }

        Ok(Self {
            api_key_id: SecretString::from(key_id),
            api_secret_key: SecretString::from(secret),
        })
    }

    /// Resolves credentials from `APCA_API_KEY_ID` / `APCA_API_SECRET_KEY` (or `ALPACA_API_KEY` / `ALPACA_API_SECRET`) environment variables.
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaError::InvalidConfig`] if environment variables are missing or invalid.
    pub fn from_env() -> AlpacaResult<Self> {
        let key_id = std::env::var("APCA_API_KEY_ID")
            .or_else(|_| std::env::var("ALPACA_API_KEY"))
            .map_err(|_| {
                AlpacaError::InvalidConfig(
                    "missing APCA_API_KEY_ID or ALPACA_API_KEY in environment".to_string(),
                )
            })?;
        let secret = std::env::var("APCA_API_SECRET_KEY")
            .or_else(|_| std::env::var("ALPACA_API_SECRET"))
            .map_err(|_| {
                AlpacaError::InvalidConfig(
                    "missing APCA_API_SECRET_KEY or ALPACA_API_SECRET in environment".to_string(),
                )
            })?;
        Self::new_checked(key_id, secret)
    }

    /// Returns the raw `APCA-API-KEY-ID` secret reference for signing/headers.
    #[must_use]
    pub fn api_key_id(&self) -> &str {
        self.api_key_id.expose_secret()
    }

    /// Returns the raw `APCA-API-SECRET-KEY` secret reference for signing/headers.
    #[must_use]
    pub fn api_secret_key(&self) -> &str {
        self.api_secret_key.expose_secret()
    }

    /// Returns a safely masked representation of the API Key ID.
    #[must_use]
    pub fn masked_key_id(&self) -> String {
        mask_api_key(self.api_key_id.expose_secret())
    }

    /// Builds the HTTP authentication header map for Alpaca REST v2 requests.
    #[must_use]
    pub fn to_http_headers(&self) -> HashMap<String, String> {
        let mut headers = HashMap::with_capacity(2);
        headers.insert(
            HEADER_APCA_API_KEY_ID.to_string(),
            self.api_key_id.expose_secret().to_string(),
        );
        headers.insert(
            HEADER_APCA_API_SECRET_KEY.to_string(),
            self.api_secret_key.expose_secret().to_string(),
        );
        headers
    }
}
