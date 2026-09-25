// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Azure Blob Storage configuration.

use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use super::StorageConfig;
use crate::Result;

/// Azure Blob Storage account name.
pub const AZBLOB_ACCOUNT_NAME: &str = "azblob.account-name";
/// Azure Blob Storage account key.
pub const AZBLOB_ACCOUNT_KEY: &str = "azblob.account-key";
/// Azure Blob Storage endpoint.
pub const AZBLOB_ENDPOINT: &str = "azblob.endpoint";

/// Azure Blob Storage configuration.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, TypedBuilder)]
pub struct AzblobConfig {
    /// Account name.
    #[builder(default, setter(strip_option, into))]
    pub account_name: Option<String>,
    /// Account key.
    #[builder(default, setter(strip_option, into))]
    pub account_key: Option<String>,
    /// Endpoint URL.
    #[builder(default, setter(strip_option, into))]
    pub endpoint: Option<String>,
}

impl TryFrom<&StorageConfig> for AzblobConfig {
    type Error = crate::Error;

    fn try_from(config: &StorageConfig) -> Result<Self> {
        let props = config.props();
        Ok(Self {
            account_name: props.get(AZBLOB_ACCOUNT_NAME).cloned(),
            account_key: props.get(AZBLOB_ACCOUNT_KEY).cloned(),
            endpoint: props.get(AZBLOB_ENDPOINT).cloned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_azblob_config_from_storage_config() {
        let storage_config = StorageConfig::new()
            .with_prop(AZBLOB_ACCOUNT_NAME, "account")
            .with_prop(AZBLOB_ACCOUNT_KEY, "key")
            .with_prop(AZBLOB_ENDPOINT, "https://account.blob.core.windows.net");

        let config = AzblobConfig::try_from(&storage_config).unwrap();
        assert_eq!(config.account_name.as_deref(), Some("account"));
        assert_eq!(config.account_key.as_deref(), Some("key"));
        assert_eq!(
            config.endpoint.as_deref(),
            Some("https://account.blob.core.windows.net")
        );
    }
}
