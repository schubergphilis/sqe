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

//! Azure Blob Storage support.

use std::collections::HashMap;

use iceberg::io::{AZBLOB_ACCOUNT_KEY, AZBLOB_ACCOUNT_NAME, AZBLOB_ENDPOINT};
use iceberg::{Error, ErrorKind, Result};
use opendal::Operator;
use opendal::services::AzblobConfig;
use url::Url;

use crate::utils::from_opendal_error;

pub(crate) fn azblob_config_parse(mut properties: HashMap<String, String>) -> AzblobConfig {
    AzblobConfig {
        account_name: properties.remove(AZBLOB_ACCOUNT_NAME),
        account_key: properties.remove(AZBLOB_ACCOUNT_KEY),
        endpoint: properties.remove(AZBLOB_ENDPOINT),
        ..Default::default()
    }
}

pub(crate) fn azblob_config_build(config: &AzblobConfig, path: &str) -> Result<Operator> {
    let url = Url::parse(path)?;
    let container = url.host_str().ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid azblob url: {path}, container is required"),
        )
    })?;

    let mut config = config.clone();
    config.container = container.to_string();
    Operator::from_config(config).map_err(from_opendal_error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_azblob_config_parse_and_build() {
        let config = azblob_config_parse(HashMap::from([
            (AZBLOB_ACCOUNT_NAME.to_string(), "account".to_string()),
            (AZBLOB_ACCOUNT_KEY.to_string(), "a2V5".to_string()),
            (
                AZBLOB_ENDPOINT.to_string(),
                "https://account.blob.core.windows.net".to_string(),
            ),
        ]));
        assert_eq!(config.account_name.as_deref(), Some("account"));
        assert_eq!(config.account_key.as_deref(), Some("a2V5"));

        let operator = azblob_config_build(&config, "azblob://container/path").unwrap();
        assert_eq!(operator.info().name(), "container");
    }
}
