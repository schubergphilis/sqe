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

//! OpenDAL-based storage implementation for Apache Iceberg.
//!
//! This crate provides [`OpenDalStorage`] and [`OpenDalStorageFactory`],
//! which implement the [`Storage`] and
//! [`StorageFactory`] traits from the `iceberg` crate
//! using [OpenDAL](https://opendal.apache.org/) as the backend.

mod utils;

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use cfg_if::cfg_if;
use futures::StreamExt;
use futures::stream::BoxStream;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, IO_CHUNK_SIZE, IO_MAX_RETRIES, IO_RETRY_MAX_DELAY_MS,
    IO_RETRY_MIN_DELAY_MS, IO_TIMEOUT_SECONDS, InputFile, ListEntry, OutputFile, Storage,
    StorageConfig, StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use once_cell::sync::OnceCell;
use opendal::Operator;
use opendal::layers::RetryLayer;
#[cfg(not(madsim))]
use opendal::layers::TimeoutLayer;
#[cfg(feature = "prometheus")]
use opendal_layer_prometheus::PrometheusLayer;
use serde::{Deserialize, Serialize};
use utils::from_opendal_error;

cfg_if! {
    if #[cfg(feature = "opendal-azdls")] {
        mod azdls;
        use azdls::*;
        use opendal::services::AzdlsConfig;
    }
}

cfg_if! {
    if #[cfg(feature = "opendal-azblob")] {
        mod azblob;
        use azblob::*;
        use opendal::services::AzblobConfig;
    }
}

cfg_if! {
    if #[cfg(feature = "opendal-hf")] {
        mod hf;
        use hf::*;
        use opendal::services::HfConfig;
    }
}

cfg_if! {
    if #[cfg(feature = "opendal-fs")] {
        mod fs;
        use fs::*;
    }
}

cfg_if! {
    if #[cfg(feature = "opendal-gcs")] {
        mod gcs;
        use gcs::*;
        use opendal::services::GcsConfig;
    }
}

cfg_if! {
    if #[cfg(feature = "opendal-memory")] {
        mod memory;
        use memory::*;
    }
}

cfg_if! {
    if #[cfg(feature = "opendal-oss")] {
        mod oss;
        use opendal::services::OssConfig;
        use oss::*;
    }
}

cfg_if! {
    if #[cfg(feature = "opendal-s3")] {
        mod s3;
        use opendal::services::S3Config;
        pub use s3::*;
    }
}

mod resolving;
pub use resolving::{OpenDalResolvingStorage, OpenDalResolvingStorageFactory};

#[cfg(feature = "prometheus")]
static PROMETHEUS_LAYER: OnceCell<PrometheusLayer> = OnceCell::new();

/// Installs OpenDAL metrics into the supplied Prometheus registry.
///
/// Call this once, before creating any [`OpenDalStorage`] operators. The first
/// successful call selects the registry used by all subsequently created
/// operators in this process.
#[cfg(feature = "prometheus")]
pub fn install_prometheus_metrics(registry: &prometheus::Registry) -> Result<()> {
    PROMETHEUS_LAYER
        .get_or_try_init(|| {
            PrometheusLayer::builder()
                .register(registry)
                .map_err(from_opendal_error)
        })
        .map(|_| ())
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct OpenDalStorageOptions {
    write_chunk_size: Option<usize>,
    timeout_seconds: Option<u64>,
    max_retries: Option<usize>,
    retry_min_delay_ms: Option<u64>,
    retry_max_delay_ms: Option<u64>,
}

impl TryFrom<&StorageConfig> for OpenDalStorageOptions {
    type Error = Error;

    fn try_from(config: &StorageConfig) -> Result<Self> {
        let options = Self {
            write_chunk_size: parse_config(config, IO_CHUNK_SIZE)?,
            timeout_seconds: parse_config(config, IO_TIMEOUT_SECONDS)?,
            max_retries: parse_config(config, IO_MAX_RETRIES)?,
            retry_min_delay_ms: parse_config(config, IO_RETRY_MIN_DELAY_MS)?,
            retry_max_delay_ms: parse_config(config, IO_RETRY_MAX_DELAY_MS)?,
        };
        if options.write_chunk_size == Some(0) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("{IO_CHUNK_SIZE} must be greater than zero"),
            ));
        }
        if options
            .retry_min_delay_ms
            .zip(options.retry_max_delay_ms)
            .is_some_and(|(min, max)| min > max)
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("{IO_RETRY_MIN_DELAY_MS} must not exceed {IO_RETRY_MAX_DELAY_MS}"),
            ));
        }
        Ok(options)
    }
}

fn parse_config<T>(config: &StorageConfig, key: &str) -> Result<Option<T>>
where T: std::str::FromStr {
    config
        .get(key)
        .map(|value| {
            value.parse::<T>().map_err(|_| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Invalid {key}: '{value}' must be a non-negative integer"),
                )
            })
        })
        .transpose()
}

type SharedOperatorCache = Arc<OperatorCache>;

/// The subset of [`OpenDalStorageOptions`] that changes a configured operator.
///
/// `write_chunk_size` is intentionally excluded because it is applied to each
/// writer after the operator is selected.
#[derive(Clone, Copy, PartialEq, Eq)]
struct OperatorLayerOptions {
    timeout_seconds: Option<u64>,
    max_retries: Option<usize>,
    retry_min_delay_ms: Option<u64>,
    retry_max_delay_ms: Option<u64>,
}

impl From<&OpenDalStorageOptions> for OperatorLayerOptions {
    fn from(options: &OpenDalStorageOptions) -> Self {
        Self {
            timeout_seconds: options.timeout_seconds,
            max_retries: options.max_retries,
            retry_min_delay_ms: options.retry_min_delay_ms,
            retry_max_delay_ms: options.retry_max_delay_ms,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
enum OperatorBackendConfig {
    #[cfg(feature = "opendal-s3")]
    S3(Arc<S3Config>),
    #[cfg(feature = "opendal-gcs")]
    Gcs(Arc<GcsConfig>),
    #[cfg(feature = "opendal-azdls")]
    Azdls(Arc<AzdlsConfig>),
}

/// Cache entries are scoped by everything that affects the resulting operator.
/// Keeping the parsed backend config in the key also normalizes aliases and
/// defaults before equality is evaluated.
#[derive(Clone, PartialEq, Eq)]
struct OperatorCacheKey {
    backend_config: OperatorBackendConfig,
    /// The bucket name for S3 and GCS. For ADLS this is
    /// `<scheme>://<filesystem>@<account host>`, because the operator's
    /// endpoint is derived from the path scheme and host unless configured.
    bucket: String,
    layer_options: OperatorLayerOptions,
}

struct OperatorCacheEntry {
    key: OperatorCacheKey,
    operator: Arc<OnceCell<Operator>>,
}

#[derive(Default)]
struct OperatorCache {
    // OpenDAL config types intentionally do not implement `Hash`. The number of
    // distinct bucket/config pairs owned by one catalog factory is expected to
    // be small, so a vector keeps equality exact without hashing credentials.
    entries: RwLock<Vec<OperatorCacheEntry>>,
}

impl std::fmt::Debug for OperatorCache {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let entry_count = self.entries.read().map(|entries| entries.len()).ok();
        formatter
            .debug_struct("OperatorCache")
            .field("entry_count", &entry_count)
            .finish()
    }
}

impl OperatorCache {
    fn get_or_create(
        &self,
        key: OperatorCacheKey,
        build: impl FnOnce() -> Result<Operator>,
    ) -> Result<Operator> {
        let operator = {
            let entries = self.entries.read().map_err(|error| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Operator cache lock poisoned: {error}"),
                )
            })?;
            entries
                .iter()
                .find(|entry| entry.key == key)
                .map(|entry| entry.operator.clone())
        };

        let operator = match operator {
            Some(operator) => operator,
            None => {
                let mut entries = self.entries.write().map_err(|error| {
                    Error::new(
                        ErrorKind::Unexpected,
                        format!("Operator cache lock poisoned: {error}"),
                    )
                })?;
                if let Some(entry) = entries.iter().find(|entry| entry.key == key) {
                    entry.operator.clone()
                } else {
                    let operator = Arc::new(OnceCell::new());
                    entries.push(OperatorCacheEntry {
                        key,
                        operator: operator.clone(),
                    });
                    operator
                }
            }
        };

        // `get_or_try_init` coalesces concurrent construction for this key,
        // while a failed build leaves the cell empty so a later call can retry.
        operator.get_or_try_init(build).cloned()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.read().unwrap().len()
    }
}

/// Installs opendal's process-wide reqwest HTTP transport before the first
/// operator is built.
///
/// SQE PATCH (ring-only TLS): the workspace enables opendal's
/// `http-transport-reqwest-rustls-no-provider` feature so the aws-lc rustls
/// provider stays out of the build. `opendal::install_default` only installs a
/// transport for the provider-carrying `http-transport-reqwest` feature, so
/// without this every S3 / GCS / Azure call fails with "default HTTP transport
/// is not installed". The TLS provider itself is the process-wide ring one the
/// embedding binary installs. First-installed wins, so a transport the host
/// installed earlier is kept.
fn install_http_transport() {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(opendal_http_transport_reqwest::install_default);
}

fn default_operator_cache() -> SharedOperatorCache {
    Arc::new(OperatorCache::default())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ConfiguredOpenDalStorage {
    storage: OpenDalStorage,
    options: OpenDalStorageOptions,
    /// Shared factory-owned cache for fully configured S3, GCS and ADLS operators.
    #[serde(skip, default = "default_operator_cache")]
    operator_cache: SharedOperatorCache,
}

impl ConfiguredOpenDalStorage {
    fn new(
        storage: OpenDalStorage,
        config: &StorageConfig,
        operator_cache: SharedOperatorCache,
    ) -> Result<Self> {
        Ok(Self {
            storage,
            options: OpenDalStorageOptions::try_from(config)?,
            operator_cache,
        })
    }
}

/// OpenDAL-based storage factory.
///
/// Maps a backend to the corresponding [`OpenDalStorage`] variant. All
/// [`FileIO`](iceberg::io::FileIO) instances built from this factory share its
/// S3/GCS operator cache for the lifetime of the factory. Cloning a factory
/// preserves that cache; calling a backend constructor creates an independent
/// factory and cache.
///
/// # Serialization
///
/// The receiving binary must enable the feature corresponding to the serialized backend.
/// For example, deserializing an S3 factory requires the `opendal-s3` feature. The operator
/// cache is process-local and is not serialized; a deserialized factory starts empty.
///
/// Serialization fails when an S3 factory holds a custom AWS credential loader because the
/// loader holds process-local state that cannot be reconstructed in another process.
/// Construct the factory without a custom loader before serializing it.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OpenDalStorageFactory {
    backend: OpenDalStorageBackend,
    #[serde(skip, default = "default_operator_cache")]
    operator_cache: SharedOperatorCache,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum OpenDalStorageBackend {
    /// Memory storage factory.
    #[cfg(feature = "opendal-memory")]
    Memory,
    /// Local filesystem storage factory.
    #[cfg(feature = "opendal-fs")]
    Fs,
    /// S3 storage factory.
    #[cfg(feature = "opendal-s3")]
    S3 {
        /// Custom AWS credential loader.
        #[serde(
            skip_deserializing,
            skip_serializing_if = "Option::is_none",
            serialize_with = "serialize_custom_credential_loader"
        )]
        customized_credential_load: Option<CustomAwsCredentialLoader>,
    },
    /// GCS storage factory.
    #[cfg(feature = "opendal-gcs")]
    Gcs,
    /// OSS storage factory.
    #[cfg(feature = "opendal-oss")]
    Oss,
    /// Azure Data Lake Storage factory.
    #[cfg(feature = "opendal-azdls")]
    Azdls,
    /// Azure Blob Storage factory.
    #[cfg(feature = "opendal-azblob")]
    Azblob,
    /// HuggingFace Hub storage factory.
    #[cfg(feature = "opendal-hf")]
    Hf,
}

#[cfg(feature = "opendal-s3")]
pub(crate) fn serialize_custom_credential_loader<S>(
    _loader: &Option<CustomAwsCredentialLoader>,
    _serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    Err(serde::ser::Error::custom(
        "custom AWS credential loaders cannot be serialized",
    ))
}

impl OpenDalStorageFactory {
    fn new(backend: OpenDalStorageBackend) -> Self {
        Self {
            backend,
            operator_cache: default_operator_cache(),
        }
    }

    /// Create a memory storage factory.
    #[cfg(feature = "opendal-memory")]
    pub fn memory() -> Self {
        Self::new(OpenDalStorageBackend::Memory)
    }

    /// Create a local filesystem storage factory.
    #[cfg(feature = "opendal-fs")]
    pub fn fs() -> Self {
        Self::new(OpenDalStorageBackend::Fs)
    }

    /// Create an S3 storage factory using the default credential chain.
    #[cfg(feature = "opendal-s3")]
    pub fn s3() -> Self {
        Self::new(OpenDalStorageBackend::S3 {
            customized_credential_load: None,
        })
    }

    /// Create an S3 storage factory using a custom credential loader.
    #[cfg(feature = "opendal-s3")]
    pub fn s3_with_credential_loader(loader: CustomAwsCredentialLoader) -> Self {
        Self::new(OpenDalStorageBackend::S3 {
            customized_credential_load: Some(loader),
        })
    }

    /// Create a GCS storage factory.
    #[cfg(feature = "opendal-gcs")]
    pub fn gcs() -> Self {
        Self::new(OpenDalStorageBackend::Gcs)
    }

    /// Create an OSS storage factory.
    #[cfg(feature = "opendal-oss")]
    pub fn oss() -> Self {
        Self::new(OpenDalStorageBackend::Oss)
    }

    /// Create an Azure Data Lake Storage factory.
    #[cfg(feature = "opendal-azdls")]
    pub fn azdls() -> Self {
        Self::new(OpenDalStorageBackend::Azdls)
    }

    /// Create an Azure Blob Storage factory.
    #[cfg(feature = "opendal-azblob")]
    pub fn azblob() -> Self {
        Self::new(OpenDalStorageBackend::Azblob)
    }

    /// Create a HuggingFace Hub storage factory.
    #[cfg(feature = "opendal-hf")]
    pub fn hf() -> Self {
        Self::new(OpenDalStorageBackend::Hf)
    }

    #[allow(unused_variables)]
    fn build_configured(&self, config: &StorageConfig) -> Result<ConfiguredOpenDalStorage> {
        let storage = match &self.backend {
            #[cfg(feature = "opendal-memory")]
            OpenDalStorageBackend::Memory => OpenDalStorage::Memory(memory_config_build()?),
            #[cfg(feature = "opendal-fs")]
            OpenDalStorageBackend::Fs => OpenDalStorage::LocalFs,
            #[cfg(feature = "opendal-s3")]
            OpenDalStorageBackend::S3 {
                customized_credential_load,
            } => OpenDalStorage::S3 {
                config: s3_config_parse(config.props().clone())?.into(),
                customized_credential_load: customized_credential_load.clone(),
            },
            #[cfg(feature = "opendal-gcs")]
            OpenDalStorageBackend::Gcs => OpenDalStorage::Gcs {
                config: gcs_config_parse(config.props().clone())?.into(),
            },
            #[cfg(feature = "opendal-oss")]
            OpenDalStorageBackend::Oss => OpenDalStorage::Oss {
                config: oss_config_parse(config.props().clone())?.into(),
            },
            #[cfg(feature = "opendal-azdls")]
            OpenDalStorageBackend::Azdls => OpenDalStorage::Azdls {
                config: azdls_config_parse(config.props().clone())?.into(),
            },
            #[cfg(feature = "opendal-azblob")]
            OpenDalStorageBackend::Azblob => OpenDalStorage::Azblob {
                config: azblob_config_parse(config.props().clone()).into(),
            },
            #[cfg(feature = "opendal-hf")]
            OpenDalStorageBackend::Hf => OpenDalStorage::Hf {
                config: hf_config_parse(config.props().clone())?.into(),
            },
            #[cfg(all(
                not(feature = "opendal-memory"),
                not(feature = "opendal-fs"),
                not(feature = "opendal-s3"),
                not(feature = "opendal-gcs"),
                not(feature = "opendal-oss"),
                not(feature = "opendal-azdls"),
                not(feature = "opendal-azblob"),
                not(feature = "opendal-hf"),
            ))]
            _ => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    "No storage service has been enabled",
                ));
            }
        };
        ConfiguredOpenDalStorage::new(storage, config, self.operator_cache.clone())
    }
}

#[typetag::serde(name = "OpenDalStorageFactory")]
impl StorageFactory for OpenDalStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(self.build_configured(config)?))
    }
}

/// Default memory operator for serde deserialization.
#[cfg(feature = "opendal-memory")]
fn default_memory_operator() -> Operator {
    memory_config_build().expect("Failed to create default memory operator")
}

/// OpenDAL-based storage implementation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OpenDalStorage {
    /// Memory storage variant.
    #[cfg(feature = "opendal-memory")]
    Memory(#[serde(skip, default = "self::default_memory_operator")] Operator),
    /// Local filesystem storage variant.
    #[cfg(feature = "opendal-fs")]
    LocalFs,
    /// S3 storage variant.
    ///
    /// Accepts any S3-family URL (`s3://`, `s3a://`, `s3n://`); the scheme is
    /// derived from the path at call time.
    #[cfg(feature = "opendal-s3")]
    S3 {
        /// S3 configuration.
        config: Arc<S3Config>,
        /// Custom AWS credential loader.
        #[serde(skip)]
        customized_credential_load: Option<CustomAwsCredentialLoader>,
    },
    /// GCS storage variant.
    #[cfg(feature = "opendal-gcs")]
    Gcs {
        /// GCS configuration.
        config: Arc<GcsConfig>,
    },
    /// OSS storage variant.
    #[cfg(feature = "opendal-oss")]
    Oss {
        /// OSS configuration.
        config: Arc<OssConfig>,
    },
    /// Azure Data Lake Storage variant.
    ///
    /// Accepts paths of the form
    /// `abfs[s]://<filesystem>@<account>.dfs.<endpoint-suffix>/<path>` or
    /// `wasb[s]://<container>@<account>.blob.<endpoint-suffix>/<path>`.
    /// The scheme is derived from the path at call time.
    #[cfg(feature = "opendal-azdls")]
    Azdls {
        /// Azure DLS configuration.
        config: Arc<AzdlsConfig>,
    },
    /// Azure Blob Storage variant.
    #[cfg(feature = "opendal-azblob")]
    Azblob {
        /// Azure Blob Storage configuration.
        config: Arc<AzblobConfig>,
    },
    /// HuggingFace Hub storage variant.
    ///
    /// Accepts paths of the form
    /// `hf://<repo_type>/<owner>/<repo>[@<revision>]/<path_in_repo>`,
    /// where `<repo_type>` must be one of `models`, `datasets`, `spaces`, or `buckets`.
    #[cfg(feature = "opendal-hf")]
    Hf {
        /// HuggingFace Hub configuration (token + endpoint).
        config: Arc<HfConfig>,
    },
}

impl OpenDalStorage {
    #[allow(unreachable_patterns)]
    fn operator_cache_key(
        &self,
        path: &str,
        options: &OpenDalStorageOptions,
    ) -> Result<Option<OperatorCacheKey>> {
        let backend_config = match self {
            #[cfg(feature = "opendal-s3")]
            OpenDalStorage::S3 { config, .. } => OperatorBackendConfig::S3(config.clone()),
            #[cfg(feature = "opendal-gcs")]
            OpenDalStorage::Gcs { config } => OperatorBackendConfig::Gcs(config.clone()),
            #[cfg(feature = "opendal-azdls")]
            OpenDalStorage::Azdls { config } => OperatorBackendConfig::Azdls(config.clone()),
            _ => return Ok(None),
        };

        let url = url::Url::parse(path)?;
        let host = url.host_str().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Invalid object storage url: {path}, missing bucket"),
            )
        })?;
        let bucket = match &backend_config {
            #[cfg(feature = "opendal-azdls")]
            OperatorBackendConfig::Azdls(_) => {
                // `azdls_config_build` selects the filesystem from the path and,
                // unless an endpoint is configured, derives the endpoint from the
                // path's scheme and account host. All of them scope the operator.
                let filesystem = url.username();
                if filesystem.is_empty() {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid object storage url: {path}, missing filesystem"),
                    ));
                }
                format!("{}://{filesystem}@{host}", url.scheme())
            }
            _ => host.to_string(),
        };
        Ok(Some(OperatorCacheKey {
            backend_config,
            bucket,
            layer_options: options.into(),
        }))
    }

    /// Creates operator from path.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`](iceberg::io::FileIO).
    ///
    /// # Returns
    ///
    /// The return value consists of two parts:
    ///
    /// * An [`opendal::Operator`] instance used to operate on file.
    /// * Relative path to the root uri of [`opendal::Operator`].
    #[allow(unreachable_code, unused_variables)]
    fn create_operator_with_options<'a>(
        &self,
        path: &'a impl AsRef<str>,
        options: &OpenDalStorageOptions,
        operator_cache: Option<&OperatorCache>,
    ) -> Result<(Operator, &'a str)> {
        install_http_transport();
        let path = path.as_ref();
        if let Some(operator_cache) = operator_cache
            && let Some(cache_key) = self.operator_cache_key(path, options)?
        {
            // Validate and relativize before consulting the cache so a cached operator
            // cannot make a malformed path appear valid.
            let relative_path = self.relativize_path(path)?;
            let operator = operator_cache.get_or_create(cache_key, || {
                self.build_operator_with_options(path, options)
                    .map(|(operator, _)| operator)
            })?;
            return Ok((operator, relative_path));
        }

        self.build_operator_with_options(path, options)
    }

    #[allow(unreachable_code, unused_variables)]
    fn build_operator_with_options<'a>(
        &self,
        path: &'a str,
        options: &OpenDalStorageOptions,
    ) -> Result<(Operator, &'a str)> {
        let (operator, relative_path): (Operator, &str) = match self {
            #[cfg(feature = "opendal-memory")]
            OpenDalStorage::Memory(op) => {
                if let Some(stripped) = path.strip_prefix("memory:/") {
                    (op.clone(), stripped)
                } else {
                    (op.clone(), &path[1..])
                }
            }
            #[cfg(feature = "opendal-fs")]
            OpenDalStorage::LocalFs => {
                let op = fs_config_build()?;
                if let Some(stripped) = path.strip_prefix("file:/") {
                    (op, stripped)
                } else {
                    (op, &path[1..])
                }
            }
            #[cfg(feature = "opendal-s3")]
            OpenDalStorage::S3 {
                config,
                customized_credential_load,
            } => {
                let op = s3_config_build(config, customized_credential_load, path)?;
                let op_info = op.info();

                // Use the URL scheme in the path for prefix matching. This enables
                // use of S3-compatible storage backends using custom schemes (e.g., `minio://`, `r2://`).
                let url = url::Url::parse(path).map_err(|e| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid s3 url: {path}: {e}"),
                    )
                })?;
                let prefix = format!("{}://{}/", url.scheme(), op_info.name());
                if path.starts_with(&prefix) {
                    (op, &path[prefix.len()..])
                } else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid s3 url: {path}, should start with {prefix}"),
                    ));
                }
            }
            #[cfg(feature = "opendal-gcs")]
            OpenDalStorage::Gcs { config } => {
                let operator = gcs_config_build(config, path)?;
                let prefix = format!("gs://{}/", operator.info().name());
                if path.starts_with(&prefix) {
                    (operator, &path[prefix.len()..])
                } else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid gcs url: {path}, should start with {prefix}"),
                    ));
                }
            }
            #[cfg(feature = "opendal-oss")]
            OpenDalStorage::Oss { config } => {
                let op = oss_config_build(config, path)?;
                let prefix = format!("oss://{}/", op.info().name());
                if path.starts_with(&prefix) {
                    (op, &path[prefix.len()..])
                } else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid oss url: {path}, should start with {prefix}"),
                    ));
                }
            }
            #[cfg(feature = "opendal-azdls")]
            OpenDalStorage::Azdls { config } => azdls_create_operator(path, config)?,
            #[cfg(feature = "opendal-azblob")]
            OpenDalStorage::Azblob { config } => {
                let operator = azblob_config_build(config, path)?;
                let prefix = format!("azblob://{}/", operator.info().name());
                if path.starts_with(&prefix) {
                    (operator, &path[prefix.len()..])
                } else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid azblob url: {path}, should start with {prefix}"),
                    ));
                }
            }
            #[cfg(feature = "opendal-hf")]
            OpenDalStorage::Hf { config } => hf_config_build(config, path)?,
            #[cfg(all(
                not(feature = "opendal-s3"),
                not(feature = "opendal-fs"),
                not(feature = "opendal-gcs"),
                not(feature = "opendal-oss"),
                not(feature = "opendal-azdls"),
                not(feature = "opendal-azblob"),
                not(feature = "opendal-hf"),
            ))]
            _ => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    "No storage service has been enabled",
                ));
            }
        };

        // Apply observability/resilience layers. TimeoutLayer must be
        // inside RetryLayer so each retry attempt is independently
        // bounded — without a per-attempt timeout, a future parked on a
        // silently dropped TCP connection never produces an `Err` and
        // RetryLayer cannot retry, leaving the caller hung indefinitely.
        // See: https://opendal.apache.org/docs/rust/opendal/layers/struct.TimeoutLayer.html
        //
        // OpenDAL's TimeoutLayer depends on Tokio's real timer and panics under
        // madsim, so simulated builds retain retries but omit only the timeout.
        #[cfg(not(madsim))]
        let operator = {
            let mut timeout = TimeoutLayer::new();
            if let Some(timeout_seconds) = options.timeout_seconds {
                timeout = timeout.with_io_timeout(Duration::from_secs(timeout_seconds));
            }
            operator.layer(timeout)
        };

        // Transient errors are common for object stores; we retry temporary
        // failures with exponential backoff. The retry behavior also
        // benefits non-object-store backends.
        let mut retry = RetryLayer::new();
        if let Some(max_retries) = options.max_retries {
            retry = retry.with_max_times(max_retries);
        }
        if let Some(min_delay_ms) = options.retry_min_delay_ms {
            retry = retry.with_min_delay(Duration::from_millis(min_delay_ms));
        }
        if let Some(max_delay_ms) = options.retry_max_delay_ms {
            retry = retry.with_max_delay(Duration::from_millis(max_delay_ms));
        }
        let operator = operator.layer(retry);

        #[cfg(feature = "prometheus")]
        let operator = match PROMETHEUS_LAYER.get() {
            Some(layer) => operator.layer(layer.clone()),
            None => operator,
        };

        Ok((operator, relative_path))
    }

    fn uses_append_mode(&self) -> bool {
        #[cfg(feature = "opendal-azdls")]
        {
            matches!(self, OpenDalStorage::Azdls { .. })
        }
        #[cfg(not(feature = "opendal-azdls"))]
        {
            false
        }
    }

    /// Returns a cache key used by `delete_stream` to group paths by storage operator.
    ///
    /// For most backends the URL host (bucket name) is sufficient. For HF the host
    /// encodes the repo type, not the repo identity, so a more specific key is used.
    fn batch_key_for_path(&self, path: &str) -> String {
        match self {
            #[cfg(feature = "opendal-hf")]
            OpenDalStorage::Hf { .. } => hf_batch_key(path),
            _ => url::Url::parse(path)
                .ok()
                .and_then(|u| u.host_str().map(|s| s.to_string()))
                .unwrap_or_default(),
        }
    }

    /// Extracts the relative path from an absolute path without building an operator.
    ///
    /// This is a lightweight alternative to constructing a full operator when
    /// only the relative path is needed, such as for grouped bulk deletes.
    #[allow(unreachable_code, unused_variables)]
    pub(crate) fn relativize_path<'a>(&self, path: &'a str) -> Result<&'a str> {
        match self {
            #[cfg(feature = "opendal-memory")]
            OpenDalStorage::Memory(_) => Ok(path.strip_prefix("memory:/").unwrap_or(&path[1..])),
            #[cfg(feature = "opendal-fs")]
            OpenDalStorage::LocalFs => Ok(path.strip_prefix("file:/").unwrap_or(&path[1..])),
            #[cfg(feature = "opendal-s3")]
            OpenDalStorage::S3 { .. } => {
                let url = url::Url::parse(path)?;
                let bucket = url.host_str().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid s3 url: {path}, missing bucket"),
                    )
                })?;
                let prefix = format!("{}://{}/", url.scheme(), bucket);
                if path.starts_with(&prefix) {
                    Ok(&path[prefix.len()..])
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid s3 url: {path}, should start with {prefix}"),
                    ))
                }
            }
            #[cfg(feature = "opendal-gcs")]
            OpenDalStorage::Gcs { .. } => {
                let url = url::Url::parse(path)?;
                let bucket = url.host_str().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid gcs url: {path}, missing bucket"),
                    )
                })?;
                let prefix = format!("gs://{}/", bucket);
                if path.starts_with(&prefix) {
                    Ok(&path[prefix.len()..])
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid gcs url: {path}, should start with {prefix}"),
                    ))
                }
            }
            #[cfg(feature = "opendal-oss")]
            OpenDalStorage::Oss { .. } => {
                let url = url::Url::parse(path)?;
                let bucket = url.host_str().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid oss url: {path}, missing bucket"),
                    )
                })?;
                let prefix = format!("oss://{}/", bucket);
                if path.starts_with(&prefix) {
                    Ok(&path[prefix.len()..])
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid oss url: {path}, should start with {prefix}"),
                    ))
                }
            }
            #[cfg(feature = "opendal-azblob")]
            OpenDalStorage::Azblob { .. } => {
                let url = url::Url::parse(path)?;
                let container = url.host_str().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid azblob url: {path}, missing container"),
                    )
                })?;
                let prefix = format!("azblob://{container}/");
                if path.starts_with(&prefix) {
                    Ok(&path[prefix.len()..])
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid azblob url: {path}, should start with {prefix}"),
                    ))
                }
            }
            #[cfg(feature = "opendal-azdls")]
            OpenDalStorage::Azdls { config } => {
                let azure_path = path.parse::<AzureStoragePath>()?;
                match_path_with_config(&azure_path, config)?;
                let relative_path_len = azure_path.path.len();
                Ok(&path[path.len() - relative_path_len..])
            }
            #[cfg(feature = "opendal-hf")]
            OpenDalStorage::Hf { .. } => {
                let parsed = HfUri::parse(path).ok_or_else(|| {
                    Error::new(ErrorKind::DataInvalid, format!("Invalid hf url: {path}"))
                })?;
                Ok(&path[path.len() - parsed.path.len()..])
            }
            #[cfg(all(
                not(feature = "opendal-s3"),
                not(feature = "opendal-fs"),
                not(feature = "opendal-gcs"),
                not(feature = "opendal-oss"),
                not(feature = "opendal-azdls"),
                not(feature = "opendal-azblob"),
                not(feature = "opendal-hf"),
            ))]
            _ => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "No storage service has been enabled",
            )),
        }
    }

    async fn exists_with_options(
        &self,
        path: &str,
        options: &OpenDalStorageOptions,
        operator_cache: Option<&OperatorCache>,
    ) -> Result<bool> {
        let (operator, relative_path) =
            self.create_operator_with_options(&path, options, operator_cache)?;
        operator
            .exists(relative_path)
            .await
            .map_err(from_opendal_error)
    }

    async fn metadata_with_options(
        &self,
        path: &str,
        options: &OpenDalStorageOptions,
        operator_cache: Option<&OperatorCache>,
    ) -> Result<FileMetadata> {
        let (operator, relative_path) =
            self.create_operator_with_options(&path, options, operator_cache)?;
        let metadata = operator
            .stat(relative_path)
            .await
            .map_err(from_opendal_error)?;
        Ok(FileMetadata {
            size: metadata.content_length(),
        })
    }

    async fn read_with_options(
        &self,
        path: &str,
        options: &OpenDalStorageOptions,
        operator_cache: Option<&OperatorCache>,
    ) -> Result<Bytes> {
        let (operator, relative_path) =
            self.create_operator_with_options(&path, options, operator_cache)?;
        Ok(operator
            .read(relative_path)
            .await
            .map_err(from_opendal_error)?
            .to_bytes())
    }

    async fn reader_with_options(
        &self,
        path: &str,
        options: &OpenDalStorageOptions,
        operator_cache: Option<&OperatorCache>,
    ) -> Result<Box<dyn FileRead>> {
        let (operator, relative_path) =
            self.create_operator_with_options(&path, options, operator_cache)?;
        Ok(Box::new(OpenDalReader(
            operator
                .reader(relative_path)
                .await
                .map_err(from_opendal_error)?,
        )))
    }

    async fn write_with_options(
        &self,
        path: &str,
        bytes: Bytes,
        options: &OpenDalStorageOptions,
        operator_cache: Option<&OperatorCache>,
    ) -> Result<()> {
        let mut writer = self
            .writer_with_options(path, options, operator_cache)
            .await?;
        writer.write(bytes).await?;
        writer.close().await.map(|_| ())
    }

    async fn writer_with_options(
        &self,
        path: &str,
        options: &OpenDalStorageOptions,
        operator_cache: Option<&OperatorCache>,
    ) -> Result<Box<dyn FileWrite>> {
        let (operator, relative_path) =
            self.create_operator_with_options(&path, options, operator_cache)?;
        let mut writer = operator.writer_with(relative_path);
        if self.uses_append_mode() {
            writer = writer.append(true);
        }
        if let Some(chunk_size) = options.write_chunk_size {
            writer = writer.chunk(chunk_size);
        }
        Ok(Box::new(OpenDalWriter::new(
            writer.await.map_err(from_opendal_error)?,
        )))
    }

    async fn delete_with_options(
        &self,
        path: &str,
        options: &OpenDalStorageOptions,
        operator_cache: Option<&OperatorCache>,
    ) -> Result<()> {
        let (operator, relative_path) =
            self.create_operator_with_options(&path, options, operator_cache)?;
        operator
            .delete(relative_path)
            .await
            .map_err(from_opendal_error)
    }

    async fn delete_prefix_with_options(
        &self,
        path: &str,
        options: &OpenDalStorageOptions,
        operator_cache: Option<&OperatorCache>,
    ) -> Result<()> {
        let (operator, relative_path) =
            self.create_operator_with_options(&path, options, operator_cache)?;
        let path = if relative_path.ends_with('/') {
            relative_path.to_string()
        } else {
            format!("{relative_path}/")
        };
        operator
            .delete_with(&path)
            .recursive(true)
            .await
            .map_err(from_opendal_error)
    }

    async fn delete_stream_with_options(
        &self,
        mut paths: BoxStream<'static, String>,
        options: &OpenDalStorageOptions,
        operator_cache: Option<&OperatorCache>,
    ) -> Result<()> {
        let mut deleters: HashMap<String, opendal::Deleter> = HashMap::new();

        while let Some(path) = paths.next().await {
            let bucket = self.batch_key_for_path(&path);
            let (relative_path, deleter) = match deleters.entry(bucket) {
                Entry::Occupied(entry) => {
                    (self.relativize_path(&path)?.to_string(), entry.into_mut())
                }
                Entry::Vacant(entry) => {
                    let (operator, relative_path) =
                        self.create_operator_with_options(&path, options, operator_cache)?;
                    let relative_path = relative_path.to_string();
                    let deleter = operator.deleter().await.map_err(from_opendal_error)?;
                    (relative_path, entry.insert(deleter))
                }
            };
            deleter
                .delete(relative_path)
                .await
                .map_err(from_opendal_error)?;
        }

        for (_, mut deleter) in deleters {
            deleter.close().await.map_err(from_opendal_error)?;
        }
        Ok(())
    }

    async fn list_with_options(
        &self,
        path: &str,
        recursive: bool,
        options: &OpenDalStorageOptions,
        operator_cache: Option<&OperatorCache>,
    ) -> Result<BoxStream<'static, Result<ListEntry>>> {
        let path: Arc<str> = Arc::from(path);
        let (operator, relative_path) =
            self.create_operator_with_options(&path, options, operator_cache)?;
        let absolute_prefix: Arc<str> =
            Arc::from(&path[..path.len().saturating_sub(relative_path.len())]);
        let list_path = if relative_path.is_empty() || relative_path.ends_with('/') {
            relative_path.to_string()
        } else {
            format!("{relative_path}/")
        };
        let lister = operator
            .lister_with(&list_path)
            .recursive(recursive)
            .await
            .map_err(from_opendal_error)?;

        Ok(lister
            .map(move |entry| {
                entry.map_err(from_opendal_error).map(|entry| {
                    let metadata = entry.metadata();
                    let last_modified_ms = metadata
                        .last_modified()
                        .and_then(|timestamp| {
                            let modified: SystemTime = timestamp.into();
                            modified.duration_since(UNIX_EPOCH).ok()
                        })
                        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX));
                    ListEntry {
                        path: format!("{absolute_prefix}{}", entry.path()),
                        size: metadata.content_length(),
                        last_modified_ms,
                        is_dir: metadata.is_dir(),
                    }
                })
            })
            .boxed())
    }
}

#[typetag::serde(name = "OpenDalStorage")]
#[async_trait]
impl Storage for OpenDalStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        self.exists_with_options(path, &OpenDalStorageOptions::default(), None)
            .await
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        self.metadata_with_options(path, &OpenDalStorageOptions::default(), None)
            .await
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        self.read_with_options(path, &OpenDalStorageOptions::default(), None)
            .await
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        self.reader_with_options(path, &OpenDalStorageOptions::default(), None)
            .await
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        self.write_with_options(path, bs, &OpenDalStorageOptions::default(), None)
            .await
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        self.writer_with_options(path, &OpenDalStorageOptions::default(), None)
            .await
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.delete_with_options(path, &OpenDalStorageOptions::default(), None)
            .await
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        self.delete_prefix_with_options(path, &OpenDalStorageOptions::default(), None)
            .await
    }

    async fn delete_stream(&self, paths: BoxStream<'static, String>) -> Result<()> {
        self.delete_stream_with_options(paths, &OpenDalStorageOptions::default(), None)
            .await
    }

    async fn list(
        &self,
        path: &str,
        recursive: bool,
    ) -> Result<BoxStream<'static, Result<ListEntry>>> {
        self.list_with_options(path, recursive, &OpenDalStorageOptions::default(), None)
            .await
    }

    #[allow(unreachable_code, unused_variables)]
    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    #[allow(unreachable_code, unused_variables)]
    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

#[typetag::serde(name = "ConfiguredOpenDalStorage")]
#[async_trait]
impl Storage for ConfiguredOpenDalStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        self.storage
            .exists_with_options(path, &self.options, Some(&self.operator_cache))
            .await
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        self.storage
            .metadata_with_options(path, &self.options, Some(&self.operator_cache))
            .await
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        self.storage
            .read_with_options(path, &self.options, Some(&self.operator_cache))
            .await
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        self.storage
            .reader_with_options(path, &self.options, Some(&self.operator_cache))
            .await
    }

    async fn write(&self, path: &str, bytes: Bytes) -> Result<()> {
        self.storage
            .write_with_options(path, bytes, &self.options, Some(&self.operator_cache))
            .await
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        self.storage
            .writer_with_options(path, &self.options, Some(&self.operator_cache))
            .await
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.storage
            .delete_with_options(path, &self.options, Some(&self.operator_cache))
            .await
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        self.storage
            .delete_prefix_with_options(path, &self.options, Some(&self.operator_cache))
            .await
    }

    async fn delete_stream(&self, paths: BoxStream<'static, String>) -> Result<()> {
        self.storage
            .delete_stream_with_options(paths, &self.options, Some(&self.operator_cache))
            .await
    }

    async fn list(
        &self,
        path: &str,
        recursive: bool,
    ) -> Result<BoxStream<'static, Result<ListEntry>>> {
        self.storage
            .list_with_options(path, recursive, &self.options, Some(&self.operator_cache))
            .await
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

// Newtype wrappers for opendal types to satisfy orphan rules.
// We can't implement iceberg's FileRead/FileWrite traits directly on opendal's
// Reader/Writer since neither trait nor type is defined in this crate.

/// Wrapper around `opendal::Reader` that implements `FileRead`.
pub(crate) struct OpenDalReader(pub(crate) opendal::Reader);

#[async_trait]
impl FileRead for OpenDalReader {
    async fn read(&self, range: std::ops::Range<u64>) -> Result<Bytes> {
        Ok(opendal::Reader::read(&self.0, range)
            .await
            .map_err(from_opendal_error)?
            .to_bytes())
    }
}

/// Wrapper around `opendal::Writer` that implements `FileWrite`.
pub(crate) struct OpenDalWriter {
    inner: opendal::Writer,
    bytes_written: u64,
}

impl OpenDalWriter {
    pub(crate) fn new(inner: opendal::Writer) -> Self {
        Self {
            inner,
            bytes_written: 0,
        }
    }
}

#[async_trait]
impl FileWrite for OpenDalWriter {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        let len = bs.len() as u64;
        opendal::Writer::write(&mut self.inner, bs)
            .await
            .map_err(from_opendal_error)?;
        self.bytes_written += len;
        Ok(())
    }

    async fn close(&mut self) -> Result<FileMetadata> {
        let metadata = opendal::Writer::close(&mut self.inner)
            .await
            .map_err(from_opendal_error)?;

        // Object stores may omit the size (reported as 0); validate only a reported nonzero size.
        let reported_size = metadata.content_length();
        if reported_size != 0 && reported_size != self.bytes_written {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!(
                    "Wrote {} bytes but storage reports {reported_size}",
                    self.bytes_written
                ),
            ));
        }

        Ok(FileMetadata {
            size: self.bytes_written,
        })
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "opendal-s3")]
    use std::sync::Barrier;
    #[cfg(feature = "opendal-s3")]
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[cfg(feature = "opendal-s3")]
    use iceberg::io::{
        CLIENT_REGION, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION,
        S3_SECRET_ACCESS_KEY,
    };

    use super::*;

    #[cfg(feature = "opendal-s3")]
    #[derive(Debug)]
    struct EmptyCredentialLoader;

    #[cfg(feature = "opendal-s3")]
    impl ProvideCredential for EmptyCredentialLoader {
        type Credential = AwsCredential;

        async fn provide_credential(
            &self,
            _ctx: &reqsign_core::Context,
        ) -> reqsign_core::Result<Option<AwsCredential>> {
            Ok(None)
        }
    }

    #[cfg(feature = "opendal-s3")]
    #[test]
    fn test_s3_factory_custom_credential_loader_serialization_fails() {
        let file_io = iceberg::io::FileIOBuilder::new(Arc::new(
            OpenDalStorageFactory::s3_with_credential_loader(CustomAwsCredentialLoader::new(
                EmptyCredentialLoader,
            )),
        ))
        .build();

        let err = file_io.serialize_all().unwrap_err();
        assert!(
            err.to_string()
                .contains("custom AWS credential loaders cannot be serialized")
        );
    }

    #[cfg(feature = "opendal-s3")]
    fn test_s3_cache_key(bucket: &str) -> OperatorCacheKey {
        let mut config = S3Config::default();
        config.region = Some("us-east-1".to_string());
        OperatorCacheKey {
            backend_config: OperatorBackendConfig::S3(Arc::new(config)),
            bucket: bucket.to_string(),
            layer_options: OperatorLayerOptions::from(&OpenDalStorageOptions::default()),
        }
    }

    #[cfg(any(feature = "opendal-s3", feature = "opendal-azdls"))]
    fn create_cached_operator(storage: &ConfiguredOpenDalStorage, path: &str) {
        storage
            .storage
            .create_operator_with_options(&path, &storage.options, Some(&storage.operator_cache))
            .unwrap();
    }

    #[cfg(feature = "opendal-azdls")]
    fn test_azdls_storage(endpoint: Option<&str>) -> ConfiguredOpenDalStorage {
        let storage = OpenDalStorage::Azdls {
            config: Arc::new(AzdlsConfig {
                account_name: Some("myaccount".to_string()),
                account_key: Some("bXlhY2NvdW50a2V5".to_string()),
                endpoint: endpoint.map(str::to_string),
                ..Default::default()
            }),
        };
        ConfiguredOpenDalStorage::new(storage, &StorageConfig::new(), default_operator_cache())
            .unwrap()
    }

    #[cfg(feature = "opendal-azdls")]
    #[test]
    fn test_azdls_operator_cache_reuses_operator_per_filesystem() {
        let storage = test_azdls_storage(None);

        create_cached_operator(
            &storage,
            "abfss://myfs@myaccount.dfs.core.windows.net/path/to/one.parquet",
        );
        create_cached_operator(
            &storage,
            "abfss://myfs@myaccount.dfs.core.windows.net/other/two.parquet",
        );
        assert_eq!(storage.operator_cache.len(), 1);

        create_cached_operator(
            &storage,
            "abfss://otherfs@myaccount.dfs.core.windows.net/path/to/one.parquet",
        );
        assert_eq!(storage.operator_cache.len(), 2);
    }

    #[cfg(feature = "opendal-azdls")]
    #[test]
    fn test_azdls_operator_cache_key_scopes_scheme_filesystem_and_host() {
        let storage = test_azdls_storage(None);
        let key = |path: &str| {
            storage
                .storage
                .operator_cache_key(path, &storage.options)
                .unwrap()
                .expect("azdls operators are cached")
        };

        let base = key("abfss://myfs@myaccount.dfs.core.windows.net/path/to/one.parquet");
        assert_eq!(base.bucket, "abfss://myfs@myaccount.dfs.core.windows.net");
        assert!(base == key("abfss://myfs@myaccount.dfs.core.windows.net/other/two.parquet"));
        assert!(base != key("abfs://myfs@myaccount.dfs.core.windows.net/path/to/one.parquet"));
        assert!(base != key("abfss://otherfs@myaccount.dfs.core.windows.net/path/to/one.parquet"));
        assert!(base != key("abfss://myfs@otheraccount.dfs.core.windows.net/path/to/one.parquet"));

        assert!(
            storage
                .storage
                .operator_cache_key(
                    "abfss://myaccount.dfs.core.windows.net/path/to/one.parquet",
                    &storage.options
                )
                .is_err()
        );
    }

    #[cfg(feature = "opendal-azdls")]
    #[test]
    fn test_azdls_operator_cache_validates_path_before_lookup() {
        let storage = test_azdls_storage(Some("https://myaccount.dfs.core.windows.net"));
        create_cached_operator(
            &storage,
            "abfss://myfs@myaccount.dfs.core.windows.net/path/to/one.parquet",
        );
        assert_eq!(storage.operator_cache.len(), 1);

        for invalid in [
            // account name differs from the configured one
            "abfss://myfs@otheraccount.dfs.core.windows.net/path/to/one.parquet",
            // endpoint suffix differs from the configured endpoint
            "abfss://myfs@myaccount.dfs.core.chinacloudapi.cn/path/to/one.parquet",
        ] {
            assert!(
                storage
                    .storage
                    .create_operator_with_options(
                        &invalid,
                        &storage.options,
                        Some(&storage.operator_cache)
                    )
                    .is_err()
            );
        }
        assert_eq!(storage.operator_cache.len(), 1);
    }

    #[cfg(feature = "opendal-s3")]
    #[test]
    fn test_operator_cache_coalesces_concurrent_builds() {
        const CONCURRENCY: usize = 16;

        let cache = default_operator_cache();
        let barrier = Arc::new(Barrier::new(CONCURRENCY));
        let build_count = Arc::new(AtomicUsize::new(0));

        std::thread::scope(|scope| {
            for _ in 0..CONCURRENCY {
                let cache = cache.clone();
                let barrier = barrier.clone();
                let build_count = build_count.clone();
                scope.spawn(move || {
                    barrier.wait();
                    cache
                        .get_or_create(test_s3_cache_key("test-bucket"), || {
                            build_count.fetch_add(1, Ordering::SeqCst);
                            let mut config = S3Config::default();
                            config.region = Some("us-east-1".to_string());
                            s3_config_build(&config, &None, "s3://test-bucket/path/to/file")
                        })
                        .unwrap();
                });
            }
        });

        assert_eq!(build_count.load(Ordering::SeqCst), 1);
        assert_eq!(cache.len(), 1);
    }

    #[cfg(feature = "opendal-s3")]
    #[test]
    fn test_failed_operator_build_can_be_retried() {
        let cache = default_operator_cache();
        let build_count = AtomicUsize::new(0);
        let key = test_s3_cache_key("test-bucket");

        let error = cache.get_or_create(key.clone(), || {
            build_count.fetch_add(1, Ordering::SeqCst);
            Err(Error::new(ErrorKind::Unexpected, "injected build failure"))
        });
        assert!(error.is_err());

        cache
            .get_or_create(key, || {
                build_count.fetch_add(1, Ordering::SeqCst);
                let mut config = S3Config::default();
                config.region = Some("us-east-1".to_string());
                s3_config_build(&config, &None, "s3://test-bucket/path/to/file")
            })
            .unwrap();

        assert_eq!(build_count.load(Ordering::SeqCst), 2);
        assert_eq!(cache.len(), 1);
    }

    #[cfg(feature = "opendal-s3")]
    #[test]
    fn test_factory_cache_is_shared_across_storages_and_bucket_scoped() {
        let factory = OpenDalStorageFactory::s3();
        let config = StorageConfig::new().with_prop(S3_REGION, "us-east-1");
        let first_storage = factory.build_configured(&config).unwrap();
        let second_storage = factory.build_configured(&config).unwrap();

        assert!(Arc::ptr_eq(
            &first_storage.operator_cache,
            &second_storage.operator_cache
        ));

        create_cached_operator(&first_storage, "s3://bucket-a/path/to/first-file");
        create_cached_operator(&second_storage, "s3://bucket-a/path/to/second-file");
        create_cached_operator(&second_storage, "s3://bucket-b/path/to/third-file");

        assert_eq!(factory.operator_cache.len(), 2);
    }

    #[cfg(feature = "opendal-s3")]
    #[test]
    fn test_factory_cache_is_scoped_by_effective_configuration() {
        let factory = OpenDalStorageFactory::s3();
        let s3_region = StorageConfig::new().with_prop(S3_REGION, "us-east-1");
        let client_region = StorageConfig::new().with_prop(CLIENT_REGION, "us-east-1");
        let different_chunk_size = s3_region.clone().with_prop(IO_CHUNK_SIZE, "1024");
        let different_region = StorageConfig::new().with_prop(S3_REGION, "us-west-2");
        let different_endpoint = s3_region
            .clone()
            .with_prop(S3_ENDPOINT, "http://localhost:9001");
        let different_credentials = s3_region
            .clone()
            .with_prop(S3_ACCESS_KEY_ID, "test-access-key")
            .with_prop(S3_SECRET_ACCESS_KEY, "test-secret-key");
        let different_path_style = s3_region.clone().with_prop(S3_PATH_STYLE_ACCESS, "true");
        let different_timeout = s3_region.clone().with_prop(IO_TIMEOUT_SECONDS, "10");
        let different_retry = s3_region.clone().with_prop(IO_MAX_RETRIES, "10");

        for config in [&s3_region, &client_region, &different_chunk_size] {
            let storage = factory.build_configured(config).unwrap();
            create_cached_operator(&storage, "s3://test-bucket/path/to/file");
        }
        // Region aliases normalize to the same S3 config, while write chunk size
        // is applied per writer and does not change the operator.
        assert_eq!(factory.operator_cache.len(), 1);

        for config in [
            &different_region,
            &different_endpoint,
            &different_credentials,
            &different_path_style,
            &different_timeout,
            &different_retry,
        ] {
            let storage = factory.build_configured(config).unwrap();
            create_cached_operator(&storage, "s3://test-bucket/path/to/file");
        }
        assert_eq!(factory.operator_cache.len(), 7);
    }

    #[cfg(feature = "opendal-s3")]
    #[test]
    fn test_operator_cache_follows_factory_lifecycle() {
        let first_factory = OpenDalStorageFactory::s3();
        let cloned_factory = first_factory.clone();
        let second_factory = OpenDalStorageFactory::s3();
        let config = StorageConfig::new().with_prop(S3_REGION, "us-east-1");

        let first_storage = first_factory.build_configured(&config).unwrap();
        let cloned_storage = cloned_factory.build_configured(&config).unwrap();
        let second_storage = second_factory.build_configured(&config).unwrap();

        assert!(Arc::ptr_eq(
            &first_storage.operator_cache,
            &cloned_storage.operator_cache
        ));
        assert!(!Arc::ptr_eq(
            &first_storage.operator_cache,
            &second_storage.operator_cache
        ));
    }

    #[cfg(feature = "opendal-s3")]
    #[test]
    fn test_factory_operator_cache_is_reset_after_deserialization() {
        let factory = OpenDalStorageFactory::s3();
        let config = StorageConfig::new().with_prop(S3_REGION, "us-east-1");
        let storage = factory.build_configured(&config).unwrap();
        create_cached_operator(&storage, "s3://test-bucket/path/to/file");

        let serialized = serde_json::to_string(&factory).unwrap();
        let deserialized: OpenDalStorageFactory = serde_json::from_str(&serialized).unwrap();

        // Keep the serialized representation compatible with the former
        // public enum even though the factory now owns runtime state.
        assert_eq!(serialized, r#"{"S3":{}}"#);
        assert_eq!(factory.operator_cache.len(), 1);
        assert_eq!(deserialized.operator_cache.len(), 0);
        assert!(!Arc::ptr_eq(
            &factory.operator_cache,
            &deserialized.operator_cache
        ));
    }

    #[cfg(feature = "opendal-gcs")]
    #[test]
    fn test_cached_gcs_operator_does_not_bypass_path_validation() {
        let factory = OpenDalStorageFactory::gcs();
        let storage = factory.build_configured(&StorageConfig::new()).unwrap();

        storage
            .storage
            .create_operator_with_options(
                &"gs://test-bucket/path/to/file",
                &storage.options,
                Some(&storage.operator_cache),
            )
            .unwrap();

        assert!(
            storage
                .storage
                .create_operator_with_options(
                    &"s3://test-bucket/path/to/file",
                    &storage.options,
                    Some(&storage.operator_cache),
                )
                .is_err()
        );
        assert_eq!(factory.operator_cache.len(), 1);
    }

    #[cfg(feature = "opendal-memory")]
    #[test]
    fn test_default_memory_operator() {
        let op = default_memory_operator();
        assert_eq!(op.info().scheme().to_string(), "memory");
    }

    #[cfg(all(feature = "opendal-memory", feature = "prometheus"))]
    #[tokio::test]
    async fn test_prometheus_metrics() {
        let registry = prometheus::Registry::new();
        install_prometheus_metrics(&registry).unwrap();

        let storage = OpenDalStorage::Memory(default_memory_operator());
        storage
            .write("memory:/metrics.txt", Bytes::from_static(b"metrics"))
            .await
            .unwrap();

        assert!(
            registry
                .gather()
                .iter()
                .any(|family| family.name() == "opendal_operation_duration_seconds")
        );
    }

    #[cfg(feature = "opendal-memory")]
    #[tokio::test]
    async fn test_writer_close_returns_stored_size() {
        use iceberg::encryption::{EncryptedOutputFile, StandardKeyMetadata};

        // Note: the memory service does report a content length, so this only pins the happy
        // path. The counter in `OpenDalWriter` is what covers services that don't, such as S3.
        let storage = Arc::new(OpenDalStorage::Memory(default_memory_operator()));
        let path = "memory:///stored-size";
        for plaintext in [
            Bytes::new(),
            Bytes::from_static(b"test data"),
            Bytes::from(vec![7; 3 * 1024]),
        ] {
            let mut writer = storage.writer(path).await.unwrap();
            for chunk in plaintext.chunks(1024) {
                writer.write(Bytes::copy_from_slice(chunk)).await.unwrap();
            }
            let metadata = writer.close().await.unwrap();
            assert_eq!(metadata.size, plaintext.len() as u64);
            assert_eq!(metadata.size, storage.metadata(path).await.unwrap().size);

            let output = EncryptedOutputFile::new(
                OutputFile::new(storage.clone(), path.to_string()),
                StandardKeyMetadata::try_new(b"0123456789abcdef").unwrap(),
            );
            let metadata = output.write(plaintext.clone()).await.unwrap();
            assert!(metadata.size > plaintext.len() as u64);
            assert_eq!(metadata.size, storage.metadata(path).await.unwrap().size);
        }
    }

    #[cfg(feature = "opendal-memory")]
    #[test]
    fn test_relativize_path_memory() {
        let storage = OpenDalStorage::Memory(default_memory_operator());

        assert_eq!(
            storage.relativize_path("memory:/path/to/file").unwrap(),
            "path/to/file"
        );
        // Without the scheme prefix, falls back to stripping the leading slash
        assert_eq!(
            storage.relativize_path("/path/to/file").unwrap(),
            "path/to/file"
        );
    }

    #[cfg(feature = "opendal-fs")]
    #[test]
    fn test_relativize_path_fs() {
        let storage = OpenDalStorage::LocalFs;

        assert_eq!(
            storage
                .relativize_path("file:/tmp/data/file.parquet")
                .unwrap(),
            "tmp/data/file.parquet"
        );
        assert_eq!(
            storage.relativize_path("/tmp/data/file.parquet").unwrap(),
            "tmp/data/file.parquet"
        );
    }

    #[cfg(feature = "opendal-s3")]
    #[test]
    fn test_relativize_path_s3() {
        let storage = OpenDalStorage::S3 {
            config: Arc::new(S3Config::default()),
            customized_credential_load: None,
        };

        // All S3-family schemes are accepted by the same storage instance.
        // Custom schemes for S3-compatible stores (e.g., `minio://`) are also
        // accepted because the path's scheme is used as-is for prefix matching.
        for scheme in ["s3", "s3a", "s3n", "minio"] {
            assert_eq!(
                storage
                    .relativize_path(&format!("{scheme}://my-bucket/path/to/file.parquet"))
                    .unwrap(),
                "path/to/file.parquet"
            );
        }
    }

    #[cfg(feature = "opendal-gcs")]
    #[test]
    fn test_relativize_path_gcs() {
        let storage = OpenDalStorage::Gcs {
            config: Arc::new(GcsConfig::default()),
        };

        assert_eq!(
            storage
                .relativize_path("gs://my-bucket/path/to/file.parquet")
                .unwrap(),
            "path/to/file.parquet"
        );
    }

    #[cfg(feature = "opendal-gcs")]
    #[test]
    fn test_relativize_path_gcs_invalid_scheme() {
        let storage = OpenDalStorage::Gcs {
            config: Arc::new(GcsConfig::default()),
        };

        assert!(
            storage
                .relativize_path("s3://my-bucket/path/to/file.parquet")
                .is_err()
        );
    }

    #[cfg(feature = "opendal-oss")]
    #[test]
    fn test_relativize_path_oss() {
        let storage = OpenDalStorage::Oss {
            config: Arc::new(OssConfig::default()),
        };

        assert_eq!(
            storage
                .relativize_path("oss://my-bucket/path/to/file.parquet")
                .unwrap(),
            "path/to/file.parquet"
        );
    }

    #[cfg(feature = "opendal-oss")]
    #[test]
    fn test_relativize_path_oss_invalid_scheme() {
        let storage = OpenDalStorage::Oss {
            config: Arc::new(OssConfig::default()),
        };

        assert!(
            storage
                .relativize_path("s3://my-bucket/path/to/file.parquet")
                .is_err()
        );
    }

    #[cfg(feature = "opendal-azdls")]
    #[test]
    fn test_relativize_path_azdls() {
        let storage = OpenDalStorage::Azdls {
            config: Arc::new(AzdlsConfig {
                account_name: Some("myaccount".to_string()),
                endpoint: Some("https://myaccount.dfs.core.windows.net".to_string()),
                ..Default::default()
            }),
        };

        assert_eq!(
            storage
                .relativize_path("abfss://myfs@myaccount.dfs.core.windows.net/path/to/file.parquet")
                .unwrap(),
            "/path/to/file.parquet"
        );
        assert!(storage.uses_append_mode());
    }

    #[cfg(feature = "opendal-azblob")]
    #[test]
    fn test_relativize_path_azblob() {
        let storage = OpenDalStorage::Azblob {
            config: Arc::new(AzblobConfig::default()),
        };
        assert_eq!(
            storage
                .relativize_path("azblob://container/path/to/file.parquet")
                .unwrap(),
            "path/to/file.parquet"
        );
    }

    #[cfg(feature = "opendal-memory")]
    #[tokio::test]
    async fn test_list_memory_storage() {
        use futures::TryStreamExt;

        let storage = OpenDalStorage::Memory(default_memory_operator());
        storage
            .write("memory:/root/direct.txt", Bytes::from_static(b"a"))
            .await
            .unwrap();
        storage
            .write(
                "memory:/root/nested/child.txt",
                Bytes::from_static(b"child"),
            )
            .await
            .unwrap();

        let entries: Vec<_> = storage
            .list("memory:/root", true)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert!(
            entries
                .iter()
                .any(|entry| entry.path == "memory:/root/direct.txt" && entry.size == 1)
        );
        assert!(
            entries
                .iter()
                .any(|entry| entry.path == "memory:/root/nested/child.txt" && entry.size == 5)
        );
    }

    #[cfg(feature = "opendal-memory")]
    #[tokio::test]
    async fn test_configured_storage_options_and_write() {
        let config = StorageConfig::new()
            .with_prop(IO_CHUNK_SIZE, "1024")
            .with_prop(IO_TIMEOUT_SECONDS, "30")
            .with_prop(IO_MAX_RETRIES, "5")
            .with_prop(IO_RETRY_MIN_DELAY_MS, "10")
            .with_prop(IO_RETRY_MAX_DELAY_MS, "100");
        let storage = ConfiguredOpenDalStorage::new(
            OpenDalStorage::Memory(default_memory_operator()),
            &config,
            default_operator_cache(),
        )
        .unwrap();

        assert_eq!(storage.options.write_chunk_size, Some(1024));
        assert_eq!(storage.options.timeout_seconds, Some(30));
        assert_eq!(storage.options.max_retries, Some(5));
        assert_eq!(storage.options.retry_min_delay_ms, Some(10));
        assert_eq!(storage.options.retry_max_delay_ms, Some(100));

        storage
            .write("memory:/configured.txt", Bytes::from_static(b"configured"))
            .await
            .unwrap();
        assert_eq!(
            storage.read("memory:/configured.txt").await.unwrap(),
            Bytes::from_static(b"configured")
        );
    }

    #[test]
    fn test_invalid_storage_options() {
        let invalid_integer = StorageConfig::new().with_prop(IO_TIMEOUT_SECONDS, "not-a-number");
        assert!(OpenDalStorageOptions::try_from(&invalid_integer).is_err());

        let invalid_delays = StorageConfig::new()
            .with_prop(IO_RETRY_MIN_DELAY_MS, "101")
            .with_prop(IO_RETRY_MAX_DELAY_MS, "100");
        assert!(OpenDalStorageOptions::try_from(&invalid_delays).is_err());

        let zero_chunk = StorageConfig::new().with_prop(IO_CHUNK_SIZE, "0");
        assert!(OpenDalStorageOptions::try_from(&zero_chunk).is_err());
    }
}
