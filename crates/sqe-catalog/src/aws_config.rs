//! AWS credential layering for ATTACH-attached catalogs.
//!
//! The `SECRET <name>` option in `ATTACH ... (TYPE glue, SECRET aws_prod)`
//! resolves to a [`Secret::Aws`] in the in-memory secret store. We layer
//! that on top of the AWS default credential chain so the priority order
//! is:
//!
//! 1. `SECRET <name>` option (highest, explicit)
//! 2. `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars
//! 3. AWS shared credentials (`~/.aws/credentials`, profile via `AWS_PROFILE`)
//! 4. EC2 IMDS, ECS task role, EKS Pod Identity
//!
//! Steps 2-4 are exactly what `aws_config::defaults` already produces. We
//! only need to layer step 1 on top — `credentials_provider()` overrides
//! the default chain when called, so explicit creds win at resolve time.
//!
//! Region precedence layered on top of credential precedence:
//! `REGION` option > secret-supplied region > `AWS_REGION` env > profile.
//!
//! This module is feature-gated on `glue`/`s3tables`. Builds without
//! either AWS feature don't link aws-config and don't compile this file.

use std::collections::BTreeMap;
use std::sync::Arc;

use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use object_store::aws::{AwsCredential, AwsCredentialProvider};
use object_store::CredentialProvider;
use sqe_core::{Secret, SecretStore};
use sqe_sql::OptionValue;
use tokio::sync::OnceCell;

/// The AWS SDK config loader every SQE-built SDK client must start from.
///
/// It carries an explicit rustls (ring) HTTPS client. The `aws-config`
/// dependency has `default-https-client` off because that feature hardcodes
/// the aws-lc provider and its C build into every binary (SQE #133), and a
/// loader without a client does not fail at load time: the first request
/// panics inside the SDK orchestrator with "a http_client is required". The
/// live Glue Hive root built its loader bare and took the server down at
/// startup that way, so the loader lives here and the call sites share it.
pub fn sdk_config_loader() -> aws_config::ConfigLoader {
    let http_client = aws_smithy_http_client::Builder::new()
        .tls_provider(aws_smithy_http_client::tls::Provider::Rustls(
            aws_smithy_http_client::tls::rustls_provider::CryptoMode::Ring,
        ))
        .build_https();
    aws_config::defaults(aws_config::BehaviorVersion::latest()).http_client(http_client)
}

/// Bridges the AWS SDK credential chain into `object_store` for S3 data I/O.
///
/// `object_store`'s own `AmazonS3Builder` resolves only environment keys and
/// EC2/ECS instance roles when no static key is set: it has no SSO and no
/// `AWS_PROFILE` support, because it is not the AWS SDK. So `aws sso login;
/// export AWS_PROFILE=...` worked for Glue metadata (SDK path) but not for the
/// S3 reads (object_store path), which fell through to IMDS. This adapter
/// hands object_store the SDK's resolved credentials, so a profile, an SSO
/// session, `credential_process`, IRSA and IMDS all work for data reads the
/// same way they do for the catalog. Credentials resolve on each call and the
/// SDK provider caches and refreshes them, so an SSO or STS session that
/// rotates mid-run keeps working (unlike keys captured once into `[storage]`).
/// How long one credential resolution through the SDK chain may take before
/// it is reported as a credential failure rather than waited on. The SDK's
/// own operation timeouts do not cover every provider in the chain.
const CREDENTIAL_RESOLVE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Debug)]
struct SdkObjectStoreCredentials {
    provider: OnceCell<SharedCredentialsProvider>,
}

impl SdkObjectStoreCredentials {
    fn new() -> Self {
        Self {
            provider: OnceCell::new(),
        }
    }

    async fn provider(&self) -> object_store::Result<&SharedCredentialsProvider> {
        self.provider
            .get_or_try_init(|| async {
                let conf = sdk_config_loader().load().await;
                conf.credentials_provider().ok_or_else(|| {
                    object_store::Error::Generic {
                        store: "S3",
                        source: "AWS credential provider chain returned no provider \
                                 (no profile, SSO session, env keys or instance role)"
                            .into(),
                    }
                })
            })
            .await
    }
}

#[async_trait::async_trait]
impl CredentialProvider for SdkObjectStoreCredentials {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
        // Bounded: a chain that waits (an SSO token exchange against a portal
        // that never answers, IMDS on a laptop, a credential_process that
        // hangs) held every scan open with no error to report, and a
        // describe_schema over 25 tables hung indefinitely on it. Past the
        // deadline this is the same credential failure, named as such.
        let provider = self.provider().await?;
        let resolved = tokio::time::timeout(CREDENTIAL_RESOLVE_TIMEOUT, provider.provide_credentials())
            .await
            .map_err(|_| object_store::Error::Generic {
                store: "S3",
                source: format!(
                    "storage credentials expired or unavailable (aws sdk default chain): the \
                     credential provider did not answer within {}s. Renew the engine's AWS \
                     session (aws sso login, or a fresh role session) and restart, or run \
                     with AWS_PROFILE so the SDK refreshes them",
                    CREDENTIAL_RESOLVE_TIMEOUT.as_secs()
                )
                .into(),
            })?;
        let creds = resolved.map_err(|e| object_store::Error::Generic {
            store: "S3",
            // The SDK's own Display is "an error occurred while loading
            // credentials"; the reason (SSO session over, no provider,
            // a failed credential_process) is in the source chain, and
            // this text is what the coordinator classifies as
            // STORAGE_CREDENTIALS_UNAVAILABLE.
            source: storage_credentials_unavailable(&e).into(),
        })?;
        Ok(Arc::new(AwsCredential {
            key_id: creds.access_key_id().to_string(),
            secret_key: creds.secret_access_key().to_string(),
            token: creds.session_token().map(str::to_string),
        }))
    }
}

/// An `object_store` credential provider backed by the AWS SDK default chain.
/// Attach it to an `AmazonS3Builder` on the no-static-key path so S3 reads use
/// the same identity resolution (profile, SSO, IMDS) as the SDK. The SDK
/// config is loaded lazily on the first credential fetch, so constructing this
/// is cheap and synchronous.
pub fn sdk_object_store_credentials() -> AwsCredentialProvider {
    Arc::new(SdkObjectStoreCredentials::new())
}

/// The distinct message for "the engine's storage credentials cannot be
/// resolved": named as such, with the provider and the SDK's full source
/// chain, and the remedy. `sqe-core` classifies this text as
/// `STORAGE_CREDENTIALS_UNAVAILABLE`, so a client can tell an expired SSO
/// session from a bad query.
pub fn storage_credentials_unavailable(e: &dyn std::error::Error) -> String {
    let mut chain = vec![e.to_string()];
    let mut cur = e.source();
    while let Some(next) = cur {
        chain.push(next.to_string());
        cur = next.source();
    }
    format!(
        "storage credentials expired or unavailable (aws sdk default chain): {}. Renew the \
         engine's AWS session (aws sso login, or a fresh role session) and restart, or run \
         with AWS_PROFILE so the SDK refreshes them",
        chain.join(": ")
    )
}

/// What a storage-credential probe reports: enough for a sidecar to decide
/// whether to start a long job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageCredentialReport {
    /// `ok`, `auth_error`, or `unknown` (static keys: valid until they are
    /// not, and nothing here can see their expiry).
    pub status: &'static str,
    /// Where the credentials come from: `aws-sdk-default-chain`,
    /// `static-keys`, `static-temporary-keys`.
    pub provider: &'static str,
    /// RFC 3339, when the SDK knows when the current credentials lapse.
    pub expires_at: Option<String>,
    /// The failure text on `auth_error`.
    pub detail: Option<String>,
}

/// Resolve credentials once through the AWS SDK default chain and report
/// the outcome. Cheap when the SDK has them cached, one provider round trip
/// (SSO token exchange, STS assume-role, IMDS) when it does not; the caller
/// bounds it. This is the same chain the S3 stores read with, so an
/// `auth_error` here is exactly the failure the next scan would hit.
pub async fn probe_storage_credentials() -> StorageCredentialReport {
    let conf = sdk_config_loader().load().await;
    let Some(provider) = conf.credentials_provider() else {
        return StorageCredentialReport {
            status: "auth_error",
            provider: "aws-sdk-default-chain",
            expires_at: None,
            detail: Some(
                "AWS credential provider chain returned no provider (no profile, SSO \
                 session, env keys or instance role)"
                    .to_string(),
            ),
        };
    };
    let resolved =
        match tokio::time::timeout(CREDENTIAL_RESOLVE_TIMEOUT, provider.provide_credentials()).await {
            Ok(r) => r,
            Err(_) => {
                return StorageCredentialReport {
                    status: "auth_error",
                    provider: "aws-sdk-default-chain",
                    expires_at: None,
                    detail: Some(format!(
                        "the credential provider did not answer within {}s",
                        CREDENTIAL_RESOLVE_TIMEOUT.as_secs()
                    )),
                }
            }
        };
    match resolved {
        Ok(creds) => StorageCredentialReport {
            status: "ok",
            provider: "aws-sdk-default-chain",
            expires_at: creds.expiry().map(|t| {
                chrono::DateTime::<chrono::Utc>::from(t).to_rfc3339_opts(
                    chrono::SecondsFormat::Secs,
                    true,
                )
            }),
            detail: None,
        },
        Err(e) => StorageCredentialReport {
            status: "auth_error",
            provider: "aws-sdk-default-chain",
            expires_at: None,
            detail: Some(storage_credentials_unavailable(&e)),
        },
    }
}


/// The region the AWS SDK resolves: `AWS_REGION`, `AWS_DEFAULT_REGION`, or the
/// active profile's `region` in `~/.aws/config`. Used to fill
/// `storage.s3_region` when nothing else set it, so `export AWS_PROFILE=...`
/// alone gives the S3 data path a region the same way it gives the Glue
/// catalog one. `None` when the chain resolves no region. Async because it
/// reads the profile and SSO config.
pub async fn sdk_resolved_region() -> Option<String> {
    sdk_config_loader()
        .load()
        .await
        .region()
        .map(|r| r.to_string())
}

/// Layer ATTACH options + secret store entries on top of the AWS default
/// credential chain. Returns an [`aws_config::SdkConfig`] callers can hand
/// to `aws_sdk_*::Client::new`. The Glue and S3 Tables backends accept
/// AWS credentials through their `(name, props)` HashMap interface
/// instead, so the typical SQE call site reads the resolved
/// `SdkConfig.credentials_provider().provide_credentials()` once and
/// translates it back into the `aws_access_key_id` / `aws_secret_access_key`
/// / `aws_session_token` / `region_name` props the upstream catalog
/// builders consume. The structured `SdkConfig` shape stays available for
/// future call sites that need it directly (credential vending, S3 tables
/// REST adapters, etc.).
///
/// # Errors
///
/// - `SECRET` references a secret that does not exist
/// - The referenced secret is not of kind `aws`
pub async fn build_aws_config(
    options: &BTreeMap<String, OptionValue>,
    secrets: &SecretStore,
) -> Result<aws_config::SdkConfig, String> {
    let mut loader = sdk_config_loader();

    // 1. Apply the explicit AWS secret first if present. The
    //    `credentials_provider` setter overrides the default chain, so
    //    static creds plumbed in here win at resolve time.
    //
    // `Secret` carries a custom `Drop` impl that zeroizes its inner
    // strings, which prevents moving fields out of the variant directly
    // (the `Drop` would still run on an owned value with moved-out
    // fields). We borrow the secret into a temporary, clone the strings
    // we need, and let the original drop normally — the clones go on to
    // the AWS credential provider untouched.
    if let Some(secret_ref) = options.get("SECRET").and_then(OptionValue::as_secret_ref) {
        let secret = secrets.get(secret_ref).map_err(|e| e.to_string())?;
        match &secret {
            Secret::Aws {
                access_key,
                secret_key,
                session_token,
                region,
                profile,
            } => {
                if let (Some(ak), Some(sk)) = (access_key.as_ref(), secret_key.as_ref()) {
                    let creds = aws_credential_types::Credentials::new(
                        ak.clone(),
                        sk.clone(),
                        session_token.clone(),
                        None,
                        "sqe-secret",
                    );
                    loader = loader.credentials_provider(creds);
                }
                if let Some(r) = region {
                    loader = loader.region(aws_config::Region::new(r.clone()));
                }
                if let Some(p) = profile {
                    loader = loader.profile_name(p.clone());
                }
            }
            other => {
                return Err(format!(
                    "secret '{secret_ref}' is not of type aws (got '{}')",
                    other.type_name()
                ));
            }
        }
    }

    // 2. Direct REGION option overrides the secret-supplied region.
    //    Run after the secret block so it always wins.
    if let Some(r) = options.get("REGION").and_then(OptionValue::as_str) {
        loader = loader.region(aws_config::Region::new(r.to_string()));
    }

    Ok(loader.load().await)
}

#[cfg(test)]
mod credential_message_tests {
    use super::*;

    #[derive(Debug)]
    struct Leaf;
    impl std::fmt::Display for Leaf {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("the SSO session has expired")
        }
    }
    impl std::error::Error for Leaf {}

    #[derive(Debug)]
    struct Outer(Leaf);
    impl std::fmt::Display for Outer {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("an error occurred while loading credentials")
        }
    }
    impl std::error::Error for Outer {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }

    #[test]
    fn the_message_names_the_class_the_chain_and_the_remedy() {
        let text = storage_credentials_unavailable(&Outer(Leaf));
        assert!(
            text.starts_with("storage credentials expired or unavailable (aws sdk default chain): "),
            "{text}"
        );
        assert!(
            text.contains("an error occurred while loading credentials: the SSO session has expired"),
            "{text}"
        );
        assert!(text.contains("aws sso login"), "{text}");
        assert!(text.contains("AWS_PROFILE"), "{text}");
    }
}
