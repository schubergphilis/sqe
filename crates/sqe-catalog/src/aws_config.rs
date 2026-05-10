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

use sqe_core::{Secret, SecretStore};
use sqe_sql::OptionValue;

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
    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());

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
        let secret = secrets.get(secret_ref)?;
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
