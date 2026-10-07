//! Catalog-vended storage credentials as they land in a table's FileIO.
//!
//! An Iceberg REST catalog answers `loadTable` / `createTable` sent with
//! `X-Iceberg-Access-Delegation: vended-credentials` with short-lived,
//! table-scoped keys. The vendored client merges them into the table's
//! FileIO properties (`SQE PATCH (vended-credentials)`), so everything that
//! holds a `Table` holds credentials with a deadline. This module reads that
//! deadline so caches never hand out a table whose keys are about to lapse.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use iceberg::table::Table;

/// Iceberg REST property names for vended S3 credentials.
pub const S3_ACCESS_KEY_ID: &str = "s3.access-key-id";
/// Secret half of the vended S3 key pair.
pub const S3_SECRET_ACCESS_KEY: &str = "s3.secret-access-key";
/// STS session token that scopes the vended pair.
pub const S3_SESSION_TOKEN: &str = "s3.session-token";
/// Expiry of the vended S3 credentials, epoch milliseconds.
pub const S3_SESSION_TOKEN_EXPIRES_AT_MS: &str = "s3.session-token-expires-at-ms";
/// Expiry of a vended GCS OAuth2 token, epoch milliseconds.
const GCS_OAUTH2_TOKEN_EXPIRES_AT: &str = "gcs.oauth2.token-expires-at";

/// How long before expiry a cached table stops being served and a
/// distributed fragment gets pushed fresh keys. Matches the coordinator's
/// credential-refresh buffer so a refresh reload never hits a cache entry
/// that still counts as fresh.
pub const VENDED_CREDENTIAL_REFRESH_MARGIN: Duration = Duration::from_secs(300);

/// The earliest credential expiry in FileIO `props`, if any vended
/// credential carries one.
pub fn credential_expiry(props: &HashMap<String, String>) -> Option<SystemTime> {
    [S3_SESSION_TOKEN_EXPIRES_AT_MS, GCS_OAUTH2_TOKEN_EXPIRES_AT]
        .iter()
        .filter_map(|k| props.get(*k))
        .filter_map(|v| v.trim().parse::<u64>().ok())
        .map(|ms| UNIX_EPOCH + Duration::from_millis(ms))
        .min()
}

/// [`credential_expiry`] of a loaded table's FileIO.
pub fn table_credential_expiry(table: &Table) -> Option<SystemTime> {
    credential_expiry(table.file_io().config().props())
}

/// When a holder of credentials expiring at `expires_at`, obtained at
/// `obtained_at`, must stop using them: [`VENDED_CREDENTIAL_REFRESH_MARGIN`]
/// before expiry, or halfway through the lifetime for credentials shorter
/// than twice the margin (so a 5-minute token is still usable once).
pub fn refresh_deadline(expires_at: SystemTime, obtained_at: SystemTime) -> SystemTime {
    let lifetime = expires_at
        .duration_since(obtained_at)
        .unwrap_or(Duration::ZERO);
    let margin = VENDED_CREDENTIAL_REFRESH_MARGIN.min(lifetime / 2);
    expires_at - margin
}

#[cfg(test)]
mod tests {
    use super::*;

    fn props(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn expiry_is_read_from_the_vended_property() {
        assert_eq!(credential_expiry(&props(&[])), None);
        assert_eq!(
            credential_expiry(&props(&[(S3_SESSION_TOKEN_EXPIRES_AT_MS, "garbage")])),
            None
        );
        assert_eq!(
            credential_expiry(&props(&[(S3_SESSION_TOKEN_EXPIRES_AT_MS, "1893456000000")])),
            Some(UNIX_EPOCH + Duration::from_secs(1_893_456_000))
        );
        // The earliest deadline wins when two schemes carry one.
        assert_eq!(
            credential_expiry(&props(&[
                (S3_SESSION_TOKEN_EXPIRES_AT_MS, "2000"),
                (GCS_OAUTH2_TOKEN_EXPIRES_AT, "1000"),
            ])),
            Some(UNIX_EPOCH + Duration::from_secs(1))
        );
    }

    #[test]
    fn refresh_deadline_keeps_a_margin_but_never_more_than_half_the_lifetime() {
        let now = UNIX_EPOCH + Duration::from_secs(1_000_000);
        let hour = now + Duration::from_secs(3600);
        assert_eq!(
            refresh_deadline(hour, now),
            hour - VENDED_CREDENTIAL_REFRESH_MARGIN
        );
        let short = now + Duration::from_secs(120);
        assert_eq!(refresh_deadline(short, now), now + Duration::from_secs(60));
        // Already expired: the deadline is the expiry itself (in the past).
        let past = now - Duration::from_secs(10);
        assert_eq!(refresh_deadline(past, now), past);
    }
}
